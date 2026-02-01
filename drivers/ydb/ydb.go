// Package ydb provides a YandexDB (YDB) driver for Queen migrations.
//
// YDB is a distributed SQL database that combines high availability and
// scalability with strong consistency and ACID transactions.
//
// # Basic Usage
//
//	import (
//	    "database/sql"
//	    _ "github.com/ydb-platform/ydb-go-sdk/v3"
//	    "github.com/honeynil/queen"
//	    "github.com/honeynil/queen/drivers/ydb"
//	)
//
//	db, _ := sql.Open("ydb", "grpc://localhost:2136/local")
//	driver, _ := ydb.New(db)
//	q := queen.New(driver)
//
// # Connection String
//
// The connection string format:
//
//	grpc://localhost:2136/local
//	grpcs://user:password@localhost:2135/local
//
// # Locking Mechanism
//
// YDB uses optimistic concurrency control and doesn't have advisory locks
// like PostgreSQL. Instead, this driver uses a lock table with expiration
// times to implement distributed locking across multiple processes/containers.
//
// The lock table is automatically created during initialization and uses
// TTL (Time To Live) for automatic cleanup of expired locks.
//
// # Compatibility
//
// This driver requires YDB with YQL support and the ydb-go-sdk/v3 driver.
// Tested with YDB 23.3+.
package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

// Driver implements the queen.Driver interface for YDB.
//
// YDB uses table-based locking since it doesn't support advisory locks.
// The driver is thread-safe and can be used concurrently by multiple goroutines.
type Driver struct {
	base.Driver
	lockTableName string
	lockKey       string
	ownerID       string
}

// New creates a new YDB driver.
//
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
//
// Example:
//
//	db, err := sql.Open("ydb", "grpc://localhost:2136/local")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	driver, err := ydb.New(db)
//	if err != nil {
//	    log.Fatal(err)
//	}
func New(db *sql.DB) (*Driver, error) {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new YDB driver with a custom table name.
//
// Use this when you need to manage multiple independent sets of migrations
// in the same database, or when you want to customize the table name
// for organizational purposes.
//
// Example:
//
//	driver, err := ydb.NewWithTableName(db, "my_custom_migrations")
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewWithTableName(db *sql.DB, tableName string) (*Driver, error) {
	ownerID, err := base.GenerateOwnerID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate lock owner ID: %w", err)
	}

	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				// YDB with go_query_bind=declare,numeric uses $1, $2, $3 placeholders
				// The driver automatically adds DECLARE statements and converts to named params
				Placeholder: base.PlaceholderDollar,
				// YDB uses backticks for identifiers by default (can use double quotes in ANSI mode)
				QuoteIdentifier: base.QuoteBackticks,
				// YDB supports TIMESTAMP natively
				ParseTime: nil,
			},
		},
		lockTableName: tableName + "_lock",
		lockKey:       "migration_lock",
		ownerID:       ownerID,
	}, nil
}

// Init creates the migrations tracking table and lock table if they don't exist.
//
// The migrations table schema:
//   - version:     Utf8 PRIMARY KEY - unique migration version
//   - name:        Utf8 NOT NULL - human-readable migration name
//   - applied_at:  Timestamp NOT NULL - when the migration was applied
//   - checksum:    Utf8 NOT NULL - hash of migration content for validation
//
// The lock table schema:
//   - lock_key:    Utf8 PRIMARY KEY - lock identifier
//   - acquired_at: Timestamp - when the lock was acquired
//   - expires_at:  Timestamp NOT NULL - when the lock expires
//   - owner_id:    Utf8 NOT NULL - unique owner identifier
//   - TTL: automatic cleanup of expired locks
//
// YDB note: YDB requires explicit PRIMARY KEY specification for all tables.
// Timestamps are stored as Timestamp type which provides microsecond precision.
//
// This method is idempotent and safe to call multiple times.
func (d *Driver) Init(ctx context.Context) error {
	// YDB requires SchemeQueryMode for DDL operations (CREATE TABLE, etc.)
	ctx = ydb.WithQueryMode(ctx, ydb.SchemeQueryMode)

	// Create migrations table
	migrationsQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version     Utf8,
			name        Utf8,
			applied_at  Timestamp,
			checksum    Utf8,
			PRIMARY KEY (version)
		)
	`, d.Config.QuoteIdentifier(d.TableName))

	if _, err := d.DB.ExecContext(ctx, migrationsQuery); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Create lock table with TTL for automatic cleanup
	lockQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			lock_key    Utf8,
			acquired_at Timestamp,
			expires_at  Timestamp NOT NULL,
			owner_id    Utf8 NOT NULL,
			PRIMARY KEY (lock_key)
		)
		WITH (
			TTL = Interval("PT10S") ON expires_at
		)
	`, d.Config.QuoteIdentifier(d.lockTableName))

	if _, err := d.DB.ExecContext(ctx, lockQuery); err != nil {
		return fmt.Errorf("failed to create lock table: %w", err)
	}

	return nil
}

// Lock acquires a distributed lock to prevent concurrent migrations.
//
// YDB doesn't have advisory locks like PostgreSQL. Instead, this driver uses
// a lock table with expiration times to implement distributed locking across
// multiple processes/containers.
//
// The lock mechanism:
// 1. Cleans up expired locks using DELETE
// 2. Checks if an active lock exists using SELECT
// 3. If no lock exists, attempts INSERT
// 4. Retries with exponential backoff until timeout or lock is acquired
//
// YDB uses optimistic concurrency control, so INSERT conflicts are handled
// gracefully by retrying with backoff.
//
// Exponential backoff starts at 50ms and doubles up to 1s maximum to reduce
// database load during lock contention.
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	cfg := base.TableLockConfig{
		CleanupQuery: fmt.Sprintf(
			"DELETE FROM %s WHERE lock_key = $1 AND expires_at < CurrentUtcTimestamp()",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		CheckQuery: fmt.Sprintf(
			"SELECT 1 FROM %s WHERE lock_key = $1 AND expires_at >= CurrentUtcTimestamp() LIMIT 1",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		InsertQuery: fmt.Sprintf(
			"INSERT INTO %s (lock_key, acquired_at, expires_at, owner_id) VALUES ($1, CurrentUtcTimestamp(), $2, $3)",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		ScanFunc: func(row *sql.Row) (bool, error) {
			var exists int
			err := row.Scan(&exists)
			if err != nil && err != sql.ErrNoRows {
				return false, err
			}
			return err != sql.ErrNoRows, nil
		},
	}

	err := base.AcquireTableLock(ctx, d.DB, cfg, d.lockKey, d.ownerID, timeout)
	if err == queen.ErrLockTimeout {
		return fmt.Errorf("%w: failed to acquire lock '%s' for table '%s'",
			queen.ErrLockTimeout, d.lockKey, d.lockTableName)
	}
	return err
}

// Unlock releases the migration lock.
//
// This removes the lock record from the lock table, allowing other processes
// to acquire the lock.
//
// The unlock operation checks the owner_id to ensure only the process that
// acquired the lock can release it. This prevents race conditions where an
// expired lock is released by the wrong process.
//
// This method is graceful: it returns nil if the lock doesn't exist, was
// already released, or belongs to another process. This prevents errors
// during cleanup when locks expire via TTL or in error recovery scenarios.
func (d *Driver) Unlock(ctx context.Context) error {
	unlockQuery := fmt.Sprintf(
		"DELETE FROM %s WHERE lock_key = $1 AND owner_id = $2",
		d.Config.QuoteIdentifier(d.lockTableName),
	)

	// Execute DELETE - it's safe even if lock doesn't exist or belongs to another process
	// We intentionally don't check if the lock exists first to avoid race conditions
	_, err := d.DB.ExecContext(ctx, unlockQuery, d.lockKey, d.ownerID)
	if err != nil {
		return fmt.Errorf("failed to release lock '%s' for table '%s': %w",
			d.lockKey, d.TableName, err)
	}

	// Gracefully ignore "no rows" scenarios - the lock may have expired via TTL,
	// been released by another cleanup process, or belong to another process
	return nil
}

// Record marks a migration as applied in the database.
// YDB-specific implementation that includes applied_at timestamp.
//
// Unlike other drivers, YDB does not support DEFAULT values with function calls
// (e.g., DEFAULT CurrentUtcTimestamp()). It only supports literal DEFAULT values.
// Therefore, this method explicitly inserts the timestamp using CurrentUtcTimestamp()
// in the SQL query instead of relying on a column DEFAULT.
func (d *Driver) Record(ctx context.Context, m *queen.Migration) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (version, name, applied_at, checksum)
		VALUES ($1, $2, CurrentUtcTimestamp(), $3)
	`,
		d.Config.QuoteIdentifier(d.TableName),
	)

	_, err := d.DB.ExecContext(ctx, query, m.Version, m.Name, m.Checksum())
	return err
}
