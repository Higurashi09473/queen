// Package clickhouse provides a ClickHouse driver for Queen migrations.
package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen/drivers/base"
)

// Driver implements the queen.Driver interface for ClickHouse
type Driver struct {
	base.Driver
	lockTableName string
	lockKey       string
}

// New creates a new ClickHouse driver.
//
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
//
// Example:
//
//	db, err := sql.Open("clickhouse", DSN)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	driver := clickhouse.New(db)
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new ClickHouse driver with a custom table name.
//
// Use this when you need to manage multiple independent sets of migrations
// in the same database, or when you want to customize the table name
// for organizational purposes.
//
// Example:
//
//	driver := clickhouse.NewWithTableName(db, "my_custom_migrations")
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderQuestion,
				QuoteIdentifier: base.QuoteDoubleQuotes,
				ParseTime:       nil,
			},
		},
		lockTableName: tableName + "_lock",
		lockKey:       "migration_lock",
	}
}

// Init creates the migrations tracking table and lock table if they don't exist.
//
// The migrations table schema:
//   - version:     String		- unique migration version
//   - name:        LowCardinality(String) - human-readable migration name
//   - applied_at:  DateTime64(3)     DEFAULT now64(3) - when the migration was applied
//   - checksum:    String            DEFAULT ” - hash of migration content for validation
//
// The lock table schema:
//   - lock_key:    LowCardinality(String) - lock identifier
//   - acquired_at: DateTime64(3)     - when the lock was acquired
//   - expires_at:  DateTime64(3)     - when the lock expires
//   - TTL: expires_at + 10 SECOND    - automatically removes expired locks
//
// The TTL (Time To Live) on the lock table provides automatic cleanup of expired
// locks as a safety mechanism. This prevents abandoned locks from blocking migrations
// indefinitely if a process crashes without releasing the lock.
//
// This method is idempotent and safe to call multiple times.
func (d *Driver) Init(ctx context.Context) error {
	migrationsQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version     String,
			name        LowCardinality(String),
			applied_at  DateTime64(3)     DEFAULT now64(3),
			checksum    String            DEFAULT ''
		)
		ENGINE = ReplacingMergeTree()
		ORDER BY version
	`, d.Config.QuoteIdentifier(d.TableName))

	if _, err := d.DB.ExecContext(ctx, migrationsQuery); err != nil {
		return err
	}

	lockQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			lock_key    LowCardinality(String),
			acquired_at DateTime64(3)     DEFAULT now64(3),
			expires_at  DateTime64(3)
		)
		ENGINE = ReplacingMergeTree()
		ORDER BY lock_key
		TTL expires_at + INTERVAL 10 SECOND DELETE
	`, d.Config.QuoteIdentifier(d.lockTableName))

	_, err := d.DB.ExecContext(ctx, lockQuery)
	return err
}

// Lock acquires a distributed lock to prevent concurrent migrations.
//
// ClickHouse doesn't have advisory locks like PostgreSQL. Instead, this driver uses
// a lock table with expiration times to implement distributed locking across multiple
// processes/containers.
//
// The lock mechanism:
// 1. Cleans up expired locks using ALTER TABLE DELETE (async in ClickHouse)
// 2. Checks if an active lock exists using SELECT with FINAL
// 3. If no lock exists, attempts INSERT
// 4. Retries with exponential backoff until timeout or lock is acquired
//
// IMPORTANT: Uses FINAL modifier with ReplacingMergeTree to ensure we see
// deduplicated data, not intermediate merge states. This is critical because
// ClickHouse operations are asynchronous by nature.
//
// Exponential backoff starts at 50ms and doubles up to 1s maximum to reduce
// database load during lock contention.
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	cfg := base.TableLockConfig{
		CleanupQuery: fmt.Sprintf(
			"ALTER TABLE %s DELETE WHERE lock_key = ? AND expires_at < now64(3)",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		CheckQuery: fmt.Sprintf(
			"SELECT count(*) FROM %s FINAL WHERE lock_key = ? AND expires_at >= now64(3)",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		InsertQuery: fmt.Sprintf(
			"INSERT INTO %s (lock_key, expires_at) VALUES (?, ?)",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		ScanFunc: func(row *sql.Row) (bool, error) {
			var count int64
			if err := row.Scan(&count); err != nil {
				return false, err
			}
			return count > 0, nil
		},
	}

	return base.AcquireTableLock(ctx, d.DB, cfg, timeout)
}

// Unlock releases the migration lock.
//
// This removes the lock record from the lock table, allowing other processes
// to acquire the lock.
//
// This method is graceful: it returns nil if the lock doesn't exist or was
// already released. This prevents errors during cleanup when locks expire
// via TTL or in error recovery scenarios.
func (d *Driver) Unlock(ctx context.Context) error {
	unlockQuery := fmt.Sprintf(`
		ALTER TABLE %s DELETE WHERE lock_key = ?
	`, d.Config.QuoteIdentifier(d.lockTableName))

	// Execute DELETE - it's safe even if lock doesn't exist
	// We intentionally don't check if the lock exists first to avoid race conditions
	_, err := d.DB.ExecContext(ctx, unlockQuery, d.lockKey)

	// Gracefully ignore "no rows" scenarios - the lock might have expired via TTL
	// or been released by another cleanup process
	return err
}
