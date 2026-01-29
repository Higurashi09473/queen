// Package cockroachdb provides a CockroachDB driver for Queen migrations.
package cockroachdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
)

// Driver implements the queen.Driver interface for CockroachDB.
type Driver struct {
	base.Driver
	lockTableName string
	lockKey       string
}

// New creates a new CockroachDB driver.
//
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
//
// Cockroach Labs officially recommends using pgx.
//
// Example:
//
//	db, err := sql.Open("pgx", DSN)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	driver := cockroachdb.New(db)
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new CockroachDB driver with a custom table name.
//
// Use this when you need to manage multiple independent sets of migrations
// in the same database, or when you want to customize the table name
// for organizational purposes.
//
// Example:
//
// driver := cockroachdb.NewWithTableName(db, "my_custom_migrations")
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderDollar,
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
//   - version:     VARCHAR(255)	PRIMARY KEY - unique migration version
//   - name:        VARCHAR(255)	NOT NULL - human-readable migration name
//   - applied_at:  TIMESTAMP		NOT NULL DEFAULT CURRENT_TIMESTAMP - when the migration was applied
//   - checksum:    VARCHAR(64)		NOT NULL - hash of migration content for validation
//
// The lock table schema:
//   - lock_key:    VARCHAR(255)	PRIMARY KEY - lock identifier
//   - acquired_at: TIMESTAMP		NOT NULL DEFAULT CURRENT_TIMESTAMP - when the lock was acquired
//   - expires_at:  TIMESTAMP		NOT NULL - when the lock expires
//
// This method is idempotent and safe to call multiple times.
func (d *Driver) Init(ctx context.Context) error {
	migrationsQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version		VARCHAR(255) PRIMARY KEY,
			name		VARCHAR(255) NOT NULL,
			applied_at  TIMESTAMP	 DEFAULT CURRENT_TIMESTAMP,
			checksum	VARCHAR(64)  NOT NULL
		)
	`, d.Config.QuoteIdentifier(d.TableName))

	if _, err := d.DB.ExecContext(ctx, migrationsQuery); err != nil {
		return err
	}

	lockQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			lock_key	VARCHAR(255)	PRIMARY KEY,
			acquired_at	TIMESTAMP		DEFAULT CURRENT_TIMESTAMP,
			expires_at	TIMESTAMP		NOT NULL
		)
	`, d.Config.QuoteIdentifier(d.lockTableName))

	_, err := d.DB.ExecContext(ctx, lockQuery)
	return err
}

// Lock acquires a distributed lock to prevent concurrent migrations.
//
// CockroachDB doesn't have advisory locks like PostgreSQL. Instead, this driver uses
// a lock table with expiration times to implement distributed locking across multiple
// processes/containers.
//
// The lock mechanism:
// 1. Cleans up expired locks using DELETE FROM
// 2. Checks if an active lock exists using SELECT with LIMIT
// 3. If no lock exists, attempts INSERT
// 4. Retries with exponential backoff until timeout or lock is acquired
//
// Exponential backoff starts at 50ms and doubles up to 1s maximum to reduce
// database load during lock contention.
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	cfg := base.TableLockConfig{
		CleanupQuery: fmt.Sprintf(
			"DELETE FROM %s WHERE lock_key = $1 AND expires_at < now()",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		CheckQuery: fmt.Sprintf(
			"SELECT 1 FROM %s WHERE lock_key = $1 AND expires_at >= now() LIMIT 1",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		InsertQuery: fmt.Sprintf(
			"INSERT INTO %s (lock_key, expires_at) VALUES ($1, $2)",
			d.Config.QuoteIdentifier(d.lockTableName),
		),
		ScanFunc: func(row *sql.Row) (bool, error) {
			var exists int
			err := row.Scan(&exists)
			if err != nil && err != sql.ErrNoRows {
				return false, err
			}
			return exists != 0, nil
		},
	}

	err := base.AcquireTableLock(ctx, d.DB, cfg, timeout)
	if err == queen.ErrLockTimeout {
		return fmt.Errorf("%w: failed to acquire lock '%s' for table '%s' (CockroachDB)",
			queen.ErrLockTimeout, d.lockKey, d.lockTableName)
	}
	return err

}

// Unlock releases the migration lock.
//
// This removes the lock record from the lock table, allowing other processes
// to acquire the lock.
//
// This method is graceful: it returns nil if the lock doesn't exist or was
// already released.
func (d *Driver) Unlock(ctx context.Context) error {
	checkQuery := fmt.Sprintf(
		"SELECT 1 FROM %s WHERE lock_key = $1 AND expires_at >= now() LIMIT 1",
		d.Config.QuoteIdentifier(d.lockTableName),
	)
	unlockQuery := fmt.Sprintf(
		"DELETE FROM %s WHERE lock_key = $1",
		d.Config.QuoteIdentifier(d.lockTableName),
	)

	var hasLock bool
	err := d.DB.QueryRowContext(ctx, checkQuery, d.lockKey).Scan(&hasLock)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to check lock existence '%s' for table '%s' (CockroachDB): %w",
			d.lockKey, d.TableName, err)
	}
	if !hasLock {
		return nil
	}

	// Execute DELETE - it's safe even if lock doesn't exist
	// We intentionally don't check if the lock exists first to avoid race conditions
	_, err = d.DB.ExecContext(ctx, unlockQuery, d.lockKey)
	if err != nil {
		return fmt.Errorf("failed to release lock '%s' for table '%s' (CockroachDB): %w",
			d.lockKey, d.TableName, err)
	}
	// Gracefully ignore "no rows" scenarios - the lock may have been released by
	// another cleanup process.
	return err
}

// QuoteIdentifier quotes a SQL identifier (table name, column name) to prevent SQL injection.
// In CockroachDB, identifiers are quoted with double quotes.
//
// This function is provided for backward compatibility.
func QuoteIdentifier(name string) string {
	return base.QuoteDoubleQuotes(name)
}
