// Package mysql provides a MySQL driver for Queen migrations.
//
// This driver supports MySQL 5.7+ and MariaDB 10.2+. It uses MySQL's GET_LOCK()
// function for distributed locking to prevent concurrent migrations.
//
// # Basic Usage
//
//	import (
//	    "database/sql"
//	    _ "github.com/go-sql-driver/mysql"
//	    "github.com/honeynil/queen"
//	    "github.com/honeynil/queen/drivers/mysql"
//	)
//
//	db, _ := sql.Open("mysql", "user:password@tcp(localhost:3306)/dbname?parseTime=true")
//	driver := mysql.New(db)
//	q := queen.New(driver)
//
// # Connection String Requirements
//
// The connection string MUST include parseTime=true to properly handle TIMESTAMP columns:
//
//	"user:password@tcp(localhost:3306)/dbname?parseTime=true"
//
// # Locking Mechanism
//
// MySQL doesn't have advisory locks like PostgreSQL. Instead, this driver uses
// GET_LOCK() which creates a named lock that's automatically released when the
// connection closes or RELEASE_LOCK() is called.
//
// The lock name is derived from the migrations table name to ensure different
// migration tables use different locks.
//
// # Compatibility
//
//   - MySQL 5.7+ (uses GET_LOCK with timeout)
//   - MariaDB 10.2+ (uses GET_LOCK with timeout)
//   - Older versions may work but are not officially supported
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
)

// Driver implements the queen.Driver interface for MySQL.
//
// The driver is thread-safe and can be used concurrently by multiple goroutines.
// However, Queen already handles locking to prevent concurrent migrations.
//
// IMPORTANT:
// MySQL GET_LOCK() is bound to a single database connection.
// Therefore this driver keeps a dedicated *sql.Conn while the lock is held,
// to ensure that both GET_LOCK() and RELEASE_LOCK() are executed on the same
// underlying connection from the pool.
type Driver struct {
	base.Driver
	lockName string
	conn     *sql.Conn
}

// New creates a new MySQL driver.
//
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
//
// Example:
//
//	db, err := sql.Open("mysql", "user:pass@tcp(localhost:3306)/db?parseTime=true")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	driver := mysql.New(db)
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new MySQL driver with a custom table name.
//
// Use this when you need to manage multiple independent sets of migrations
// in the same database, or when you want to customize the table name for
// organizational purposes.
//
// Example:
//
//	driver := mysql.NewWithTableName(db, "my_custom_migrations")
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderQuestion,
				QuoteIdentifier: base.QuoteBackticks,
				// ParseTime is nil because MySQL driver handles TIMESTAMP parsing internally
				// when parseTime=true is set in the DSN.
				ParseTime: nil,
			},
		},
		lockName: "queen_lock_" + tableName,
	}
}

// Init creates the migrations tracking table if it doesn't exist.
//
// The table schema:
//   - version: VARCHAR(255) PRIMARY KEY - unique migration version
//   - name: VARCHAR(255) NOT NULL - human-readable migration name
//   - applied_at: TIMESTAMP - when the migration was applied
//   - checksum: VARCHAR(64) - hash of migration content for validation
//
// This method is idempotent and safe to call multiple times.
func (d *Driver) Init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(255) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			checksum VARCHAR(64) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
	`, d.Config.QuoteIdentifier(d.TableName))

	_, err := d.DB.ExecContext(ctx, query)
	return err
}

// Lock acquires a named lock to prevent concurrent migrations.
//
// MySQL uses GET_LOCK() which creates a named lock bound to the current database
// connection. The lock is automatically released when the connection closes
// or when Unlock() is called.
//
// The lock name is based on the migrations table name, so different migration
// tables will use different locks.
//
// Because database/sql uses a connection pool, this method explicitly acquires
// and holds a dedicated *sql.Conn to guarantee that both GET_LOCK() and
// RELEASE_LOCK() are executed on the same underlying connection.
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	// Acquire a dedicated connection from the pool.
	// MySQL named locks (GET_LOCK) are bound to a single connection,
	// so we must ensure that the same connection is later used in Unlock().
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	// GET_LOCK returns:
	// 1    if the lock was obtained successfully
	// 0    if the attempt timed out
	// NULL if an error occurred
	var result sql.NullInt64
	query := "SELECT GET_LOCK(?, ?)"

	err = conn.QueryRowContext(ctx, query, d.lockName, int(timeout.Seconds())).Scan(&result)
	if err != nil {
		// Close the connection on error to avoid leaking it.
		_ = conn.Close()
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// If the result is not valid or not equal to 1, the lock was not acquired.
	if !result.Valid || result.Int64 != 1 {
		_ = conn.Close()
		return queen.ErrLockTimeout
	}

	// Store the connection to ensure RELEASE_LOCK() is executed
	// on the same underlying connection.
	d.conn = conn
	return nil
}

// Unlock releases the migration lock.
//
// This should be called in a defer statement after acquiring the lock.
// It's safe to call even if the lock wasn't acquired.
//
// This method releases the named lock using the same database connection
// that was used in Lock(). After releasing the lock, the connection is closed
// and returned back to the pool.
func (d *Driver) Unlock(ctx context.Context) error {
	// If no connection was stored, there is no lock to release.
	if d.conn == nil {
		return nil
	}

	// RELEASE_LOCK returns:
	// 1    if the lock was released
	// 0    if the lock was not held by this connection
	// NULL if the named lock did not exist
	var result sql.NullInt64
	query := "SELECT RELEASE_LOCK(?)"

	err := d.conn.QueryRowContext(ctx, query, d.lockName).Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	// We don't check the result value because RELEASE_LOCK might return
	// 0 or NULL if the lock was already released (for example, if the
	// connection was closed), which is safe in this context.
	//
	// After releasing the lock, close the dedicated connection and
	// return it back to the pool.
	_ = d.conn.Close()
	d.conn = nil

	return nil
}
