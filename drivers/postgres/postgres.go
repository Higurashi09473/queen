// Package postgres provides a PostgreSQL driver for Queen migrations.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
)

// Driver implements the queen.Driver interface for PostgreSQL.
type Driver struct {
	base.Driver
	lockID int64
}

// New creates a new PostgreSQL driver.
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new PostgreSQL driver with a custom table name.
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderDollar,
				QuoteIdentifier: base.QuoteDoubleQuotes,
				ParseTime:       nil, // PostgreSQL supports TIMESTAMP natively
			},
		},
		lockID: hashTableName(tableName), // Unique lock ID based on table name
	}
}

// Init creates the migrations tracking table if it doesn't exist.
func (d *Driver) Init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version VARCHAR(255) PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			checksum VARCHAR(64) NOT NULL
		)
	`, d.Config.QuoteIdentifier(d.TableName))

	_, err := d.DB.ExecContext(ctx, query)
	return err
}

// Lock acquires an advisory lock to prevent concurrent migrations.
// PostgreSQL advisory locks are automatically released when the connection closes
// or when explicitly unlocked.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	// Set lock timeout
	_, err := d.DB.ExecContext(ctx, fmt.Sprintf("SET lock_timeout = '%dms'", timeout.Milliseconds()))
	if err != nil {
		return err
	}

	// Try to acquire advisory lock
	var acquired bool
	err = d.DB.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", d.lockID).Scan(&acquired)
	if err != nil {
		return err
	}

	if !acquired {
		return fmt.Errorf("%w: failed to acquire advisory lock '%d' for table '%s' (PostgreSQL)",
			queen.ErrLockTimeout, d.lockID, d.TableName)
	}

	return nil
}

// Unlock releases the advisory lock.
func (d *Driver) Unlock(ctx context.Context) error {
	_, err := d.DB.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", d.lockID)
	if err != nil {
		return fmt.Errorf("failed to release advisory lock '%d' for table '%s' (PostgreSQL): %w",
			d.lockID, d.TableName, err)
	}
	return nil
}

// hashTableName creates a unique int64 hash from the table name for advisory locks.
// This ensures different migration tables use different locks.
func hashTableName(name string) int64 {
	var hash int64
	for i, c := range name {
		hash = hash*31 + int64(c) + int64(i)
	}
	return hash
}
