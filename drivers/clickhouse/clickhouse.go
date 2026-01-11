// Package clickhouse provides a ClickHouse driver for Queen migrations.
package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/honeynil/queen"
)

// Driver implements the queen.Driver interface for ClickHouse
type Driver struct {
	db        *sql.DB
	tableName string
	isLocked  atomic.Bool
}

// New creates a new ClickHouse driver.
//
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
//
// Example:
//
//	db, err := sql.Open("clickhouse",  "clickhouse://default:password@localhost:9000/default?")
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
		db:        db,
		tableName: tableName,
	}
}

// Init creates the migrations tracking table if it doesn't exist.
//
// The table schema:
//   - version:     String		- unique migration version
//   - name:        LowCardinality(String) - human-readable migration name
//   - applied_at:  DateTime64(3)     DEFAULT now64(3) - when the migration was applied
//   - checksum:    String            DEFAULT ” - hash of migration content for validation
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
		ORDER BY version;
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, migrationsQuery)
	return err
}

// GetApplied returns all applied migrations sorted by applied_at in ascending order.
//
// This is used by Queen to determine which migrations have already been applied
// and which are pending.
func (d *Driver) GetApplied(ctx context.Context) ([]queen.Applied, error) {
	query := fmt.Sprintf(`
		SELECT version, name, applied_at, checksum
		FROM %s
		ORDER BY applied_at ASC
	`, quoteIdentifier(d.tableName))

	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var applied []queen.Applied
	for rows.Next() {
		var a queen.Applied
		if err := rows.Scan(&a.Version, &a.Name, &a.AppliedAt, &a.Checksum); err != nil {
			return nil, err
		}
		applied = append(applied, a)
	}

	return applied, rows.Err()
}

// Record marks a migration as applied in the database.
//
// This should be called after successfully executing a migration's up function.
// The checksum is automatically computed from the migration content.
func (d *Driver) Record(ctx context.Context, m *queen.Migration) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (version, name, checksum)
		VALUES ($1, $2, $3)
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query, m.Version, m.Name, m.Checksum())
	return err
}

// Remove removes a migration record from the database.
//
// This should be called after successfully rolling back a migration's down function.
func (d *Driver) Remove(ctx context.Context, version string) error {
	query := fmt.Sprintf(`
		DELETE FROM %s WHERE version = $1
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query, version)
	return err
}

// Lock acquires an advisory lock to prevent concurrent migrations.
//
// ClickHouse doesn't have advisory locks like PostgreSQL. Instead, this driver uses
// atomic.bool to avoid data race conditions.
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	start := time.Now()
	tick := time.NewTicker(10 * time.Millisecond) 
	defer tick.Stop()

	for {
		if d.isLocked.CompareAndSwap(false, true) {
			return nil
		}

		if time.Since(start) >= timeout {
			return queen.ErrLockTimeout
		}

		select {
		case <-tick.C:

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Unlock releases the migration lock.
func (d *Driver) Unlock(ctx context.Context) error {
	if d.isLocked.CompareAndSwap(true, false) {
		return nil
	}

	return fmt.Errorf("failed to unlock transaction")
}


// Exec executes a function within a transaction. (needs refactoring)
func (d *Driver) Exec(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		// Ignore rollback error, return original error
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Close closes the database connection.
func (d *Driver) Close() error {
	return d.db.Close()
}

// quoteIdentifier quotes a SQL identifier (table name, column name) to prevent SQL injection.
// In ClickHouse, identifiers are quoted with double quotes.
func quoteIdentifier(name string) string {
	// Replace any existing double quotes with two double quotes (escaping)
	// and wrap the identifier in double quotes
	escaped := ""
	for _, c := range name {
		if c == '"' {
			escaped += "\"\""
		} else {
			escaped += string(c)
		}
	}
	return `"` + escaped + `"`
}
