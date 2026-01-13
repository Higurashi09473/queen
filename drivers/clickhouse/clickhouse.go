// Package clickhouse provides a ClickHouse driver for Queen migrations.
package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
)

// Driver implements the queen.Driver interface for ClickHouse
type Driver struct {
	db            *sql.DB
	tableName     string
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
		db:            db,
		tableName:     tableName,
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
	`, quoteIdentifier(d.tableName))

	if _, err := d.db.ExecContext(ctx, migrationsQuery); err != nil {
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
	`, quoteIdentifier(d.lockTableName))

	_, err := d.db.ExecContext(ctx, lockQuery)
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
		VALUES (?, ?, ?)
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query, m.Version, m.Name, m.Checksum())
	return err
}

// Remove removes a migration record from the database.
//
// This should be called after successfully rolling back a migration's down function.
func (d *Driver) Remove(ctx context.Context, version string) error {
	query := fmt.Sprintf(`
		DELETE FROM %s WHERE version = ?
	`, quoteIdentifier(d.tableName))

	_, err := d.db.ExecContext(ctx, query, version)
	return err
}

// Lock acquires a distributed lock to prevent concurrent migrations.
//
// ClickHouse doesn't have advisory locks like PostgreSQL. Instead, this driver uses
// a lock table with expiration times to implement distributed locking across multiple
// processes/containers.
//
// The lock mechanism:
// 1. Check for active locks
// 2. If there are no active locks, tries to insert a lock record with expiration time
// 3. Retries until timeout or lock is acquired
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	start := time.Now()
	expiresAt := time.Now().Add(timeout)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	selectQuery := fmt.Sprintf(`
		SELECT 1 FROM %s WHERE lock_key = ? LIMIT 1
	`, quoteIdentifier(d.lockTableName))

	cleanupQuery := fmt.Sprintf(`
		ALTER TABLE %s DELETE WHERE lock_key = ? AND expires_at < now64(3)
	`, quoteIdentifier(d.lockTableName))

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (lock_key, expires_at)
		VALUES (?, ?)
	`, quoteIdentifier(d.lockTableName))

	var hasLock bool

	for {
		// ClickHouse doesn't support INSERT ... ON CONFLICT, so we clean expired locks first
		_, _ = d.db.ExecContext(ctx, cleanupQuery, d.lockKey)

		_ = d.db.QueryRowContext(ctx, selectQuery, d.lockKey).Scan(&hasLock)
		if !hasLock {
			_, err := d.db.ExecContext(ctx, insertQuery, d.lockKey, expiresAt)
			if err == nil {
				return nil
			}
		}

		if time.Since(start) >= timeout {
			return queen.ErrLockTimeout
		}

		select {
		case <-tick.C:
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Unlock releases the migration lock.
//
// This removes the lock record from the lock table, allowing other processes
// to acquire the lock.
func (d *Driver) Unlock(ctx context.Context) error {
	selectQuery := fmt.Sprintf(`
		SELECT 1 FROM %s WHERE lock_key = ? LIMIT 1
	`, quoteIdentifier(d.lockTableName))

	var hasLock bool
	err := d.db.QueryRowContext(ctx, selectQuery, d.lockKey).Scan(&hasLock)
	if err != nil {
		return err
	}
	if !hasLock {
		return nil
	}

	unlockQuery := fmt.Sprintf(`
		ALTER TABLE %s DELETE WHERE lock_key = ?
	`, quoteIdentifier(d.lockTableName))

	_, err = d.db.ExecContext(ctx, unlockQuery, d.lockKey)
	return err
}

// Exec executes a function within a transaction.
//
// Note: ClickHouse has limited transaction support compared to traditional RDBMS.
// Transactions work only for tables with MergeTree engine family and provide
// atomicity guarantees for the current session.
//
// If the function returns an error, the transaction is rolled back.
// Otherwise, the transaction is committed.
func (d *Driver) Exec(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
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
