// Package cockroachdb provides a CockroachDB driver for Queen migrations.
package cockroachdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
)

// Driver implements the queen.Driver interface for CockroachDB.
type Driver struct {
	db            *sql.DB
	tableName     string
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
		db:            db,
		tableName:     tableName,
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
	`, quoteIdentifier(d.tableName))

	if _, err := d.db.ExecContext(ctx, migrationsQuery); err != nil {
		return err
	}

	lockQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			lock_key	VARCHAR(255)	PRIMARY KEY,
			acquired_at	TIMESTAMP		DEFAULT CURRENT_TIMESTAMP,
			expires_at	TIMESTAMP		NOT NULL
		)
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
	start := time.Now()
	expiresAt := time.Now().Add(timeout)

	// Exponential backoff: start at 50ms, max 1s
	backoff := 50 * time.Millisecond
	maxBackoff := 1 * time.Second

	// Clean up expired locks
	cleanupQuery := fmt.Sprintf(`
		DELETE FROM %s
		WHERE lock_key = $1 AND expires_at <= now()
	`, quoteIdentifier(d.lockTableName))

	// Check if active lock exists
	checkQuery := fmt.Sprintf(`
		SELECT 1 FROM %s
		WHERE lock_key = $1 AND expires_at >= now()
		LIMIT 1
	`, quoteIdentifier(d.lockTableName))

	// Simple insert query
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (lock_key, expires_at) VALUES ($1, $2)
	`, quoteIdentifier(d.lockTableName))

	for {
		// Clean expired locks first (best effort, ignore errors)
		_, _ = d.db.ExecContext(ctx, cleanupQuery, d.lockKey)

		// Check if an active lock exists
		var count int64
		err := d.db.QueryRowContext(ctx, checkQuery, d.lockKey).Scan(&count)
		if err != nil && err != sql.ErrNoRows {
			// Database error, wait and retry
			goto retry
		}

		// If no active lock exists, try to insert
		if count == 0 {
			_, err := d.db.ExecContext(ctx, insertQuery, d.lockKey, expiresAt)
			if err == nil {
				return nil // Lock acquired successfully
			}
			// Insert failed (possible race condition), retry with backoff
		}

	retry:
		// Check timeout
		if time.Since(start) >= timeout {
			return queen.ErrLockTimeout
		}

		// Wait with exponential backoff
		select {
		case <-time.After(backoff):
			// Double the backoff for next iteration, up to maxBackoff
			backoff = min(backoff*2, maxBackoff)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Unlock releases the migration lock.
//
// This removes the lock record from the lock table, allowing other processes
// to acquire the lock.
//
// This method is graceful: it returns nil if the lock doesn't exist or was
// already released.
func (d *Driver) Unlock(ctx context.Context) error {
	unlockQuery := fmt.Sprintf(`
		DELETE FROM %s WHERE lock_key = $1
	`, quoteIdentifier(d.lockTableName))

	// Execute DELETE - it's safe even if lock doesn't exist
	// We intentionally don't check if the lock exists first to avoid race conditions
	_, err := d.db.ExecContext(ctx, unlockQuery, d.lockKey)

	// Gracefully ignore "no rows" scenarios - the lock may have been released by
	// another cleanup process.
	return err
}

// Exec executes a function within a transaction.
//
// If the function returns an error, the transaction is rolled back.
// Otherwise, the transaction is committed.
//
// This provides ACID guarantees for migration execution.
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
//
// Any locks held by this connection will be automatically released.
func (d *Driver) Close() error {
	return d.db.Close()
}

// quoteIdentifier quotes a SQL identifier (table name, column name) to prevent SQL injection.
// In CockroachDB, identifiers are quoted with double quotes.
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
