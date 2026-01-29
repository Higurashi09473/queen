// Package sqlite provides a SQLite driver for Queen migrations.
//
// This driver supports SQLite 3.8+ and is ideal for embedded databases,
// development, testing, and single-server applications.
//
// # Basic Usage
//
//	import (
//	    "database/sql"
//	    _ "github.com/mattn/go-sqlite3"
//	    "github.com/honeynil/queen"
//	    "github.com/honeynil/queen/drivers/sqlite"
//	)
//
//	db, _ := sql.Open("sqlite3", "myapp.db")
//	driver := sqlite.New(db)
//	q := queen.New(driver)
//
// # Database File
//
// SQLite stores the database in a single file. Common patterns:
//
//   - Persistent: "myapp.db" or "/path/to/database.db"
//   - In-memory: ":memory:" (lost when connection closes)
//   - Temporary: "" (empty string, deleted when closed)
//
// For production use, always use a persistent file path.
//
// # Locking Mechanism
//
// Unlike PostgreSQL and MySQL, SQLite is a file-based database with different
// locking characteristics:
//
//   - SQLite uses database-level locks, not connection-level locks
//   - This driver uses BEGIN EXCLUSIVE transaction for migration locking
//   - The lock is automatically released when the transaction commits/rolls back
//   - Only one writer can access the database at a time (by design)
//
// # WAL Mode (Recommended)
//
// For better concurrent read/write performance, enable WAL (Write-Ahead Logging):
//
//	db, _ := sql.Open("sqlite3", "myapp.db?_journal_mode=WAL")
//
// WAL mode allows readers to access the database while a migration is running,
// though only one migration can run at a time.
//
// # Compatibility
//
//   - SQLite 3.8+ (uses WITHOUT ROWID optimization where available)
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
)

// Driver implements the queen.Driver interface for SQLite.
//
// The driver is thread-safe for concurrent reads, but SQLite's database-level
// locking means only one write operation (migration) can occur at a time.
// This is handled automatically by PRAGMA locking_mode=EXCLUSIVE.
type Driver struct {
	base.Driver
}

// New creates a new SQLite driver.
//
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
//
// Example:
//
//	db, err := sql.Open("sqlite3", "myapp.db")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	driver := sqlite.New(db)
//
// For better performance with concurrent reads, use WAL mode:
//
//	db, err := sql.Open("sqlite3", "myapp.db?_journal_mode=WAL")
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new SQLite driver with a custom table name.
//
// Use this when you need to manage multiple independent sets of migrations
// in the same database file, or when you want to customize the table name
// for organizational purposes.
//
// Example:
//
//	driver := sqlite.NewWithTableName(db, "my_migrations")
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderQuestion,
				QuoteIdentifier: base.QuoteDoubleQuotes,
				ParseTime:       base.ParseTimeISO8601, // SQLite stores timestamps as TEXT
			},
		},
	}
}

// Init creates the migrations tracking table if it doesn't exist.
//
// The table schema:
//   - version: TEXT PRIMARY KEY - unique migration version
//   - name: TEXT NOT NULL - human-readable migration name
//   - applied_at: TEXT - ISO8601 timestamp when migration was applied
//   - checksum: TEXT - hash of migration content for validation
//
// This method is idempotent and safe to call multiple times.
//
// Note: SQLite doesn't have a native TIMESTAMP type. We use TEXT with
// ISO8601 format (YYYY-MM-DD HH:MM:SS) which sorts correctly and is human-readable.
func (d *Driver) Init(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			version TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TEXT NOT NULL DEFAULT (datetime('now')),
			checksum TEXT NOT NULL
		) WITHOUT ROWID
	`, d.Config.QuoteIdentifier(d.TableName))

	_, err := d.DB.ExecContext(ctx, query)
	return err
}

// Lock acquires an exclusive database lock to prevent concurrent migrations.
//
// SQLite uses database-level locking. This driver uses PRAGMA locking_mode=EXCLUSIVE
// to acquire an exclusive lock on the entire database file. This prevents any other
// connections from writing to the database until the lock is released.
//
// The lock is connection-based (similar to PostgreSQL advisory locks) rather than
// transaction-based, allowing individual migration transactions to be created and
// committed independently.
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	// Set busy_timeout for lock acquisition attempts
	_, err := d.DB.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", timeout.Milliseconds()))
	if err != nil {
		return fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	// Set EXCLUSIVE locking mode - this locks the database file
	// preventing other connections from acquiring locks
	_, err = d.DB.ExecContext(ctx, "PRAGMA locking_mode = EXCLUSIVE")
	if err != nil {
		return fmt.Errorf("failed to set locking mode: %w", err)
	}

	// Force the lock to be acquired immediately using BEGIN IMMEDIATE
	// This acquires a RESERVED lock right away instead of waiting for the first write
	_, err = d.DB.ExecContext(ctx, "BEGIN IMMEDIATE")
	if err != nil {
		if strings.Contains(err.Error(), "database is locked") {
			return fmt.Errorf("%w: failed to acquire exclusive lock for table '%s' (SQLite)",
				queen.ErrLockTimeout, d.TableName)
		}
		return fmt.Errorf("failed to begin immediate transaction: %w", err)
	}

	// Commit the transaction - we don't need to keep it open
	// The EXCLUSIVE locking mode remains in effect for the connection
	_, err = d.DB.ExecContext(ctx, "COMMIT")
	if err != nil {
		return fmt.Errorf("failed to commit lock transaction: %w", err)
	}

	return nil
}

// Unlock releases the migration lock.
//
// This resets the locking mode to NORMAL, allowing other connections to
// write to the database.
//
// This should be called in a defer statement after acquiring the lock.
// It's safe to call even if the lock wasn't acquired.
func (d *Driver) Unlock(ctx context.Context) error {
	// Reset locking mode to NORMAL
	_, err := d.DB.ExecContext(ctx, "PRAGMA locking_mode = NORMAL")
	if err != nil {
		return fmt.Errorf("failed to reset locking mode for table '%s' (SQLite): %w",
			d.TableName, err)
	}

	// Execute a transaction to force the locking mode change to take effect
	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin unlock transaction for table '%s' (SQLite): %w",
			d.TableName, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit unlock transaction for table '%s' (SQLite): %w",
			d.TableName, err)
	}

	return nil
}
