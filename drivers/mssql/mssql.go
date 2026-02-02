// Package mssql provides a MS SQL Server driver for Queen migrations.
//
// This driver supports SQL Server 2012+ and Azure SQL Database. It uses SQL Server's
// sp_getapplock/sp_releaseapplock stored procedures for distributed locking to prevent
// concurrent migrations.
//
// # Basic Usage
//
//	import (
//	    "database/sql"
//	    _ "github.com/microsoft/go-mssqldb"
//	    "github.com/honeynil/queen"
//	    "github.com/honeynil/queen/drivers/mssql"
//	)
//
//	db, _ := sql.Open("sqlserver", "sqlserver://user:password@localhost:1433?database=dbname")
//	driver := mssql.New(db)
//	q := queen.New(driver)
//
// # Locking Mechanism
//
// SQL Server doesn't have advisory locks like PostgreSQL. Instead, this driver uses
// sp_getapplock() which creates an application lock that's automatically released when
// the session ends or sp_releaseapplock() is called.
//
// The lock name is derived from the migrations table name to ensure different
// migration tables use different locks.
//
// # Compatibility
//
//   - SQL Server 2012+
//   - Azure SQL Database
//   - Azure SQL Managed Instance
package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/base"
)

// Driver implements the queen.Driver interface for MS SQL Server.
//
// The driver is thread-safe and can be used concurrently by multiple goroutines.
// However, Queen already handles locking to prevent concurrent migrations.
//
// IMPORTANT:
// SQL Server sp_getapplock() is bound to a database session/connection.
// Therefore this driver keeps a dedicated *sql.Conn while the lock is held,
// to ensure that both sp_getapplock() and sp_releaseapplock() are executed on the same
// underlying connection from the pool.
type Driver struct {
	base.Driver
	lockName string
	conn     *sql.Conn
}

// New creates a new MS SQL Server driver.
//
// The database connection should already be open and configured.
// The default migrations table name is "queen_migrations".
//
// Example:
//
//	db, err := sql.Open("sqlserver", "sqlserver://user:pass@localhost:1433?database=mydb")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	driver := mssql.New(db)
func New(db *sql.DB) *Driver {
	return NewWithTableName(db, "queen_migrations")
}

// NewWithTableName creates a new MS SQL Server driver with a custom table name.
//
// Use this when you need to manage multiple independent sets of migrations
// in the same database, or when you want to customize the table name for
// organizational purposes.
//
// Example:
//
//	driver := mssql.NewWithTableName(db, "my_custom_migrations")
func NewWithTableName(db *sql.DB, tableName string) *Driver {
	return &Driver{
		Driver: base.Driver{
			DB:        db,
			TableName: tableName,
			Config: base.Config{
				Placeholder:     base.PlaceholderQuestion,
				QuoteIdentifier: base.QuoteBrackets,
				ParseTime:       nil, // SQL Server driver handles DATETIME2 parsing internally
			},
		},
		lockName: "queen_lock_" + tableName,
	}
}

// Init creates the migrations tracking table if it doesn't exist.
//
// The table schema:
//   - version: NVARCHAR(255) PRIMARY KEY - unique migration version
//   - name: NVARCHAR(255) NOT NULL - human-readable migration name
//   - applied_at: DATETIME2 - when the migration was applied
//   - checksum: NVARCHAR(64) - hash of migration content for validation
//
// This method is idempotent and safe to call multiple times.
func (d *Driver) Init(ctx context.Context) error {
	// SQL Server doesn't support CREATE TABLE IF NOT EXISTS syntax,
	// so we check for table existence first using OBJECT_ID.
	query := fmt.Sprintf(`
		IF OBJECT_ID(N'%s', N'U') IS NULL
		BEGIN
			CREATE TABLE %s (
				version NVARCHAR(255) PRIMARY KEY,
				name NVARCHAR(255) NOT NULL,
				applied_at DATETIME2 DEFAULT GETUTCDATE(),
				checksum NVARCHAR(64) NOT NULL
			)
		END
	`, d.TableName, d.Config.QuoteIdentifier(d.TableName))

	_, err := d.DB.ExecContext(ctx, query)
	return err
}

// Lock acquires an application lock to prevent concurrent migrations.
//
// SQL Server uses sp_getapplock() which creates an application lock bound to the
// current database session. The lock is automatically released when the session ends
// or when Unlock() is called.
//
// The lock name is based on the migrations table name, so different migration
// tables will use different locks.
//
// Because database/sql uses a connection pool, this method explicitly acquires
// and holds a dedicated *sql.Conn to guarantee that both sp_getapplock() and
// sp_releaseapplock() are executed on the same underlying connection.
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func (d *Driver) Lock(ctx context.Context, timeout time.Duration) error {
	// Acquire a dedicated connection from the pool.
	// SQL Server application locks are bound to a session,
	// so we must ensure that the same connection is later used in Unlock().
	conn, err := d.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}

	// sp_getapplock returns:
	//  0   = lock was granted synchronously
	//  1   = lock was granted after waiting for other incompatible locks to be released
	// -1   = lock request timed out
	// -2   = lock request was cancelled
	// -3   = lock request was chosen as a deadlock victim
	// -999 = parameter validation or other call error
	var result int
	query := `
		DECLARE @result INT;
		EXEC @result = sp_getapplock
			@Resource = ?,
			@LockMode = 'Exclusive',
			@LockOwner = 'Session',
			@LockTimeout = ?;
		SELECT @result;
	`

	err = conn.QueryRowContext(ctx, query, d.lockName, int(timeout.Milliseconds())).Scan(&result)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// result >= 0 means lock was acquired (0 = immediate, 1 = after wait)
	if result < 0 {
		_ = conn.Close()
		var reason string
		switch result {
		case -1:
			reason = "timeout"
		case -2:
			reason = "cancelled"
		case -3:
			reason = "deadlock"
		default:
			reason = "error"
		}
		return fmt.Errorf("%w: failed to acquire lock '%s' for table '%s' (reason: %s, code: %d)",
			queen.ErrLockTimeout, d.lockName, d.TableName, reason, result)
	}

	// Store the connection to ensure sp_releaseapplock() is executed
	// on the same underlying connection.
	d.conn = conn
	return nil
}

// Unlock releases the migration lock.
//
// This should be called in a defer statement after acquiring the lock.
// It's safe to call even if the lock wasn't acquired.
//
// This method releases the application lock using the same database connection
// that was used in Lock(). After releasing the lock, the connection is closed
// and returned back to the pool.
func (d *Driver) Unlock(ctx context.Context) error {
	// If no connection was stored, there is no lock to release.
	if d.conn == nil {
		return nil
	}

	// sp_releaseapplock returns:
	//  0   = lock was released successfully
	// -999 = parameter validation or other error
	var result int
	query := `
		DECLARE @result INT;
		EXEC @result = sp_releaseapplock
			@Resource = ?,
			@LockOwner = 'Session';
		SELECT @result;
	`

	err := d.conn.QueryRowContext(ctx, query, d.lockName).Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to release lock '%s' for table '%s': %w",
			d.lockName, d.TableName, err)
	}

	// Close the dedicated connection and return it back to the pool.
	_ = d.conn.Close()
	d.conn = nil

	return nil
}
