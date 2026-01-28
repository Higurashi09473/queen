// Package base provides common functionality for Queen database drivers.
//
// This package contains shared code to reduce duplication across different
// database drivers (PostgreSQL, MySQL, SQLite, ClickHouse, CockroachDB).
//
// The base package provides:
//   - Transaction management (Exec)
//   - Connection lifecycle (Close)
//   - Common migration operations (GetApplied, Record, Remove)
//   - SQL identifier quoting strategies
//   - Placeholder formatting strategies
package base

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/honeynil/queen"
)

// Config contains configuration for the base driver.
// Each concrete driver provides these strategies to customize behavior.
type Config struct {
	// Placeholder generates SQL placeholders for the n-th argument (1-based).
	// PostgreSQL/CockroachDB: func(n int) string { return fmt.Sprintf("$%d", n) }
	// MySQL/SQLite/ClickHouse: func(n int) string { return "?" }
	Placeholder func(n int) string

	// QuoteIdentifier escapes SQL identifiers (table names, column names).
	// PostgreSQL/SQLite/ClickHouse/CockroachDB: double quotes
	// MySQL: backticks
	QuoteIdentifier func(name string) string

	// ParseTime parses time from query result (optional).
	// Most drivers: nil (use standard scanning)
	// SQLite: parses from ISO8601 string
	ParseTime func(src interface{}) (time.Time, error)
}

// Driver provides base implementation of common queen.Driver methods.
//
// Concrete drivers should embed this type and provide:
//   - Init() - database-specific schema creation
//   - Lock()/Unlock() - database-specific locking mechanisms
//
// Note:
// This base driver does not manage database connections explicitly.
// Some databases (e.g. MySQL with GET_LOCK) require a dedicated connection
// for locking. In such cases, the concrete driver (mysql.Driver) is responsible
// for managing its own *sql.Conn for lock lifetime.
type Driver struct {
	DB        *sql.DB
	TableName string
	Config    Config
	conn      *sql.Conn
}

// Exec executes a function within a transaction.
//
// If the function returns an error, the transaction is rolled back.
// Otherwise, the transaction is committed.
//
// This provides ACID guarantees for migration execution.
// Identical implementation for all database drivers.
func (d *Driver) Exec(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := d.DB.BeginTx(ctx, nil)
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
// Identical implementation for all database drivers.
func (d *Driver) Close() error {
	return d.DB.Close()
}

// GetApplied returns all applied migrations sorted by applied_at in ascending order.
//
// Uses the QuoteIdentifier strategy for SQL identifier escaping.
// Uses the optional ParseTime strategy for SQLite compatibility.
func (d *Driver) GetApplied(ctx context.Context) ([]queen.Applied, error) {
	query := fmt.Sprintf(`
		SELECT version, name, applied_at, checksum
		FROM %s
		ORDER BY applied_at ASC
	`, d.Config.QuoteIdentifier(d.TableName))

	rows, err := d.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var applied []queen.Applied
	for rows.Next() {
		var a queen.Applied

		// If custom time parser is provided (for SQLite)
		if d.Config.ParseTime != nil {
			var appliedAtStr string
			if err := rows.Scan(&a.Version, &a.Name, &appliedAtStr, &a.Checksum); err != nil {
				return nil, err
			}
			parsedTime, err := d.Config.ParseTime(appliedAtStr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse applied_at: %w", err)
			}
			a.AppliedAt = parsedTime
		} else {
			// Standard scanning for other databases
			if err := rows.Scan(&a.Version, &a.Name, &a.AppliedAt, &a.Checksum); err != nil {
				return nil, err
			}
		}

		applied = append(applied, a)
	}

	return applied, rows.Err()
}

// Record marks a migration as applied in the database.
//
// Uses Placeholder and QuoteIdentifier strategies to generate
// database-specific SQL queries.
func (d *Driver) Record(ctx context.Context, m *queen.Migration) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (version, name, checksum)
		VALUES (%s, %s, %s)
	`,
		d.Config.QuoteIdentifier(d.TableName),
		d.Config.Placeholder(1),
		d.Config.Placeholder(2),
		d.Config.Placeholder(3),
	)

	_, err := d.DB.ExecContext(ctx, query, m.Version, m.Name, m.Checksum())
	return err
}

// Remove removes a migration record from the database (for rollback).
//
// Uses Placeholder and QuoteIdentifier strategies to generate
// database-specific SQL queries.
func (d *Driver) Remove(ctx context.Context, version string) error {
	query := fmt.Sprintf(`
		DELETE FROM %s WHERE version = %s
	`,
		d.Config.QuoteIdentifier(d.TableName),
		d.Config.Placeholder(1),
	)

	_, err := d.DB.ExecContext(ctx, query, version)
	return err
}

// --- Placeholder Strategies ---

// PlaceholderDollar creates placeholders in the format $1, $2, $3...
// Used by PostgreSQL and CockroachDB.
func PlaceholderDollar(n int) string {
	return fmt.Sprintf("$%d", n)
}

// PlaceholderQuestion creates placeholders in the format ?, ?, ?...
// Used by MySQL, SQLite, and ClickHouse.
func PlaceholderQuestion(n int) string {
	return "?"
}

// --- Time Parsing Strategies ---

// ParseTimeISO8601 parses time from ISO8601 string format.
// Used by SQLite which stores timestamps as TEXT.
//
// SQLite default format: "YYYY-MM-DD HH:MM:SS"
func ParseTimeISO8601(src interface{}) (time.Time, error) {
	str, ok := src.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("expected string, got %T", src)
	}
	return time.Parse("2006-01-02 15:04:05", str)
}
