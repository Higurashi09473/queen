// drivers/base/quote.go
package base

import "strings"

// QuoteChar represents a SQL identifier quote character.
type QuoteChar string

const (
	DoubleQuote QuoteChar = `"` // PostgreSQL, SQLite, ClickHouse, CockroachDB
	Backtick    QuoteChar = "`" // MySQL
)

// QuoteIdentifier escapes and wraps a SQL identifier using the provided quote character.
//
// It prevents SQL injection by doubling any existing quote characters inside the identifier.
//
// Supported quote characters:
//   - DoubleQuote (") for PostgreSQL, SQLite, ClickHouse, CockroachDB
//   - Backtick (`) for MySQL
//
// Examples:
//
//	QuoteIdentifier("users", DoubleQuote)     -> "users"
//	QuoteIdentifier(`my"table`, DoubleQuote) -> "my""table"
//	QuoteIdentifier("users", Backtick)        -> `users`
//
// Use this function whenever constructing SQL queries with dynamic table or column names.
func QuoteIdentifier(name string, quoteChar QuoteChar) string {
	quote := string(quoteChar)
	escaped := strings.ReplaceAll(name, quote, quote+quote)
	return quote + escaped + quote
}

// QuoteDoubleQuotes is a convenience wrapper for QuoteIdentifier with DoubleQuote.
//
// Used by PostgreSQL, SQLite, ClickHouse, and CockroachDB.
//
// Examples:
//
//	QuoteDoubleQuotes("users")       -> "users"
//	QuoteDoubleQuotes(`my"table`)   -> "my""table"
func QuoteDoubleQuotes(name string) string {
	return QuoteIdentifier(name, DoubleQuote)
}

// QuoteBackticks is a convenience wrapper for QuoteIdentifier with Backtick.
//
// Used by MySQL.
//
// Examples:
//
//	QuoteBackticks("users")      -> `users`
//	QuoteBackticks("my`table")   -> `my``table`
func QuoteBackticks(name string) string {
	return QuoteIdentifier(name, Backtick)
}
