// drivers/base/quote.go
package base

import "strings"

// QuoteChar represents a SQL identifier quote character.
type QuoteChar string

const (
	DoubleQuote QuoteChar = `"`  // PostgreSQL, SQLite, ClickHouse, CockroachDB
	Backtick    QuoteChar = "`"  // MySQL
	Bracket     QuoteChar = `[]` // MS SQL Server
)

// QuoteIdentifier escapes and wraps a SQL identifier using the provided quote character.
//
// It prevents SQL injection by doubling any existing quote characters inside the identifier.
//
// Supported quote characters:
//   - DoubleQuote (") for PostgreSQL, SQLite, ClickHouse, CockroachDB
//   - Backtick (`) for MySQL
//   - Bracket ([]) for MS SQL Server
//
// Examples:
//
//	QuoteIdentifier("users", DoubleQuote)     -> "users"
//	QuoteIdentifier(`my"table`, DoubleQuote) -> "my""table"
//	QuoteIdentifier("users", Backtick)        -> `users`
//	QuoteIdentifier("users", Bracket)         -> [users]
//
// Use this function whenever constructing SQL queries with dynamic table or column names.
func QuoteIdentifier(name string, quoteChar QuoteChar) string {
	// Special handling for MS SQL Server brackets
	if quoteChar == Bracket {
		escaped := strings.ReplaceAll(name, "]", "]]")
		return "[" + escaped + "]"
	}

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

// QuoteBrackets is a convenience wrapper for QuoteIdentifier with Bracket.
//
// Used by MS SQL Server.
//
// Examples:
//
//	QuoteBrackets("users")      -> [users]
//	QuoteBrackets("my]table")   -> [my]]table]
func QuoteBrackets(name string) string {
	return QuoteIdentifier(name, Bracket)
}
