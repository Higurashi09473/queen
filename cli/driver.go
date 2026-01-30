package cli

import (
	"database/sql"
	"fmt"

	"github.com/honeynil/queen"
	"github.com/honeynil/queen/drivers/clickhouse"
	"github.com/honeynil/queen/drivers/mysql"
	"github.com/honeynil/queen/drivers/postgres"
	"github.com/honeynil/queen/drivers/sqlite"
)

// Driver name constants.
//
// These constants define the recognized driver names that can be used
// in configuration. Some drivers have multiple aliases for convenience.
const (
	DriverPostgres   = "postgres"
	DriverPostgreSQL = "postgresql"
	DriverMySQL      = "mysql"
	DriverSQLite     = "sqlite"
	DriverSQLite3    = "sqlite3"
	DriverClickHouse = "clickhouse"

	// SQL driver names used with database/sql.
	// These are the actual driver names registered with sql.Register().
	SQLDriverPostgres   = "pgx"
	SQLDriverMySQL      = "mysql"
	SQLDriverSQLite     = "sqlite3"
	SQLDriverClickHouse = "clickhouse"
)

// getSQLDriverName maps Queen driver names to their corresponding SQL driver names.
//
// This function handles driver name aliases and returns the canonical SQL driver name
// that should be used with database/sql.Open(). For example, both "postgres" and
// "postgresql" map to "pgx".
//
// If the driver name is not recognized, it returns the input unchanged as a passthrough.
func getSQLDriverName(driverName string) string {
	switch driverName {
	case DriverPostgres, DriverPostgreSQL:
		return SQLDriverPostgres
	case DriverMySQL:
		return SQLDriverMySQL
	case DriverSQLite, DriverSQLite3:
		return SQLDriverSQLite
	case DriverClickHouse:
		return SQLDriverClickHouse
	default:
		return driverName
	}
}

// createDriver creates the appropriate driver based on the driver name.
func (app *App) createDriver(db *sql.DB) (queen.Driver, error) {
	switch app.config.Driver {
	case DriverPostgres, DriverPostgreSQL, "pgx":
		return postgres.New(db), nil

	case "mysql":
		return mysql.New(db), nil

	case "sqlite", "sqlite3":
		return sqlite.New(db), nil

	case "clickhouse":
		return clickhouse.New(db), nil

	default:
		return nil, fmt.Errorf("unsupported driver: %s (supported: postgres, mysql, sqlite, clickhouse)", app.config.Driver)
	}
}
