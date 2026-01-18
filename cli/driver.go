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

// createDriver creates the appropriate driver based on the driver name.
func (app *App) createDriver(db *sql.DB) (queen.Driver, error) {
	switch app.config.Driver {
	case "postgres", "postgresql", "pgx":
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
