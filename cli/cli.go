// Package cli provides a command-line interface for Queen migrations.
//
// The CLI allows users to manage database migrations through a simple
// command-line tool. Users create their own binary that imports their
// migrations and calls cli.Run().
//
// Example usage:
//
//	// cmd/migrate/main.go
//	package main
//
//	import (
//	    "github.com/honeynil/queen/cli"
//	    "myapp/migrations"
//	)
//
//	func main() {
//	    cli.Run(migrations.Register)
//	}
//
// The CLI supports configuration through flags, environment variables,
// and an optional .queen.yaml config file.
package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
)

// RegisterFunc is a function that registers migrations with Queen.
// Users provide this function to register all their migrations.
type RegisterFunc func(*queen.Queen)

// DBOpener is a function that opens a database connection.
// It receives the DSN (data source name) and returns a *sql.DB.
type DBOpener func(dsn string) (*sql.DB, error)

// App holds the CLI application state.
type App struct {
	registerFunc RegisterFunc
	dbOpener     DBOpener
	config       *Config
	rootCmd      *cobra.Command
}

// Run starts the CLI with the given migration registration function.
// This is the main entry point for users.
//
// Configuration priority:
//  1. Command-line flags (highest)
//  2. Environment variables
//  3. Config file .queen.yaml (lowest, requires --use-config)
func Run(register RegisterFunc) {
	RunWithDB(register, nil)
}

// RunWithDB starts the CLI with a custom database opener.
// If dbOpener is nil, uses sql.Open with the driver name.
func RunWithDB(register RegisterFunc, dbOpener DBOpener) {
	app := &App{
		registerFunc: register,
		dbOpener:     dbOpener,
		config:       &Config{},
	}

	app.rootCmd = &cobra.Command{
		Use:   "queen",
		Short: "Queen migration CLI",
		Long: `Queen migration CLI - Database migrations for Go.

Configuration priority:
  1. Command-line flags (highest)
  2. Environment variables (QUEEN_DRIVER, QUEEN_DSN, etc.)
  3. Config file .queen.yaml (lowest, requires --use-config)

Examples:
  # Apply all pending migrations
  queen up

  # Apply next 3 migrations
  queen up --steps 3

  # Rollback last migration
  queen down

  # Show migration status
  queen status

  # Create new migration
  queen create add_users_table`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	app.addGlobalFlags()
	app.addCommands()

	if err := app.rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// addGlobalFlags adds flags that are available to all commands.
func (app *App) addGlobalFlags() {
	flags := app.rootCmd.PersistentFlags()

	flags.StringVar(&app.config.Driver, "driver", "", "Database driver (postgres, mysql, sqlite, clickhouse)")
	flags.StringVar(&app.config.DSN, "dsn", "", "Database connection string")
	flags.StringVar(&app.config.Table, "table", "queen_migrations", "Migration table name")
	flags.DurationVar(&app.config.LockTimeout, "timeout", 0, "Lock timeout (e.g. 30m, 1h)")
	flags.BoolVar(&app.config.UseConfig, "use-config", false, "Enable config file (.queen.yaml)")
	flags.StringVar(&app.config.Env, "env", "", "Environment from config file (development, staging, production)")
	flags.BoolVar(&app.config.UnlockProduction, "unlock-production", false, "Unlock production environment")
	flags.BoolVar(&app.config.Yes, "yes", false, "Automatic yes to prompts (for CI/CD)")
	flags.BoolVar(&app.config.JSON, "json", false, "Output in JSON format")
	flags.BoolVar(&app.config.Verbose, "verbose", false, "Verbose output")
}

// addCommands registers all CLI commands.
func (app *App) addCommands() {
	app.rootCmd.AddCommand(
		app.createCmd(),
		app.upCmd(),
		app.downCmd(),
		app.resetCmd(),
		app.statusCmd(),
		app.validateCmd(),
		app.versionCmd(),
	)
}

// setupQueen creates a Queen instance with the current configuration.
func (app *App) setupQueen(ctx context.Context) (*queen.Queen, error) {
	if err := app.loadConfig(); err != nil {
		return nil, err
	}

	if app.config.Driver == "" {
		return nil, fmt.Errorf("driver is required (use --driver or QUEEN_DRIVER)")
	}
	if app.config.DSN == "" {
		return nil, fmt.Errorf("dsn is required (use --dsn or QUEEN_DSN)")
	}

	var db *sql.DB
	var err error

	if app.dbOpener != nil {
		db, err = app.dbOpener(app.config.DSN)
	} else {
		sqlDriverName := getSQLDriverName(app.config.Driver)
		db, err = sql.Open(sqlDriverName, app.config.DSN)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	driver, err := app.createDriver(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	queenConfig := &queen.Config{
		TableName: app.config.Table,
	}
	if app.config.LockTimeout > 0 {
		queenConfig.LockTimeout = app.config.LockTimeout
	}

	q := queen.NewWithConfig(driver, queenConfig)
	app.registerFunc(q)

	return q, nil
}

func getSQLDriverName(driverName string) string {
	switch driverName {
	case "postgres", "postgresql":
		return "pgx"
	case "mysql":
		return "mysql"
	case "sqlite", "sqlite3":
		return "sqlite3"
	case "clickhouse":
		return "clickhouse"
	default:
		return driverName
	}
}

// loadConfig loads configuration from all sources.
// Priority: flags > env > config file.
func (app *App) loadConfig() error {
	if app.config.UseConfig {
		if err := app.loadConfigFile(); err != nil {
			return err
		}
	}
	app.loadEnv()
	return nil
}

func (app *App) loadEnv() {
	if app.config.Driver == "" {
		if driver := os.Getenv("QUEEN_DRIVER"); driver != "" {
			app.config.Driver = driver
		}
	}

	if app.config.DSN == "" {
		if dsn := os.Getenv("QUEEN_DSN"); dsn != "" {
			app.config.DSN = dsn
		}
	}

	if app.config.Table == "queen_migrations" {
		if table := os.Getenv("QUEEN_TABLE"); table != "" {
			app.config.Table = table
		}
	}
}
