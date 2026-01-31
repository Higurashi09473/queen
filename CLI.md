# Queen CLI

Command-line interface for managing database migrations with Queen.

## Overview

Queen CLI is an embedded library - you create your own binary that imports your migrations. This approach:

- Provides direct access to Go function migrations
- Follows the "migrations are code, not files" philosophy
- Works like goose v3 for Go migrations

## Quick Start

### 1. Create your migration binary

```go
// cmd/migrate/main.go
package main

import (
    _ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver

    "github.com/honeynil/queen/cli"
    "myapp/migrations"
)

func main() {
    cli.Run(migrations.Register)
}
```

### 2. Create migrations

```go
// migrations/register.go
package migrations

import "github.com/honeynil/queen"

func Register(q *queen.Queen) {
    q.MustAdd(Migration001CreateUsers)
    q.MustAdd(Migration002AddEmail)
}
```

```go
// migrations/001_create_users.go
package migrations

import "github.com/honeynil/queen"

var Migration001CreateUsers = queen.M{
    Version: "001",
    Name:    "create_users",
    UpSQL: `
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE
        )
    `,
    DownSQL: `DROP TABLE users`,
}
```

### 3. Build and run

```bash
go build -o migrate cmd/migrate/main.go

# Using environment variables
export QUEEN_DRIVER=postgres
export QUEEN_DSN="postgres://localhost/myapp?sslmode=disable"

./migrate up
./migrate status
```

## Commands

### create

Create a new migration file.

```bash
migrate create <name> [--type sql|go]
```

**Options:**
- `--type sql` (default): SQL migration with UpSQL/DownSQL
- `--type go`: Go function migration with UpFunc/DownFunc

**Examples:**
```bash
migrate create add_users_table              # SQL migration
migrate create migrate_user_data --type go  # Go function migration
```

After creating, add the migration to `migrations/register.go`:
```go
q.MustAdd(Migration003AddUsersTable)
```

### up

Apply pending migrations.

```bash
migrate up [--steps N]
```

**Options:**
- `--steps N`: Apply only N migrations (default: all pending)

**Examples:**
```bash
migrate up           # Apply all pending
migrate up --steps 3 # Apply next 3 migrations
```

### down

Rollback migrations.

```bash
migrate down [--steps N]
```

**Options:**
- `--steps N`: Rollback N migrations (default: 1)

**Examples:**
```bash
migrate down           # Rollback last migration
migrate down --steps 3 # Rollback last 3 migrations
```

### reset

Rollback all applied migrations.

```bash
migrate reset
```

### plan

Show migration execution plan (dry-run mode).

```bash
migrate plan [--direction up|down] [--limit N] [--json]
```

**Options:**
- `--direction`: Migration direction - `up` (default) or `down`
- `--limit N`: Show only N migrations (default: all)
- `--json`: Output in JSON format for CI/CD integration

**Examples:**
```bash
migrate plan                    # Show pending migrations
migrate plan --direction down   # Show what would be rolled back
migrate plan --limit 3          # Show next 3 pending migrations
migrate plan --json             # JSON output for CI/CD
```

**Table output:**
```
Migration Plan (UP)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
→ 002  add_email      sql      pending
→ 003  migrate_data   go-func  pending  ⚠️  No rollback defined

2 migration(s) will be applied
⚠️  1 migration(s) with warnings
```

**JSON output:**
```json
{
  "direction": "up",
  "plans": [
    {
      "version": "002",
      "name": "add_email",
      "direction": "up",
      "status": "pending",
      "type": "sql",
      "sql": "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
      "has_rollback": true,
      "is_destructive": false,
      "checksum": "abc123...",
      "warnings": []
    }
  ],
  "summary": {
    "total": 2,
    "with_rollback": 1,
    "with_warnings": 1
  }
}
```

### explain

Explain a specific migration.

```bash
migrate explain <version> [--json]
```

**Options:**
- `--json`: Output in JSON format

**Examples:**
```bash
migrate explain 001       # Explain migration 001
migrate explain 001 --json # JSON output
```

**Output:**
```
Migration: 001
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Name:          create_users
Status:        applied
Type:          sql
Direction:     up
Has Rollback:  true
Checksum:      a1b2c3d4...

UP SQL:
------------------------------------------------------------
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) NOT NULL UNIQUE
)
------------------------------------------------------------
```

### status

Show migration status.

```bash
migrate status [--json]
```

**Options:**
- `--json`: Output in JSON format for CI/CD integration

**Table output:**

| Version | Name         | Status  | Applied At          | Checksum    | Rollback |
|---------|--------------|---------|---------------------|-------------|----------|
| 001     | create_users | applied | 2026-01-16 10:30:00 | a1b2c3d4... | yes      |
| 002     | add_email    | applied | 2026-01-16 10:30:05 | e5f6g7h8... | yes      |
| 003     | migrate_data | pending | -                   | i9j0k1l2... | yes      |

```
Summary: 3 total, 2 applied, 1 pending
```

**JSON output:**
```json
{
  "migrations": [
    {
      "version": "001",
      "name": "create_users",
      "status": "applied",
      "applied_at": "2026-01-16T10:30:00Z",
      "checksum": "a1b2c3d4...",
      "has_rollback": true
    }
  ],
  "summary": {
    "total": 3,
    "applied": 2,
    "pending": 1,
    "modified": 0
  }
}
```

### validate

Validate all registered migrations.

```bash
migrate validate
```

Checks for:
- Duplicate version identifiers
- Invalid migration definitions
- Checksum mismatches (modified applied migrations)

### version

Show current migration version.

```bash
migrate version
```

## Configuration

Configuration priority (highest to lowest):
1. Command-line flags
2. Environment variables
3. Config file `.queen.yaml` (requires `--use-config`)

### Command-line flags

```bash
migrate --driver postgres --dsn "postgres://localhost/myapp" up
```

| Flag | Description |
|------|-------------|
| `--driver` | Database driver: postgres, mysql, sqlite, clickhouse |
| `--dsn` | Database connection string |
| `--table` | Migration table name (default: queen_migrations) |
| `--timeout` | Lock timeout (e.g. 30m, 1h) |
| `--use-config` | Enable config file |
| `--env` | Environment from config file |
| `--unlock-production` | Unlock production environment |
| `--yes` | Skip confirmation prompts (for CI/CD) |
| `--json` | JSON output for status command |
| `--verbose` | Verbose output |

### Environment variables

```bash
export QUEEN_DRIVER=postgres
export QUEEN_DSN="postgres://localhost/myapp?sslmode=disable"
export QUEEN_TABLE=queen_migrations
export QUEEN_LOCK_TIMEOUT=30m
```

### Config file (.queen.yaml)

```yaml
# Safety lock - prevents accidental use
config_locked: false

development:
  driver: postgres
  dsn: postgres://localhost/myapp_dev?sslmode=disable
  table: queen_migrations
  lock_timeout: 30m

staging:
  driver: postgres
  dsn: postgres://staging.example.com/myapp?sslmode=require
  require_confirmation: true

production:
  driver: postgres
  dsn: postgres://prod.example.com/myapp?sslmode=require
  require_confirmation: true
  require_explicit_unlock: true
```

**Usage:**
```bash
# Development
migrate --use-config --env development up

# Production (requires --unlock-production)
migrate --use-config --env production --unlock-production up
```

## Safety Features

### Config locking

Set `config_locked: true` in `.queen.yaml` to prevent accidental usage:

```yaml
config_locked: true
```

This requires explicit flags or environment variables instead of config file.

### Confirmation prompts

Environments with `require_confirmation: true` will prompt before destructive operations:

```
⚠️  WARNING: You are about to apply migrations on STAGING environment
Database: postgres://staging.example.com/myapp
Continue? (yes/no):
```

### Production unlock

Environments with `require_explicit_unlock: true` require `--unlock-production` flag:

```bash
# This will fail
migrate --use-config --env production up

# This works
migrate --use-config --env production --unlock-production up
```

Production also requires typing "production" to confirm:

```
⚠️  DANGER: You are about to apply migrations on PRODUCTION environment
Type 'production' to confirm:
```

### CI/CD mode

Use `--yes` to skip all confirmations:

```bash
migrate --driver postgres --dsn "$DATABASE_URL" --yes up
```

## Custom DB Connection

For custom connection setup (connection pooling, etc.):

```go
package main

import (
    "database/sql"

    _ "github.com/jackc/pgx/v5/stdlib"

    "github.com/honeynil/queen/cli"
    "myapp/migrations"
)

func main() {
    cli.RunWithDB(migrations.Register, func(dsn string) (*sql.DB, error) {
        db, err := sql.Open("pgx", dsn)
        if err != nil {
            return nil, err
        }

        // Custom settings
        db.SetMaxOpenConns(10)
        db.SetMaxIdleConns(5)

        return db, nil
    })
}
```

## Supported Databases

| Database | Driver name | Connection string example |
|----------|-------------|---------------------------|
| PostgreSQL | `postgres` | `postgres://user:pass@localhost/db?sslmode=disable` |
| MySQL | `mysql` | `user:pass@tcp(localhost:3306)/db?parseTime=true` |
| SQLite | `sqlite` | `./app.db?_journal_mode=WAL` |
| ClickHouse | `clickhouse` | `tcp://localhost:9000/db` |

## Project Structure

Recommended project layout:

```
myapp/
├── cmd/
│   └── migrate/
│       └── main.go         # CLI entry point
├── migrations/
│   ├── register.go         # Migration registration
│   ├── 001_create_users.go
│   ├── 002_add_email.go
│   └── 003_migrate_data.go
├── .queen.yaml             # Optional config
└── go.mod
```
