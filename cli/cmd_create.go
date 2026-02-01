package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/honeynil/queen"
	"github.com/spf13/cobra"
)

func (app *App) createCmd() *cobra.Command {
	var migrationType string

	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new migration",
		Long: `Create a new migration file with a template.

This command generates a new migration file in the migrations/ directory
with a sequential version number and the specified name.

Migration types:
  - sql (default): SQL migration with UpSQL and DownSQL
  - go: Go function migration with UpFunc and DownFunc

The command will:
  1. Scan migrations/ directory to find the next version number
  2. Create a new file: migrations/<version>_<name>.go
  3. Print instructions for adding to register.go

Examples:
  # Create SQL migration
  migrate create add_users_table

  # Create Go function migration
  migrate create migrate_user_data --type go`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			// Validate name
			if !queen.IsValidMigrationName(name) {
				return fmt.Errorf("invalid migration name: must contain only lowercase letters, numbers, and underscores")
			}

			// Load config file to get naming pattern
			if err := app.loadConfigFile(); err != nil {
				// If config doesn't exist, use default pattern
				if !os.IsNotExist(err) && err.Error() != "config file not found: .queen.yaml (use --use-config only when config file exists)" {
					return fmt.Errorf("failed to load config: %w", err)
				}
			}

			// Determine next version
			nextVersion, err := app.findNextVersion()
			if err != nil {
				return err
			}

			// Generate file
			filename := fmt.Sprintf("migrations/%s_%s.go", nextVersion, name)
			variableName := migrationVariableName(nextVersion, name)

			var content string
			switch migrationType {
			case "sql":
				content = generateSQLTemplate(nextVersion, name, variableName)
			case "go":
				content = generateGoTemplate(nextVersion, name, variableName)
			default:
				return fmt.Errorf("invalid migration type: %s (must be 'sql' or 'go')", migrationType)
			}

			// Create migrations directory if it doesn't exist
			if err := os.MkdirAll("migrations", 0755); err != nil {
				return fmt.Errorf("failed to create migrations directory: %w", err)
			}

			// Write file
			if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
				return fmt.Errorf("failed to create migration file: %w", err)
			}

			// Success message
			fmt.Printf("✓ Created migration file: %s\n\n", filename)
			fmt.Println("Next steps:")
			fmt.Printf("1. Edit %s and add your migration logic\n", filename)
			fmt.Println("2. Add this line to migrations/register.go:")
			fmt.Printf("\n   q.MustAdd(%s)\n\n", variableName)

			return nil
		},
	}

	cmd.Flags().StringVar(&migrationType, "type", "sql", "Migration type: sql or go")

	return cmd
}

// findNextVersion scans the migrations directory and returns the next version number
// based on the naming pattern from config.
func (app *App) findNextVersion() (string, error) {
	// Get naming config
	namingConfig := app.getNamingConfig()

	// If no naming config, use default sequential-padded with padding 3
	if namingConfig == nil {
		namingConfig = &queen.NamingConfig{
			Pattern: queen.NamingPatternSequentialPadded,
			Padding: 3,
			Enforce: true,
		}
	}

	// Scan existing migrations
	existingVersions, err := app.getExistingVersions()
	if err != nil {
		return "", err
	}

	// If no existing migrations, return first version
	if len(existingVersions) == 0 {
		switch namingConfig.Pattern {
		case queen.NamingPatternSequential:
			return "1", nil
		case queen.NamingPatternSequentialPadded:
			padding := namingConfig.Padding
			if padding <= 0 {
				padding = 3
			}
			return fmt.Sprintf("%0*d", padding, 1), nil
		case queen.NamingPatternSemver:
			return "", fmt.Errorf("semver pattern requires manual version specification, use --version flag")
		default:
			return "", fmt.Errorf("unknown naming pattern: %s", namingConfig.Pattern)
		}
	}

	// Use naming config to find next version
	return namingConfig.FindNextVersion(existingVersions)
}

// getExistingVersions scans the migrations directory and returns all existing version strings.
func (app *App) getExistingVersions() ([]string, error) {
	entries, err := os.ReadDir("migrations")
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var versions []string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse version from filename (e.g., "001_create_users.go")
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") {
			continue
		}

		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 2 {
			continue
		}

		versions = append(versions, parts[0])
	}

	return versions, nil
}

// migrationVariableName generates a Go variable name from version and name.
// Example: "001", "create_users" -> "Migration001CreateUsers"
func migrationVariableName(version, name string) string {
	// Convert snake_case to PascalCase
	parts := strings.Split(name, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}

	return fmt.Sprintf("Migration%s%s", version, strings.Join(parts, ""))
}

// generateSQLTemplate generates a SQL migration template.
func generateSQLTemplate(version, name, variableName string) string {
	description := strings.ReplaceAll(name, "_", " ")

	return fmt.Sprintf(`package migrations

import "github.com/honeynil/queen"

// %s %s
var %s = queen.M{
	Version: "%s",
	Name:    "%s",
	UpSQL: `+"`"+`
		-- Write your migration here
		-- Example: CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));
	`+"`"+`,
	DownSQL: `+"`"+`
		-- Write your rollback here
		-- Example: DROP TABLE users;
	`+"`"+`,
}
`, variableName, description, variableName, version, name)
}

// generateGoTemplate generates a Go function migration template.
func generateGoTemplate(version, name, variableName string) string {
	description := strings.ReplaceAll(name, "_", " ")
	upFuncName := fmt.Sprintf("up%s%s", version, toPascalCase(name))
	downFuncName := fmt.Sprintf("down%s%s", version, toPascalCase(name))

	return fmt.Sprintf(`package migrations

import (
	"context"
	"database/sql"

	"github.com/honeynil/queen"
)

// %s %s
var %s = queen.M{
	Version:        "%s",
	Name:           "%s",
	ManualChecksum: "v1", // Update this when you change the function
	UpFunc:         %s,
	DownFunc:       %s,
}

func %s(ctx context.Context, tx *sql.Tx) error {
	// TODO: Implement your migration logic
	// Example:
	// rows, err := tx.QueryContext(ctx, "SELECT id, name FROM users")
	// if err != nil {
	//     return err
	// }
	// defer rows.Close()
	//
	// for rows.Next() {
	//     var id int
	//     var name string
	//     if err := rows.Scan(&id, &name); err != nil {
	//         return err
	//     }
	//     // Process data...
	// }
	//
	// return rows.Err()
	return nil
}

func %s(ctx context.Context, tx *sql.Tx) error {
	// TODO: Implement your rollback logic
	return nil
}
`, variableName, description, variableName, version, name, upFuncName, downFuncName, upFuncName, downFuncName)
}

// toPascalCase converts snake_case to PascalCase.
func toPascalCase(s string) string {
	parts := strings.Split(s, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, "")
}
