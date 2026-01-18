package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

func (app *App) validateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate migrations",
		Long: `Validate all registered migrations.

This command checks for:
  - Duplicate version identifiers
  - Invalid migration definitions
  - Checksum mismatches (applied migrations that have been modified)

If any issues are found, the command will exit with an error.

Examples:
  # Validate all migrations
  migrate validate`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			if err := q.Validate(ctx); err != nil {
				return fmt.Errorf("validation failed: %w", err)
			}

			fmt.Println("✓ All migrations are valid")
			return nil
		},
	}
}
