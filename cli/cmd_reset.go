package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

func (app *App) resetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reset",
		Short: "Rollback all migrations",
		Long: `Rollback all applied migrations.

⚠️  WARNING: This is a destructive operation that will rollback ALL
migrations, potentially dropping tables and losing data.

Examples:
  # Rollback all migrations
  migrate reset`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			operation := "RESET ALL MIGRATIONS (⚠️  DESTRUCTIVE)"
			if err := app.checkConfirmation(operation); err != nil {
				return err
			}

			if !app.config.Yes {
				if !confirm("⚠️  This will rollback ALL migrations. Are you absolutely sure?") {
					return fmt.Errorf("operation cancelled")
				}
			}

			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer q.Close()

			if err := q.Reset(ctx); err != nil {
				return fmt.Errorf("failed to reset migrations: %w", err)
			}

			fmt.Println("✓ All migrations have been rolled back")
			return nil
		},
	}
}
