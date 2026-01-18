package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

func (app *App) downCmd() *cobra.Command {
	var steps int

	cmd := &cobra.Command{
		Use:   "down",
		Short: "Rollback migrations",
		Long: `Rollback migrations from the database.

By default, rolls back the last migration. Use --steps to specify
how many migrations to rollback.

Examples:
  # Rollback last migration
  migrate down

  # Rollback last 3 migrations
  migrate down --steps 3`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			if steps == 0 {
				steps = 1
			}

			operation := fmt.Sprintf("rollback %d migration(s)", steps)
			if err := app.checkConfirmation(operation); err != nil {
				return err
			}

			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer q.Close()

			if err := q.Down(ctx, steps); err != nil {
				return fmt.Errorf("failed to rollback migrations: %w", err)
			}

			fmt.Printf("✓ Rolled back %d migration(s)\n", steps)
			return nil
		},
	}

	cmd.Flags().IntVar(&steps, "steps", 0, "Number of migrations to rollback (default: 1)")

	return cmd
}
