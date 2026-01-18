package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

func (app *App) upCmd() *cobra.Command {
	var steps int

	cmd := &cobra.Command{
		Use:   "up",
		Short: "Apply pending migrations",
		Long: `Apply pending migrations to the database.

By default, applies all pending migrations. Use --steps to limit the
number of migrations to apply.

Examples:
  # Apply all pending migrations
  migrate up

  # Apply next 3 migrations
  migrate up --steps 3`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			operation := "apply migrations"
			if steps > 0 {
				operation = fmt.Sprintf("apply %d migration(s)", steps)
			}
			if err := app.checkConfirmation(operation); err != nil {
				return err
			}

			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer q.Close()

			if steps > 0 {
				if err := q.UpSteps(ctx, steps); err != nil {
					return fmt.Errorf("failed to apply migrations: %w", err)
				}
				fmt.Printf("✓ Applied %d migration(s)\n", steps)
			} else {
				if err := q.Up(ctx); err != nil {
					return fmt.Errorf("failed to apply migrations: %w", err)
				}
				fmt.Println("✓ All migrations applied successfully")
			}

			return nil
		},
	}

	cmd.Flags().IntVar(&steps, "steps", 0, "Number of migrations to apply (0 = all)")

	return cmd
}
