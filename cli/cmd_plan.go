package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/honeynil/queen"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func (app *App) planCmd() *cobra.Command {
	var direction string
	var limit int

	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show migration execution plan",
		Long: `Show migration execution plan without applying changes (dry-run mode).

This command previews which migrations will be applied or rolled back
without actually modifying the database.

Direction:
  - up (default): Show pending migrations that will be applied
  - down: Show applied migrations that can be rolled back

Output format:
  - Table format (default): human-readable table with warnings
  - JSON format (--json): machine-readable JSON output for CI/CD

Examples:
  # Show pending migrations (up)
  migrate plan

  # Show what would be rolled back
  migrate plan --direction down

  # Show next 3 pending migrations
  migrate plan --limit 3

  # JSON output for CI/CD
  migrate plan --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			q, err := app.setupQueen(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = q.Close() }()

			plans, err := q.DryRun(ctx, direction, limit)
			if err != nil {
				return fmt.Errorf("failed to generate migration plan: %w", err)
			}

			if app.config.JSON {
				return app.outputPlanJSON(plans, direction)
			}
			return app.outputPlanTable(plans, direction)
		},
	}

	cmd.Flags().StringVar(&direction, "direction", "up", "Migration direction: up or down")
	cmd.Flags().IntVar(&limit, "limit", 0, "Limit number of migrations to show (0 = all)")

	return cmd
}

func (app *App) outputPlanTable(plans []queen.MigrationPlan, direction string) error {
	directionLabel := strings.ToUpper(direction)
	fmt.Printf("Migration Plan (%s)\n", directionLabel)
	fmt.Println(strings.Repeat("━", 60))

	if len(plans) == 0 {
		if direction == "up" {
			fmt.Println("No pending migrations")
		} else {
			fmt.Println("No applied migrations to roll back")
		}
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{"Version", "Name", "Type", "Status", "Warnings"})

	var withRollback, withWarnings int

	for _, plan := range plans {
		arrow := "→"
		if direction == "down" {
			arrow = "←"
		}

		warnings := ""
		if len(plan.Warnings) > 0 {
			withWarnings++
			// Show warning icon
			warnings = "⚠️  " + strings.Join(plan.Warnings, "; ")
		}

		if plan.HasRollback {
			withRollback++
		}

		row := []string{
			arrow + " " + plan.Version,
			plan.Name,
			string(plan.Type),
			plan.Status,
			warnings,
		}

		if err := table.Append(row); err != nil {
			return err
		}
	}

	if err := table.Render(); err != nil {
		return err
	}

	fmt.Println()
	if direction == "up" {
		fmt.Printf("%d migration(s) will be applied\n", len(plans))
	} else {
		fmt.Printf("%d migration(s) will be rolled back\n", len(plans))
	}

	if withRollback < len(plans) && direction == "up" {
		fmt.Printf("⚠️  %d migration(s) without rollback\n", len(plans)-withRollback)
	}

	if withWarnings > 0 {
		fmt.Printf("⚠️  %d migration(s) with warnings\n", withWarnings)
	}

	return nil
}

func (app *App) outputPlanJSON(plans []queen.MigrationPlan, direction string) error {
	var withRollback, withWarnings int

	for _, plan := range plans {
		if plan.HasRollback {
			withRollback++
		}
		if len(plan.Warnings) > 0 {
			withWarnings++
		}
	}

	output := struct {
		Direction string                `json:"direction"`
		Plans     []queen.MigrationPlan `json:"plans"`
		Summary   struct {
			Total        int `json:"total"`
			WithRollback int `json:"with_rollback"`
			WithWarnings int `json:"with_warnings"`
		} `json:"summary"`
	}{
		Direction: direction,
		Plans:     plans,
	}

	output.Summary.Total = len(plans)
	output.Summary.WithRollback = withRollback
	output.Summary.WithWarnings = withWarnings

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}
