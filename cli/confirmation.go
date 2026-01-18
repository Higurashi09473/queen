package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// confirm prompts the user for confirmation.
// Returns true if user confirms, false otherwise.
func confirm(message string) bool {
	fmt.Printf("%s (yes/no): ", message)

	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}

	response = strings.TrimSpace(strings.ToLower(response))
	return response == "yes" || response == "y"
}

// confirmExact prompts the user to type an exact string for confirmation.
// Returns true if user types the exact string, false otherwise.
func confirmExact(message, expected string) bool {
	fmt.Printf("%s\nType '%s' to confirm: ", message, expected)

	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}

	response = strings.TrimSpace(response)
	return response == expected
}

// checkConfirmation checks if confirmation is required and prompts if needed.
func (app *App) checkConfirmation(operation string) error {
	if !app.requiresConfirmation() {
		return nil
	}

	env := app.getEnvironmentName()
	message := fmt.Sprintf("⚠️  WARNING: You are about to %s on %s environment\nDatabase: %s",
		operation, strings.ToUpper(env), app.config.DSN)

	// For production, require exact confirmation
	if env == "production" {
		if !confirmExact(message, "production") {
			return fmt.Errorf("operation cancelled")
		}
	} else {
		if !confirm(message + "\nContinue?") {
			return fmt.Errorf("operation cancelled")
		}
	}

	return nil
}
