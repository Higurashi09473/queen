// drivers/base/owner.go
package base

import (
	"crypto/rand"
	"encoding/hex"
)

// GenerateOwnerID generates a unique owner identifier for lock ownership tracking.
//
// This ID is used to prevent race conditions where one process unlocks another
// process's lock. Each Driver instance gets a unique owner ID when created.
//
// The owner ID is a cryptographically secure random string of 32 hexadecimal
// characters (16 random bytes encoded as hex). This provides sufficient uniqueness
// across processes, containers, and hosts without requiring external dependencies.
//
// Returns an error if the system's random number generator fails.
//
// Example usage:
//
//	ownerID, err := base.GenerateOwnerID()
//	if err != nil {
//	    return fmt.Errorf("failed to generate owner ID: %w", err)
//	}
func GenerateOwnerID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
