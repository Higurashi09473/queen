package queen

import (
	"errors"
	"fmt"
)

// Common errors returned by Queen operations.
var (
	ErrNoMigrations      = errors.New("no migrations registered")
	ErrVersionConflict   = errors.New("version conflict")
	ErrMigrationNotFound = errors.New("migration not found")
	ErrChecksumMismatch  = errors.New("checksum mismatch")
	ErrLockTimeout       = errors.New("lock timeout")
	ErrNoDriver          = errors.New("driver not initialized")
	ErrInvalidMigration  = errors.New("invalid migration")
	ErrAlreadyApplied    = errors.New("migration already applied")
)

// MigrationError wraps an error with migration context.
//
// This structured error provides rich context for debugging migration failures,
// including which migration failed, what operation was being performed, and
// which database driver was in use.
type MigrationError struct {
	Version   string // Migration version (e.g., "001", "002")
	Name      string // Migration name (e.g., "create_users")
	Operation string // Operation being performed: "up", "down", "validate"
	Driver    string // Database driver name (e.g., "postgres", "mysql", "sqlite")
	Cause     error  // The underlying error that occurred
}

func (e *MigrationError) Error() string {
	if e.Driver != "" && e.Operation != "" {
		return fmt.Sprintf("migration %s (%s) failed during %s operation on %s: %v",
			e.Version, e.Name, e.Operation, e.Driver, e.Cause)
	}
	if e.Operation != "" {
		return fmt.Sprintf("migration %s (%s) failed during %s: %v",
			e.Version, e.Name, e.Operation, e.Cause)
	}
	return fmt.Sprintf("migration %s (%s): %v", e.Version, e.Name, e.Cause)
}

func (e *MigrationError) Unwrap() error {
	return e.Cause
}

// newMigrationError creates a new MigrationError with full context.
func newMigrationError(version, name, operation, driver string, err error) error {
	return &MigrationError{
		Version:   version,
		Name:      name,
		Operation: operation,
		Driver:    driver,
		Cause:     err,
	}
}
