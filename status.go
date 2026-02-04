package queen

import "time"

// Status represents the current state of a migration.
type Status int

const (
	// StatusPending indicates the migration has not been applied yet.
	StatusPending Status = iota

	// StatusApplied indicates the migration has been successfully applied.
	StatusApplied

	// StatusModified indicates the migration has been applied,
	// but its content has changed (checksum mismatch).
	StatusModified
)

// String returns a human-readable representation of the status.
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusApplied:
		return "applied"
	case StatusModified:
		return "modified"
	default:
		return "unknown"
	}
}

// MigrationStatus contains detailed information about a migration's current state.
// This is returned by Queen.Status().
type MigrationStatus struct {
	// Version is the unique version identifier of the migration.
	Version string

	// Name is the human-readable name of the migration.
	Name string

	// Status indicates whether the migration is pending, applied, or modified.
	Status Status

	// AppliedAt is when the migration was applied (nil if not applied).
	AppliedAt *time.Time

	// Checksum is the current checksum of the migration.
	Checksum string

	// HasRollback indicates if the migration has a down migration.
	HasRollback bool

	// Destructive indicates if the down migration contains destructive operations.
	Destructive bool
}

// MigrationType represents the type of migration implementation.
type MigrationType string

const (
	// MigrationTypeSQL indicates the migration uses SQL (UpSQL/DownSQL).
	MigrationTypeSQL MigrationType = "sql"

	// MigrationTypeGoFunc indicates the migration uses Go functions (UpFunc/DownFunc).
	MigrationTypeGoFunc MigrationType = "go-func"

	// MigrationTypeMixed indicates the migration uses both SQL and Go functions.
	MigrationTypeMixed MigrationType = "mixed"
)

// MigrationPlan represents a migration execution plan for dry-run mode.
// This is returned by Queen.DryRun() and Queen.Explain().
type MigrationPlan struct {
	// Version is the unique version identifier of the migration.
	Version string `json:"version"`

	// Name is the human-readable name of the migration.
	Name string `json:"name"`

	// Direction indicates the migration direction: "up" or "down".
	Direction string `json:"direction"`

	// Status indicates whether the migration is pending, applied, or modified.
	Status string `json:"status"`

	// Type indicates the migration type: "sql", "go-func", or "mixed".
	Type MigrationType `json:"type"`

	// SQL contains the SQL that will be executed (if applicable).
	// Empty for Go function-only migrations.
	SQL string `json:"sql,omitempty"`

	// HasRollback indicates if the migration has a down migration.
	HasRollback bool `json:"has_rollback"`

	// IsDestructive indicates if the migration contains destructive operations.
	// Only applicable for down migrations.
	IsDestructive bool `json:"is_destructive"`

	// Checksum is the current checksum of the migration.
	Checksum string `json:"checksum"`

	// Warnings contains any warnings about this migration.
	// Examples: "No rollback defined", "Destructive operation", etc.
	Warnings []string `json:"warnings,omitempty"`
}
