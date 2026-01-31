package queen

import (
	"fmt"
	"regexp"
	"strconv"
)

// NamingPattern defines the migration version naming convention.
type NamingPattern string

const (
	// NamingPatternNone disables naming pattern validation (default for backward compatibility).
	NamingPatternNone NamingPattern = ""

	// NamingPatternSequential enforces sequential numbering: 1, 2, 3, ...
	NamingPatternSequential NamingPattern = "sequential"

	// NamingPatternSequentialPadded enforces padded sequential numbering: 001, 002, 003, ...
	// This is the recommended default for most projects.
	NamingPatternSequentialPadded NamingPattern = "sequential-padded"

	// NamingPatternSemver enforces semantic versioning: 1.0.0, 1.1.0, 2.0.0, ...
	NamingPatternSemver NamingPattern = "semver"
)

// NamingConfig configures migration version naming validation.
type NamingConfig struct {
	// Pattern specifies the naming pattern to enforce.
	// Default: NamingPatternNone (no validation).
	Pattern NamingPattern

	// Padding specifies the number of digits for sequential-padded pattern.
	// Only used when Pattern is NamingPatternSequentialPadded.
	// Default: 3 (generates 001, 002, 003, ...)
	Padding int

	// Enforce determines whether to return an error on validation failure.
	// If false, validation failures are logged as warnings but don't prevent migration.
	// Default: true
	Enforce bool
}

// DefaultNamingConfig returns the default naming configuration.
func DefaultNamingConfig() *NamingConfig {
	return &NamingConfig{
		Pattern: NamingPatternNone,
		Padding: 3,
		Enforce: true,
	}
}

// Validate checks if a version string matches the configured naming pattern.
func (nc *NamingConfig) Validate(version string) error {
	if nc == nil || nc.Pattern == NamingPatternNone {
		return nil
	}

	switch nc.Pattern {
	case NamingPatternSequential:
		return validateSequential(version)
	case NamingPatternSequentialPadded:
		return validateSequentialPadded(version, nc.Padding)
	case NamingPatternSemver:
		return validateSemver(version)
	default:
		return fmt.Errorf("unknown naming pattern: %s", nc.Pattern)
	}
}

// validateSequential checks if version matches sequential pattern (1, 2, 3, ...).
func validateSequential(version string) error {
	matched, _ := regexp.MatchString(`^\d+$`, version)
	if !matched {
		return fmt.Errorf("version must be a positive integer (e.g., 1, 2, 3): got %q", version)
	}

	// Check that it's not padded (no leading zeros)
	if len(version) > 1 && version[0] == '0' {
		return fmt.Errorf("version must not have leading zeros (use 'sequential-padded' pattern instead): got %q", version)
	}

	return nil
}

// validateSequentialPadded checks if version matches padded sequential pattern (001, 002, 003, ...).
func validateSequentialPadded(version string, padding int) error {
	if padding <= 0 {
		padding = 3 // Default padding
	}

	pattern := fmt.Sprintf(`^\d{%d}$`, padding)
	matched, _ := regexp.MatchString(pattern, version)
	if !matched {
		return fmt.Errorf("version must be %d-digit format (e.g., %s): got %q",
			padding, fmt.Sprintf("%0*d", padding, 1), version)
	}

	return nil
}

// validateSemver checks if version matches semantic versioning pattern (1.0.0, 1.1.0, ...).
func validateSemver(version string) error {
	matched, _ := regexp.MatchString(`^\d+\.\d+\.\d+$`, version)
	if !matched {
		return fmt.Errorf("version must be semantic version format (e.g., 1.0.0, 1.1.0): got %q", version)
	}

	return nil
}

// FindNextVersion finds the next version based on the pattern and existing versions.
// This is primarily used by CLI tools for auto-generating version numbers.
func (nc *NamingConfig) FindNextVersion(existingVersions []string) (string, error) {
	if nc == nil || nc.Pattern == NamingPatternNone {
		return "", fmt.Errorf("naming pattern not configured")
	}

	switch nc.Pattern {
	case NamingPatternSequential:
		return findNextSequential(existingVersions, false, 0)
	case NamingPatternSequentialPadded:
		padding := nc.Padding
		if padding <= 0 {
			padding = 3
		}
		return findNextSequential(existingVersions, true, padding)
	case NamingPatternSemver:
		return "", fmt.Errorf("auto-generation not supported for semver pattern, please specify version manually")
	default:
		return "", fmt.Errorf("unknown naming pattern: %s", nc.Pattern)
	}
}

// findNextSequential finds the next sequential version number.
func findNextSequential(versions []string, padded bool, padding int) (string, error) {
	maxVersion := 0

	for _, v := range versions {
		num, err := strconv.Atoi(v)
		if err != nil {
			continue
		}

		if num > maxVersion {
			maxVersion = num
		}
	}

	nextVersion := maxVersion + 1

	if padded {
		return fmt.Sprintf("%0*d", padding, nextVersion), nil
	}

	return strconv.Itoa(nextVersion), nil
}
