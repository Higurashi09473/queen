// drivers/base/tablelock.go
package base

import (
	"context"
	"database/sql"
	"time"

	"github.com/honeynil/queen"
)

// TableLockConfig configures table-based distributed locking for databases
// that do not support native advisory locks.
//
// This is used by ClickHouse, CockroachDB, and other databases where
// distributed locks are implemented via a dedicated table with keys and
// expiration times.
type TableLockConfig struct {
	// CleanupQuery removes expired locks from the table.
	// For ClickHouse this can be ALTER TABLE DELETE (async),
	// for CockroachDB DELETE FROM.
	CleanupQuery string

	// CheckQuery checks for the existence of an active lock.
	// For ClickHouse, FINAL must be used with ReplacingMergeTree to ensure
	// deduplicated results, accounting for async merges.
	// For CockroachDB, a simple SELECT with LIMIT 1 is sufficient.
	CheckQuery string

	// InsertQuery inserts a new lock entry.
	InsertQuery string

	// ScanFunc processes the result of CheckQuery and returns true if
	// a lock exists, false otherwise. Returns an error if SQL execution failed.
	ScanFunc func(*sql.Row) (bool, error)
}

// AcquireTableLock implements distributed locking using a lock table.
//
// This function is intended for databases without native advisory locks
// (e.g., ClickHouse, CockroachDB).
//
// Lock mechanism:
// 1. Cleans up expired locks using CleanupQuery (best effort, ignores errors)
// 2. Checks if an active lock exists using CheckQuery and ScanFunc
// 3. If no active lock exists, attempts to insert a new lock record using InsertQuery
// 4. Retries with exponential backoff until the lock is acquired or timeout is reached
//
// Exponential backoff:
// - Starts at 50ms and doubles after each retry
// - Maximum backoff is 1 second
//
// If the lock cannot be acquired within the timeout, returns queen.ErrLockTimeout.
func AcquireTableLock(ctx context.Context, db *sql.DB, config TableLockConfig, timeout time.Duration) error {
	start := time.Now()
	expiresAt := time.Now().Add(timeout)

	backoff := 50 * time.Millisecond
	maxBackoff := 1 * time.Second

	for {
		// Step 1: clean up expired locks (best effort)
		_, _ = db.ExecContext(ctx, config.CleanupQuery, expiresAt)

		// Step 2: check if an active lock exists
		row := db.QueryRowContext(ctx, config.CheckQuery)
		hasLock, err := config.ScanFunc(row)
		if err != nil && err != sql.ErrNoRows {
			// Database error, retry with backoff
			goto retry
		}

		// Step 3: if no lock exists, try to insert
		if !hasLock {
			_, err := db.ExecContext(ctx, config.InsertQuery, expiresAt)
			if err == nil {
				// Lock acquired successfully
				return nil
			}
			// Insert failed (possible race condition), retry with backoff
		}

	retry:
		// Check if timeout is exceeded
		if time.Since(start) >= timeout {
			return queen.ErrLockTimeout
		}

		// Wait with exponential backoff
		select {
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
