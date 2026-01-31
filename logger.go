package queen

import "context"

// Logger defines a structured logging interface compatible with slog.
//
// This interface is intentionally compatible with *slog.Logger from the standard library,
// allowing direct usage of slog loggers without adapters.
//
// Example using slog:
//
//	import "log/slog"
//
//	logger := slog.Default()
//	q := queen.New(driver, queen.WithLogger(logger))
//
// Example using custom logger:
//
//	type MyLogger struct{}
//
//	func (l *MyLogger) InfoContext(ctx context.Context, msg string, args ...any) {
//	    // Custom implementation
//	}
//
//	func (l *MyLogger) WarnContext(ctx context.Context, msg string, args ...any) {
//	    // Custom implementation
//	}
//
//	func (l *MyLogger) ErrorContext(ctx context.Context, msg string, args ...any) {
//	    // Custom implementation
//	}
//
//	q := queen.New(driver, queen.WithLogger(&MyLogger{}))
type Logger interface {
	// InfoContext logs an informational message with structured fields.
	// Compatible with slog.Logger.InfoContext.
	InfoContext(ctx context.Context, msg string, args ...any)

	// WarnContext logs a warning message with structured fields.
	// Compatible with slog.Logger.WarnContext.
	WarnContext(ctx context.Context, msg string, args ...any)

	// ErrorContext logs an error message with structured fields.
	// Compatible with slog.Logger.ErrorContext.
	ErrorContext(ctx context.Context, msg string, args ...any)
}

// noopLogger is a no-op logger implementation that discards all log messages.
// Used as the default when no logger is configured.
type noopLogger struct{}

func (n *noopLogger) InfoContext(ctx context.Context, msg string, args ...any)  {}
func (n *noopLogger) WarnContext(ctx context.Context, msg string, args ...any)  {}
func (n *noopLogger) ErrorContext(ctx context.Context, msg string, args ...any) {}

// defaultLogger returns the default noop logger.
func defaultLogger() Logger {
	return &noopLogger{}
}
