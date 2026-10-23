package migrateup

import (
	"context"
	"log/slog"
)

// readFloor drops the migrator's Info records while quiet reports true. The
// status reads that run before the migration lock put the schema writer in
// dry-run mode so they create nothing, and in that mode the migrator narrates
// `[DRY RUN] Would initialize migrations metadata` for an empty database. The
// user did not ask for a dry run, so that record would be wrong on the log
// stream. Warnings and errors always pass.
type readFloor struct {
	slog.Handler
	quiet func() bool
}

// Enabled reports false for an Info record or below while quiet holds.
func (h readFloor) Enabled(ctx context.Context, level slog.Level) bool {
	if level < slog.LevelWarn && h.quiet() {
		return false
	}
	return h.Handler.Enabled(ctx, level)
}

// WithAttrs keeps the floor on the handler the attributes return.
func (h readFloor) WithAttrs(attrs []slog.Attr) slog.Handler {
	return readFloor{Handler: h.Handler.WithAttrs(attrs), quiet: h.quiet}
}

// WithGroup keeps the floor on the handler the group returns.
func (h readFloor) WithGroup(name string) slog.Handler {
	return readFloor{Handler: h.Handler.WithGroup(name), quiet: h.quiet}
}
