package dbcli

import (
	"context"
	"log/slog"

	"ptah.run/dbschema"
)

// SuppressSchemaWrites puts the connection's schema writer in dry-run mode and
// returns the function that restores the mode it had. A migrator that reads
// through a dry-run writer inspects the revision table instead of creating it,
// and an absent table reads as no migrations applied.
//
// The migrator narrates that inspection at Info as an apply would, with
// `[DRY RUN] Would initialize migrations metadata`; give a read's migrator a
// [WarnFloor] logger so the narration stays off the command's output.
func SuppressSchemaWrites(conn *dbschema.DatabaseConnection) (restore func()) {
	writer := conn.SchemaWriter()
	previous := writer.IsDryRun()
	writer.SetDryRun(true)
	return func() { writer.SetDryRun(previous) }
}

// WarnFloor returns handler with every record below Warn dropped: the
// migrator's warnings and errors reach it, and its Info records do not. Every
// Info record the migrator writes narrates an apply, which a read does not do.
// A logger derived with attributes or a group keeps the floor.
func WarnFloor(handler slog.Handler) slog.Handler {
	return warnFloor{handler}
}

type warnFloor struct {
	slog.Handler
}

// Enabled reports whether level is Warn or above and the wrapped handler takes
// it.
func (h warnFloor) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn && h.Handler.Enabled(ctx, level)
}

// WithAttrs keeps the floor on the handler the attributes return.
func (h warnFloor) WithAttrs(attrs []slog.Attr) slog.Handler {
	return warnFloor{h.Handler.WithAttrs(attrs)}
}

// WithGroup keeps the floor on the handler the group returns.
func (h warnFloor) WithGroup(name string) slog.Handler {
	return warnFloor{h.Handler.WithGroup(name)}
}
