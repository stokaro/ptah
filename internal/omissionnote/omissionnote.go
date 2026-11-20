// Package omissionnote reports the declarations a render goes on without: a
// property the target keeps only in part, such as the length of a
// VARCHAR(255) on an engine whose string type has none, or an object the target
// has no statement for.
//
// The statements carry no trace of such a loss, except the comment a
// PostgreSQL-family target writes beside a statement, and a target served by a
// provider writes no comment at all. So `schema render` and every command that
// renders a plan raise one run warning per omission
// ([ptah.run/internal/rundiag.RenderOmissions]), and a generated migration file
// carries the same text as comments. A render raises an omission only for what
// it writes, so a plan with no statement raises none, however much the declared
// schema holds that the target cannot carry.
package omissionnote

import (
	"context"
	"io"
	"log/slog"

	"ptah.run/core/renderer"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/rundiag"
)

// Line is the warning for omission, without the "warning: " prefix: the
// target, the owning object, then the lost property and its declared value
// where there is one, as `schema validate --no-skipped` names them, then the
// omission's reason token and the remedy where the target has one. The line
// is one line whatever the declared text holds: a line break or another control
// character in a name or a value is written as an escape
// ([ptah.run/core/sqlutil.CommentText]), so the same text is safe as a stderr
// warning and as a `--` comment in a migration file.
func Line(omission renderer.Omission) string {
	subject := omission.Kind + " " + omission.Name
	if omission.Property != "" {
		subject += ": " + omission.Property
	}
	if omission.Detail != "" {
		subject += "=" + omission.Detail
	}
	line := omission.Dialect + ": " + subject + " was skipped (" + omission.Reason + ")"
	if omission.Remedy != "" {
		line += "; " + omission.Remedy
	}
	return sqlutil.CommentText(line)
}

// Report raises a warning on out for each of omissions, for the run ctx
// carries, and writes them now, ahead of the plan or the statements they
// concern: a warning written when the run ends would follow the confirmation
// it should have informed. A run warns about an omission once, however many
// renders record it. A nil out raises nothing, and without a run each line is
// written to out at once.
func Report(ctx context.Context, out io.Writer, omissions []renderer.Omission) {
	for _, omission := range omissions {
		line := Line(omission)
		_ = rundiag.Warn(ctx, out, rundiag.RenderOmissions, rundiag.Record{
			Key:  line,
			Line: line,
			Attrs: []slog.Attr{
				slog.String("target", omission.Dialect),
				slog.String("reason", omission.Reason),
			},
		})
	}
	rundiag.Settle(ctx, rundiag.RenderOmissions)
}

// Comments is omissions as SQL line comments, the text of each warning once,
// in the order given, for a migration file whose statements leave them out.
func Comments(omissions []renderer.Omission) []string {
	seen := make(map[string]bool, len(omissions))
	comments := make([]string, 0, len(omissions))
	for _, omission := range omissions {
		line := Line(omission)
		if seen[line] {
			continue
		}
		seen[line] = true
		comments = append(comments, "-- "+line)
	}
	return comments
}
