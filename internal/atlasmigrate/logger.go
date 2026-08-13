package atlasmigrate

import (
	"log/slog"
	"os"
)

// compatMigratorLogger is the logger the Atlas-compatible migrate commands give
// the migrator.
//
// It was `io.Discard` at every level. Silencing the migrator is right for the
// Info stream -- Atlas CE narrates nothing, and Ptah's per-statement Info lines
// folded into a `--format` document by the usual `2>&1 | jq` idiom is what broke
// stokaro/ptah#967 -- but discarding is not the same as silencing by level, and
// the difference is a whole class of diagnostic. A Warn here exists on NO other
// channel: it is not returned as an error and it is not in the report, so
// dropping it lets a run report success while quietly doing something other
// than what the file asked for. That is precisely the shape issue #996 names --
// a transaction-mode directive the run did not honor and did not mention.
//
// Warn and above on stderr is the rule the compat surface already documents
// (docs/site/src/content/docs/atlas/migrate-commands.md, "Three things still
// reach stderr, by design"), and the rule cmd/internal/cliobs applies to the
// process-wide default logger for the same reason. Parity survives it because a
// clean run emits nothing at that level: the diagnostics appear only when
// something is genuinely wrong.
func compatMigratorLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}
