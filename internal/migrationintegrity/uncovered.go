package migrationintegrity

import (
	"context"
	"io"
	"io/fs"
	"log/slog"
	"path"
	"strings"

	"ptah.run/internal/atlasmigrateimport"
	"ptah.run/internal/migratesum"
	"ptah.run/internal/rundiag"
	"ptah.run/migration/migrationfile"
)

// ReportUncovered raises, for the run ctx carries, every SQL file fsys holds
// that atlas.sum cannot cover and that a run of the directory therefore
// skips, on out; see [rundiag.UncoveredMigrationFiles].
//
// An Atlas-format sum covers the top-level files whose name ends in exactly
// ".sql", and a run executes those files and no others, so a migration in a
// subdirectory or spelled `.SQL` never runs. Saying nothing about it is the
// silent drop the narrowing exists to prevent: on a directory whose only
// migration is `sub/2_b.sql`, every gate passes, the run executes nothing and
// the database stays empty. The community binary says nothing either; this is
// where both of Ptah's surfaces say more.
//
// The uncovered set is every *.sql in the tree, the extension matched without
// regard to case, minus the covered set, and the covered set comes from
// [atlasmigrateimport.SumFileNames], the function `migrate hash` writes from
// and `migrate validate` verifies against. Restating the top-level,
// case-sensitive rule here would be a second view of one directory that could
// drift from the first.
//
// A directory that cannot be walked raises nothing: there is nothing
// trustworthy to report, and the verbs that call this read it again at once.
func ReportUncovered(ctx context.Context, out io.Writer, fsys fs.FS) {
	uncovered, err := uncoveredFiles(fsys)
	if err != nil {
		return
	}
	for _, name := range uncovered {
		line := name + " is not covered by atlas.sum and will not run; Atlas migrations are top-level files named *.sql"
		_ = rundiag.Warn(ctx, out, rundiag.UncoveredMigrationFiles,
			rundiag.Record{Key: name, Line: line, Attrs: []slog.Attr{slog.String("file", name)}})
	}
}

// ReportUncoveredIn is [ReportUncovered] for fsys read in format. It raises
// the files only when atlas.sum governs the directory, which is when a run
// narrows what it executes to the files atlas.sum covers; see
// [migratesum.AtlasGoverns]. A directory atlas.sum does not govern runs its
// nested files, so none of them is left out.
//
// [GateWithPolicy] calls it for every directory it lets through. A verb that
// reports on a directory without gating it, such as `migrations status`
// without --verify-sum, calls it on its own.
func ReportUncoveredIn(ctx context.Context, out io.Writer, fsys fs.FS, format migrationfile.DirFormat) {
	if governs, err := migratesum.AtlasGoverns(fsys, format); err == nil && governs {
		ReportUncovered(ctx, out, fsys)
	}
}

// uncoveredFiles returns, in walk order, the SQL files fsys holds that the
// Atlas file selection leaves out: anything below the top level, and any
// top-level name whose extension is spelled other than ".sql".
func uncoveredFiles(fsys fs.FS) ([]string, error) {
	names, err := atlasmigrateimport.SumFileNames(fsys, atlasmigrateimport.FormatAtlas)
	if err != nil {
		return nil, err
	}
	covered := make(map[string]struct{}, len(names))
	for _, name := range names {
		covered[name] = struct{}{}
	}

	var uncovered []string
	err = fs.WalkDir(fsys, ".", func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.EqualFold(path.Ext(p), ".sql") {
			return nil
		}
		if _, ok := covered[p]; ok {
			return nil
		}
		uncovered = append(uncovered, p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return uncovered, nil
}
