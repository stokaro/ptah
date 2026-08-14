package atlasschema_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
)

// TestDiffResolvesTheDropToggleBeforeSelectionCanReturn covers the run that
// exits before any virtual table is ever classified.
//
// `schema diff` refuses an --include that matched neither side, and that
// refusal happens during scoping -- before the comparison the drop toggle
// belongs to. Resolving the toggle after scoping therefore left a malformed
// value unreported on exactly the runs an operator is already debugging: the
// selector answer came back and the misconfiguration stayed hidden until some
// later run got far enough to look at a virtual table.
//
// The rows without a malformed value are what keep this from becoming "every
// empty selection fails for the wrong reason": a sound toggle, and an unset
// one, must both still produce the selector's own answer.
func TestDiffResolvesTheDropToggleBeforeSelectionCanReturn(t *testing.T) {
	tests := []struct {
		name            string
		env             func(testing.TB)
		include         []string
		toURL           func(database string) string
		wantErrContains string
	}{
		{
			name:            "a malformed toggle is refused before the empty selection is",
			env:             envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe"),
			include:         []string{"nothing_matches_this"},
			toURL:           sqliteDatabaseURL,
			wantErrContains: `invalid boolean value "maybe" for ` + sqlitevirtual.AllowDropEnvVar,
		},
		{
			name:            "a sound toggle leaves the selector's own answer",
			env:             envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "false"),
			include:         []string{"nothing_matches_this"},
			toURL:           sqliteDatabaseURL,
			wantErrContains: "matched no objects",
		},
		{
			name:            "an unset toggle leaves it too",
			env:             envbooltest.Unset(sqlitevirtual.AllowDropEnvVar),
			include:         []string{"nothing_matches_this"},
			toURL:           sqliteDatabaseURL,
			wantErrContains: "matched no objects",
		},
		{
			// Source resolution runs before the comparison and is not free --
			// a migration-directory source replays into the dev database. A
			// malformed toggle must be refused before any of that, not after a
			// source error masks it.
			name:            "a malformed toggle is refused before the source is even loaded",
			env:             envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe"),
			toURL:           missingSchemaFileURL,
			wantErrContains: `invalid boolean value "maybe" for ` + sqlitevirtual.AllowDropEnvVar,
		},
		{
			name:            "a sound toggle leaves the source error",
			env:             envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "false"),
			toURL:           missingSchemaFileURL,
			wantErrContains: "schema file does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tt.env(t)

			from := virtualToggleFixture(c, t.TempDir(), "from.db",
				`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
				`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
			)
			to := virtualToggleFixture(c, t.TempDir(), "to.db",
				`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
			)

			_, err := atlasschema.Diff(context.Background(), atlasschema.DiffOptions{
				FromURLs: []string{"sqlite://" + from},
				ToURLs:   []string{tt.toURL(to)},
				Include:  tt.include,
			})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, tt.wantErrContains)
		})
	}
}

// Each row names the --to source it stands behind, which is what selects the
// early return the toggle has to beat: scoping refuses the empty selection,
// source loading refuses the missing file, and both run before the comparison.
func sqliteDatabaseURL(database string) string {
	return "sqlite://" + database
}

func missingSchemaFileURL(database string) string {
	return "file://" + database + ".missing.sql"
}

func virtualToggleFixture(c *qt.C, dir, name string, statements ...string) string {
	c.Helper()

	path := filepath.Join(dir, name)
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { _ = db.Close() }()

	for _, statement := range statements {
		_, err := db.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil)
	}
	return path
}
