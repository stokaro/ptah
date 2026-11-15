package atlasschema_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/sqlitevirtual"
)

// A comparison settles what its exclude selectors matched once both sides are
// scoped, before the dev database root is set and before the comparison's own
// refusals run. So a selector that matched nothing is reported, or refused,
// ahead of a later failure, and the operator learns about it on a run that
// fails for another reason too. The fixture's later failure is the SQLite
// virtual table on the --from side, which the comparison refuses to drop.
//
// These tests call [atlasschema.Diff], the comparison itself, and say nothing
// about a command. The warning is the order `ptah schema diff` reports in, and
// the refusal the order `ptah migrations down --plan` refuses in; each command
// is measured in its own package, in internal/cli/schema and
// internal/cli/migratedown.

// unmatchedExcludeOrderSides writes the two sides: --from holds a virtual
// table that --to does not declare.
func unmatchedExcludeOrderSides(c *qt.C) (from, to string) {
	c.Helper()
	from = virtualToggleFixture(c, c.TempDir(), "from.db",
		`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
		`CREATE VIRTUAL TABLE docs USING fts5(title, body)`,
	)
	to = virtualToggleFixture(c, c.TempDir(), "to.db",
		`CREATE TABLE users (id INTEGER PRIMARY KEY)`,
	)
	return "sqlite://" + from, "sqlite://" + to
}

// TestDiffWarnsAboutAnUnmatchedExcludeBeforeAComparisonRefusal_FailurePath
// warns, then fails at the virtual table.
func TestDiffWarnsAboutAnUnmatchedExcludeBeforeAComparisonRefusal_FailurePath(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(sqlitevirtual.AllowDropEnvVar)(t)
	from, to := unmatchedExcludeOrderSides(c)
	var diagnostics bytes.Buffer

	_, err := atlasschema.Diff(c.Context(), atlasschema.DiffOptions{
		FromURLs:    []string{from},
		ToURLs:      []string{to},
		Exclude:     []string{"nosuch_object"},
		Diagnostics: &diagnostics,
		Runtime:     must.Must(builtin.New()),
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `unsupported feature: the database has virtual table "docs" .*`)
	c.Assert(diagnostics.String(), qt.Equals, "Warning: the --exclude selection matched no objects: \"nosuch_object\".\n")
}

// TestDiffRefusesAnUnmatchedExcludeBeforeAComparisonRefusal_FailurePath asks
// for the refusal: it comes first, and nothing is written as a warning.
func TestDiffRefusesAnUnmatchedExcludeBeforeAComparisonRefusal_FailurePath(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(sqlitevirtual.AllowDropEnvVar)(t)
	from, to := unmatchedExcludeOrderSides(c)
	var diagnostics bytes.Buffer

	_, err := atlasschema.Diff(c.Context(), atlasschema.DiffOptions{
		FromURLs:               []string{from},
		ToURLs:                 []string{to},
		Exclude:                []string{"nosuch_object"},
		RefuseUnmatchedExclude: true,
		Diagnostics:            &diagnostics,
		Runtime:                must.Must(builtin.New()),
	})

	c.Assert(err, qt.ErrorIs, atlasfilter.ErrUnmatchedExclude)
	c.Assert(err, qt.ErrorMatches, `the --exclude selection matched no objects: "nosuch_object"`)
	c.Assert(diagnostics.String(), qt.Equals, "")
}
