package lintdialect_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/lintdialect"
)

// servedEngines registers the provider engines duckdb, with the alias duck,
// and otherdb, beside the built-in oracle, as a runtime that selected two
// providers holds them.
type servedEngines struct{}

func (servedEngines) ResolveTarget(name string) (schemaext.TargetSelection, error) {
	selections := map[string][]string{"duckdb": {"duckdb", "duck"}, "duck": {"duckdb", "duck"}, "otherdb": {"otherdb"}, "oracle": {"oracle"}}
	names, found := selections[name]
	if !found {
		return schemaext.TargetSelection{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, name)
	}
	return schemaext.NewTargetSelection(names[0], names[1:]...)
}

// A name the runtime serves that is not a built-in dialect is a lint dialect,
// and resolves to the target the runtime selects for it; a built-in name
// resolves as Canonical resolves it.
func TestCanonicalIn_HappyPath(t *testing.T) {
	for _, row := range []struct{ dialect, want string }{
		{"duckdb", "duckdb"}, {" DuckDB ", "duckdb"}, {"duck", "duckdb"}, {"otherdb", "otherdb"},
		{"pgx", "postgres"}, {"", ""},
	} {
		t.Run(row.dialect, func(t *testing.T) {
			c := qt.New(t)

			canonical, ok := lintdialect.CanonicalIn(row.dialect, servedEngines{})

			c.Assert(ok, qt.IsTrue)
			c.Assert(canonical, qt.Equals, row.want)
		})
	}
}

// A name the runtime does not serve, a URL, and a built-in dialect lint does
// not analyze are refused, the last even though the runtime registers it; with
// no runtime, a provider engine is refused as Canonical refuses it.
func TestCanonicalIn_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name, dialect string
		served        schemaext.TargetResolver
	}{
		{"an engine the runtime does not serve", "nosuch", servedEngines{}},
		{"a URL", "duckdb:///tmp/app.db", servedEngines{}},
		{"a built-in dialect lint does not analyze", "oracle", servedEngines{}},
		{"no runtime", "duckdb", nil},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			canonical, ok := lintdialect.CanonicalIn(row.dialect, row.served)

			c.Assert(ok, qt.IsFalse)
			c.Assert(canonical, qt.Equals, "")
		})
	}
}

// An engine the runtime serves is a family of its own: a policy that names it
// governs that engine under any of its names, and no other engine, built-in or
// served. An empty side constrains nothing.
func TestCompatibleIn(t *testing.T) {
	for _, row := range []struct {
		policy, database string
		want             bool
	}{
		{"duckdb", "duckdb", true},
		{"duck", "duckdb", true},
		{"", "duckdb", true},
		{"duckdb", "", true},
		{"duckdb", "otherdb", false},
		{"duckdb", "postgres", false},
		{"postgres", "duckdb", false},
		{"postgres", "cockroachdb", true},
		{"duckdb", "nosuch", false},
	} {
		t.Run(row.policy+"/"+row.database, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintdialect.CompatibleIn(row.policy, row.database, servedEngines{}), qt.Equals, row.want)
		})
	}
}
