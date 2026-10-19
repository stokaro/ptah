package builtin_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtindialects"
	"ptah.run/internal/dburlext/builtin"
)

// The dialects below are the arms of the switches of internal/atlasurl that
// named engines, written out as they stood when these services replaced them:
// networkDatabaseIdentity read the PostgreSQL family with pgconn, SQL Server
// with msdsn, ClickHouse with clickhouse-go and YDB with internal/ydburl;
// SameDatabaseEndpoint compared SQLite by file; WithDatabaseName refused YDB.
var (
	switchEndpoints = []string{"postgres", "cockroachdb", "yugabytedb", "spanner", "sqlserver", "clickhouse", "ydb"}
	switchDatabases = []string{"sqlite"}
	switchRename    = []string{"ydb"}
)

func TestLookup_ServesTheArmsOfTheSwitchesItReplaces(t *testing.T) {
	for _, spelling := range platform.DialectSpellings() {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			dialect := platform.NormalizeDialect(spelling)

			services := builtin.Lookup(spelling)

			c.Assert(services.Endpoints != nil, qt.Equals, slices.Contains(switchEndpoints, dialect))
			c.Assert(services.Databases != nil, qt.Equals, slices.Contains(switchDatabases, dialect))
			c.Assert(services.Rename != nil, qt.Equals, slices.Contains(switchRename, dialect))
		})
	}
}

func TestLookup_FoldsSpellings(t *testing.T) {
	c := qt.New(t)
	c.Assert(builtin.Lookup(" PGX ").Dialects, qt.DeepEquals, builtin.Lookup(platform.Postgres).Dialects)
	c.Assert(builtin.Lookup("mssql").Dialects, qt.DeepEquals, []string{platform.SQLServer})
}

func TestLookup_UnknownDialectTakesTheGenericBehavior(t *testing.T) {
	for _, dialect := range []string{"", "nonesuch", platform.MySQL, platform.Oracle} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			services := builtin.Lookup(dialect)
			c.Assert(services.Dialects, qt.IsNil)
			c.Assert(services.Endpoints, qt.IsNil)
			c.Assert(services.Databases, qt.IsNil)
			c.Assert(services.Rename, qt.IsNil)
		})
	}
}

// Every dialect a service serves is the canonical name of a built-in dialect,
// so a misspelled one cannot leave its engine on the generic behavior.
func TestDialects_AreBuiltinNames(t *testing.T) {
	c := qt.New(t)
	var names []string
	for _, described := range builtindialects.Descriptors() {
		names = append(names, described.Name)
	}
	for _, dialect := range builtin.Dialects() {
		c.Assert(names, qt.Contains, dialect)
	}
	c.Assert(builtin.Dialects(), qt.HasLen, len(switchEndpoints)+len(switchDatabases))
}
