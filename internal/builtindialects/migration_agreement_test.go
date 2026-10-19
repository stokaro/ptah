package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_MigrationAgreesWithTheRetiredSwitches pins every built-in
// descriptor's Migration section against the switches it replaces, written
// out here as they stood:
//
//   - internal/ptahdirective read the directives of a file without a dialect
//     under the lexers of PostgreSQL, MySQL, MariaDB, SQLite, ClickHouse,
//     CockroachDB, YugabyteDB, SQL Server and Spanner. YDB was left out on
//     purpose. Oracle was left out because the list predates it, and the
//     shared lexer reads its text under SQLite's options and not its q'...'
//     strings, so it could add nothing.
//   - internal/migrationreplay captured and restored SQLite's
//     PRAGMA foreign_keys around a replay, and no setting of any other engine.
//   - internal/txrequire sent a generated ALTER TYPE ... ADD VALUE out of the
//     transactional file on the PostgreSQL family.
//
// One difference is deliberate: Spanner's PostgreSQL interface has no enum
// type, so its descriptor states no enum value rule, which the descriptor
// validation refuses for an engine without enum types. No plan for Spanner
// holds an ALTER TYPE, since an enum column is TEXT with a CHECK there.
func TestDescriptors_MigrationAgreesWithTheRetiredSwitches(t *testing.T) {
	sqliteForeignKeys := descriptor.SessionSetting{
		Name:  "SQLite foreign key state",
		Query: "PRAGMA foreign_keys",
		On:    "PRAGMA foreign_keys = ON",
		Off:   "PRAGMA foreign_keys = OFF",
	}
	postgresFamily := descriptor.Migration{DirectivesWithoutDialect: true, EnumValuesCommitFirst: true}
	reader := descriptor.Migration{DirectivesWithoutDialect: true}
	want := map[string]descriptor.Migration{
		platform.Postgres:    postgresFamily,
		platform.CockroachDB: postgresFamily,
		platform.YugabyteDB:  postgresFamily,
		platform.Spanner:     reader,
		platform.MySQL:       reader,
		platform.MariaDB:     reader,
		platform.ClickHouse:  reader,
		platform.SQLServer:   reader,
		platform.SQLite: {
			DirectivesWithoutDialect: true,
			ReplayedSession:          []descriptor.SessionSetting{sqliteForeignKeys},
		},
		platform.Oracle: {},
		platform.YDB:    {},
	}
	described := builtindialects.Descriptors()
	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(want))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			expected, found := want[dialect.Name]
			c.Assert(found, qt.IsTrue)
			c.Assert(dialect.Migration, qt.CmpEquals(cmpopts.EquateEmpty()), expected)
		})
	}
}

// TestDescriptors_DirectivesWithoutDialectInRegistryOrder pins the order a
// read of directives without a dialect asks the engines in: the registry's.
// The answer does not depend on it, since every reader has to agree, but the
// first reader is the one whose markers the others are checked against.
func TestDescriptors_DirectivesWithoutDialectInRegistryOrder(t *testing.T) {
	c := qt.New(t)
	var readers []string
	for _, dialect := range builtindialects.Descriptors() {
		if dialect.Migration.DirectivesWithoutDialect {
			readers = append(readers, dialect.Name)
		}
	}
	c.Assert(readers, qt.DeepEquals, []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.ClickHouse, platform.SQLite,
		platform.SQLServer, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	})
}
