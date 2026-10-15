package builtindialects_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_ConversionAgreesWithTheRetiredSwitches pins each built-in
// engine's Conversion section against the answers of the switches the schema
// readers and converters kept before they read the descriptor: the MySQL
// family's property shortcuts, INT foreign keys, row format and constraint
// comments; ClickHouse's owned row policies; the PostgreSQL family's owned
// sequences, implicit routine grants, index methods, row-level security, PUBLIC
// policies and identity imports; and YDB's database-root paths and index
// methods. The rows are keyed by the constants those switches compared with,
// so a descriptor that drifts from the switch it replaced fails here by name.
func TestDescriptors_ConversionAgreesWithTheRetiredSwitches(t *testing.T) {
	postgresFamily := descriptor.Conversion{
		OwnedSequences:            true,
		ImplicitRoutineGrants:     true,
		IndexTypeIsMethod:         true,
		RowLevelSecurity:          true,
		EmptyPolicyRolesArePublic: true,
		AutoIncrementIdentity:     true,
	}
	mysqlFamily := descriptor.Conversion{
		PropertyShortcuts:    true,
		SerialForeignKeyType: "INT",
		RowFormatProperty:    true,
		ConstraintComments:   true,
	}
	want := map[string]descriptor.Conversion{
		platform.Postgres:    postgresFamily,
		platform.CockroachDB: postgresFamily,
		platform.YugabyteDB:  postgresFamily,
		platform.Spanner:     postgresFamily,
		platform.MySQL:       mysqlFamily,
		platform.MariaDB:     mysqlFamily,
		platform.ClickHouse:  {RowPolicyOwned: true},
		platform.YDB:         {DatabaseRootPaths: true, IndexTypeIsMethod: true},
		platform.SQLite:      {},
		platform.SQLServer:   {},
		platform.Oracle:      {},
	}

	described := builtindialects.Descriptors()

	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(want))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			expected, found := want[dialect.Name]
			c.Assert(found, qt.IsTrue)
			c.Assert(dialect.Conversion, qt.DeepEquals, expected)
		})
	}
}

// TestDescriptors_RowLevelSecurityStartsWithPostgres pins the order the
// row-level security reader takes its identifier rules from: the first
// registered dialect that holds the model is PostgreSQL, whose default
// schema is public.
func TestDescriptors_RowLevelSecurityStartsWithPostgres(t *testing.T) {
	c := qt.New(t)
	described := builtindialects.Descriptors()

	first := slices.IndexFunc(described, func(dialect descriptor.Dialect) bool {
		return dialect.Conversion.RowLevelSecurity
	})

	c.Assert(first, qt.Not(qt.Equals), -1)
	c.Assert(described[first].Name, qt.Equals, platform.Postgres)
}
