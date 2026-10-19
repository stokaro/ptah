package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_DataAgreesWithTheRetiredSwitches pins each built-in
// engine's Literals and ManagedData sections, the parts of its Seeds section
// other than the tracker statement, and its Savepoint, against the answers of
// the switches migration/datadiff, internal/managedrows and migration/seeder
// kept before they read the descriptor. The rows are keyed by the constants those
// switches compared with, so a descriptor that drifts from the switch it
// replaced fails here by name.
func TestDescriptors_DataAgreesWithTheRetiredSwitches(t *testing.T) {
	type data struct {
		literals    descriptor.Literals
		managedData descriptor.ManagedData
		seeds       descriptor.Seeds
		savepoint   descriptor.Savepoint
	}
	const mysqlTables = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'"
	portableSavepoint := descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT", Release: "RELEASE SAVEPOINT"}
	postgresFamily := data{
		literals:    descriptor.Literals{BinaryPrefix: `'\x`, BinarySuffix: `'::bytea`, TimestampOffset: true},
		managedData: descriptor.ManagedData{IdentityRefusesInsert: descriptor.IdentityInsertGeneratedAlways},
		seeds:       descriptor.Seeds{Transactional: true},
		savepoint:   portableSavepoint,
	}
	mysqlFamily := data{
		literals:  descriptor.Literals{NumericBool: true, BackslashEscapes: true, BinaryPrefix: "X'", BinarySuffix: "'"},
		seeds:     descriptor.Seeds{Transactional: true, TablesQuery: mysqlTables},
		savepoint: portableSavepoint,
	}
	want := map[string]data{
		platform.Postgres:    postgresFamily,
		platform.CockroachDB: postgresFamily,
		platform.YugabyteDB:  postgresFamily,
		platform.Spanner:     postgresFamily,
		platform.MySQL:       mysqlFamily,
		platform.MariaDB:     mysqlFamily,
		platform.SQLite: {
			literals:  descriptor.Literals{BinaryPrefix: "X'", BinarySuffix: "'", TimestampOffset: true},
			seeds:     descriptor.Seeds{Transactional: true},
			savepoint: portableSavepoint,
		},
		platform.ClickHouse: {
			literals:    descriptor.Literals{NumericBool: true, BackslashEscapes: true, BinaryPrefix: "X'", BinarySuffix: "'"},
			managedData: descriptor.ManagedData{AlterTableUpdate: true},
			seeds:       descriptor.Seeds{TablesQuery: "SELECT name FROM system.tables WHERE database = currentDatabase() AND is_temporary = 0"},
		},
		platform.SQLServer: {
			literals:    descriptor.Literals{NumericBool: true, BinaryPrefix: "0x"},
			managedData: descriptor.ManagedData{IdentityRefusesInsert: descriptor.IdentityInsertAutoIncrement},
			seeds:       descriptor.Seeds{Transactional: true},
			savepoint:   descriptor.Savepoint{Create: "SAVE TRANSACTION", Rollback: "ROLLBACK TRANSACTION"},
		},
		platform.Oracle: {
			literals: descriptor.Literals{
				NumericBool: true, BinaryPrefix: "HEXTORAW('", BinarySuffix: "')",
				TimestampPrefix: "TIMESTAMP ", TimestampOffset: true, ByOwner: true,
			},
			managedData: descriptor.ManagedData{IdentityRefusesInsert: descriptor.IdentityInsertGeneratedAlways},
			seeds:       descriptor.Seeds{Transactional: true},
			savepoint:   descriptor.Savepoint{Create: "SAVEPOINT", Rollback: "ROLLBACK TO SAVEPOINT"},
		},
		platform.YDB: {
			literals:    descriptor.Literals{ByOwner: true},
			managedData: descriptor.ManagedData{TypedRows: true},
			seeds:       descriptor.Seeds{Transactional: true, ByOwner: true},
		},
	}

	described := builtindialects.Descriptors()

	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(want))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			expected, found := want[dialect.Name]
			c.Assert(found, qt.IsTrue)
			seeds := dialect.Seeds
			seeds.Tracker = ""
			c.Assert(dialect.Literals, qt.DeepEquals, expected.literals)
			c.Assert(dialect.ManagedData, qt.DeepEquals, expected.managedData)
			c.Assert(seeds, qt.DeepEquals, expected.seeds)
			c.Assert(dialect.Savepoint, qt.DeepEquals, expected.savepoint)
		})
	}
}

// TestDescriptors_TrackerStatements pins which engines create the seed
// tracker with a statement of their own, and what makes each one theirs. The
// rest take the portable CREATE TABLE IF NOT EXISTS, which migration/seeder
// holds; its white-box tests pin the SQL Server and Oracle statements whole.
func TestDescriptors_TrackerStatements(t *testing.T) {
	want := map[string]string{
		platform.ClickHouse: "ENGINE = MergeTree ORDER BY seed_path",
		platform.SQLServer:  "IF OBJECT_ID(N'schema_seeds', N'U') IS NULL",
		platform.Spanner:    "applied_at TIMESTAMPTZ NOT NULL",
		platform.Oracle:     "IF SQLCODE != -955 THEN",
		platform.YDB:        "PRIMARY KEY (seed_path)",
	}
	for _, dialect := range builtindialects.Descriptors() {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dialect.Seeds.Tracker, qt.Contains, want[dialect.Name])
			c.Assert(dialect.Seeds.Tracker == "", qt.Equals, want[dialect.Name] == "")
		})
	}
}
