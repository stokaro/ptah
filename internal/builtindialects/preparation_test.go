package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_PreparationAgreesWithTheRetiredSwitches pins each built-in
// engine's Preparation section against the answers of the switches
// internal/schemaprep kept before it read the descriptor: how an enum column
// is built, which declared types are rewritten, where a foreign key name is
// unique and how it compares, which engine
// folds equal index constraints, and which reads a bare name as a PostgreSQL
// built-in type. The rows are keyed by the constants those switches compared
// with, so a descriptor that drifts from the switch it replaced fails here by
// name.
func TestDescriptors_PreparationAgreesWithTheRetiredSwitches(t *testing.T) {
	mysqlFamily := descriptor.Preparation{
		Enums:                   descriptor.EnumColumnType,
		SerialAutoIncrement:     true,
		ForeignKeyNamespace:     descriptor.NamespaceDatabase,
		ForeignKeyNamesFoldCase: true,
	}
	want := map[string]descriptor.Preparation{
		platform.Postgres: {
			FoldsIndexConstraints: true,
			PostgresBuiltInTypes:  true,
		},
		platform.CockroachDB: {PostgresBuiltInTypes: true},
		platform.YugabyteDB:  {PostgresBuiltInTypes: true},
		platform.Spanner: {
			Enums:                   descriptor.EnumCheck,
			EnumCheckType:           "TEXT",
			ForeignKeyNamespace:     descriptor.NamespaceSchema,
			ForeignKeyNamesFoldCase: true,
			PostgresBuiltInTypes:    true,
		},
		platform.MySQL:      mysqlFamily,
		platform.MariaDB:    mysqlFamily,
		platform.ClickHouse: {Enums: descriptor.EnumNumbered},
		platform.SQLite:     {Enums: descriptor.EnumCheck, EnumCheckType: "TEXT"},
		platform.SQLServer: {
			Enums:                   descriptor.EnumCheck,
			EnumCheckType:           "NVARCHAR(255)",
			EnumCheckQuotesColumn:   true,
			SerialAutoIncrement:     true,
			TypeSubstitutions:       map[string]string{"TEXT": "NVARCHAR(MAX)", "VARCHAR": "NVARCHAR(MAX)"},
			ForeignKeyNamespace:     descriptor.NamespaceSchema,
			ForeignKeyNamesFoldCase: true,
		},
		platform.Oracle: {Enums: descriptor.EnumCheck, EnumCheckType: "VARCHAR2(255)", EnumCheckQuotesColumn: true},
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
			c.Assert(dialect.Preparation, qt.DeepEquals, expected)
		})
	}
}

// TestDescriptors_IdentifierLimitAgreesWithTheForeignKeyNames pins the
// identifier limits internal/schemaprep now reads to name a foreign key
// against the limits it wrote out before: 63 bytes on the PostgreSQL family
// but Spanner, 128 characters on SQL Server and Spanner, 64 characters on
// MySQL and MariaDB, and none elsewhere. PostgreSQL and YugabyteDB cut a
// longer name to the limit; CockroachDB keeps it whole.
func TestDescriptors_IdentifierLimitAgreesWithTheForeignKeyNames(t *testing.T) {
	bytes63 := descriptor.IdentifierLimit{Max: 63, Unit: "bytes"}
	cut63 := descriptor.IdentifierLimit{Max: 63, Unit: "bytes", Truncates: true}
	characters := func(limit int) descriptor.IdentifierLimit {
		return descriptor.IdentifierLimit{Max: limit, Unit: "characters"}
	}
	want := map[string]descriptor.IdentifierLimit{
		platform.Postgres: cut63, platform.CockroachDB: bytes63, platform.YugabyteDB: cut63,
		platform.Spanner: characters(128), platform.SQLServer: characters(128),
		platform.MySQL: characters(64), platform.MariaDB: characters(64),
		platform.ClickHouse: {}, platform.SQLite: {}, platform.Oracle: {}, platform.YDB: {},
	}
	for _, dialect := range builtindialects.Descriptors() {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dialect.IdentifierLimit, qt.Equals, want[dialect.Name])
		})
	}
}
