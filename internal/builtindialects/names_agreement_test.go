package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_SchemaFileFactsAgreeWithTheSwitchesTheyReplace pins the
// facts the SQL reader reads, internal/sqlschema, against the switches it
// chose by before the descriptors carried them, written out here as they
// stood:
//
//   - identifierPart folded an unquoted name's ASCII letters on PostgreSQL
//     and YugabyteDB and every letter on CockroachDB; roleName folded every
//     role name on CockroachDB; resolveDeclaredName fell back to an ASCII
//     fold on SQLite and Oracle and to a Unicode fold on SQL Server, MySQL
//     and MariaDB; normalizeGrantee read PUBLIC through roleName on the
//     PostgreSQL family.
//   - namingFor named indexes on MySQL (64 bytes, `functional_index`) and
//     MariaDB (64 bytes, a descending key covers a foreign key); DROP INDEX
//     and MODIFY read a UNIQUE as an index on both; primaryKeyIndexName named
//     an unnamed primary key `<table>_pkey` on the PostgreSQL family.
//   - the CHECK naming was MySQL's `<table>_chk_<n>` and MariaDB's column
//     or `CONSTRAINT_<n>`; namesConstraintsLikePostgres was PostgreSQL alone.
//   - an identity column was NOT NULL on PostgreSQL alone, a column CHECK
//     naming another column was refused on MySQL alone, and MODIFY kept the
//     rest of the column on SQL Server alone.
//   - a repeated CREATE ROLE was refused on PostgreSQL, a DO block declaring
//     roles was read as PL/pgSQL on PostgreSQL and on a read with no
//     dialect, and a view kept a table from a plain DROP TABLE on the
//     PostgreSQL family.
//   - STRICT and WITHOUT ROWID set SQLite's strict and without_rowid
//     properties.
func TestDescriptors_SchemaFileFactsAgreeWithTheSwitchesTheyReplace(t *testing.T) {
	pgFamily := descriptor.IndexNaming{PrimaryKeySuffix: "pkey"}
	tests := []struct {
		dialect          string
		names            descriptor.Names
		indexNaming      descriptor.IndexNaming
		checkNaming      descriptor.CheckNaming
		constraintNaming descriptor.ConstraintNaming
		columns          descriptor.Columns
		statements       descriptor.Statements
		tableOptions     []descriptor.TableOption
	}{
		{
			dialect:          platform.Postgres,
			names:            descriptor.Names{Unquoted: "ascii_case_insensitive", PublicKeyword: true},
			indexNaming:      pgFamily,
			constraintNaming: descriptor.ConstraintNamingTableColumnsSuffix,
			columns:          descriptor.Columns{IdentityNotNull: true},
			statements:       descriptor.Statements{ViewsBlockDropTable: true, CreateRoleOnce: true, DoBlocks: "plpgsql"},
		},
		{
			dialect:     platform.MySQL,
			names:       descriptor.Names{Lookup: "unicode_case_insensitive"},
			indexNaming: descriptor.IndexNaming{MaxBytes: 64, FunctionalBase: "functional_index", UniqueIsIndex: true},
			checkNaming: descriptor.CheckNamingTableNumbered,
			columns:     descriptor.Columns{CheckOwnColumnOnly: true},
		},
		{
			dialect:     platform.MariaDB,
			names:       descriptor.Names{Lookup: "unicode_case_insensitive"},
			indexNaming: descriptor.IndexNaming{MaxBytes: 64, DescendingCovers: true, UniqueIsIndex: true},
			checkNaming: descriptor.CheckNamingColumnOrConstraint,
		},
		{dialect: platform.ClickHouse},
		{
			dialect: platform.SQLite,
			names:   descriptor.Names{Lookup: "ascii_case_insensitive"},
			tableOptions: []descriptor.TableOption{
				{Option: "STRICT", Property: "strict"},
				{Option: "WITHOUT_ROWID", Property: "without_rowid"},
			},
		},
		{
			dialect: platform.SQLServer,
			names:   descriptor.Names{Lookup: "unicode_case_insensitive"},
			columns: descriptor.Columns{AlterColumnKeepsRest: true},
		},
		{
			dialect: platform.CockroachDB,
			names: descriptor.Names{
				Unquoted: "unicode_case_insensitive", Roles: "unicode_case_insensitive", PublicKeyword: true,
			},
			indexNaming: pgFamily,
			statements:  descriptor.Statements{ViewsBlockDropTable: true},
		},
		{
			dialect:     platform.YugabyteDB,
			names:       descriptor.Names{Unquoted: "ascii_case_insensitive", PublicKeyword: true},
			indexNaming: pgFamily,
			statements:  descriptor.Statements{ViewsBlockDropTable: true},
		},
		{
			dialect:     platform.Spanner,
			names:       descriptor.Names{PublicKeyword: true},
			indexNaming: pgFamily,
			statements:  descriptor.Statements{ViewsBlockDropTable: true},
		},
		{dialect: platform.Oracle, names: descriptor.Names{Lookup: "ascii_case_insensitive"}},
		{dialect: platform.YDB},
	}
	c := qt.New(t)
	// Every built-in dialect has a row, so a new one cannot go unpinned.
	c.Assert(builtindialects.Descriptors(), qt.HasLen, len(tests))
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			got, found := platform.Lookup(test.dialect)

			c.Assert(found, qt.IsTrue)
			c.Assert(got.Names, qt.DeepEquals, test.names)
			c.Assert(got.IndexNaming, qt.DeepEquals, test.indexNaming)
			c.Assert(got.CheckNaming, qt.Equals, test.checkNaming)
			c.Assert(got.ConstraintNaming, qt.Equals, test.constraintNaming)
			c.Assert(got.Columns, qt.DeepEquals, test.columns)
			c.Assert(got.Statements, qt.DeepEquals, test.statements)
			c.Assert(got.TableOptions, qt.DeepEquals, test.tableOptions)
		})
	}
}
