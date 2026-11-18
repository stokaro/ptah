package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestColumnsWithDialect_ComparesAPostgreSQLColumnCollation pins when a
// declared collation is the one the catalog reports.
//
// A read fills Collate only for a collation the column declares, and
// EffectiveCollate with the one it compares with, empty for the database
// default. So a declaration restating what the column takes from its type
// holds: "C" on a column of type name, "default" on text. A collation of
// pg_catalog is read without its schema, and one of another schema with it.
func TestColumnsWithDialect_ComparesAPostgreSQLColumnCollation(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		read     catalog.Column
		want     string
	}{
		{name: "the collation the column declares", declared: "C",
			read: catalog.Column{Collate: "C", EffectiveCollate: "C"}, want: ""},
		{name: "a collation the column does not declare", declared: "C",
			read: catalog.Column{}, want: " -> C"},
		{name: "a collation the declaration leaves out", declared: "",
			read: catalog.Column{Collate: "C", EffectiveCollate: "C"}, want: "C -> "},
		{name: "neither names one", declared: "", read: catalog.Column{}, want: ""},
		{name: "another collation", declared: "POSIX",
			read: catalog.Column{Collate: "C", EffectiveCollate: "C"}, want: "C -> POSIX"},
		{name: "default restated on text", declared: "default", read: catalog.Column{}, want: ""},
		{name: "C restated on a column of type name", declared: "C",
			read: catalog.Column{EffectiveCollate: "C"}, want: ""},
		{name: "default on a column that takes C from its domain", declared: "default",
			read: catalog.Column{EffectiveCollate: "C"}, want: " -> default"},
		{name: "pg_catalog named", declared: `"pg_catalog"."C"`,
			read: catalog.Column{Collate: "C", EffectiveCollate: "C"}, want: ""},
		{name: "a schema's mixed-case collation", declared: `"app"."MixedCase"`,
			read: catalog.Column{Collate: `"app"."MixedCase"`, EffectiveCollate: "MixedCase"}, want: ""},
		{name: "a schema's collation declared without its schema", declared: "MixedCase",
			read: catalog.Column{Collate: `"app"."MixedCase"`, EffectiveCollate: "MixedCase"}, want: ""},
		{name: "the collation of another schema", declared: `"app"."MixedCase"`,
			read: catalog.Column{Collate: `"other"."MixedCase"`, EffectiveCollate: "MixedCase"},
			want: `"other"."MixedCase" -> "app"."MixedCase"`},
		{name: "a name in another case", declared: "c",
			read: catalog.Column{Collate: "C", EffectiveCollate: "C"}, want: "C -> c"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			read := test.read
			read.Name, read.DataType, read.IsNullable = "c", "text", "YES"

			diff := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "c", Type: "text", Nullable: true, Collate: test.declared}, read, "postgres")

			c.Assert(diff.Changes["collation"], qt.Equals, test.want)
		})
	}
}

// TestColumnsWithDialect_LeavesAColumnCollationAloneElsewhere is the control:
// an engine whose descriptor does not state Columns.Collations is not
// compared, since its renderer writes no COLLATE or its reader has not been
// measured.
func TestColumnsWithDialect_LeavesAColumnCollationAloneElsewhere(t *testing.T) {
	for _, dialect := range []string{"cockroachdb", "yugabytedb", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "c", Type: "text", Nullable: true, Collate: "C"},
				catalog.Column{Name: "c", DataType: "text", IsNullable: "YES"},
				dialect,
			)

			_, changed := diff.Changes["collation"]
			c.Assert(changed, qt.IsFalse)
		})
	}
}

// TestColumnsWithDialect_ComparesASQLiteColumnCollation pins SQLite's names:
// one identifier, found in any case, and BINARY for a column that declares
// none. A read reports no collation for a column that declares BINARY, so a
// declaration of BINARY holds against it.
func TestColumnsWithDialect_ComparesASQLiteColumnCollation(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		read     string
		want     string
	}{
		{name: "the collation the column declares", declared: "NOCASE", read: "NOCASE", want: ""},
		{name: "a name in another case", declared: "nocase", read: "NOCASE", want: ""},
		{name: "a collation the column does not declare", declared: "NOCASE", read: "", want: " -> NOCASE"},
		{name: "a collation the declaration leaves out", declared: "", read: "RTRIM", want: "RTRIM -> "},
		{name: "another collation", declared: "RTRIM", read: "NOCASE", want: "NOCASE -> RTRIM"},
		{name: "BINARY restated", declared: "binary", read: "", want: ""},
		{name: "BINARY against a declared collation", declared: "BINARY", read: "NOCASE", want: "NOCASE -> BINARY"},
		{name: "neither names one", declared: "", read: "", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "c", Type: "TEXT", Nullable: true, Collate: test.declared},
				catalog.Column{Name: "c", DataType: "TEXT", IsNullable: "YES", Collate: test.read, EffectiveCollate: test.read},
				"sqlite",
			)

			c.Assert(diff.Changes["collation"], qt.Equals, test.want)
		})
	}
}

// TestColumnsWithDialect_ComparesAMySQLColumnCollationWhereDeclared pins the
// MySQL family: a collation the column declares is compared with the one the
// column holds, which a read reports as EffectiveCollate whether the column
// declared it or took it from its table, and a column that declares none is
// not compared.
func TestColumnsWithDialect_ComparesAMySQLColumnCollationWhereDeclared(t *testing.T) {
	tests := []struct {
		name      string
		declared  string
		read      string
		effective string
		want      string
	}{
		{name: "the collation the column declares", declared: "utf8mb4_bin", read: "utf8mb4_bin", effective: "utf8mb4_bin", want: ""},
		{name: "a name in another case", declared: "UTF8MB4_BIN", read: "utf8mb4_bin", effective: "utf8mb4_bin", want: ""},
		{name: "the table's default restated", declared: "utf8mb4_unicode_ci", read: "", effective: "utf8mb4_unicode_ci", want: ""},
		{
			name: "a collation other than the table's default", declared: "utf8mb4_bin", read: "", effective: "utf8mb4_unicode_ci",
			want: "utf8mb4_unicode_ci -> utf8mb4_bin",
		},
		{name: "another collation", declared: "utf8mb4_unicode_ci", read: "utf8mb4_bin", effective: "utf8mb4_bin", want: "utf8mb4_bin -> utf8mb4_unicode_ci"},
		{name: "a collation the declaration leaves out", declared: "", read: "utf8mb4_bin", effective: "utf8mb4_bin", want: ""},
		{name: "a declaration read back without an effective collation", declared: "utf8mb4_bin", read: "utf8mb4_bin", effective: "", want: ""},
		{name: "a column that holds none", declared: "utf8mb4_bin", read: "", effective: "", want: " -> utf8mb4_bin"},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				diff := compare.ColumnsWithDialect(
					schemamodel.Field{Name: "c", Type: "varchar(20)", Nullable: true, Collate: test.declared},
					catalog.Column{
						Name: "c", DataType: "varchar", ColumnType: "varchar(20)", IsNullable: "YES",
						Collate: test.read, EffectiveCollate: test.effective,
					},
					dialect,
				)

				c.Assert(diff.Changes["collation"], qt.Equals, test.want)
			})
		}
	}
}
