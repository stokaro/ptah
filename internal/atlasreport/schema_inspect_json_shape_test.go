package atlasreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasreport"
)

// TestSchemaInspectJSONNeverInventsTheConnectedSchema pins that the realm
// document names the schemas the READER described and never the one the
// connection happens to sit in.
//
// The distinction has a price attached. Seeding the connected schema into the
// document closes the empty-database cell of stokaro/ptah#1235 (finding 6.1) on
// its own, and it was measured reopening two shapes that already matched. All
// four rows below were run against the pinned Atlas community binary v1.3.0 on
// live PostgreSQL 17 and SQLite, each at exit 0, with `schema inspect --format
// '{{ json . }}'`:
//
//	sqlite, empty database                  {"schemas":[{"name":"main"}]}
//	postgres, empty public                  {"schemas":[{"name":"public",…}]}
//	postgres, realm URL, --schema extra     {"schemas":[{"name":"extra",…}]}
//	postgres, realm URL, --schema nosuch    {}
//
// With the connected schema seeded, the third answered a second, empty
// `{"name":"public"}` entry that binary never prints, and the fourth answered
// `{"schemas":[{"name":"public"}]}` where it answers `{}` — a schema described
// to an operator who had just been told it does not exist.
//
// So the seed is the cheaper wrong implementation of finding 6.1, and these
// rows are what refuse it. The empty-database rows are here in the same table
// on purpose: they are the cell the seed was reaching for, and they hold
// without it because the reader describes the schema itself.
func TestSchemaInspectJSONNeverInventsTheConnectedSchema(t *testing.T) {
	tests := []struct {
		name   string
		schema *types.DBSchema
		info   types.DBInfo
		want   string
	}{
		{
			name: "an empty sqlite database reports the schema its reader described",
			schema: &types.DBSchema{
				Schemas: []types.DBSchemaInfo{{Name: "main"}},
			},
			info: types.DBInfo{Dialect: "sqlite", Schema: "main"},
			want: `{"schemas":[{"name":"main"}]}`,
		},
		{
			name: "an empty postgres database reports the schema its reader described",
			schema: &types.DBSchema{
				Schemas: []types.DBSchemaInfo{{Name: "public", Comment: "standard public schema"}},
			},
			info: types.DBInfo{Dialect: "postgres", Schema: "public"},
			want: `{"schemas":[{"name":"public","comment":"standard public schema"}]}`,
		},
		{
			name: "a schema selection on a realm URL does not gain the connected schema",
			schema: &types.DBSchema{
				Schemas: []types.DBSchemaInfo{{Name: "extra"}},
				Tables: []types.DBTable{{
					Name:    "ext_t",
					Schema:  "extra",
					Columns: []types.DBColumn{{Name: "id", DataType: "integer"}},
				}},
			},
			info: types.DBInfo{Dialect: "postgres", Schema: "public"},
			want: `{"schemas":[{"name":"extra","tables":[{"name":"ext_t",` +
				`"columns":[{"name":"id","type":"integer"}]}]}]}`,
		},
		{
			name:   "a selection naming a schema that does not exist stays empty",
			schema: &types.DBSchema{},
			info:   types.DBInfo{Dialect: "postgres", Schema: "public"},
			want:   `{}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report := atlasreport.NewSchemaInspectReport(
				&goschema.Database{},
				test.schema,
				test.info,
				nil,
				atlasreport.SchemaInspectReportOptions{DescribeSchemas: true},
			)
			output, err := atlasreport.RenderSchemaInspect(`{{ json . }}`, report)

			c.Assert(err, qt.IsNil)
			c.Assert(output.Text, qt.Equals, test.want)
		})
	}
}

// TestSchemaInspectJSONReportsABackedUniqueConstraintOnce pins that a UNIQUE
// constraint and the index that backs it are one entry in `indexes`, not two.
//
// SQLite reports both halves: `pragma index_list` names the implicit
// `sqlite_autoindex_<table>_<n>` and the table's UNIQUE constraint carries that
// same name. Measured over
// `CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT UNIQUE, b TEXT UNIQUE, c TEXT)`
// plus `CREATE UNIQUE INDEX ux_t_c ON t (c)`, the pinned community binary
// v1.3.0 printed three indexes and Ptah printed five, with each autoindex
// listed twice (stokaro/ptah#1235 finding 6.2).
//
// It is not a SQLite-only fold. Measured live on PostgreSQL 17, a plain
// `email text UNIQUE` column printed `users_email_key` twice with the
// deduplication reverted in place, and once with it.
//
// The second row is the control the fix must not break: a reader that reports a
// UNIQUE constraint with no index row of its own -- which is why the constraint
// branch exists at all -- must still produce an index entry.
func TestSchemaInspectJSONReportsABackedUniqueConstraintOnce(t *testing.T) {
	tests := []struct {
		name    string
		indexes []types.DBIndex
		want    string
	}{
		{
			name: "the backing index is reported and the constraint is not repeated",
			indexes: []types.DBIndex{{
				Name:      "sqlite_autoindex_t_1",
				TableName: "t",
				Columns:   []string{"a"},
				IsUnique:  true,
			}},
			want: `{"schemas":[{"name":"main","tables":[{"name":"t",` +
				`"columns":[{"name":"a","type":"TEXT","null":true}],` +
				`"indexes":[{"name":"sqlite_autoindex_t_1","unique":true,"parts":[{"column":"a"}]}]}]}]}`,
		},
		{
			name:    "a constraint with no backing index row still becomes an index",
			indexes: nil,
			want: `{"schemas":[{"name":"main","tables":[{"name":"t",` +
				`"columns":[{"name":"a","type":"TEXT","null":true}],` +
				`"indexes":[{"name":"sqlite_autoindex_t_1","unique":true,"parts":[{"column":"a"}]}]}]}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report := atlasreport.NewSchemaInspectReport(
				&goschema.Database{},
				&types.DBSchema{
					Schemas: []types.DBSchemaInfo{{Name: "main"}},
					Tables: []types.DBTable{{
						Name:    "t",
						Columns: []types.DBColumn{{Name: "a", DataType: "TEXT", IsNullable: "YES"}},
					}},
					Indexes: test.indexes,
					Constraints: []types.DBConstraint{{
						Name:        "sqlite_autoindex_t_1",
						TableName:   "t",
						Type:        "UNIQUE",
						ColumnNames: []string{"a"},
					}},
				},
				types.DBInfo{Dialect: "sqlite", Schema: "main"},
				nil,
				atlasreport.SchemaInspectReportOptions{DescribeSchemas: true},
			)
			output, err := atlasreport.RenderSchemaInspect(`{{ json . }}`, report)

			c.Assert(err, qt.IsNil)
			c.Assert(output.Text, qt.Equals, test.want)
		})
	}
}
