package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// columnsByName lists each table's columns under the table's own name, as the
// model spells it.
func columnsByName(database schemamodel.Database) map[string][]string {
	columns := make(map[string][]string, len(database.Tables))
	for _, table := range database.Tables {
		columns[table.Name] = make([]string, 0)
		for _, field := range database.Fields {
			if field.StructName == table.StructName {
				columns[table.Name] = append(columns[table.Name], field.Name)
			}
		}
	}
	return columns
}

// TestRead_OracleQuotedNames reads a file that creates tables on Oracle,
// where an unquoted name is stored in upper case and a quoted one as written.
// A quoted name no unquoted name reaches keeps its quotes, so the file's
// `"orders"` stays the table stored as orders, apart from ORDERS, and a key
// and an index naming its quoted column land on that column. A quoted name in
// upper case reads as the bare name it equals, and PostgreSQL, which states no
// such fold, reads every quoted name without its quotes.
func TestRead_OracleQuotedNames(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    map[string][]string
		indexed []string
	}{
		{
			name:    "a quoted lower-case table beside its folded namesake",
			dialect: "oracle",
			sql: `CREATE TABLE "orders" ("id" NUMBER(10), qty NUMBER(10), CONSTRAINT "pk_orders" PRIMARY KEY ("id"));
CREATE TABLE ORDERS (ID NUMBER(10) PRIMARY KEY);
CREATE INDEX "idx_id" ON "orders" ("id");
`,
			want:    map[string][]string{`"orders"`: {`"id"`, "qty"}, "ORDERS": {"ID"}},
			indexed: []string{`"id"`},
		},
		{
			name:    "a quoted upper-case table",
			dialect: "oracle",
			sql:     `CREATE TABLE "ORDERS" ("ID" NUMBER(10) PRIMARY KEY);` + "\n",
			want:    map[string][]string{"ORDERS": {"ID"}},
		},
		{
			name:    "a dialect that states no fold",
			dialect: "postgres",
			sql:     `CREATE TABLE "Orders" ("Id" integer PRIMARY KEY);` + "\n",
			want:    map[string][]string{"Orders": {"Id"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(columnsByName(database), qt.DeepEquals, test.want)
			indexed := make([]string, 0, len(database.Indexes))
			for _, index := range database.Indexes {
				indexed = append(indexed, index.Fields...)
			}
			c.Assert(indexed, qt.DeepEquals, append(make([]string, 0), test.indexed...))
		})
	}
}
