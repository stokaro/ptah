package schemafile_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// These tests cover how a SQL schema file names a table and the objects on it
// (stokaro/ptah#3592). Every statement that names the table has to land on one
// name: the table was read as written and the row-level security statements
// folded, so the render created "Docs" and enabled row-level security on
// "docs", which PostgreSQL refused.

// tableCaseSchema declares a table and every kind of statement that names it
// again, each spelling the table, its columns and the index the way the
// argument does.
func tableCaseSchema(table, column string) string {
	return strings.NewReplacer("TABLE_NAME", table, "COLUMN_NAME", column).Replace(`CREATE TABLE TABLE_NAME (Id integer PRIMARY KEY, COLUMN_NAME text NOT NULL, CONSTRAINT Docs_Unique UNIQUE (COLUMN_NAME));
CREATE INDEX Docs_Lookup ON TABLE_NAME (COLUMN_NAME);
ALTER TABLE TABLE_NAME ADD COLUMN Extra text;
ALTER TABLE TABLE_NAME ADD CONSTRAINT Docs_Extra CHECK (Extra <> '');
GRANT SELECT (COLUMN_NAME) ON TABLE_NAME TO PUBLIC;
ALTER TABLE TABLE_NAME ENABLE ROW LEVEL SECURITY;
CREATE POLICY Docs_Read ON TABLE_NAME USING (true);
`)
}

// relationNames is every place the model records the table or a name on it, in
// a fixed order, so a test sees at once whether the file names one table.
type relationNames struct {
	Table, Columns, Constraints, Index, IndexTable, IndexFields, Grant, GrantColumns, Enabled, Policy, PolicyTable string
}

func recordedTableNames(c *qt.C, db *schemamodel.Database) relationNames {
	c.Helper()
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Grants, qt.HasLen, 1)
	c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(db.RLSPolicies, qt.HasLen, 1)
	columns := make([]string, 0, len(db.Fields))
	for _, field := range db.Fields {
		columns = append(columns, field.Name)
	}
	constraints := make([]string, 0, len(db.Constraints))
	for _, constraint := range db.Constraints {
		constraints = append(constraints, constraint.Name+" on "+constraint.Table)
	}
	return relationNames{
		Table:        db.Tables[0].Name,
		Columns:      strings.Join(columns, ", "),
		Constraints:  strings.Join(constraints, ", "),
		Index:        db.Indexes[0].Name,
		IndexTable:   db.Indexes[0].TableName,
		IndexFields:  strings.Join(db.Indexes[0].Fields, ", "),
		Grant:        db.Grants[0].OnTable,
		GrantColumns: strings.Join(db.Grants[0].Columns, ", "),
		Enabled:      db.RLSEnabledTables[0].Table,
		Policy:       db.RLSPolicies[0].Name,
		PolicyTable:  db.RLSPolicies[0].Table,
	}
}

// TestLoadAll_TableNamesFoldLikeTheServer reads the same table under each
// dialect that folds an unquoted name. The expected names are what each
// engine's pg_class and pg_attribute reported for the same CREATE TABLE:
// PostgreSQL 18.6 and YugabyteDB 2026.1 lower ASCII letters, CockroachDB
// 26.3.2 lowers every letter, and a quoted name keeps its case on all three.
func TestLoadAll_TableNamesFoldLikeTheServer(t *testing.T) {
	tests := []struct {
		dialect string
		name    string
		table   string
		column  string
		want    relationNames
	}{
		{
			dialect: platform.Postgres, name: "unquoted mixed case folds", table: "Docs", column: "Title",
			want: relationNames{
				Table: "docs", Columns: "id, title, extra", Constraints: "docs_unique on docs, docs_extra on docs", Index: "docs_lookup", IndexTable: "docs", IndexFields: "title",
				Grant: "docs", GrantColumns: "title", Enabled: "docs", Policy: "docs_read", PolicyTable: "docs",
			},
		},
		{
			dialect: platform.Postgres, name: "quoted keeps its case", table: `"Docs"`, column: `"Title"`,
			want: relationNames{
				Table: "Docs", Columns: "id, Title, extra", Constraints: "docs_unique on Docs, docs_extra on Docs", Index: "docs_lookup", IndexTable: "Docs", IndexFields: "Title",
				Grant: "Docs", GrantColumns: "Title", Enabled: "Docs", Policy: "docs_read", PolicyTable: "Docs",
			},
		},
		{
			dialect: platform.Postgres, name: "unquoted folds ASCII only", table: "Ärger", column: "Äb",
			want: relationNames{
				Table: "Ärger", Columns: "id, Äb, extra", Constraints: "docs_unique on Ärger, docs_extra on Ärger", Index: "docs_lookup", IndexTable: "Ärger", IndexFields: "Äb",
				Grant: "Ärger", GrantColumns: "Äb", Enabled: "Ärger", Policy: "docs_read", PolicyTable: "Ärger",
			},
		},
		{
			dialect: platform.YugabyteDB, name: "unquoted mixed case folds", table: "Docs", column: "Title",
			want: relationNames{
				Table: "docs", Columns: "id, title, extra", Constraints: "docs_unique on docs, docs_extra on docs", Index: "docs_lookup", IndexTable: "docs", IndexFields: "title",
				Grant: "docs", GrantColumns: "title", Enabled: "docs", Policy: "docs_read", PolicyTable: "docs",
			},
		},
		{
			dialect: platform.YugabyteDB, name: "unquoted folds ASCII only", table: "Ärger", column: "Äb",
			want: relationNames{
				Table: "Ärger", Columns: "id, Äb, extra", Constraints: "docs_unique on Ärger, docs_extra on Ärger", Index: "docs_lookup", IndexTable: "Ärger", IndexFields: "Äb",
				Grant: "Ärger", GrantColumns: "Äb", Enabled: "Ärger", Policy: "docs_read", PolicyTable: "Ärger",
			},
		},
		{
			dialect: platform.CockroachDB, name: "unquoted mixed case folds", table: "Docs", column: "Title",
			want: relationNames{
				Table: "docs", Columns: "id, title, extra", Constraints: "docs_unique on docs, docs_extra on docs", Index: "docs_lookup", IndexTable: "docs", IndexFields: "title",
				Grant: "docs", GrantColumns: "title", Enabled: "docs", Policy: "docs_read", PolicyTable: "docs",
			},
		},
		{
			dialect: platform.CockroachDB, name: "quoted keeps its case", table: `"Docs"`, column: `"Title"`,
			want: relationNames{
				Table: "Docs", Columns: "id, Title, extra", Constraints: "docs_unique on Docs, docs_extra on Docs", Index: "docs_lookup", IndexTable: "Docs", IndexFields: "Title",
				Grant: "Docs", GrantColumns: "Title", Enabled: "Docs", Policy: "docs_read", PolicyTable: "Docs",
			},
		},
		{
			dialect: platform.CockroachDB, name: "unquoted non-ASCII folds too", table: "Ärger", column: "Äb",
			want: relationNames{
				Table: "ärger", Columns: "id, äb, extra", Constraints: "docs_unique on ärger, docs_extra on ärger", Index: "docs_lookup", IndexTable: "ärger", IndexFields: "äb",
				Grant: "ärger", GrantColumns: "äb", Enabled: "ärger", Policy: "docs_read", PolicyTable: "ärger",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			db := loadRoleCaseSchema(c, test.dialect, tableCaseSchema(test.table, test.column))
			c.Assert(recordedTableNames(c, db), qt.DeepEquals, test.want)
		})
	}
}

// TestLoadAll_TableRendersOneRelation is the rendering half: the table and
// every statement on it name the relation the server creates for `Docs`.
func TestLoadAll_TableRendersOneRelation(t *testing.T) {
	c := qt.New(t)
	db := loadRoleCaseSchema(c, platform.Postgres, tableCaseSchema("Docs", "Title"))

	rendered := strings.Join(renderPostgres(c, db), "\n")

	c.Assert(rendered, qt.Contains, `CREATE TABLE "docs" (`)
	c.Assert(rendered, qt.Contains, `ALTER TABLE "docs" ENABLE ROW LEVEL SECURITY;`)
	c.Assert(rendered, qt.Contains, `CREATE POLICY "docs_read" ON "docs"`)
	c.Assert(rendered, qt.Contains, `GRANT SELECT ("title") ON TABLE "docs" TO PUBLIC;`)
	c.Assert(rendered, qt.Contains, `ON "docs" ("title")`)
	c.Assert(rendered, qt.Not(qt.Contains), `"Docs"`)
}

// TestLoadAll_DialectNeutralTableNamesKeepTheirCase is the control: a read
// with no dialect or a dialect that does not fold keeps every name as written,
// the row-level security statements included, so the file still names one
// table.
func TestLoadAll_DialectNeutralTableNamesKeepTheirCase(t *testing.T) {
	for _, dialect := range []string{"", platform.Spanner} {
		t.Run("dialect="+dialect, func(t *testing.T) {
			c := qt.New(t)
			db := loadRoleCaseSchema(c, dialect, tableCaseSchema("Docs", "Title"))
			c.Assert(recordedTableNames(c, db), qt.DeepEquals, relationNames{
				Table: "Docs", Columns: "Id, Title, Extra", Constraints: "Docs_Unique on Docs, Docs_Extra on Docs", Index: "Docs_Lookup", IndexTable: "Docs", IndexFields: "Title",
				Grant: "Docs", GrantColumns: "Title", Enabled: "Docs", Policy: "Docs_Read", PolicyTable: "Docs",
			})
		})
	}
}
