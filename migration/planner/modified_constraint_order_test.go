package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestGenerateSchemaDiffSQLStatements_AModifiedConstraintDropsBeforeItAdds is
// the enumeration stokaro/ptah#1987 needed, on the shape that produced it:
// a named UNIQUE constraint whose columns change, on a table the DESCRIPTION
// leaves unqualified and the CATALOG reports with its schema.
//
// A constraint's name belongs to its backing index too, so the ADD cannot
// precede the DROP. Measured on PostgreSQL 17 before this was ordered:
//
//	ALTER TABLE widget ADD CONSTRAINT uq_widget_scope UNIQUE (tenant);
//	  ERROR:  relation "uq_widget_scope" already exists
//	ALTER TABLE widget DROP CONSTRAINT IF EXISTS uq_widget_scope;
//	  ALTER TABLE
//
// Inside a transaction that rolls the whole change back; OUTSIDE one the ADD
// fails, the DROP still runs, and the table is left with no unique constraint
// at all.
//
// Every dialect that plans the pair is a row, not only the one the issue named.
// Four of the six were already correct, and they are here because the fix is a
// change to how a HOST is keyed: a rule that fixed PostgreSQL by reordering
// emissions would leave them alone, and a rule that broke one of them would
// otherwise be found by an operator.
func TestGenerateSchemaDiffSQLStatements_AModifiedConstraintDropsBeforeItAdds(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		schema   string
		wantDrop string
		wantAdd  string
	}{
		{
			// The reported row.
			name:     "postgres",
			dialect:  "postgres",
			schema:   "public",
			wantDrop: `DROP CONSTRAINT IF EXISTS "uq_widget_scope"`,
			wantAdd:  `ADD CONSTRAINT "uq_widget_scope"`,
		},
		{
			name:     "sqlserver",
			dialect:  "sqlserver",
			schema:   "dbo",
			wantDrop: "DROP CONSTRAINT IF EXISTS [uq_widget_scope]",
			wantAdd:  "ADD CONSTRAINT [uq_widget_scope]",
		},
		{
			name:     "oracle",
			dialect:  "oracle",
			schema:   "APP",
			wantDrop: "DROP CONSTRAINT uq_widget_scope",
			wantAdd:  "ADD CONSTRAINT uq_widget_scope",
		},
		{
			// MySQL drops the constraint through its backing index, which is
			// the same statement under another name.
			name:     "mysql",
			dialect:  "mysql",
			schema:   "testdb",
			wantDrop: "DROP INDEX `uq_widget_scope`",
			wantAdd:  "ADD CONSTRAINT `uq_widget_scope`",
		},
		{
			name:     "mariadb",
			dialect:  "mariadb",
			schema:   "testdb",
			wantDrop: "DROP INDEX IF EXISTS `uq_widget_scope`",
			wantAdd:  "ADD CONSTRAINT `uq_widget_scope`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := widgetDeclaredWithoutASchema()

			statements := planModifiedConstraint(c, desired, test.dialect, test.schema)

			drops := statementIndexesContaining(statements, test.wantDrop)
			adds := statementIndexesContaining(statements, test.wantAdd)

			c.Assert(drops, qt.HasLen, 1, qt.Commentf("%v", statements))
			c.Assert(adds, qt.HasLen, 1, qt.Commentf("%v", statements))
			c.Assert(drops[0] < adds[0], qt.IsTrue,
				qt.Commentf("the re-add precedes the drop it needs: %v", statements))
		})
	}
}

// TestGenerateSchemaDiffSQLStatements_SQLiteRebuildsTheTableOnce is the same
// mismatch on the target that answers a constraint change by rewriting the
// table.
//
// SQLite has no ALTER TABLE for a constraint, so the plan is
// CREATE/INSERT/DROP/RENAME. The two spellings of one table made it TWO
// rebuild targets, and the plan copied every row of the table twice -- the
// second copy out of a table the first had just rewritten. It converges, which
// is why nothing failed; it also doubles the work and the window in which the
// table does not exist.
func TestGenerateSchemaDiffSQLStatements_SQLiteRebuildsTheTableOnce(t *testing.T) {
	c := qt.New(t)
	desired := widgetDeclaredWithoutASchema()

	statements := planModifiedConstraint(c, desired, "sqlite", "main")

	rebuilds := 0
	for _, statement := range statements {
		rebuilds += strings.Count(statement, `CREATE TABLE "__ptah_rebuild_widget"`)
	}
	c.Assert(rebuilds, qt.Equals, 1, qt.Commentf("%v", statements))
}

// widgetDeclaredWithoutASchema is what `//ptah:schema:constraint` on a struct
// with no explicit schema produces: a table spelled bare, and a named UNIQUE
// constraint over one column.
func widgetDeclaredWithoutASchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "W", Name: "widget"}},
		Fields: []schemamodel.Field{
			{StructName: "W", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "W", Name: "tenant", Type: "TEXT"},
			{StructName: "W", Name: "code", Type: "TEXT"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "W", Name: "uq_widget_scope", Type: "UNIQUE",
			Table: "widget", Columns: []string{"tenant"},
		}},
	}
}

// planModifiedConstraint compares that declaration against a catalog that
// qualifies the same table and covers two columns with the constraint, and
// plans the difference.
//
// The semantics carry the schema a CONNECTION supplies. [identifier.ForDialect]
// cannot name it on MySQL, MariaDB or Oracle -- offline, the dialect string is
// all there is -- so a comparison run without them reports the table itself as
// added and removed, which is a different question from this one.
func planModifiedConstraint(c *qt.C, desired *schemamodel.Database, dialect, schema string) []string {
	c.Helper()
	database := &catalog.Database{
		Tables: []catalog.Table{{Schema: schema, Name: "widget", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", IsPrimaryKey: true, IsNullable: "NO"},
			{Name: "tenant", DataType: "text", IsNullable: "NO"},
			{Name: "code", DataType: "text", IsNullable: "NO"},
		}}},
		Constraints: []catalog.Constraint{{
			Schema: schema, TableName: "widget", Name: "uq_widget_scope",
			Type: "UNIQUE", ColumnNames: []string{"tenant", "code"},
		}},
	}

	semantics := connectionSemantics(dialect, schema)
	opts := config.DefaultCompareOptions()
	opts.Dialect = dialect
	opts.IdentifierSemantics = &semantics
	diff := schemadiff.CompareWithOptions(desired, database, opts)
	diff.IdentifierSemantics = &semantics

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, dialect)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded, qt.HasLen, 0, qt.Commentf("the table itself must pair"))
	c.Assert(diff.TablesRemoved, qt.HasLen, 0, qt.Commentf("the table itself must pair"))
	return statements
}

// connectionSemantics is what a live connection reports: the dialect's own
// rules, plus the schema unqualified names resolve in.
//
// SQL Server needs its catalog collation and the names it resolved, because
// an unresolved name there shares one conservative conflict key and the planner
// refuses to act on names it cannot tell apart.
func connectionSemantics(dialect, schema string) identifier.Semantics {
	if dialect == "sqlserver" {
		return identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
			WithResolvedNames([]identifier.ResolvedName{
				{Name: "dbo", Key: "dbo"}, {Name: "widget", Key: "widget"},
				{Name: "id", Key: "id"}, {Name: "tenant", Key: "tenant"},
				{Name: "code", Key: "code"},
				{Name: "uq_widget_scope", Key: "uq_widget_scope"},
			})
	}
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = schema
	return semantics
}

// statementIndexesContaining reports every statement a fragment appears in.
//
// Every index is collected rather than the first, because the count is half the
// claim. A pre-drop the pure-removal path does not recognize as one is emitted
// a SECOND time, after the re-add, and deletes the constraint that was just
// created -- `IF EXISTS` is no protection against dropping something that now
// exists again (stokaro/ptah#229). Asserting only that a drop comes first
// passes on exactly that plan.
func statementIndexesContaining(statements []string, fragment string) []int {
	indexes := make([]int, 0, len(statements))
	for index, statement := range statements {
		if strings.Contains(statement, fragment) {
			indexes = append(indexes, index)
		}
	}
	return indexes
}
