//go:build integration

package ydb_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// dottedDirectory is the directory the dotted-name test writes into, and the
// first half of the root table's name. A Ptah schema is a directory on YDB and
// a table name may contain a dot, so the root table `ptah_ydb_dotted.b` and
// the table `b` in the directory `ptah_ydb_dotted` are two tables, which a key
// that joined schema and name with a dot would read as one.
const dottedDirectory = "ptah_ydb_dotted"

// dottedTables are the two tables, as schema and name.
var dottedTables = []catalog.Table{
	{Schema: "", Name: dottedDirectory + ".b"},
	{Schema: dottedDirectory, Name: "b"},
}

// dottedDeclaration declares both tables. Each has a column the other does
// not, so a read that described one table twice would show it, and each has
// an index of the same name, which YDB scopes to its table.
func dottedDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Root", Name: dottedDirectory + ".b"},
			{StructName: "Nested", Schema: dottedDirectory, Name: "b"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Root", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Root", Name: "root_note", Type: "TEXT", Nullable: true},
			{StructName: "Nested", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Nested", Name: "nested_note", Type: "TEXT", Nullable: true},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Root", Name: "note_ix", Fields: []string{"root_note"}},
			{StructName: "Nested", Name: "note_ix", Fields: []string{"nested_note"}},
		},
	}
}

// TestYDBDottedNames_ARootTableAndADirectoryTableStayTwo applies both tables,
// reads them back as two tables with their own columns and indexes, and plans
// nothing on a second comparison.
//
// The read covers the root, where other tests may leave tables, so it is
// narrowed to the two tables the test owns before anything is compared.
func TestYDBDottedNames_ARootTableAndADirectoryTableStayTwo(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropDottedTables(c, conn)
			c.Cleanup(func() { dropDottedTables(c, conn) })

			first := planDotted(c, conn)
			c.Assert(first, qt.Not(qt.HasLen), 0)
			apply(c, conn, first)

			live := readDotted(c, c.Context(), conn)
			c.Assert(tableNames(live), qt.DeepEquals, []string{"ptah_ydb_dotted|b", "|ptah_ydb_dotted.b"})
			c.Assert(columnNames(tableNamed(c, live, "", dottedDirectory+".b")), qt.DeepEquals,
				[]string{"id", "root_note"})
			c.Assert(columnNames(tableNamed(c, live, dottedDirectory, "b")), qt.DeepEquals,
				[]string{"id", "nested_note"})
			c.Assert(indexTables(live), qt.DeepEquals, []string{
				"ptah_ydb_dotted|b|note_ix|nested_note",
				"|ptah_ydb_dotted.b|note_ix|root_note",
			})

			c.Assert(planDotted(c, conn), qt.HasLen, 0)
		})
	}
}

// readDotted reads the root and the directory, and keeps the two tables the
// test owns.
func readDotted(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) *catalog.Database {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"", dottedDirectory})
	c.Assert(err, qt.IsNil)
	owned := func(schema, name string) bool {
		return slices.ContainsFunc(dottedTables, func(table catalog.Table) bool {
			return table.Schema == schema && table.Name == name
		})
	}
	narrowed := *live
	narrowed.Tables = slices.DeleteFunc(slices.Clone(live.Tables), func(table catalog.Table) bool {
		return !owned(table.Schema, table.Name)
	})
	narrowed.Indexes = slices.DeleteFunc(slices.Clone(live.Indexes), func(index catalog.Index) bool {
		return !owned(index.Schema, index.TableName)
	})
	narrowed.Constraints = slices.DeleteFunc(slices.Clone(live.Constraints), func(constraint catalog.Constraint) bool {
		return !owned(constraint.Schema, constraint.TableName)
	})
	return &narrowed
}

// planDotted plans the statements that take the two tables to the
// declaration.
func planDotted(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), dottedDeclaration(), readDotted(c, c.Context(), conn), info, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, info.Dialect, planner.Options{Capabilities: info.Capabilities},
	)
	c.Assert(err, qt.IsNil)
	return statements
}

// dropDottedTables drops whichever of the two tables exists. The directory
// stays, as YDB keeps one after its last table goes. It runs from Cleanup too,
// after the test's context is done, so it uses a context of its own.
func dropDottedTables(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	for _, table := range readDotted(c, context.Background(), conn).Tables {
		path := table.Name
		if table.Schema != "" {
			path = table.Schema + "/" + table.Name
		}
		c.Assert(conn.Writer().ExecuteSQL(context.Background(), "DROP TABLE "+sqlident.Quote("ydb", path)), qt.IsNil)
	}
}

// columnNames lists a table's columns in the order the reader reports them.
func columnNames(table catalog.Table) []string {
	names := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		names = append(names, column.Name)
	}
	return names
}

// indexTables names each index with its table and its columns, sorted.
func indexTables(live *catalog.Database) []string {
	names := make([]string, 0, len(live.Indexes))
	for _, index := range live.Indexes {
		names = append(names, index.Schema+"|"+index.TableName+"|"+index.Name+"|"+strings.Join(index.Columns, ","))
	}
	slices.Sort(names)
	return names
}
