//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/sijms/go-ora/v3"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/dbexprprobe"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/genexprprobe"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestOracleDeclarationConvergesE2E is the assertion every other engine here
// carries: render a declaration, apply it, read it back, and find nothing left
// to do.
//
// It is the whole reason the Oracle work in stokaro/ptah#1875 is more than a
// renderer. Each of the five defects the first live run found would redden a
// different line of it:
//
//   - the declared TEXT reads back as CLOB, and an INT as NUMBER(10), so a
//     comparison that asks whether the words match reports an ALTER for every
//     column;
//   - the identity column's DATA_DEFAULT is the nextval of a sequence Oracle
//     named, whose name differs in every database;
//   - the virtual column's expression lives in that same column, and read as a
//     default it turns a generated column into an ordinary one;
//   - ALL_SEQUENCES lists the sequence behind the identity column, which nobody
//     declared;
//   - the unnamed CHECK is named SYS_C008794 by the server, and the number is
//     per database.
//
// The comparison at the end is what makes it a round trip rather than a read
// check, and it reports nothing -- the generated column included, which takes a
// dev account to resolve. See the comment beside it.
func TestOracleDeclarationConvergesE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	// A clean schema, and the same cleanup afterwards: a leftover table from an
	// earlier run would be read back as a table the declaration does not
	// declare, and the convergence assertion would fail against a defect that
	// is not there.
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	defer func() {
		c.Check(conn.SchemaWriter().DropAllTables(context.WithoutCancel(ctx)), qt.IsNil)
	}()

	declared := oracleConvergenceDeclaration()

	statements, err := renderer.RenderSQLWithCapabilities(
		platform.Oracle,
		capability.ForServerVersion(platform.Oracle, conn.Info().Version),
		oracleConvergenceNodes(declared)...,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Not(qt.Equals), "")

	for _, statement := range splitOracleStatements(statements) {
		c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil,
			qt.Commentf("statement: %s", statement))
	}

	// The read side. Every field the comparison reads has to survive this.
	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	tables := make(map[string]int, len(read.Tables))
	for _, table := range read.Tables {
		tables[table.Name] = len(table.Columns)
	}
	c.Assert(tables, qt.DeepEquals, map[string]int{"ORA_AUTHORS": 4, "ORA_POSTS": 4})

	// The sequence Oracle owns behind the identity column is not a sequence
	// anybody declared, and reading it back would plan a DROP for it every run.
	c.Assert(read.Sequences, qt.HasLen, 0)

	// The backing index of the primary key is not a declared index either; the
	// one index here is the one the declaration asked for.
	indexNames := make([]string, 0, len(read.Indexes))
	for _, index := range read.Indexes {
		indexNames = append(indexNames, index.Name)
	}
	c.Assert(indexNames, qt.DeepEquals, []string{"IDX_ORA_POSTS_TITLE"})

	authors := oracleTableByName(c, read.Tables, "ORA_AUTHORS")
	id := oracleColumnByName(c, authors.Columns, "ID")
	c.Assert(id.IsAutoIncrement, qt.IsTrue)
	// The identity column's default is Oracle's own bookkeeping -- the nextval
	// of a sequence it named -- and reporting it as a declared default is what
	// makes two catalogs of the same schema never compare equal.
	c.Assert(id.ColumnDefault, qt.IsNil)
	c.Assert(id.DataType, qt.Equals, "NUMBER(10)")

	bio := oracleColumnByName(c, authors.Columns, "BIO")
	c.Assert(bio.DataType, qt.Equals, "CLOB")
	c.Assert(bio.IsNullable, qt.Equals, "YES")

	rating := oracleColumnByName(c, authors.Columns, "RATING")
	c.Assert(rating.DataType, qt.Equals, "NUMBER(5,2)")

	// And the assertion the rest of this test exists to make possible: what the
	// comparison between the declaration and the catalog still reports.
	//
	// The read-back assertions above are not a substitute for it. Each names
	// one fact a reader has to get right, and a reader could get all of them
	// right and still hand the comparator something it plans an ALTER against
	// -- a type spelled differently, a default the server wrote itself, a
	// constraint it named. Only the comparison says whether there is anything
	// left to do.
	//
	// Nothing left to do, generated column included.
	//
	// Getting there takes a second connection. Oracle stores a REWRITE of a
	// virtual column's expression -- every reference quoted and upper-cased,
	// the spaces around operators gone -- so the declaration and the catalog
	// disagree textually about an expression they agree about semantically. The
	// declaration is put through a server to be comparable, and that probe
	// creates a permanent table: Oracle commits its own DDL, and a virtual
	// column on a temporary table is ORA-54010 on both spellings. So the probe
	// goes to a DEV account and never to the schema under comparison
	// (stokaro/ptah#1915).
	compareOpts := oracleGeneratedExpressionOptions(ctx, c, conn, declared)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, declared, read, compareOpts)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleDiffSummary(diff), qt.DeepEquals, []string(nil))
}

// TestOracleGeneratedExpressionChangeIsStillReportedE2E is the control that
// separates a fix from a silencer.
//
// Resolving the declared expression through a server makes the two sides
// converge when they agree. The risk it carries is the opposite error: a rule
// that made everything converge would hide a real edit, and the round trip
// above would still pass while `schema apply` quietly stopped planning
// anything.
//
// So the same database is compared against a declaration whose expression
// really changed, `view_count * 2` to `view_count * 3`. Measured live, the two
// resolve to `"VIEW_COUNT"*2` and `"VIEW_COUNT"*3`, which are still different
// (stokaro/ptah#1915).
func TestOracleGeneratedExpressionChangeIsStillReportedE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	defer func() {
		c.Check(conn.SchemaWriter().DropAllTables(context.WithoutCancel(ctx)), qt.IsNil)
	}()

	applied := oracleConvergenceDeclaration()
	statements, err := renderer.RenderSQLWithCapabilities(
		platform.Oracle,
		capability.ForServerVersion(platform.Oracle, conn.Info().Version),
		oracleConvergenceNodes(applied)...,
	)
	c.Assert(err, qt.IsNil)
	for _, statement := range splitOracleStatements(statements) {
		c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil,
			qt.Commentf("statement: %s", statement))
	}

	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// The one edit, on the declaration only.
	changed := withGeneratedExpression(oracleConvergenceDeclaration(), "doubled", "view_count * 3")

	compareOpts := oracleGeneratedExpressionOptions(ctx, c, conn, changed)
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, changed, read, compareOpts)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleDiffSummary(diff), qt.DeepEquals, []string{
		`column modified: ora_posts.doubled map[generated:VIRTUAL "VIEW_COUNT"*2 -> VIRTUAL "VIEW_COUNT"*3]`,
	})
}

// withGeneratedExpression rewrites one column's generated expression, leaving
// the declaration otherwise as it was.
func withGeneratedExpression(declared *schemamodel.Database, column, expression string) *schemamodel.Database {
	for i := range declared.Fields {
		if declared.Fields[i].Name == column {
			declared.Fields[i].GeneratedExpression = expression
		}
	}
	return declared
}

// oracleGeneratedExpressionOptions resolves each declared generated expression
// through the dev account and returns the options carrying the answers.
//
// The dev address is a separate variable rather than the target's, and the
// separation is the assertion: a probe that wrote into the schema this test
// then reads back would make the round trip agree with objects the declaration
// never had.
func oracleGeneratedExpressionOptions(
	ctx context.Context,
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *schemamodel.Database,
) *config.CompareOptions {
	devURL := dbtarget.URL(c.TB.(*testing.T), dbtarget.OracleDev)
	dev, err := dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(dev)

	probes, err := genexprprobe.For(conn.Info().Dialect, conn.Info().Capabilities, declared)
	c.Assert(err, qt.IsNil)
	c.Assert(probes, qt.Not(qt.HasLen), 0)

	resolved, err := dbexprprobe.ResolveGeneratedExpressions(ctx, dev, probes)
	c.Assert(err, qt.IsNil)
	// Asserted rather than assumed: an empty map is what a resolver that
	// silently did nothing returns, and it would make the comparison below pass
	// through the uncompared branch instead of the resolved one.
	c.Assert(resolved, qt.HasLen, 1)

	// And nothing of the probe survives it. A leaked table is read by the next
	// run as an object nobody declared.
	var left int
	c.Assert(dev.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM all_tables WHERE owner = USER AND table_name LIKE 'PTAH_GENEXPR_PROBE%'").
		Scan(&left), qt.IsNil)
	c.Assert(left, qt.Equals, 0)

	opts := config.DefaultCompareOptions()
	opts.GeneratedExpressions = resolved
	return opts
}

// oracleDiffSummary names every change a comparison reported, so a failure says
// which one rather than that a count was not zero.
func oracleDiffSummary(diff *difftypes.SchemaDiff) []string {
	var changes []string
	for _, name := range diff.TablesAdded.Names() {
		changes = append(changes, "table added: "+name)
	}
	for _, name := range diff.TablesRemoved {
		changes = append(changes, "table removed: "+name)
	}
	for _, table := range diff.TablesModified {
		for _, name := range table.ConstraintsAdded {
			changes = append(changes, "constraint added: "+table.TableName+"."+name)
		}
		for _, name := range table.ConstraintsRemoved {
			changes = append(changes, "constraint removed: "+table.TableName+"."+name)
		}
		for _, column := range table.ColumnsAdded {
			changes = append(changes, "column added: "+table.TableName+"."+column.Name)
		}
		for _, column := range table.ColumnsRemoved {
			changes = append(changes, "column removed: "+table.TableName+"."+column.Name)
		}
		for _, column := range table.ColumnsModified {
			changes = append(changes, fmt.Sprintf("column modified: %s.%s %v",
				table.TableName, column.ColumnName, column.Changes))
		}
	}
	for _, index := range diff.IndexesAdded {
		changes = append(changes, "index added: "+index.TableName+"."+index.Name)
	}
	for _, index := range diff.IndexesRemoved {
		changes = append(changes, "index removed: "+index.TableName+"."+index.Name)
	}
	return changes
}

// oracleConvergenceDeclaration is the schema the test declares.
//
// Each column is here because a different part of the read side has to survive
// it: an identity column, a type with no Oracle counterpart, a decimal that
// keeps its scale, a foreign key, and a CHECK the declaration does not name.
func oracleConvergenceDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Author", Name: "ora_authors"},
			{StructName: "Post", Name: "ora_posts"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Author", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Author", Name: "email", Type: "VARCHAR(255)", Unique: true},
			{StructName: "Author", Name: "bio", Type: "TEXT", Nullable: true},
			{StructName: "Author", Name: "rating", Type: "DECIMAL(5,2)", Nullable: true},
			{StructName: "Post", Name: "id", Type: "INT", Primary: true},
			{StructName: "Post", Name: "author_id", Type: "INT"},
			{StructName: "Post", Name: "view_count", Type: "INT", Check: "view_count >= 0"},
			{
				StructName:          "Post",
				Name:                "doubled",
				Type:                "INT",
				Nullable:            true,
				GeneratedExpression: "view_count * 2",
				GeneratedKind:       "VIRTUAL",
			},
		},
		Indexes: []schemamodel.Index{
			{
				StructName: "Post",
				// TableName is the owner identity the planner applies before
				// conversion; without it FromIndex names the Go struct, and
				// Oracle answers ORA-00942 for a table called POST.
				TableName: "ora_posts",
				Name:      "idx_ora_posts_title",
				Fields:    []string{"view_count"},
			},
		},
	}
}

// oracleConvergenceNodes renders the declaration into the nodes the Oracle
// renderer takes.
//
// The declaration is a schemamodel.Database rather than a hand-built node list so
// that ONE declaration reaches both halves of the round trip: the same value is
// rendered here and compared against the catalog below. A node list would need
// a second spelling of the same schema for the comparison, and two spellings of
// one schema is how a round trip comes to agree with itself while disagreeing
// with the database.
func oracleConvergenceNodes(declared *schemamodel.Database) []ast.Node {
	nodes := make([]ast.Node, 0, len(declared.Tables)+len(declared.Indexes))
	for _, table := range declared.Tables {
		nodes = append(nodes, fromschema.FromTable(table, declared.Fields, declared.Enums, platform.Oracle))
	}
	for _, index := range declared.Indexes {
		nodes = append(nodes, fromschema.FromIndex(index))
	}
	return nodes
}

// splitOracleStatements splits rendered DDL into the statements a driver takes
// one at a time.
//
// Oracle's driver refuses a batch, and it refuses a trailing semicolon on a
// single statement too, so both have to go.
func splitOracleStatements(rendered string) []string {
	var statements []string
	for _, chunk := range strings.Split(rendered, ";\n") {
		var kept []string
		for _, line := range strings.Split(strings.TrimSpace(chunk), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "--") {
				continue
			}
			kept = append(kept, line)
		}
		statement := strings.TrimSuffix(strings.TrimSpace(strings.Join(kept, "\n")), ";")
		if statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}

func oracleTableByName(c *qt.C, tables []catalog.Table, name string) catalog.Table {
	c.Helper()
	for _, table := range tables {
		if table.Name == name {
			return table
		}
	}
	c.Fatalf("table %q is absent from the read schema", name)
	return catalog.Table{}
}

func oracleColumnByName(c *qt.C, columns []catalog.Column, name string) catalog.Column {
	c.Helper()
	for _, column := range columns {
		if column.Name == name {
			return column
		}
	}
	c.Fatalf("column %q is absent from the read table", name)
	return catalog.Column{}
}
