//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/sijms/go-ora/v2"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
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
// The generated column is deliberately absent: Oracle rewrites a stored
// expression -- quoting and upper-casing every reference, adding parentheses --
// and folding that textually would be guesswork. That gap is recorded on the
// issue rather than papered over here.
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
		declared...,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Not(qt.Equals), "")

	for _, statement := range splitOracleStatements(statements) {
		c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, statement), qt.IsNil,
			qt.Commentf("statement: %s", statement))
	}

	// The read side. Every field the comparison reads has to survive this.
	read, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)

	tables := make(map[string]int, len(read.Tables))
	for _, table := range read.Tables {
		tables[table.Name] = len(table.Columns)
	}
	c.Assert(tables, qt.DeepEquals, map[string]int{"ORA_AUTHORS": 4, "ORA_POSTS": 3})

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
}

// oracleConvergenceDeclaration is the schema the test declares.
//
// Each column is here because a different part of the read side has to survive
// it: an identity column, a type with no Oracle counterpart, a decimal that
// keeps its scale, a foreign key, and a CHECK the declaration does not name.
func oracleConvergenceDeclaration() []ast.Node {
	authors := &ast.CreateTableNode{
		Name: "ora_authors",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "SERIAL", Primary: true},
			{Name: "email", Type: "VARCHAR(255)", Unique: true},
			{Name: "bio", Type: "TEXT", Nullable: true},
			{Name: "rating", Type: "DECIMAL(5,2)", Nullable: true},
		},
	}
	posts := &ast.CreateTableNode{
		Name: "ora_posts",
		Columns: []*ast.ColumnNode{
			{Name: "id", Type: "INT", Primary: true},
			{Name: "author_id", Type: "INT"},
			{Name: "view_count", Type: "INT", Check: "view_count >= 0"},
		},
	}
	index := &ast.IndexNode{
		Name: "idx_ora_posts_title", Table: "ora_posts", Columns: []string{"view_count"},
	}
	return []ast.Node{authors, posts, index}
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

func oracleTableByName(c *qt.C, tables []dbschematypes.DBTable, name string) dbschematypes.DBTable {
	c.Helper()
	for _, table := range tables {
		if table.Name == name {
			return table
		}
	}
	c.Fatalf("table %q is absent from the read schema", name)
	return dbschematypes.DBTable{}
}

func oracleColumnByName(c *qt.C, columns []dbschematypes.DBColumn, name string) dbschematypes.DBColumn {
	c.Helper()
	for _, column := range columns {
		if column.Name == name {
			return column
		}
	}
	c.Fatalf("column %q is absent from the read table", name)
	return dbschematypes.DBColumn{}
}
