//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
)

// defaultChangeTable declares table t of a schema with the columns given.
func defaultChangeTable(schemaName string, fields ...schemamodel.Field) *schemamodel.Database {
	id := schemamodel.Field{StructName: "T", Name: "id", Type: "INT", Primary: true}
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Schema: schemaName, Name: "t"}},
		Fields: []schemamodel.Field{id},
	}
	for _, field := range fields {
		field.StructName = "T"
		database.Fields = append(database.Fields, field)
	}
	return database
}

// applyDeclared plans the declaration against the live schema, applies every
// statement, and answers the statements and the safety report on them.
func applyDeclared(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *schemamodel.Database,
	schemaName string,
) ([]string, []safety.StatementAssessment) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, live, nil)
	c.Assert(err, qt.IsNil)
	nodes, err := planner.GenerateSchemaDiffAST(diff, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	assessments, err := safety.AssessRendered(nodes, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(c.Context(), statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	return statements, assessments
}

// storedDefaults answers each column of table t with the definition of its
// default as sys.default_constraints holds it, or "none".
func storedDefaults(c *qt.C, conn *dbschema.DatabaseConnection, quotedSchema string) map[string]string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), "SELECT c.name, COALESCE(dc.definition, 'none') FROM sys.columns AS c"+
		" LEFT JOIN sys.default_constraints AS dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id"+
		" WHERE c.object_id = OBJECT_ID(N'"+quotedSchema+".[t]')")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	defaults := make(map[string]string)
	for rows.Next() {
		var column, definition string
		c.Assert(rows.Scan(&column, &definition), qt.IsNil)
		defaults[column] = definition
	}
	c.Assert(rows.Err(), qt.IsNil)
	return defaults
}

// Every default change the SQL Server planner refused, applied to a real
// server: a default changed, added, removed, set as an expression, kept across
// a type change, removed with one, and kept across a change of length and one
// of nullability.
//
// A default is a constraint in SQL Server, named by the server when Ptah
// creates it, so the plan cannot name the one it drops; it reads the name from
// sys.default_constraints when it runs. INT to BIGINT is a change the server
// refuses while a default is bound to the column (Msg 5074), so the default is
// dropped first and set again after. Before this, each change was
// refused as "SQL Server planner only supports ALTER COLUMN for
// type/nullability changes" (stokaro/ptah#3650).
//
// The second plan is empty: what the migration set reads back as what was
// declared, so the next plan does not set it again.
func TestSQLServerLiveColumnDefaultChangesApply(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	schemaName := fmt.Sprintf("ptah_3650_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	_, err = conn.ExecContext(c.Context(), "EXEC('CREATE SCHEMA "+quoted+"')")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+quoted+".[t]")
		c.Check(dropErr, qt.IsNil)
		_, dropErr = conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+quoted)
		c.Check(dropErr, qt.IsNil)
	})
	before := defaultChangeTable(schemaName,
		schemamodel.Field{Name: "status", Type: "NVARCHAR(20)", Nullable: true, Default: "inactive"},
		schemamodel.Field{Name: "note", Type: "NVARCHAR(20)", Nullable: true},
		schemamodel.Field{Name: "flag", Type: "INT", Nullable: true, Default: "0"},
		schemamodel.Field{Name: "stamp", Type: "DATETIME2", Nullable: true},
		schemamodel.Field{Name: "code", Type: "INT", Nullable: true, Default: "7"},
		schemamodel.Field{Name: "legacy", Type: "INT", Nullable: true, Default: "8"},
		schemamodel.Field{Name: "label", Type: "NVARCHAR(10)", Nullable: true, Default: "x"},
		schemamodel.Field{Name: "level", Type: "INT", Nullable: true, Default: "1"},
	)
	after := defaultChangeTable(schemaName,
		schemamodel.Field{Name: "status", Type: "NVARCHAR(20)", Nullable: true, Default: "active"},
		schemamodel.Field{Name: "note", Type: "NVARCHAR(20)", Nullable: true, Default: "none"},
		schemamodel.Field{Name: "flag", Type: "INT", Nullable: true},
		schemamodel.Field{Name: "stamp", Type: "DATETIME2", Nullable: true, DefaultExpr: "getdate()"},
		schemamodel.Field{Name: "code", Type: "BIGINT", Nullable: true, Default: "7"},
		schemamodel.Field{Name: "legacy", Type: "BIGINT", Nullable: true},
		schemamodel.Field{Name: "label", Type: "NVARCHAR(20)", Nullable: true, Default: "x"},
		schemamodel.Field{Name: "level", Type: "INT", Default: "1"},
	)
	applyDeclared(c, conn, before, schemaName)
	// The starting table reads back as declared, so every statement below is
	// one of the changes rather than drift the starting table carried.
	c.Assert(planDocumentAgainstLive(c, conn, before, schemaName), qt.HasLen, 0)

	statements, assessments := applyDeclared(c, conn, after, schemaName)

	c.Assert(statements, qt.Not(qt.HasLen), 0)
	c.Assert(storedDefaults(c, conn, quoted), qt.DeepEquals, map[string]string{
		"id":     "none",
		"status": "('active')",
		"note":   "('none')",
		"flag":   "none",
		"stamp":  "(getdate())",
		"code":   "('7')",
		"legacy": "none",
		"label":  "('x')",
		"level":  "('1')",
	})
	_, err = conn.ExecContext(c.Context(), "INSERT INTO "+quoted+".[t] (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var status, note, label string
	var flag, legacy *int64
	var stamped, code, level int64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT status, note, flag, CASE WHEN stamp IS NULL THEN 0 ELSE 1 END, code, legacy, label, level FROM "+
		quoted+".[t] WHERE id = 1").Scan(&status, &note, &flag, &stamped, &code, &legacy, &label, &level), qt.IsNil)
	c.Assert([]any{status, note, flag, stamped, code, legacy, label, level}, qt.DeepEquals,
		[]any{"active", "none", (*int64)(nil), int64(1), int64(7), (*int64)(nil), "x", int64(1)})
	// Dropping a default is a warning, not the removal of a data protection a
	// DROP CONSTRAINT reads as.
	c.Assert(safety.HasDestructiveAssessment(assessments), qt.IsFalse)
	c.Assert(safety.HighestAssessment(assessments), qt.Equals, safety.Warning)
	c.Assert(planDocumentAgainstLive(c, conn, after, schemaName), qt.HasLen, 0)
}
