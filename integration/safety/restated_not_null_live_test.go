//go:build integration

package safety_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// notNullEngine is one engine the NOT NULL statement is planned for: how its
// dialect is named, how a nullable integer column is declared there, and what
// it answers when the planned statement meets a NULL row.
type notNullEngine struct {
	name       string
	engine     dbtarget.Engine
	dialect    string
	columnType string
	dropSuffix string
	wantReason string
	wantError  string
}

// The engines, with what each answered when measured: MySQL 26.7 and MariaDB
// 12.3 under their default strict SQL mode, SQL Server 2022 and Oracle 23.26.
var notNullEngines = []notNullEngine{
	{
		name: "MySQL", engine: dbtarget.MySQL, dialect: platform.MySQL, columnType: "INT",
		wantReason: modifyReason, wantError: `(?s).*Error 1138.*Invalid use of NULL value.*`,
	},
	{
		name: "MariaDB", engine: dbtarget.MariaDB, dialect: platform.MariaDB, columnType: "INT",
		wantReason: modifyReason, wantError: `(?s).*Error 1265.*Data truncated for column 'c'.*`,
	},
	{
		name: "SQL Server", engine: dbtarget.SQLServer, dialect: platform.SQLServer, columnType: "INT",
		wantReason: "ALTER COLUMN ... NOT NULL fails when a row holds NULL", wantError: `(?s).*Cannot insert the value NULL into column 'c'.*`,
	},
	{
		name: "Oracle", engine: dbtarget.Oracle, dialect: platform.Oracle, columnType: "NUMBER(10)", dropSuffix: " PURGE",
		wantReason: modifyReason, wantError: `(?s).*ORA-02296.*`,
	},
}

const modifyReason = "MODIFY or CHANGE ... NOT NULL fails when a row holds NULL; " +
	"outside strict SQL mode MySQL and MariaDB rewrite the NULL to the type's zero value instead"

// notNullPlan is the plan that makes column c of table NOT NULL: its nodes, the
// statements they render with comments removed, and the report on them.
type notNullPlan struct {
	statements  []string
	assessments []safety.StatementAssessment
}

func planNotNull(c *qt.C, dialect, table string) notNullPlan {
	c.Helper()
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: table,
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "c",
			Desired:    schemamodel.Field{Name: "c", Type: "INTEGER", StructName: "T3669"},
			Changes:    map[string]string{"nullable": "true -> false"},
		}},
	}}}
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(diff, dialect, planner.Options{})
	c.Assert(err, qt.IsNil)
	assessments, err := safety.AssessRendered(nodes, dialect)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, dialect)
	c.Assert(err, qt.IsNil)
	executable := make([]string, 0, len(statements))
	for _, statement := range statements {
		executable = append(executable, strings.TrimSpace(sqlutil.StripCommentsForDialect(statement, dialect)))
	}
	return notNullPlan{
		statements: slices.DeleteFunc(executable, func(statement string) bool { return statement == "" }),
		assessments: slices.DeleteFunc(assessments, func(assessment safety.StatementAssessment) bool {
			return assessment.NodeType == fmt.Sprintf("%T", &ast.CommentNode{})
		}),
	}
}

// nullRowTable creates a table holding a row whose c is NULL on the engine,
// and drops it afterwards. The name is unique, because the servers are shared.
func nullRowTable(c *qt.C, conn *dbschema.DatabaseConnection, engine notNullEngine) string {
	c.Helper()
	table := fmt.Sprintf("t3669_%d_%d", os.Getpid(), time.Now().UnixNano())
	_, err := conn.ExecContext(context.Background(), "CREATE TABLE "+table+" (id INT PRIMARY KEY, c "+engine.columnType+")")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), "DROP TABLE "+table+engine.dropSuffix)
		c.Check(dropErr, qt.IsNil)
	})
	_, err = conn.ExecContext(context.Background(), "INSERT INTO "+table+" (id, c) VALUES (1, NULL)")
	c.Assert(err, qt.IsNil)
	return table
}

// nullMarker is what readC answers for a row whose c is NULL. No row of these
// tests holds -1.
const nullMarker = -1

// readC answers the c of row 1, or nullMarker for a NULL, in one query every
// engine here accepts.
func readC(c *qt.C, conn *dbschema.DatabaseConnection, table string) int64 {
	c.Helper()
	var value int64
	c.Assert(conn.QueryRowContext(context.Background(),
		"SELECT CASE WHEN c IS NULL THEN -1 ELSE c END FROM "+table+" WHERE id = 1").Scan(&value), qt.IsNil)
	return value
}

// The statement Ptah plans to make a column NOT NULL, on each engine that
// spells it as a restatement of the column, over a table holding a NULL row.
// The engine refuses it and the row keeps its NULL, which is what the safety
// report now says for each spelling: a warning that names how it fails. Read
// by its words alone, each of these reported "does not remove data or tighten
// constraints" (stokaro/ptah#3669).
func TestRestatedNotNullOnANullRow_FailurePath(t *testing.T) {
	for _, engine := range notNullEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(context.Background(), dbtarget.URL(c, engine.engine))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			table := nullRowTable(c, conn, engine)
			plan := planNotNull(c, engine.dialect, table)
			c.Assert(plan.statements, qt.HasLen, 1)

			_, execErr := conn.ExecContext(context.Background(), plan.statements[0])

			c.Assert(execErr, qt.ErrorMatches, engine.wantError)
			c.Assert(readC(c, conn, table), qt.Equals, int64(nullMarker))
			c.Assert(plan.assessments, qt.HasLen, 1)
			c.Assert(plan.assessments[0].Severity, qt.Equals, safety.Warning)
			c.Assert(plan.assessments[0].Reason, qt.Equals, engine.wantReason)
		})
	}
}

// Outside strict SQL mode MySQL and MariaDB apply the same statement, and the
// NULL becomes the type's zero value. That is the second half of the MODIFY
// reason, and why the report does not call the statement safe even where it
// does not fail.
func TestRestatedNotNullOutsideStrictMode_HappyPath(t *testing.T) {
	for _, engine := range notNullEngines[:2] {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(context.Background(), dbtarget.URL(c, engine.engine))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			table := nullRowTable(c, conn, engine)
			plan := planNotNull(c, engine.dialect, table)
			c.Assert(plan.statements, qt.HasLen, 1)
			session, err := conn.Conn(context.Background())
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(session.Close(), qt.IsNil) }()
			_, err = session.ExecContext(context.Background(), "SET SESSION sql_mode = ''")
			c.Assert(err, qt.IsNil)

			_, execErr := session.ExecContext(context.Background(), plan.statements[0])

			c.Assert(execErr, qt.IsNil)
			c.Assert(readC(c, conn, table), qt.Equals, int64(0))
			c.Assert(plan.assessments[0].Reason, qt.Equals, modifyReason)
		})
	}
}
