//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/microsoft/go-mssqldb" // registers the SQL Server driver for database/sql

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestSQLServerCheckExpressionConvergesE2E is the SQL Server half of
// stokaro/ptah#2044, filed as stokaro/ptah#2054.
//
// SQL Server rewrites a CHECK before storing it, its own way: measured on 2025
// (RTM-CU8), a declared `price >= 0` on a decimal column is stored as
// `([price]>=(0))` -- brackets around the identifier, parentheses around the
// literal, and the spaces gone. Comparing the two texts planned a DROP and an
// ADD on every run of the same document, at severity destructive.
//
// The probe differs from the PostgreSQL one in two mechanical ways, and both
// are the kind that fail at runtime rather than at compile time: a temporary
// table is `#name`, and a savepoint is `SAVE TRANSACTION` -- SQL Server answers
// `Could not find stored procedure 'SAVEPOINT'` for the other spelling.
func TestSQLServerCheckExpressionConvergesE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("sqlserver", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_check_expr_e2e_%d", time.Now().UnixNano())
	_, err = adminDB.ExecContext(ctx, "CREATE DATABASE ["+testDBName+"]")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, dropErr := adminDB.ExecContext(context.Background(),
			"ALTER DATABASE ["+testDBName+"] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ["+testDBName+"]")
		c.Check(dropErr, qt.IsNil)
	}()

	scopedURL := replaceSQLServerDatabase(c, dbURL, testDBName)
	setupDB, err := sql.Open("sqlserver", scopedURL)
	c.Assert(err, qt.IsNil)
	defer setupDB.Close()
	_, err = setupDB.ExecContext(ctx, `CREATE TABLE items (
		id int NOT NULL PRIMARY KEY,
		price decimal(10,2) NOT NULL,
		CONSTRAINT ck_price CHECK (price >= 0))`)
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(ctx, scopedURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	// The evidence the rewrite happened. Without it, a server that stored the
	// text it was given would make the convergence below vacuous.
	c.Assert(storedSQLServerCheck(c, ctx, setupDB, "ck_price"), qt.Equals, "([price]>=(0))")

	diff, err := schemadiff.CompareWithDatabase(
		ctx, conn, sqlServerCheckDeclaration("price >= 0"), read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)

	// The control: a check that really did change is still reported.
	changed, err := schemadiff.CompareWithDatabase(
		ctx, conn, sqlServerCheckDeclaration("price >= 1"), read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(changed.ConstraintsAdded.Names(), qt.DeepEquals, []string{"ck_price"})
}

// replaceSQLServerDatabase points a SQL Server URL at another database.
//
// It is not [replaceDatabaseName]: that one rewrites the PATH, which is where
// PostgreSQL and MySQL carry the database name, while SQL Server carries it in
// a query parameter. Using the wrong one silently left the connection on
// `master`, so the fixture was created there and the second run of this test
// collided with the first.
func replaceSQLServerDatabase(c *qt.C, dbURL, database string) string {
	c.Helper()
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("database", database)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// sqlServerCheckDeclaration is the same table as a description, carrying the
// expression as it was WRITTEN.
func sqlServerCheckDeclaration(expression string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "I", Name: "items"}},
		Fields: []schemamodel.Field{
			{StructName: "I", Name: "id", Type: "int", Primary: true},
			{StructName: "I", Name: "price", Type: "decimal(10,2)"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "I", Name: "ck_price", Table: "items",
			Type: "CHECK", CheckExpression: expression,
		}},
	}
}

// storedSQLServerCheck asks the server how it spells one constraint.
func storedSQLServerCheck(c *qt.C, ctx context.Context, db *sql.DB, name string) string {
	c.Helper()
	var stored string
	err := db.QueryRowContext(ctx,
		"SELECT definition FROM sys.check_constraints WHERE name = @p1", name).Scan(&stored)
	c.Assert(err, qt.IsNil)
	return stored
}
