//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// prepareEnumOrderFixture creates three enum types whose names do not arrive in
// creation order, so a read that returns creation order rather than the query's
// ORDER BY is also caught.
func prepareEnumOrderFixture(c *qt.C, ctx context.Context) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })

	schemaName := fmt.Sprintf("ptah_enum_order_%d", time.Now().UnixNano())
	dropSchema(c, conn, schemaName)
	c.Cleanup(func() { dropSchema(c, conn, schemaName) })

	statements := []string{
		fmt.Sprintf("CREATE SCHEMA %q", schemaName),
		fmt.Sprintf("CREATE TYPE %q.ptah_order_mm AS ENUM ('a','b')", schemaName),
		fmt.Sprintf("CREATE TYPE %q.ptah_order_zz AS ENUM ('c','d')", schemaName),
		fmt.Sprintf("CREATE TYPE %q.ptah_order_aa AS ENUM ('e','f')", schemaName),
	}
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute %s", statement))
	}
	return conn, schemaName
}

// prepareConstraintOrderFixture creates three named UNIQUE constraints across
// two tables, which is the shape the report reproduces with.
//
// It creates TABLES in the database it connected to rather than a database of
// its own. CI's ptah_user has no CREATE privilege at the server level -- an
// earlier version issued CREATE DATABASE, passed locally against root, and
// failed in CI with `Access denied for user 'ptah_user'@'%'`. The prefix is what
// separates this fixture's constraints from everything else already in that
// shared database.
func prepareConstraintOrderFixture(
	c *qt.C,
	ctx context.Context,
	engine dbtarget.Engine,
) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })

	prefix := fmt.Sprintf("ptah_con_order_%d", time.Now().UnixNano())
	dropOrderTables(c, conn, prefix)
	c.Cleanup(func() { dropOrderTables(c, conn, prefix) })

	statements := []string{
		fmt.Sprintf(`CREATE TABLE %s_users (
			id BIGINT NOT NULL PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			code VARCHAR(64) NOT NULL,
			CONSTRAINT uq_%[1]s_email UNIQUE (email),
			CONSTRAINT uq_%[1]s_code UNIQUE (code))`, prefix),
		fmt.Sprintf(`CREATE TABLE %s_orders (
			id BIGINT NOT NULL PRIMARY KEY,
			ref VARCHAR(64) NOT NULL,
			CONSTRAINT uq_%[1]s_ref UNIQUE (ref))`, prefix),
	}
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute %s", statement))
	}
	return conn, prefix
}

func dropOrderTables(c *qt.C, conn *dbschema.DatabaseConnection, prefix string) {
	c.Helper()
	for _, suffix := range []string{"_users", "_orders"} {
		_, err := conn.ExecContext(context.Background(),
			fmt.Sprintf("DROP TABLE IF EXISTS %s%s", prefix, suffix))
		c.Check(err, qt.IsNil)
	}
}

func dropSchema(c *qt.C, conn *dbschema.DatabaseConnection, name string) {
	c.Helper()
	_, err := conn.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", name))
	c.Check(err, qt.IsNil)
}
