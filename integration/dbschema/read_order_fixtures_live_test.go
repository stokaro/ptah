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
func prepareConstraintOrderFixture(
	c *qt.C,
	ctx context.Context,
	engine dbtarget.Engine,
) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })

	schemaName := fmt.Sprintf("ptah_con_order_%d", time.Now().UnixNano())
	dropDatabase(c, conn, schemaName)
	c.Cleanup(func() { dropDatabase(c, conn, schemaName) })

	statements := []string{
		fmt.Sprintf("CREATE DATABASE %s", schemaName),
		fmt.Sprintf(`CREATE TABLE %s.users (
			id BIGINT NOT NULL PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			code VARCHAR(64) NOT NULL,
			CONSTRAINT uq_users_email UNIQUE (email),
			CONSTRAINT uq_users_code UNIQUE (code))`, schemaName),
		fmt.Sprintf(`CREATE TABLE %s.orders (
			id BIGINT NOT NULL PRIMARY KEY,
			ref VARCHAR(64) NOT NULL,
			CONSTRAINT uq_orders_ref UNIQUE (ref))`, schemaName),
	}
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute %s", statement))
	}
	return conn, schemaName
}

func dropSchema(c *qt.C, conn *dbschema.DatabaseConnection, name string) {
	c.Helper()
	_, err := conn.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", name))
	c.Check(err, qt.IsNil)
}

func dropDatabase(c *qt.C, conn *dbschema.DatabaseConnection, name string) {
	c.Helper()
	_, err := conn.ExecContext(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", name))
	c.Check(err, qt.IsNil)
}
