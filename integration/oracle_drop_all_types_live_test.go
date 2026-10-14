//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestOracleDropAllRemovesDomainsAndTypesLive pins that the cleanup behind
// `ptah db drop-all` leaves no domain and no object type behind.
//
// It listed tables, views, sequences, synonyms, triggers and types, and no
// domain, so every domain survived it, measured on 23.26.3.0.0. A flexible
// domain names the domains it chooses from, and dropping one of those first
// answers ORA-11502; the members here sort before the flexible domain, so a
// cleanup that dropped domains by name alone fails on them.
func TestOracleDropAllRemovesDomainsAndTypesLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropOracleDropAllFixture(context.WithoutCancel(ctx), conn)
	defer dropOracleDropAllFixture(context.WithoutCancel(ctx), conn)
	for _, statement := range oracleDropAllFixture(conn.Info().Capabilities) {
		execOracle(ctx, c, conn, statement)
	}
	// Non-vacuity: the fixture is there before the cleanup runs.
	c.Assert(oracleObjectsNamed(ctx, c, conn, "DAT_%"), qt.Not(qt.HasLen), 0)

	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)

	c.Assert(oracleObjectsNamed(ctx, c, conn, "DAT_%"), qt.HasLen, 0)
}

// oracleDropAllFixture creates object types, one whose attribute names a
// domain, domains of every shape Oracle 23 has, and tables typed by them.
// Oracle 21 has no domains, so its fixture holds the object types alone.
func oracleDropAllFixture(caps capability.Capabilities) []string {
	types := []string{
		"CREATE OR REPLACE TYPE dat_point AS OBJECT (x NUMBER, y NUMBER)",
		"CREATE TABLE dat_places (id NUMBER PRIMARY KEY, at dat_point)",
	}
	if !caps.Has(capability.DomainTypes) {
		return types
	}
	return append(types,
		"CREATE DOMAIN dat_m1 AS NUMBER",
		"CREATE DOMAIN dat_m2 AS NUMBER CHECK (VALUE > 0)",
		"CREATE DOMAIN dat_email AS VARCHAR2(100) NOT NULL",
		"CREATE DOMAIN dat_color AS ENUM (red, green)",
		"CREATE DOMAIN dat_pair AS (lat AS NUMBER, lon AS NUMBER)",
		"CREATE FLEXIBLE DOMAIN dat_z_flex (a) CHOOSE DOMAIN USING (k NUMBER) FROM (CASE k WHEN 1 THEN dat_m1(a) WHEN 2 THEN dat_m2(a) END)",
		"CREATE TABLE dat_users (id NUMBER PRIMARY KEY, email dat_email, color dat_color)",
		"CREATE TABLE dat_readings (a NUMBER, k NUMBER, DOMAIN dat_z_flex(a) USING (k))",
		// Oracle creates this type with compilation errors, and records its
		// dependency on the domain all the same.
		"CREATE OR REPLACE TYPE dat_contact AS OBJECT (email dat_email)",
	)
}

// dropOracleDropAllFixture removes what a run that failed left behind,
// dependents first, tolerating what is absent.
func dropOracleDropAllFixture(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, statement := range []string{
		"DROP TABLE dat_readings PURGE", "DROP TABLE dat_users PURGE", "DROP TABLE dat_places PURGE",
		"DROP TYPE dat_contact FORCE", "DROP TYPE dat_point FORCE",
		"DROP DOMAIN dat_z_flex", "DROP DOMAIN dat_m1", "DROP DOMAIN dat_m2", "DROP DOMAIN dat_email",
		"DROP DOMAIN dat_color", "DROP DOMAIN dat_pair",
	} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, statement)
	}
}

// oracleObjectsNamed lists the account's objects whose name matches pattern,
// as "<type> <name>".
func oracleObjectsNamed(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection, pattern string) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx,
		"SELECT object_type || ' ' || object_name FROM user_objects WHERE object_name LIKE :1 ORDER BY 1", pattern)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var objects []string
	for rows.Next() {
		var object string
		c.Assert(rows.Scan(&object), qt.IsNil)
		objects = append(objects, object)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return objects
}
