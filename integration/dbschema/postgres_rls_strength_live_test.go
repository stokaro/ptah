//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// rlsStrengthSchema declares one table and one policy over it, at the strength
// the arguments name.
func rlsStrengthSchema(schemaName string, forced, restrictive bool) *schemamodel.Database {
	composition := map[bool]pgpolicy.Composition{true: pgpolicy.Restrictive, false: pgpolicy.Permissive}[restrictive]
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Doc", Name: "docs", Schema: schemaName,
		}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Doc", Name: "tenant", Type: "TEXT"},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(
			pgpolicy.PolicyRef(schemaName, "docs", "docs_tenant"),
			pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}},
				Using: new("true"), Composition: composition})))),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)),
	}
	db.Tables[0].Facets = must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: true, Forced: forced}))
	return db
}

// readPolicy returns the one policy a read reported, as the row-security owner
// observed it.
func readPolicy(c *qt.C, live *catalog.Database) *pgpolicy.ObservedPolicy {
	c.Helper()
	objects := must.Must(live.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	return objects[0].Value.(*pgpolicy.ObservedPolicy)
}

// readSwitches returns the row-level security switches a read reported for
// table.
func readSwitches(c *qt.C, table catalog.Table) *pgpolicy.ObservedTableState {
	c.Helper()
	switches, found, err := schemaext.FacetAs[*pgpolicy.ObservedTableState](table.Facets, pgpolicy.TableStateKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return switches
}

// rowSecurityChanges returns the changes the row-security owner planned,
// wherever the diff carries them.
func rowSecurityChanges(diff *difftypes.SchemaDiff) []schemaext.ChangeRecord {
	changes := slices.Clone(diff.FeatureChanges)
	for _, table := range diff.TablesModified {
		changes = append(changes, table.FeatureChanges...)
	}
	return slices.DeleteFunc(changes, func(record schemaext.ChangeRecord) bool {
		kind := record.Value.Kind()
		return kind != pgpolicy.PolicyChangeKind && kind != pgpolicy.TableStateChangeKind
	})
}

// tableNamed returns the table the read reports under the given name.
func tableNamed(c *qt.C, tables []catalog.Table, name string) catalog.Table {
	for _, table := range tables {
		if table.Name == name {
			return table
		}
	}
	c.Fatalf("no table named %q in the read schema", name)
	return catalog.Table{}
}

// TestPostgresLiveRLSStrengthConverges is the test the two strength flags could
// not have been added without.
//
// Each step is one only a server answers:
//
//  1. `AS RESTRICTIVE` and `FORCE ROW LEVEL SECURITY` are statements this
//     engine accepts. A renderer that put the AS clause in the wrong place --
//     PostgreSQL takes it between the table and FOR, and nowhere else -- fails
//     here, where an offline test would have compared strings and passed.
//  2. The catalog reports both flags back. The reader projects
//     `NOT pol.polpermissive` and `pg_class.relforcerowsecurity`, and a fake
//     server answers by column name, so it would agree with a column that does
//     not exist. Only a real catalog says these two do.
//  3. Comparing the same declaration against what the server now holds finds
//     nothing to do. Without the read-back the restrictive policy would be
//     planned again on every run, and each apply would leave the table
//     permissive between the DROP and the CREATE.
func TestPostgresLiveRLSStrengthConverges(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_rlsstr_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	description := rlsStrengthSchema(schemaName, true, true)

	// 1. The rendered statements are the ones the server is given.
	statements, err := builtin.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	joined := strings.Join(statements, "\n")
	c.Assert(joined, qt.Contains, "FORCE ROW LEVEL SECURITY")
	c.Assert(joined, qt.Contains, "AS RESTRICTIVE")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The catalog is asked what it holds.
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(readPolicy(c, live).Composition, qt.Equals, pgpolicy.Restrictive)
	c.Assert(readSwitches(c, tableNamed(c, live.Tables, "docs")), qt.DeepEquals, &pgpolicy.ObservedTableState{Enabled: true, Forced: true})

	// 3. The convergence assertion.
	settled := must.Must(schemadiff.CompareWithDialect(t.Context(), description, live, platform.Postgres, must.Must(builtin.New())))
	c.Assert(rowSecurityChanges(settled), qt.HasLen, 0)
}

// TestPostgresLiveRLSStrengthReadsBackTheWeakerHalf is the control for the read
// above.
//
// A reader that reported both flags as set would satisfy every assertion in
// that test while telling the comparison nothing. The weaker declaration has to
// come back as the weaker one, from the same two projections.
func TestPostgresLiveRLSStrengthReadsBackTheWeakerHalf(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_rlsweak_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	description := rlsStrengthSchema(schemaName, false, false)

	statements, err := builtin.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	joined := strings.Join(statements, "\n")
	c.Assert(joined, qt.Not(qt.Contains), "FORCE ROW LEVEL SECURITY")
	c.Assert(joined, qt.Not(qt.Contains), "AS RESTRICTIVE")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(readPolicy(c, live).Composition, qt.Equals, pgpolicy.Permissive)
	c.Assert(readSwitches(c, tableNamed(c, live.Tables, "docs")), qt.DeepEquals, &pgpolicy.ObservedTableState{Enabled: true})

	settled := must.Must(schemadiff.CompareWithDialect(t.Context(), description, live, platform.Postgres, must.Must(builtin.New())))
	c.Assert(rowSecurityChanges(settled), qt.HasLen, 0)
}

// TestPostgresLiveRLSStrengthDifferenceIsPlanned pins that a strength the
// server does not have is a difference rather than silence.
//
// The convergence test above passes just as well when nothing is compared at
// all, so this drives the opposite case: a database holding the weaker policy
// against a declaration asking for the stronger one has to produce a change.
func TestPostgresLiveRLSStrengthDifferenceIsPlanned(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_rlsdiff_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	// The server is given the permissive, unforced schema.
	statements, err := builtin.GetOrderedCreateStatements(
		rlsStrengthSchema(schemaName, false, false), platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)

	// The declaration asks for the stronger one.
	diff := must.Must(schemadiff.CompareWithDialect(
		t.Context(), rlsStrengthSchema(schemaName, true, true), live, platform.Postgres, must.Must(builtin.New())))

	// One change for the policy's AS clause and one for FORCE.
	kinds := make([]schemaext.Kind, 0, 2)
	for _, change := range rowSecurityChanges(diff) {
		kinds = append(kinds, change.Value.Kind())
	}
	slices.Sort(kinds)
	c.Assert(kinds, qt.DeepEquals, []schemaext.Kind{pgpolicy.PolicyChangeKind, pgpolicy.TableStateChangeKind})
}
