package atlasschema

// White-box testing required: the scope rule is applied by the creation plan
// Inspect builds from a live read, and Inspect takes a live connection. A
// read holding an extension and a grant on the database is a PostgreSQL or
// YDB read, so a black-box test needs a server; the live half is
// TestCompatScopedInspectPlanLeavesOutTheDatabaseObjectsPostgres.

import (
	"bytes"
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/convert/dbschematogo"
)

// scopedPlanRead is a PostgreSQL read of schema app that holds what belongs
// to the database: pgcrypto installed in public, citext installed in app, and
// plpgsql, which the comparison ignores.
func scopedPlanRead(c *qt.C) *catalog.Database {
	c.Helper()
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app"}},
		Tables: []catalog.Table{{Schema: "app", Name: "users", Columns: []catalog.Column{
			{Name: "id", DataType: "integer", UDTName: "int4"},
		}}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			must.Must(pgextension.ObservedObject(pgextension.Ref("pgcrypto"), pgextension.ObservedExtension{Schema: "public", Version: "1.4"})),
			must.Must(pgextension.ObservedObject(pgextension.Ref("citext"), pgextension.ObservedExtension{Schema: "app", Version: "1.6"})),
			must.Must(pgextension.ObservedObject(pgextension.Ref("plpgsql"), pgextension.ObservedExtension{Schema: "pg_catalog", Version: "1.0"})),
		)),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
}

// planScopedRead writes the creation plan of read under scope, and the notes
// the plan wrote.
func planScopedRead(c *qt.C, read *catalog.Database, scope creationScope) (statements, notes string, err error) {
	c.Helper()
	runtime := must.Must(builtin.New())
	info := catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", IdentifierSemantics: identifier.ForDialect(platform.Postgres)}
	desired := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(context.Background(), read, platform.Postgres, runtime))
	var diagnostics bytes.Buffer
	changes, err := inspectCreationPlan(runtime, desired, read, info, scope, &diagnostics)(context.Background())
	var cmds []string
	for _, change := range changes {
		cmds = append(cmds, change.Cmd)
	}
	return strings.Join(cmds, "\n"), diagnostics.String(), err
}

// TestInspectCreationPlan_ScopedReadLeavesOutTheDatabase pins the rule for a
// read of some schemas: the plan creates the schema, its table and the
// extension installed in it, and leaves out the extension installed in public,
// which one note names. plpgsql is planned by no scope and is not named.
func TestInspectCreationPlan_ScopedReadLeavesOutTheDatabase(t *testing.T) {
	c := qt.New(t)

	statements, notes, err := planScopedRead(c, scopedPlanRead(c), creationScope{describeSchemas: true, partial: true})

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Equals, `CREATE SCHEMA IF NOT EXISTS "app"`+"\n"+
		`CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "app" VERSION '1.6'`+"\n"+
		`CREATE TABLE "app"."users" (`+"\n"+`  "id" integer NOT NULL`+"\n)")
	c.Assert(notes, qt.Equals, "note: the read covers some schemas of the database, so the plan creates what they hold and leaves out "+
		`what belongs to the database: extension "pgcrypto" in schema "public"; inspect the whole database to plan them`+"\n")
}

// TestInspectCreationPlan_WholeReadPlansTheDatabase pins the other half: a
// read of the whole database plans the extension installed in public too, and
// writes no note.
func TestInspectCreationPlan_WholeReadPlansTheDatabase(t *testing.T) {
	c := qt.New(t)

	statements, notes, err := planScopedRead(c, scopedPlanRead(c), creationScope{describeSchemas: true})

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Equals, `CREATE SCHEMA IF NOT EXISTS "app"`+"\n"+
		`CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "app" VERSION '1.6'`+"\n"+
		`CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "public" VERSION '1.4'`+"\n"+
		`CREATE TABLE "app"."users" (`+"\n"+`  "id" integer NOT NULL`+"\n)")
	c.Assert(notes, qt.Equals, "")
}

// TestDatabaseObjectsOutsideScope_GrantsOnTheDatabase pins the other object
// a scoped read leaves out, which no built-in dialect reads: the grants on the
// database, named by role in one note, while a grant on a schema stays in the
// plan.
func TestDatabaseObjectsOutsideScope_GrantsOnTheDatabase(t *testing.T) {
	c := qt.New(t)
	read := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "dir"}},
		Grants: []catalog.Grant{
			{Role: "USERS", Privilege: "ydb.database.connect", ObjectType: "DATABASE"},
			{Role: "ACCESS-ADMINS", Privilege: "ydb.access.grant", ObjectType: "DATABASE"},
			{Role: "USERS", Privilege: "ydb.generic.list", ObjectType: "database"},
			{Role: "app", Privilege: "ydb.generic.list", ObjectType: "SCHEMA", ObjectName: "dir"},
		},
	}

	found, err := databaseObjectsOutsideScope(read, config.DefaultCompareOptions())

	c.Assert(err, qt.IsNil)
	c.Assert(found.grants, qt.DeepEquals, read.Grants[:3])
	var notes bytes.Buffer
	found.report(&notes)
	c.Assert(notes.String(), qt.Equals, "note: the read covers some schemas of the database, so the plan creates what they hold and leaves out "+
		`what belongs to the database: the grants on the database to "ACCESS-ADMINS", "USERS"; inspect the whole database to plan them`+"\n")
}
