package atlasschema

// White-box testing required: the scope rule is applied by the creation plan
// Inspect builds from a live read, and Inspect takes a live connection. A
// read holding an extension and a grant on the database is a PostgreSQL or
// YDB read, so a black-box test needs a server; the live half is
// TestCompatScopedInspectPlanLeavesOutTheDatabaseObjectsE2E.

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

// scopedPlanNote is the note the plan of scopedPlanRead writes when the read
// covers part of the database.
const scopedPlanNote = "note: the read covers some schemas of the database, so the plan creates what they hold and leaves out " +
	`what belongs to the database: extension "pgcrypto" in schema "public"; inspect the whole database to plan them` + "\n"

// appOnly and appAndPublic are the schemas a read of app and a whole read
// hold.
var (
	appOnly      = []catalog.Schema{{Name: "app"}}
	appAndPublic = []catalog.Schema{{Name: "app"}, {Name: "public"}}
)

// TestCreationPlanOf_ScopeFromTheRun pins how the run's own scope reaches the
// plan: the connection URL, --schema and the selectors, read off a real
// PostgreSQL URL rather than a scope built by hand.
func TestCreationPlanOf_ScopeFromTheRun(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		connected string
		schemas   []string
		include   []string
		exclude   []string
		// read are the schemas the read holds: a selector keeps every schema
		// of a whole read, and --schema or a URL keeps the one it names.
		read      []catalog.Schema
		want      string
		wantNotes string
	}{
		{name: "a URL whose search_path names app", url: "postgres://u@h/shop?search_path=app", connected: "app", read: appOnly,
			want: `CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "app" VERSION '1.6'` + "\n" +
				`CREATE TABLE "app"."users" (` + "\n" + `  "id" integer NOT NULL` + "\n)",
			wantNotes: scopedPlanNote},
		{name: "--schema app on a whole URL", url: "postgres://u@h/shop", connected: "public", schemas: []string{"app"}, read: appOnly,
			want: `CREATE SCHEMA IF NOT EXISTS "app"` + "\n" +
				`CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "app" VERSION '1.6'` + "\n" +
				`CREATE TABLE "app"."users" (` + "\n" + `  "id" integer NOT NULL` + "\n)",
			wantNotes: scopedPlanNote},
		{name: "--include on a whole URL keeps public, and pgcrypto in it", url: "postgres://u@h/shop", connected: "public",
			include: []string{"app.*"}, read: appAndPublic,
			want: `CREATE SCHEMA IF NOT EXISTS "app"` + "\n" +
				`CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "app" VERSION '1.6'` + "\n" +
				`CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "public" VERSION '1.4'` + "\n" +
				`CREATE TABLE "app"."users" (` + "\n" + `  "id" integer NOT NULL` + "\n)"},
		{name: "a whole URL", url: "postgres://u@h/shop", connected: "public", read: appAndPublic,
			want: `CREATE SCHEMA IF NOT EXISTS "app"` + "\n" +
				`CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "app" VERSION '1.6'` + "\n" +
				`CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "public" VERSION '1.4'` + "\n" +
				`CREATE TABLE "app"."users" (` + "\n" + `  "id" integer NOT NULL` + "\n)"},
		{name: "--exclude on a whole URL", url: "postgres://u@h/shop", connected: "public", exclude: []string{"app.orders"}, read: appAndPublic,
			want: `CREATE SCHEMA IF NOT EXISTS "app"` + "\n" +
				`CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "app" VERSION '1.6'` + "\n" +
				`CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "public" VERSION '1.4'` + "\n" +
				`CREATE TABLE "app"."users" (` + "\n" + `  "id" integer NOT NULL` + "\n)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			read := scopedPlanRead(c)
			read.Schemas = test.read
			info := catalog.ServerInfo{Dialect: platform.Postgres, URL: test.url, Schema: test.connected,
				IdentifierSemantics: identifier.ForDialect(platform.Postgres)}
			desired := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(context.Background(), read, platform.Postgres, runtime))
			var diagnostics bytes.Buffer
			opts := InspectOptions{SQLCreationPlan: runtime, Schemas: test.schemas, Include: test.include, Exclude: test.exclude, Diagnostics: &diagnostics}

			changes, err := creationPlanOf(opts, desired, read, info)(context.Background())

			c.Assert(err, qt.IsNil)
			var cmds []string
			for _, change := range changes {
				cmds = append(cmds, change.Cmd)
			}
			c.Assert(strings.Join(cmds, "\n"), qt.Equals, test.want)
			c.Assert(diagnostics.String(), qt.Equals, test.wantNotes)
		})
	}
}

// TestReadIsPartial pins the URL forms a read is partial by on the dialects
// that limit a connection to one schema, and on one that limits nothing.
func TestReadIsPartial(t *testing.T) {
	tests := []struct {
		name    string
		info    catalog.ServerInfo
		schemas []string
		include []string
		exclude []string
		want    bool
	}{
		{name: "a MySQL URL that names a database", info: catalog.ServerInfo{Dialect: platform.MySQL, URL: "mysql://u@h:3306/shop", Schema: "shop"}, want: true},
		{name: "a MySQL URL of the whole server", info: catalog.ServerInfo{Dialect: platform.MySQL, URL: "mysql://u@h:3306/"}},
		{name: "a PostgreSQL URL whose search_path names app", info: catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://u@h/shop?search_path=app"}, want: true},
		{name: "a whole PostgreSQL URL", info: catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://u@h/shop"}},
		{name: "a YDB URL, which names a database", info: catalog.ServerInfo{Dialect: "ydb", URL: "ydb://h:2136/local"}},
		{name: "--schema on a YDB URL", info: catalog.ServerInfo{Dialect: "ydb", URL: "ydb://h:2136/local"}, schemas: []string{"dir"}, want: true},
		{name: "--include on a YDB URL", info: catalog.ServerInfo{Dialect: "ydb", URL: "ydb://h:2136/local"}, include: []string{"dir.*"}, want: true},
		{name: "--exclude on a YDB URL", info: catalog.ServerInfo{Dialect: "ydb", URL: "ydb://h:2136/local"}, exclude: []string{"dir.t"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(readIsPartial(InspectOptions{Schemas: test.schemas, Include: test.include, Exclude: test.exclude}, test.info), qt.Equals, test.want)
		})
	}
}

// ydbRead is a read of YDB directory dir in database /local that holds the
// grants on the database local-ydb makes.
func ydbRead() *catalog.Database {
	return &catalog.Database{
		DatabasePath: "/local",
		Schemas:      []catalog.Schema{{Name: "dir"}},
		Grants: []catalog.Grant{
			{Role: "ACCESS-ADMINS", Privilege: "ydb.access.grant", ObjectType: "DATABASE"},
			{Role: "app", Privilege: "ydb.generic.list", ObjectType: "SCHEMA", ObjectName: "dir"},
		},
	}
}

// TestNewCreationDatabase_TheInspectedDatabasePath pins the database a plan
// starts from: it has the path of the inspected database, which a YDB grant
// on the database names, and a partial read gives it the grant on the
// database so that the plan creates none. A whole read gives it none, and the
// grant is planned against the path.
func TestNewCreationDatabase_TheInspectedDatabasePath(t *testing.T) {
	tests := []struct {
		name       string
		partial    bool
		wantGrants []catalog.Grant
	}{
		{name: "a whole read"},
		{name: "a partial read", partial: true, wantGrants: ydbRead().Grants[:1]},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			info := catalog.ServerInfo{Dialect: "ydb", Schema: "dir"}

			got, leftOut, err := newCreationDatabase(ydbRead(), info, creationScope{describeSchemas: true, partial: test.partial}, config.DefaultCompareOptions())

			c.Assert(err, qt.IsNil)
			c.Assert(got.DatabasePath, qt.Equals, "/local")
			c.Assert(got.Grants, qt.DeepEquals, test.wantGrants)
			c.Assert(leftOut.grants, qt.DeepEquals, test.wantGrants)
		})
	}
}

// TestInspectCreationPlan_PlansOnce pins that a template calling the helper
// twice is answered twice from one plan, with one note.
func TestInspectCreationPlan_PlansOnce(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	read := scopedPlanRead(c)
	info := catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", IdentifierSemantics: identifier.ForDialect(platform.Postgres)}
	desired := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(context.Background(), read, platform.Postgres, runtime))
	var diagnostics bytes.Buffer
	plan := inspectCreationPlan(runtime, desired, read, info, creationScope{describeSchemas: true, partial: true}, &diagnostics)

	first, firstErr := plan(context.Background())
	second, secondErr := plan(context.Background())

	c.Assert(firstErr, qt.IsNil)
	c.Assert(secondErr, qt.IsNil)
	c.Assert(second, qt.DeepEquals, first)
	c.Assert(diagnostics.String(), qt.Equals, scopedPlanNote)
}
