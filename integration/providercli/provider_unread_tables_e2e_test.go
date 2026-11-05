//go:build integration

package providercli_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
	"ptah.run/migration/schemadiff"
)

// unreadReason is the reason the unread-tables fixture gives for the table
// orphan_replica, which its read leaves out.
const unreadReason = "orphan_replica is a replica table no replication of this database writes any more, which the fixture keeps read-only"

// unreadTablesCLI is the unread-tables fixture over a database that holds the
// described table records and the table orphan_replica, which the fixture's
// read leaves out. environment adds to the fixture's settings.
func unreadTablesCLI(c *qt.C, environment ...string) providerCLI {
	c.Helper()
	spec := fixtureReleaseSpec(c, "unread-tables", testutils.FixtureRelease{})
	spec.Environment = environment
	f := newProviderCLI(c, spec)
	host, err := providerhost.Start(c.Context(), spec)
	c.Assert(err, qt.IsNil)
	c.Cleanup(host.Close)
	ctx, err := dbschema.WithNativeConnections(c.Context(), host.Connections())
	c.Assert(err, qt.IsNil)
	connection, err := dbschema.ConnectToDatabase(ctx, f.address)
	c.Assert(err, qt.IsNil)
	c.Assert(connection.Writer().ExecuteSQL(ctx, `CREATE TABLE "records" ("Id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
	c.Assert(connection.Writer().ExecuteSQL(ctx, `CREATE TABLE "orphan_replica" ("Id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
	c.Assert(connection.Close(), qt.IsNil)
	host.Close()
	return f
}

// recordsSchema declares records as the database holds it, and declared adds
// to it.
func recordsSchema(c *qt.C, f providerCLI, declared string) {
	c.Helper()
	body := "tables:\n  records:\n    columns:\n      Id:\n        type: INTEGER\n        not_null: true\n        primary: true\n" + declared
	c.Assert(os.WriteFile(f.schemaPath, []byte(body), 0o600), qt.IsNil)
}

// The read names the table it leaves out with the provider's reason and does
// not describe it; a comparison against a schema without it plans nothing.
func TestProviderUnreadTablesRead_HappyPath(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "unread-tables")
	connection := observedConnection(c, host, "records")
	c.Assert(connection.Writer().ExecuteSQL(c.Context(), `CREATE TABLE "orphan_replica" ("Id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)

	observed, err := connection.Reader().ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables, qt.HasLen, 1)
	c.Assert(observed.Tables[0].Name, qt.Equals, "records")
	c.Assert(observed.UnreadTables, qt.HasLen, 1)
	c.Assert(observed.UnreadTables[0].Identity.Name.Source, qt.Equals, "orphan_replica")
	c.Assert(observed.UnreadTables[0].Reason, qt.Equals, unreadReason)
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "records", StructName: "Record"}},
		Fields: []schemamodel.Field{{Name: "Id", StructName: "Record", Type: "INTEGER", Primary: true}}}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, observed, connection.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// A declared table at the unread table, in another spelling of its name, is
// refused with the provider's reason before preparation. The fixture's
// contract refuses every foreign key before the comparison, so the refusal
// of a foreign key into an unread table is a unit test of the comparison.
func TestProviderUnreadTablesCompare_FailurePath(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "unread-tables")
	connection := observedConnection(c, host, "records")
	c.Assert(connection.Writer().ExecuteSQL(c.Context(), `CREATE TABLE "orphan_replica" ("Id" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "ORPHAN_REPLICA", StructName: "Orphan"}},
		Fields: []schemamodel.Field{{Name: "Id", StructName: "Orphan", Type: "INTEGER", Primary: true}}}

	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, observed, connection.Info(), nil, runtime)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `table main\.orphan_replica: `+unreadReason)
	c.Assert(diff, qt.IsNil)
}

// Through the CLI: a plan that declares the table is refused before any step
// with the provider's reason and writes no plan; one without it plans
// nothing; db read and schema inspect print the note; and the inspected
// document names nothing about the table.
func TestProviderUnreadTablesCLI(t *testing.T) {
	t.Run("a plan that declares the table", func(t *testing.T) {
		c := qt.New(t)
		f := unreadTablesCLI(c)
		recordsSchema(c, f, "  orphan_replica:\n    columns:\n      Id:\n        type: INTEGER\n        not_null: true\n        primary: true\n")

		_, stderr, code := f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

		c.Assert(code, qt.Equals, 2, qt.Commentf("%s", stderr))
		c.Assert(stderr, qt.Contains, "table main.orphan_replica: "+unreadReason)
		_, err := os.Stat(f.planPath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
	t.Run("a plan without the table", func(t *testing.T) {
		c := qt.New(t)
		f := unreadTablesCLI(c)
		recordsSchema(c, f, "")

		planned := f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")

		c.Assert(planned, qt.Contains, `"outcome": "no-changes"`)
	})
	t.Run("db read", func(t *testing.T) {
		c := qt.New(t)
		f := unreadTablesCLI(c)

		stdout, stderr, code := executeProviderCLI(c.Context(), []string{"db", "read", "--provider-config", f.manifestPath, "--db-url", f.address})

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
		c.Assert(stderr, qt.Contains, "note: 1 table this read does not describe: main.orphan_replica: "+unreadReason+"\n")
		c.Assert(stdout, qt.Contains, "records")
		c.Assert(stdout, qt.Not(qt.Contains), "orphan_replica")
	})
	t.Run("schema inspect", func(t *testing.T) {
		c := qt.New(t)
		f := unreadTablesCLI(c)

		stdout, stderr, code := executeProviderCLI(c.Context(), f.arguments("inspect", "--format", "hcl"))

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
		c.Assert(stderr, qt.Contains, "note: 1 table this read does not describe: main.orphan_replica: "+unreadReason+"\n")
		c.Assert(stdout, qt.Contains, `table "records"`)
		c.Assert(stdout, qt.Not(qt.Contains), "orphan_replica")
	})
}

// A provider that does not list the feature leaves the table out silently,
// as a provider of an earlier contract does: the read names nothing, and a
// schema that declares the table plans its creation.
func TestProviderUnreadTablesUnlisted(t *testing.T) {
	c := qt.New(t)
	f := unreadTablesCLI(c, "FIXTURE_UNREAD_TABLES=silent")
	recordsSchema(c, f, "  orphan_replica:\n    columns:\n      Id:\n        type: INTEGER\n        not_null: true\n        primary: true\n")

	planned := f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")
	_, stderr, code := executeProviderCLI(c.Context(), []string{"db", "read", "--provider-config", f.manifestPath, "--db-url", f.address})

	c.Assert(planned, qt.Contains, "orphan_replica")
	c.Assert(planned, qt.Not(qt.Contains), `"outcome": "no-changes"`)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(stderr, qt.Not(qt.Contains), "does not describe")
}
