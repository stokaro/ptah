package migrator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// describedSession is a connection that describes itself as info, with the
// metadata stores of metadataSession behind it.
type describedSession struct {
	*metadataSession
	info catalog.ServerInfo
}

func (s describedSession) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return s, nil
}
func (s describedSession) OpenSession(context.Context) (dbschema.NativeSession, error) { return s, nil }
func (s describedSession) Info() catalog.ServerInfo                                    { return s.info }

// sqlServerConnection is a SQL Server connection working in schema, whose
// metadata schema is metadataSchema.
func sqlServerConnection(c *qt.C, schema, metadataSchema string) *dbschema.DatabaseConnection {
	c.Helper()
	info := (&metadataSession{}).Info()
	info.Dialect = "sqlserver"
	info.Schema = schema
	info.MetadataSchema = metadataSchema
	info.IdentifierSemantics = identifier.ForDialect("sqlserver")
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{
		"described": {Target: "sqlserver", Connector: describedSession{metadataSession: &metadataSession{}, info: info}},
	})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "described://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return conn
}

// TestMigrationsTableIdentifier_SQLServerMetadataSchema pins where the
// migrator keeps its tables on SQL Server when no schema is configured: in
// the connection's metadata schema, not in the schema the account works in.
// A database migrated as sa keeps its history in dbo, so an account whose
// default schema is app reads the same history rather than an empty one in
// app. A configured schema wins, and a connection that states no metadata
// schema keeps the tables in its own schema. MigrationsSchema names the
// schema of the table, which status prints.
func TestMigrationsTableIdentifier_SQLServerMetadataSchema(t *testing.T) {
	tests := []struct {
		name           string
		metadataSchema string
		configured     string
		format         migrator.RevisionTableFormat
		want           string
		wantSchema     string
	}{
		{name: "native layout in the metadata schema", metadataSchema: "dbo", format: migrator.RevisionTableFormatPtah, want: "[dbo].[schema_migrations]", wantSchema: "dbo"},
		{name: "Atlas layout in the metadata schema", metadataSchema: "dbo", format: migrator.RevisionTableFormatAtlas, want: "[dbo].[atlas_schema_revisions]", wantSchema: "dbo"},
		{name: "a schema the URL names", metadataSchema: "sales", format: migrator.RevisionTableFormatPtah, want: "[sales].[schema_migrations]", wantSchema: "sales"},
		{name: "a configured schema wins", metadataSchema: "dbo", configured: "ops", format: migrator.RevisionTableFormatPtah, want: "[ops].[schema_migrations]", wantSchema: "ops"},
		{name: "no metadata schema stated", format: migrator.RevisionTableFormatPtah, want: "[app].[schema_migrations]", wantSchema: "app"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := sqlServerConnection(c, "app", test.metadataSchema)

			mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider()).
				WithRevisionTableFormat(test.format).
				WithMigrationsTable(test.configured, "")

			c.Assert(mig.MigrationsTableIdentifier(), qt.Equals, test.want)
			c.Assert(mig.MigrationsSchema(), qt.Equals, test.wantSchema)
		})
	}
}
