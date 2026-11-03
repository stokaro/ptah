package schemaclean_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/schemaclean"
)

// The provider here serves the engine "ydb", whose built-in descriptor says
// the revision tables are found by name through the writer. The provider's
// writer cannot do that, so a plan that read the descriptor by the dialect's
// name instead of asking the provider fails with "cannot find tables by name"
// rather than naming the provider's objects.

// providerWriter is the writer of a provider's session: it lists what the
// provider's cleanup removes and runs that cleanup, as internal/providerhost
// does through the provider's dev cleanup.
type providerWriter struct {
	baseWriter
	offers  bool
	listed  []dbreset.Object
	listErr error
	lists   int
}

func (w *providerWriter) OffersCleanup() bool { return w.offers }
func (w *providerWriter) CleanupObjects(context.Context) ([]dbreset.Object, error) {
	w.lists++
	return w.listed, w.listErr
}

// baseWriter is a provider session's writer with no cleanup to offer. It
// counts the cleanups that ran, and runs no statement.
type baseWriter struct {
	dry        bool
	cleanups   int
	statements []string
}

func (w *baseWriter) ExecuteSQL(_ context.Context, sql string, _ ...any) error {
	w.statements = append(w.statements, sql)
	return nil
}
func (w *baseWriter) IsDryRun() bool       { return w.dry }
func (w *baseWriter) SetDryRun(value bool) { w.dry = value }
func (w *baseWriter) DropAllTables(context.Context) error {
	if !w.dry {
		w.cleanups++
	}
	return nil
}
func (*baseWriter) BeginTransaction(context.Context) (catalog.SchemaTransaction, error) {
	return nil, ptaherr.ErrUnsupportedFeature
}

// providerSession is the session a provider opens, with writer as its writer
// and reader as its reader.
type providerSession struct {
	writer catalog.SchemaWriter
	reader catalog.SchemaReader
}

func (s providerSession) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: "ydb", IdentifierSemantics: identifier.ForDialect("ydb"), Provider: catalog.ProviderIdentity{
		ID: "example.provider", Version: "test", SemanticIdentity: "test-1", ProtocolRevision: 2,
		ArtifactSHA256: strings.Repeat("a", 64), ContractSHA256: strings.Repeat("b", 64),
	}}
}
func (s providerSession) Reader([]string) catalog.SchemaReader { return s.reader }
func (s providerSession) Writer() catalog.SchemaWriter         { return s.writer }
func (s providerSession) OpenSession(context.Context) (dbschema.NativeSession, error) {
	return s, nil
}
func (providerSession) RestrictUntrustedSQL(context.Context) error {
	return ptaherr.ErrUnsupportedFeature
}
func (providerSession) Close() error { return nil }

// failingReader is a provider's reader that cannot describe the database,
// as a provider whose inspection refuses an object its cleanup can still
// name.
type failingReader struct{}

func (failingReader) ReadSchema() (*catalog.Database, error) {
	return nil, errors.New("provider inspect failed")
}
func (failingReader) ReadSchemaContext(context.Context) (*catalog.Database, error) {
	return nil, errors.New("provider inspect failed")
}

type providerConnector struct {
	writer catalog.SchemaWriter
	reader catalog.SchemaReader
}

func (p providerConnector) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return providerSession(p), nil
}

// connectProvider opens a connection to a database the provider whose
// session writer is writer serves. Its reader cannot describe the database:
// a plan is the provider's list, and does not depend on a reading.
func connectProvider(c *qt.C, writer catalog.SchemaWriter) *dbschema.DatabaseConnection {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{
		"provided": {Target: "ydb", Connector: providerConnector{writer: writer, reader: failingReader{}}},
	})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "provided://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return conn
}

// providerListing is what the provider's cleanup removes: a table at the
// root, its revision table and a topic in a directory.
func providerListing() []dbreset.Object {
	return []dbreset.Object{
		{Kind: "table", Name: "users"},
		{Kind: "topic", Schema: "app", Name: "events"},
		{Kind: "table", Name: "schema_migrations"},
	}
}

func TestProviderCleanup_HappyPath(t *testing.T) {
	t.Run("the plan is the provider's list, each object under its own kind and with no statement", func(t *testing.T) {
		c := qt.New(t)
		writer := &providerWriter{offers: true, listed: providerListing()}

		plan, err := schemaclean.Inspect(c.Context(), connectProvider(c, writer))

		c.Assert(err, qt.IsNil)
		c.Assert(plan.Objects, qt.DeepEquals, []schemaclean.Object{
			{Type: "table", Name: "schema_migrations"},
			{Type: "table", Name: "users"},
			{Type: "topic", Schema: "app", Name: "events"},
		})
		c.Assert(plan.Changes, qt.DeepEquals, []schemaclean.Change{
			{Type: "table", Name: "schema_migrations"},
			{Type: "table", Name: "users"},
			{Type: "topic", Schema: "app", Name: "events"},
		})
		c.Assert(writer.lists, qt.Equals, 1)
		c.Assert(writer.statements, qt.IsNil)
	})
	t.Run("a dry run removes nothing, and a run hands the cleanup to the provider", func(t *testing.T) {
		c := qt.New(t)
		writer := &providerWriter{offers: true, listed: providerListing()}
		conn := connectProvider(c, writer)

		dry, err := schemaclean.Execute(c.Context(), conn, schemaclean.Options{DryRun: true})
		c.Assert(err, qt.IsNil)
		c.Assert(dry.Objects, qt.HasLen, 3)
		c.Assert(writer.cleanups, qt.Equals, 0)

		ran, err := schemaclean.Execute(c.Context(), conn, schemaclean.Options{})
		c.Assert(err, qt.IsNil)
		c.Assert(ran.Objects, qt.DeepEquals, dry.Objects)
		c.Assert(writer.cleanups, qt.Equals, 1)
		c.Assert(writer.statements, qt.IsNil)
	})
	t.Run("a plan naming every object the provider lists runs the provider's cleanup", func(t *testing.T) {
		c := qt.New(t)
		writer := &providerWriter{offers: true, listed: providerListing()}
		conn := connectProvider(c, writer)
		plan, err := schemaclean.Inspect(c.Context(), conn)
		c.Assert(err, qt.IsNil)

		c.Assert(schemaclean.ApplyPlan(c.Context(), conn, plan), qt.IsNil)

		c.Assert(writer.cleanups, qt.Equals, 1)
		c.Assert(writer.statements, qt.IsNil)
	})
	t.Run("a narrowed plan carries no statement either", func(t *testing.T) {
		c := qt.New(t)
		writer := &providerWriter{offers: true, listed: providerListing()}
		plan, err := schemaclean.Inspect(c.Context(), connectProvider(c, writer))
		c.Assert(err, qt.IsNil)

		narrowed := plan.WithObjects(plan.Objects[1:2], "ydb")

		c.Assert(narrowed.Changes, qt.DeepEquals, []schemaclean.Change{{Type: "table", Name: "users"}})
	})
}

func TestProviderCleanup_FailurePath(t *testing.T) {
	const noCleanup = `unsupported feature: provider "example\.provider" serves the database and offers no database cleanup ` +
		`\(database\.dev-cleanup\), so Ptah cannot list or drop what the database holds`
	t.Run("a provider that offers no cleanup is refused before it is asked to list", func(t *testing.T) {
		c := qt.New(t)
		writer := &providerWriter{listed: providerListing()}
		conn := connectProvider(c, writer)

		plan, err := schemaclean.Inspect(c.Context(), conn)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, noCleanup)
		c.Assert(plan.Objects, qt.IsNil)

		_, err = schemaclean.Execute(c.Context(), conn, schemaclean.Options{})
		c.Assert(err, qt.ErrorMatches, noCleanup)
		c.Assert(writer.lists, qt.Equals, 0)
		c.Assert(writer.cleanups, qt.Equals, 0)
	})
	t.Run("a session writer that lists nothing it removes is refused the same way", func(t *testing.T) {
		c := qt.New(t)
		writer := &baseWriter{}

		plan, err := schemaclean.Inspect(c.Context(), connectProvider(c, writer))

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, noCleanup)
		c.Assert(plan.Objects, qt.IsNil)
		c.Assert(writer.cleanups, qt.Equals, 0)
	})
	t.Run("an object the provider cannot remove refuses the plan, since the cleanup would remove nothing", func(t *testing.T) {
		c := qt.New(t)
		writer := &providerWriter{offers: true, listed: append(providerListing(),
			dbreset.Object{Kind: "view", Schema: "app", Name: "recent", Refusal: "Ptah has no statement to drop a view"})}
		conn := connectProvider(c, writer)

		plan, err := schemaclean.Inspect(c.Context(), conn)
		c.Assert(err, qt.ErrorMatches, `inspect schema before cleanup: provider "example\.provider" would remove nothing, `+
			`because it cannot remove view "recent" in schema "app": Ptah has no statement to drop a view`)
		c.Assert(plan.Objects, qt.IsNil)

		_, err = schemaclean.Execute(c.Context(), conn, schemaclean.Options{})
		c.Assert(err, qt.ErrorMatches, `inspect schema before cleanup: provider "example\.provider" would remove nothing, .*`)
		c.Assert(writer.cleanups, qt.Equals, 0)
	})
	t.Run("a listing that fails is the plan's error", func(t *testing.T) {
		c := qt.New(t)
		listing := errors.New("the provider did not answer")
		writer := &providerWriter{offers: true, listErr: listing}

		plan, err := schemaclean.Inspect(c.Context(), connectProvider(c, writer))

		c.Assert(err, qt.ErrorIs, listing)
		c.Assert(err, qt.ErrorMatches, `inspect schema before cleanup: the provider did not answer`)
		c.Assert(plan.Objects, qt.IsNil)
	})
	t.Run("a narrowed plan is refused, since the provider removes everything at once", func(t *testing.T) {
		c := qt.New(t)
		writer := &providerWriter{offers: true, listed: providerListing()}
		conn := connectProvider(c, writer)
		plan, err := schemaclean.Inspect(c.Context(), conn)
		c.Assert(err, qt.IsNil)

		err = schemaclean.ApplyPlan(c.Context(), conn, plan.WithObjects(plan.Objects[1:], "ydb"))

		c.Assert(err, qt.ErrorMatches, `provider "example\.provider" removes everything the database holds at once `+
			`and cannot run a plan that names part of it: the plan leaves out table "schema_migrations"`)
		c.Assert(writer.cleanups, qt.Equals, 0)
	})
	t.Run("a plan naming an object the provider no longer lists is refused", func(t *testing.T) {
		c := qt.New(t)
		writer := &providerWriter{offers: true, listed: providerListing()}
		conn := connectProvider(c, writer)
		plan, err := schemaclean.Inspect(c.Context(), conn)
		c.Assert(err, qt.IsNil)
		writer.listed = providerListing()[:2]

		err = schemaclean.ApplyPlan(c.Context(), conn, plan)

		c.Assert(err, qt.ErrorMatches, `provider "example\.provider" removes everything the database holds at once `+
			`and cannot run a plan that names part of it: the provider does not list table "schema_migrations"`)
		c.Assert(writer.cleanups, qt.Equals, 0)
	})
}

// TestChangeSummary pins how a report names a change: by its statement where
// it has one, and by its kind and place where it has none.
func TestChangeSummary(t *testing.T) {
	tests := []struct {
		name   string
		change schemaclean.Change
		want   string
	}{
		{"a statement", schemaclean.Change{Type: "table", Name: "users", Cmd: `DROP TABLE IF EXISTS "users"`}, `DROP TABLE IF EXISTS "users"`},
		{"an object at the root", schemaclean.Change{Type: "table", Name: "users"}, `table "users"`},
		{"an object in a schema", schemaclean.Change{Type: "column table", Schema: "app", Name: "facts"}, `column table "facts" in schema "app"`},
		{"an object on a table", schemaclean.Change{Type: "foreign_key", Schema: "public", Table: "posts", Name: "fk"},
			`foreign_key "fk" on table "posts" in schema "public"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.Summary(), qt.Equals, test.want)
		})
	}
}
