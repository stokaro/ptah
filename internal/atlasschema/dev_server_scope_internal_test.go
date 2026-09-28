package atlasschema

// White-box testing required: scopeOnDevServer runs only after both sides of a
// diff are resolved, which through the exported API takes a live server for
// the database side and a dev server for the other. The decision itself reads
// two resolved states and crosses no boundary, so its rows are pinned here and
// the e2e tests drive the path that joins it to the servers.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlassource"
)

// document is a schema file state declaring the named databases.
func document(names ...string) atlassource.State {
	schemas := make([]schemamodel.Schema, 0, len(names))
	for _, name := range names {
		schemas = append(schemas, schemamodel.Schema{Name: name})
	}
	return atlassource.State{Kind: atlassource.KindLocalFile, Schema: &schemamodel.Database{Schemas: schemas}}
}

var (
	wholeServer = atlassource.State{Kind: atlassource.KindDatabase, WholeServer: true}
	appDatabase = atlassource.State{Kind: atlassource.KindDatabase, DefaultSchema: "app"}
)

// TestScopeOnDevServer_HappyPath reads each side beside a dev server: a
// document or a replayed directory is a whole server, a database keeps its
// URL's scope, and a document beside one database declaring at most one is
// compared with that database. Without a dev server nothing changes.
func TestScopeOnDevServer_HappyPath(t *testing.T) {
	rows := []struct {
		name                     string
		from, to                 atlassource.State
		devServer                bool
		wantFromWhole, wantWhole bool
	}{
		{name: "a server and a document", from: wholeServer, to: document("app", "more"), devServer: true,
			wantFromWhole: true, wantWhole: true},
		{name: "a replayed directory and a document",
			from: atlassource.State{Kind: atlassource.KindMigrationDir}, to: document("app", "more"), devServer: true,
			wantFromWhole: true, wantWhole: true},
		{name: "a document and a server", from: document("app"), to: wholeServer, devServer: true,
			wantFromWhole: true, wantWhole: true},
		{name: "one database and a document declaring it", from: appDatabase, to: document("app"), devServer: true},
		{name: "one database and a document declaring none", from: appDatabase, to: document(), devServer: true},
		{name: "a document declaring one database and one database", from: document("app"), to: appDatabase, devServer: true},
		{name: "no dev server", from: wholeServer, to: document("app", "more"), wantFromWhole: true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			from, to, err := scopeOnDevServer(row.from, row.to, devServerSides{server: row.devServer})

			c.Assert(err, qt.IsNil)
			c.Assert(from.WholeServer, qt.Equals, row.wantFromWhole)
			c.Assert(to.WholeServer, qt.Equals, row.wantWhole)
		})
	}
}

// TestScopeOnDevServer_FailurePath refuses one database beside a document
// declaring several, in the sentence the pinned community binary v1.3.0 gives
// `schema apply` for the pair, naming the flag whose URL is limited.
func TestScopeOnDevServer_FailurePath(t *testing.T) {
	rows := []struct {
		name     string
		from, to atlassource.State
		sides    devServerSides
		wantErr  string
	}{
		{name: "one database to a realm", from: appDatabase, to: document("app", "more"),
			sides:   devServerSides{server: true},
			wantErr: `cannot use HCL with more than 1 schema when --from is limited to schema "app"`},
		{name: "a realm to one database", from: document("app", "more"), to: appDatabase,
			sides:   devServerSides{server: true},
			wantErr: `cannot use HCL with more than 1 schema when --to is limited to schema "app"`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			from, to, err := scopeOnDevServer(row.from, row.to, row.sides)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(err, qt.ErrorAs, new(*OneDatabaseBesideDocumentError))
			c.Assert(from, qt.DeepEquals, atlassource.State{})
			c.Assert(to, qt.DeepEquals, atlassource.State{})
		})
	}
}

// TestScopeOnDevServer_SQLBesideOneDatabase refuses one database beside SQL
// the dev server has to run, a SQL file or a migration directory, with the
// sentence the pinned community binary v1.3.0 gives the pair, measured on
// MySQL 8.4.11: whether the SQL declares one database or several, and in
// either order. Narrowed, a replayed directory declaring `app` planned
// CREATE TABLE app.t and DROP TABLE t against the database `app`.
func TestScopeOnDevServer_SQLBesideOneDatabase(t *testing.T) {
	rows := []struct {
		name     string
		from, to atlassource.State
		sides    devServerSides
		wantErr  string
	}{
		{name: "one database to SQL declaring it", from: appDatabase, to: document("app"),
			sides:   devServerSides{server: true, toRunsSQL: true},
			wantErr: `cannot diff a database connection with a schema "app"`},
		{name: "a replayed directory to one database",
			from:    atlassource.State{Kind: atlassource.KindMigrationDir, Schema: &schemamodel.Database{}},
			to:      appDatabase,
			sides:   devServerSides{server: true, fromRunsSQL: true},
			wantErr: `cannot diff a schema "app" with a database connection`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			from, to, err := scopeOnDevServer(row.from, row.to, row.sides)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(err, qt.ErrorAs, new(*ServerScopeMismatchError))
			c.Assert(from, qt.DeepEquals, atlassource.State{})
			c.Assert(to, qt.DeepEquals, atlassource.State{})
		})
	}
}
