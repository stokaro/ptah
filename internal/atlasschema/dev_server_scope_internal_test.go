package atlasschema

// White-box testing required: scopeOnDevServer runs only after both sides of a
// diff are resolved, which through the exported API takes a live server for
// the database side and a dev server for the other. The decision itself reads
// two resolved states and crosses no boundary, so its rows are pinned here and
// the e2e tests drive the path that joins it to the servers. The schema apply
// refusal of SQL beside one database on a dev server, and the guard of a
// rehearsal on a whole dev server, run only once a target, and for the guard a
// dev server, are connected, and are pinned here for the same reason.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/devdocker"
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

var (
	wholeMySQLServerInfo = catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true}
	appDatabaseInfo      = catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"}
	sqlFileTo            = atlassource.Set{Kind: atlassource.KindLocalFile,
		Sources: []atlassource.Source{{Kind: atlassource.KindLocalFile, Path: "realm.sql"}}}
	hclFileTo = atlassource.Set{Kind: atlassource.KindLocalFile,
		Sources: []atlassource.Source{{Kind: atlassource.KindLocalFile, Path: "realm.hcl"}}}
)

// TestRefuseSQLBesideOneDatabaseOnDevServer_FailurePath refuses SQL beside a
// target naming one database on a whole dev server, in the pinned community
// binary v1.3.0's words, measured on MySQL 8.4.11 and MariaDB 11.8.9.
func TestRefuseSQLBesideOneDatabaseOnDevServer_FailurePath(t *testing.T) {
	c := qt.New(t)

	file := refuseSQLBesideOneDatabaseOnDevServer(appDatabaseInfo, "mysql://root@localhost:3307/", sqlFileTo)
	dir := refuseSQLBesideOneDatabaseOnDevServer(appDatabaseInfo, "mysql://root@localhost:3307/",
		atlassource.Set{Kind: atlassource.KindMigrationDir})

	c.Assert(file, qt.ErrorMatches, `cannot diff a database connection with a schema "app"`)
	c.Assert(dir, qt.ErrorMatches, `cannot diff a database connection with a schema "app"`)
}

// TestRefuseSQLBesideOneDatabaseOnDevServer_HappyPath is the control: a
// document beside one database, SQL beside a whole server, and SQL beside a
// dev database are not refused here.
func TestRefuseSQLBesideOneDatabaseOnDevServer_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Assert(refuseSQLBesideOneDatabaseOnDevServer(appDatabaseInfo, "mysql://root@localhost:3307/", hclFileTo), qt.IsNil)
	c.Assert(refuseSQLBesideOneDatabaseOnDevServer(wholeMySQLServerInfo, "mysql://root@localhost:3307/", sqlFileTo),
		qt.IsNil)
	c.Assert(refuseSQLBesideOneDatabaseOnDevServer(appDatabaseInfo, "mysql://root@localhost:3307/dev", sqlFileTo),
		qt.IsNil)
}

// TestGuardServerRehearsal_HappyPath takes what the reset of a whole dev
// server removes: databases, and what is in them.
func TestGuardServerRehearsal_HappyPath(t *testing.T) {
	c := qt.New(t)

	err := guardServerRehearsal([]string{
		"CREATE DATABASE `more`",
		"CREATE TABLE `more`.`m` (`id` int NOT NULL, PRIMARY KEY (`id`))",
		"ALTER TABLE `app`.`t` ADD COLUMN `name` varchar(10)",
		"DROP DATABASE `old`",
	}, wholeMySQLServerInfo)

	c.Assert(err, qt.IsNil)
}

// serverRehearsalOwnedStatements are what the reset of a whole dev server the
// operator named leaves behind, and a server the run owns rehearses.
var serverRehearsalOwnedStatements = []struct {
	name      string
	statement string
}{
	{name: "a user", statement: "CREATE USER 'u'@'%'"},
	{name: "a privilege", statement: "GRANT SELECT ON `app`.* TO 'u'@'%'"},
	{name: "a stored body", statement: "CREATE PROCEDURE `app`.`add_t`(IN v int) BEGIN INSERT INTO `app`.`t` (id) VALUES (v); END"},
}

// TestGuardServerRehearsal_FailurePath refuses what the reset leaves behind,
// naming the statement, and the two ways to a server the run owns.
func TestGuardServerRehearsal_FailurePath(t *testing.T) {
	for _, test := range serverRehearsalOwnedStatements {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := guardServerRehearsal([]string{"CREATE DATABASE `more`", test.statement}, wholeMySQLServerInfo)

			c.Assert(err, qt.ErrorMatches, `(?s)statement 2 cannot be rehearsed on a whole dev server: `+
				`mysql migration replay rejects .* because its effects cannot be confined to the disposable database realm; `+
				`if nothing else uses this server, declare it disposable with PTAH_DEV_SERVER_DISPOSABLE=1, `+
				`or use a docker:// or docker\+<driver>:// dev URL`)
		})
	}
}

// TestGuardServerRehearsal_OwnedServer rehearses the same statements on a
// whole dev server the operator declared the run's own, as a migration replay
// there runs them (stokaro/ptah#4060). A scheduled event stays refused there,
// without the remedy, because owning the server does not lift it.
func TestGuardServerRehearsal_OwnedServer(t *testing.T) {
	owned := catalog.ServerInfo{Dialect: platform.MariaDB, WholeServer: true, URL: "mariadb://root@guard-owned-server:3306/"}
	for _, test := range serverRehearsalOwnedStatements {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, release, err := devdocker.Resolve(c.Context(), owned.URL, devdocker.Options{DeclaredDisposable: true})
			c.Assert(err, qt.IsNil)
			c.Cleanup(release)

			c.Assert(guardServerRehearsal([]string{"CREATE DATABASE `more`", test.statement}, owned), qt.IsNil)
			c.Assert(guardServerRehearsal([]string{"CREATE EVENT purge ON SCHEDULE EVERY 1 MINUTE DO DELETE FROM `app`.`t`"}, owned),
				qt.ErrorMatches, `statement 1 cannot be rehearsed on a whole dev server: `+
					`mariadb migration replay rejects CREATE executable stored body because its effects cannot be confined to the disposable database realm`)
		})
	}
}

// TestIsDevServer reads a whole dev server out of the operator's spelling: a
// MySQL-family URL naming no database, written out or as a docker URL that
// starts one. A `docker://` URL always names a database.
func TestIsDevServer(t *testing.T) {
	tests := []struct {
		devURL string
		want   bool
	}{
		{devURL: "mysql://root@localhost:3307/", want: true},
		{devURL: "mariadb://root@localhost:3307/", want: true},
		{devURL: "docker+mysql://_/mysql:8.4.11", want: true},
		{devURL: "docker+mariadb://_/mariadb:11.8.9", want: true},
		{devURL: "mysql://root@localhost:3307/dev", want: false},
		{devURL: "docker+mysql://_/mysql:8.4.11/dev", want: false},
		{devURL: "docker://mysql/8.4.11", want: false},
		{devURL: "docker://mysql/8.4.11/dev", want: false},
		{devURL: "docker+postgres://_/postgres:18", want: false},
		{devURL: "postgres://root@localhost:5432/", want: false},
	}
	for _, test := range tests {
		t.Run(test.devURL, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(isDevServer(test.devURL), qt.Equals, test.want)
		})
	}
}
