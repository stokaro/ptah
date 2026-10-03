package devdocker_test

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
)

// The rows below are the pinned community binary v1.3.0's own answers for a
// `docker+<driver>://` dev URL, measured on 2026-10-03 on a Linux host with a
// local daemon, each exit status read from an unpiped
// `atlas schema inspect -u file://schema.sql --dev-url <value>`. The package
// documentation carries the full table (stokaro/ptah#4040).

func TestParseImageURL_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		rawURL       string
		wantImage    string
		wantDatabase string
		wantDialect  string
	}{
		{
			name:         "image and database",
			rawURL:       "docker+postgres://_/postgres:17/dev",
			wantImage:    "postgres:17",
			wantDatabase: "dev",
			wantDialect:  "postgres",
		},
		{
			// Measured exit 0. The binary connects to `postgres` when the URL
			// names no database, as its own PostgreSQL default does.
			name:         "no database connects to postgres",
			rawURL:       "docker+postgres://_/postgres:17",
			wantImage:    "postgres:17",
			wantDatabase: "postgres",
			wantDialect:  "postgres",
		},
		{
			name:         "an empty host is the underscore",
			rawURL:       "docker+postgres:///postgres:17/dev",
			wantImage:    "postgres:17",
			wantDatabase: "dev",
			wantDialect:  "postgres",
		},
		{
			name:         "the host leads the image",
			rawURL:       "docker+postgres://docker.io/library/postgres:17/dev",
			wantImage:    "docker.io/library/postgres:17",
			wantDatabase: "dev",
			wantDialect:  "postgres",
		},
		{
			name:         "the image may hold a slash",
			rawURL:       "docker+postgres://_/library/postgres:17/dev",
			wantImage:    "library/postgres:17",
			wantDatabase: "dev",
			wantDialect:  "postgres",
		},
		{
			// Measured exit 0: the daemon resolves an image with no tag to
			// `latest`, so nothing is appended here.
			name:         "an image with no tag",
			rawURL:       "docker+postgres://_/ptah-img/dev",
			wantImage:    "ptah-img",
			wantDatabase: "dev",
			wantDialect:  "postgres",
		},
		{
			name:         "one segment is the image",
			rawURL:       "docker+postgres://_/ptah-img",
			wantImage:    "ptah-img",
			wantDatabase: "postgres",
			wantDialect:  "postgres",
		},
		{
			// The binary's own reading: the last segment holds no colon, so it
			// is the database, and the image is what precedes it. The pull then
			// fails loudly, on both binaries.
			name:         "an image with a slash and no tag loses its last segment",
			rawURL:       "docker+postgres://_/org/img",
			wantImage:    "org",
			wantDatabase: "img",
			wantDialect:  "postgres",
		},
		{
			// Measured exit 1, at `docker run`: the host leads the image, so the
			// image is `postgres:17/dev`, which no registry serves.
			name:         "a tag in the host makes the path part of the image",
			rawURL:       "docker+postgres://postgres:17/dev",
			wantImage:    "postgres:17/dev",
			wantDatabase: "postgres",
			wantDialect:  "postgres",
		},
		{
			name:         "the scheme is not case-sensitive",
			rawURL:       "DOCKER+POSTGRES://_/postgres:17/dev",
			wantImage:    "postgres:17",
			wantDatabase: "dev",
			wantDialect:  "postgres",
		},
		{
			name:         "mysql",
			rawURL:       "docker+mysql://_/mysql:8.4.11/dev",
			wantImage:    "mysql:8.4.11",
			wantDatabase: "dev",
			wantDialect:  "mysql",
		},
		{
			// No database is the whole server on the MySQL family, as on the
			// binary, whose run then reads every user database.
			name:         "mysql with no database is the whole server",
			rawURL:       "docker+mysql://_/mysql:8.4.11",
			wantImage:    "mysql:8.4.11",
			wantDatabase: "",
			wantDialect:  "mysql",
		},
		{
			name:         "maria",
			rawURL:       "docker+maria://_/mariadb:11.8.9/dev",
			wantImage:    "mariadb:11.8.9",
			wantDatabase: "dev",
			wantDialect:  "mariadb",
		},
		{
			name:         "mariadb",
			rawURL:       "docker+mariadb://_/mariadb:11.8.9/dev",
			wantImage:    "mariadb:11.8.9",
			wantDatabase: "dev",
			wantDialect:  "mariadb",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			spec, err := devdocker.Parse(tc.rawURL)
			c.Assert(err, qt.IsNil)
			c.Check(spec.Image, qt.Equals, tc.wantImage)
			c.Check(spec.Database, qt.Equals, tc.wantDatabase)
			c.Check(spec.Dialect, qt.Equals, tc.wantDialect)
		})
	}
}

func TestParseImageURL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{
			// Measured exit 1 on the binary, with `invalid configuration` and a
			// dump of its own configuration struct.
			name:    "no image after the host",
			rawURL:  "docker+postgres://_/",
			wantErr: `docker+postgres --dev-url names no image`,
		},
		{
			name:    "no path at all",
			rawURL:  "docker+postgres://_",
			wantErr: `docker+postgres --dev-url names no image`,
		},
		{
			// The binary registers these two and starts their engines. Ptah
			// starts neither from a docker URL, and says so rather than
			// answering an unknown driver for a scheme the binary knows.
			name:   "clickhouse",
			rawURL: "docker+clickhouse://_/clickhouse/clickhouse-server:24/dev",
			wantErr: `docker+clickhouse --dev-url names an engine Ptah does not start from a docker URL;` +
				` pass a directly connectable dev database URL instead`,
		},
		{
			name:   "sqlserver",
			rawURL: "docker+sqlserver://_/mcr.microsoft.com/mssql/server:2022-latest/dev",
			wantErr: `docker+sqlserver --dev-url names an engine Ptah does not start from a docker URL;` +
				` pass a directly connectable dev database URL instead`,
		},
		{
			name:    "a query separator in the database name",
			rawURL:  "docker+mysql://_/mysql:8/foo%3Fbar",
			wantErr: `docker --dev-url database name "foo?bar" contains a query separator`,
		},
		{
			name:   "a host parameter would redirect the connection",
			rawURL: "docker+postgres://_/postgres:17/dev?host=prod.example",
			wantErr: `docker --dev-url parameter "host" would point the connection away from` +
				` the container this URL provisions; remove it, or pass a directly` +
				` connectable dev database URL instead`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			spec, err := devdocker.Parse(tc.rawURL)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(tc.wantErr))
			c.Check(spec.Image, qt.Equals, "")
			c.Check(spec.Dialect, qt.Equals, "")
		})
	}
}

// TestIsURLRecognizesTheImageSchemesTheCommunityBinaryRegisters pins which
// `docker+` schemes are routed to the provisioner. A scheme the binary does not
// register is left to the connector, whose answer is the binary's: measured,
// `docker+sqlite`, `docker+postgis` and `docker+nosuch` all exit 1 with
// `sql/sqlclient: unknown driver`.
func TestIsURLRecognizesTheImageSchemesTheCommunityBinaryRegisters(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   bool
	}{
		{name: "postgres", rawURL: "docker+postgres://_/postgres:17/dev", want: true},
		{name: "mysql", rawURL: "docker+mysql://_/mysql:8.4/dev", want: true},
		{name: "maria", rawURL: "docker+maria://_/mariadb:11/dev", want: true},
		{name: "mariadb", rawURL: "docker+mariadb://_/mariadb:11/dev", want: true},
		{name: "clickhouse", rawURL: "docker+clickhouse://_/clickhouse/clickhouse-server:24/dev", want: true},
		{name: "sqlserver", rawURL: "docker+sqlserver://_/mssql:2022/dev", want: true},
		{name: "uppercase", rawURL: "DOCKER+POSTGRES://_/postgres:17/dev", want: true},
		{name: "sqlite", rawURL: "docker+sqlite://_/postgres:17/dev", want: false},
		{name: "postgis", rawURL: "docker+postgis://_/postgres:17/dev", want: false},
		{name: "no driver", rawURL: "docker+://_/postgres:17/dev", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			c.Check(devdocker.IsURL(tc.rawURL), qt.Equals, tc.want)
		})
	}
}

// TestCreatesDatabaseOnlyForADatabaseAnImageURLNames pins which provisioned
// databases are created once the server is ready. An engine's own image creates
// the database it is told to, and every image has the server's own database.
func TestCreatesDatabaseOnlyForADatabaseAnImageURLNames(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   bool
	}{
		{name: "an engine's own image", rawURL: "docker://postgres/16/dev", want: false},
		{name: "an image and a database", rawURL: "docker+postgres://_/postgres:17/dev", want: true},
		{name: "an image and postgres", rawURL: "docker+postgres://_/postgres:17/postgres", want: false},
		{name: "an image and no database", rawURL: "docker+postgres://_/postgres:17", want: false},
		{name: "a mysql image and a database", rawURL: "docker+mysql://_/mysql:8.4/dev", want: true},
		{name: "a mysql image and the whole server", rawURL: "docker+mysql://_/mysql:8.4", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			spec, err := devdocker.Parse(tc.rawURL)
			c.Assert(err, qt.IsNil)
			c.Check(spec.CreatesDatabase(), qt.Equals, tc.want)
		})
	}
}

// TestReadyURLProbesTheServerDatabaseOfAnImage pins what the readiness wait
// connects to. On an image the URL names, the database may not exist until it
// is created after the wait, so the wait probes the server's own database.
func TestReadyURLProbesTheServerDatabaseOfAnImage(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name:   "an engine's own image probes the database it creates",
			rawURL: "docker://postgres/16/dev?search_path=app",
			want:   "postgres://postgres:" + testPassword + "@127.0.0.1:15432/dev?sslmode=disable",
		},
		{
			name:   "a postgres image probes postgres",
			rawURL: "docker+postgres://_/postgres:17/dev?search_path=app",
			want:   "postgres://postgres:" + testPassword + "@127.0.0.1:15432/postgres?sslmode=disable",
		},
		{
			name:   "a mysql image probes the server",
			rawURL: "docker+mysql://_/mysql:8.4/dev",
			want:   "mysql://root:" + testPassword + "@tcp(127.0.0.1:15432)/",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			t.Parallel()
			spec, err := devdocker.Parse(tc.rawURL)
			c.Assert(err, qt.IsNil)
			c.Check(spec.ReadyURL("127.0.0.1:15432", testPassword), qt.Equals, tc.want)
		})
	}
}

// creatorCall is one call a recordingCreator saw.
type creatorCall struct {
	serverURL string
	dialect   string
	database  string
}

// recordingCreator is a DatabaseCreator that records its calls and answers
// err.
type recordingCreator struct {
	mu    sync.Mutex
	err   error
	calls []creatorCall
}

func (r *recordingCreator) create(_ context.Context, serverURL, dialect, database string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, creatorCall{serverURL: serverURL, dialect: dialect, database: database})
	return r.err
}

func (r *recordingCreator) seen() []creatorCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append(make([]creatorCall, 0), r.calls...)
}

// TestResolveCreatesTheDatabaseAnImageURLNames drives the provisioner with an
// image URL and checks the database is created on the server the readiness wait
// probed, and only for such a URL. The calls are compared without the password,
// which is random per instance.
func TestResolveCreatesTheDatabaseAnImageURLNames(t *testing.T) {
	tests := []struct {
		name         string
		rawURL       string
		wantDialect  []string
		wantDatabase []string
	}{
		{
			name:         "a postgres image",
			rawURL:       "docker+postgres://_/postgres:17/dev",
			wantDialect:  []string{"postgres"},
			wantDatabase: []string{"dev"},
		},
		{
			name:         "a mysql image",
			rawURL:       "docker+mysql://_/mysql:8.4/app",
			wantDialect:  []string{"mysql"},
			wantDatabase: []string{"app"},
		},
		{
			// No call: the slices stay nil.
			name:   "an engine's own image",
			rawURL: "docker://postgres/16/dev",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
			creator := &recordingCreator{}
			_, release, err := devdocker.Resolve(t.Context(), tc.rawURL, devdocker.Options{
				Runner:         runner,
				Ready:          alwaysReady,
				CreateDatabase: creator.create,
			})
			c.Assert(err, qt.IsNil)
			release()

			var dialects, databases []string
			for _, call := range creator.seen() {
				dialects = append(dialects, call.dialect)
				databases = append(databases, call.database)
			}
			c.Check(dialects, qt.DeepEquals, tc.wantDialect)
			c.Check(databases, qt.DeepEquals, tc.wantDatabase)
		})
	}
}

// TestResolveCreatesTheDatabaseOnTheServerTheWaitProbed pins the URL the
// creator is handed: the server's own database, as the wait probed it, and not
// the database that does not exist yet.
func TestResolveCreatesTheDatabaseOnTheServerTheWaitProbed(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	creator := &recordingCreator{}
	resolved, release, err := devdocker.Resolve(t.Context(), "docker+postgres://_/postgres:17/dev", devdocker.Options{
		Runner:         runner,
		Ready:          alwaysReady,
		CreateDatabase: creator.create,
	})
	c.Assert(err, qt.IsNil)
	t.Cleanup(release)

	calls := creator.seen()
	c.Assert(calls, qt.HasLen, 1)
	c.Check(calls[0].serverURL, qt.Matches, `postgres://postgres:[0-9a-f]{48}@127\.0\.0\.1:15432/postgres\?sslmode=disable`)
	c.Check(resolved, qt.Matches, `postgres://postgres:[0-9a-f]{48}@127\.0\.0\.1:15432/dev\?sslmode=disable`)
}

// TestResolveRemovesTheContainerWhenTheDatabaseCannotBeCreated holds the
// release discipline on the one exit path an image URL adds: the caller
// receives no instance, so the provisioner has to remove the container itself.
func TestResolveRemovesTheContainerWhenTheDatabaseCannotBeCreated(t *testing.T) {
	c := qt.New(t)
	runner := &fakeRunner{hostPort: "127.0.0.1:15432"}
	errRefused := errors.New("permission denied to create database")
	creator := &recordingCreator{err: errRefused}
	resolved, release, err := devdocker.Resolve(t.Context(), "docker+postgres://_/postgres:17/dev", devdocker.Options{
		Runner:         runner,
		Ready:          alwaysReady,
		CreateDatabase: creator.create,
	})
	t.Cleanup(release)

	c.Assert(err, qt.ErrorIs, errRefused)
	c.Check(err, qt.ErrorMatches, `create database "dev" in dev database postgres:17: permission denied to create database`)
	c.Check(resolved, qt.Equals, "")
	started, removed := runner.calls()
	c.Assert(started, qt.HasLen, 1)
	c.Check(removed, qt.DeepEquals, started)
}
