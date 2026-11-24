package sqlite_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dburlext/sqlite"
)

func TestSameDatabase_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	existing := filepath.Join(dir, "app.db")
	c.Assert(os.WriteFile(existing, nil, 0o600), qt.IsNil)
	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{name: "a private and a shared database in memory", left: "sqlite://:memory:", right: "sqlite3://file::memory:?cache=shared", want: false},
		{name: "two private databases in memory under one URL", left: "sqlite://dev?mode=memory", right: "sqlite://dev?mode=memory", want: false},
		{name: "two unnamed databases in memory", left: "sqlite://:memory:", right: "sqlite:///:memory:", want: false},
		{
			name: "one shared-cache name", left: "sqlite://memdb1?mode=memory&cache=shared",
			right: "sqlite:file:memdb1?cache=shared&mode=memory&_fk=1", want: true,
		},
		{
			name: "two shared-cache names", left: "sqlite://memdb1?mode=memory&cache=shared",
			right: "sqlite://memdb2?mode=memory&cache=shared", want: false,
		},
		{
			name: "a shared-cache name and a private one", left: "sqlite://memdb1?mode=memory&cache=shared",
			right: "sqlite://memdb1?mode=memory", want: false,
		},
		{
			name: "a percent escape in a shared-cache name", left: "sqlite:file:a%2Db?mode=memory&cache=shared",
			right: "sqlite:file:a-b?mode=memory&cache=shared", want: true,
		},
		{
			name: "cache=shared outside a URI", left: "sqlite://:memory:?cache=shared",
			right: "sqlite://:memory:?cache=shared", want: false,
		},
		{name: "a database in memory and a file", left: "sqlite://:memory:", right: atlasurl.SQLiteURLFromPath(existing), want: false},
		{
			name: "one existing file in two spellings", left: atlasurl.SQLiteURLFromPath(existing),
			right: "sqlite://" + filepath.ToSlash(existing) + "?_fk=1", want: true,
		},
		{
			name: "two files that do not exist", left: atlasurl.SQLiteURLFromPath(filepath.Join(dir, "a.db")),
			right: atlasurl.SQLiteURLFromPath(filepath.Join(dir, "b.db")), want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := sqlite.Services().Databases.SameDatabase(must.Must(atlasurl.Parse(test.left)), must.Must(atlasurl.Parse(test.right)))

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestSameDatabase_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		parsed *url.URL
	}{
		{name: "an escape in a file URI", parsed: &url.URL{Scheme: "sqlite", Opaque: "file:%zz"}},
		{name: "an escape in the name of a database in memory", parsed: &url.URL{Scheme: "sqlite", Path: "/a%zz", RawQuery: "mode=memory"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := sqlite.Services().Databases.SameDatabase(test.parsed, must.Must(url.Parse("sqlite://:memory:")))

			c.Assert(err, qt.ErrorMatches, `invalid SQLite database URL`)
			c.Assert(got, qt.IsFalse)
		})
	}
}

// TestSameDatabase_FailurePathRefusesARepeatedMode compares URLs that give
// mode twice. SQLite applies the last mode and the connector reads the first,
// so whether the URL names a database in memory has two answers, and the
// comparison refuses it on either side, as the connector refuses to open it.
func TestSameDatabase_FailurePathRefusesARepeatedMode(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
	}{
		{name: "on the left", left: "sqlite://x?mode=memory&cache=private&mode=rwc", right: "sqlite://:memory:"},
		{name: "on the right", left: "sqlite://:memory:", right: "sqlite:file:x?mode=rwc&mode=memory&cache=shared"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := sqlite.Services().Databases.SameDatabase(must.Must(atlasurl.Parse(test.left)), must.Must(atlasurl.Parse(test.right)))

			c.Assert(err, qt.ErrorIs, sqlite.ErrRepeatedMode)
			c.Assert(got, qt.IsFalse)
		})
	}
}

// TestSameDatabase_AgreesWithTheDriver opens each pair of URLs naming a
// database in memory, creates a table through the first connection and looks
// for it through the second. SameDatabase answers what the driver does: a
// pair it calls two databases while the driver gives them one is a dev
// database that is the target, and a pair it calls one while the driver gives
// two is a refusal of a distinct dev database.
func TestSameDatabase_AgreesWithTheDriver(t *testing.T) {
	// A spelling the driver read as a file name would create the file here.
	t.Chdir(t.TempDir())
	tests := []struct {
		name  string
		left  string
		right string
	}{
		{name: "two private databases under one URL", left: "sqlite://dev?mode=memory", right: "sqlite://dev?mode=memory"},
		{name: "two unnamed databases", left: "sqlite://:memory:", right: "sqlite://:memory:"},
		{name: "two unnamed databases with an empty host", left: "sqlite:///:memory:", right: "sqlite:///:memory:"},
		{name: "two private names", left: "sqlite://a?mode=memory", right: "sqlite://b?mode=memory"},
		{name: "one shared-cache name", left: "sqlite://memdb1?mode=memory&cache=shared", right: "sqlite://memdb1?cache=shared&mode=memory&_fk=1"},
		{name: "a shared-cache name in two spellings", left: "sqlite:file:memdb1?mode=memory&cache=shared", right: "sqlite://memdb1?mode=memory&cache=shared"},
		{name: "shared-cache names that differ in case", left: "sqlite://Memdb1?mode=memory&cache=shared", right: "sqlite://memdb1?mode=memory&cache=shared"},
		{name: "two shared-cache names", left: "sqlite://memdb1?mode=memory&cache=shared", right: "sqlite://memdb2?mode=memory&cache=shared"},
		{name: "a shared-cache name with a directory", left: "sqlite://dir/memdb1?mode=memory&cache=shared", right: "sqlite://memdb1?mode=memory&cache=shared"},
		{name: "a shared-cache name and a private one", left: "sqlite://memdb1?mode=memory&cache=shared", right: "sqlite://memdb1?mode=memory"},
		{name: "a percent escape in a shared-cache name", left: "sqlite:file:a%2Db?mode=memory&cache=shared", right: "sqlite:file:a-b?mode=memory&cache=shared"},
		{name: "the shared unnamed URI", left: "sqlite://file::memory:?cache=shared", right: "sqlite://file::memory:?cache=shared"},
		{name: "cache=shared outside a URI", left: "sqlite://:memory:?cache=shared", right: "sqlite://:memory:?cache=shared"},
		{name: "cache=shared on an unnamed database", left: "sqlite://?mode=memory&cache=shared", right: "sqlite://?mode=memory&cache=shared"},
		{name: "cache=shared on an unnamed URI", left: "sqlite:file:?cache=shared", right: "sqlite:file:?cache=shared"},
		{name: "a repeated cache that ends shared", left: "sqlite://x?mode=memory&cache=private&cache=shared", right: "sqlite://x?mode=memory&cache=private&cache=shared"},
		{name: "a repeated cache that ends private", left: "sqlite://x?mode=memory&cache=shared&cache=private", right: "sqlite://x?mode=memory&cache=shared&cache=private"},
		{name: "a percent escape in a name without file:", left: "sqlite:a%2Db?mode=memory&cache=shared", right: "sqlite:a-b?mode=memory&cache=shared"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			shared := driverShares(c, test.left, test.right)

			got, err := sqlite.Services().Databases.SameDatabase(must.Must(atlasurl.Parse(test.left)), must.Must(atlasurl.Parse(test.right)))

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, shared)
		})
	}
}

// driverShares opens left and right and reports whether a table created
// through the first is visible through the second.
func driverShares(c *qt.C, left, right string) bool {
	c.Helper()
	first, err := dbschema.ConnectToDatabase(c.Context(), left)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(first) })
	second, err := dbschema.ConnectToDatabase(c.Context(), right)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(second) })
	_, err = first.ExecContext(c.Context(), "CREATE TABLE marker (id INTEGER)")
	c.Assert(err, qt.IsNil)
	var tables int
	c.Assert(second.QueryRowContext(c.Context(), "SELECT count(*) FROM sqlite_master WHERE name = 'marker'").Scan(&tables), qt.IsNil)
	return tables == 1
}
