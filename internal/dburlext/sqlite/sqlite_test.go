package sqlite_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

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
		{name: "two databases in memory", left: "sqlite://:memory:", right: "sqlite3://file::memory:?cache=shared", want: true},
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
	c := qt.New(t)

	got, err := sqlite.Services().Databases.SameDatabase(&url.URL{Scheme: "sqlite", Opaque: "file:%zz"}, must.Must(url.Parse("sqlite://:memory:")))

	c.Assert(err, qt.ErrorMatches, `invalid SQLite database URL`)
	c.Assert(got, qt.IsFalse)
}
