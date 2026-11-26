package atlasschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
)

// unansweredSQLServerDev is a SQL Server dev URL taken from a project file
// beside a pinned server version: the diff of two documents opens nothing, so
// no server says how it compares names. Nothing listens on the port either.
const unansweredSQLServerDev = "sqlserver://sa:secret@127.0.0.1:1?database=dev&encrypt=disable"

// offlineSQLServerDiff compares two schema files on SQL Server with no
// connection.
func offlineSQLServerDiff(c *qt.C, from, to string) atlasschema.DiffOptions {
	dir := c.TempDir()
	fromPath := filepath.Join(dir, "from.sql")
	toPath := filepath.Join(dir, "to.sql")
	c.Assert(os.WriteFile(fromPath, []byte(from), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(to), 0o600), qt.IsNil)
	return atlasschema.DiffOptions{
		FromURLs:         []string{"file://" + filepath.ToSlash(fromPath)},
		ToURLs:           []string{"file://" + filepath.ToSlash(toPath)},
		DevURL:           unansweredSQLServerDev,
		SkipUnusedDevURL: true,
		ServerVersion:    "17",
		Runtime:          must.Must(builtin.New()),
	}
}

// TestDiff_SQLServerNamesNoServerAnswered_FailurePath compares two documents
// whose names carry non-ASCII letters. No rule Ptah holds offline says
// whether such a name is another one, so the diff is refused, and the refusal
// names the flags that would have given it a server to ask.
func TestDiff_SQLServerNamesNoServerAnswered_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		from, to string
		want     string
	}{
		{
			name: "a table created and another dropped",
			from: "CREATE TABLE [Größe] (id int NOT NULL);\n",
			to:   "CREATE TABLE [Zähler] (id int NOT NULL);\n",
			want: "the plan creates table Zähler and drops table Größe, and without a connection Ptah cannot " +
				"tell whether they are one name under the database collation",
		},
		{
			name: "two declared views",
			to:   "CREATE VIEW [Größe] AS SELECT 1 AS a;\nGO\nCREATE VIEW [v2] AS SELECT 2 AS a;\nGO\n",
			want: "without a connection Ptah cannot tell whether target views dbo\\.Größe and dbo\\.v2 are one " +
				"name under the database collation",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := atlasschema.Diff(c.Context(), offlineSQLServerDiff(c, test.from, test.to))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*`+test.want+`; give --dev-url a database Ptah can connect to, `+
				`or a database URL as --from, and the server says how it compares the names`)
			c.Assert(report.Changes, qt.HasLen, 0)
		})
	}
}

// TestDiff_SQLServerNamesEveryCollationKeepsApart_HappyPath is the control:
// two ASCII names that differ after case folding are two names under every
// collation SQL Server offers, so the offline diff needs no server.
func TestDiff_SQLServerNamesEveryCollationKeepsApart_HappyPath(t *testing.T) {
	c := qt.New(t)

	report, err := atlasschema.Diff(c.Context(), offlineSQLServerDiff(c,
		"CREATE TABLE [archived] (id int NOT NULL);\n",
		"CREATE TABLE [recent] (id int NOT NULL);\n"))

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 2)
}
