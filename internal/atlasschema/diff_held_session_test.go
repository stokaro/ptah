package atlasschema_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
)

// heldDatabase opens the SQLite database at url and runs the statements in
// it. A private in-memory database lives in the returned connection's one
// session.
func heldDatabase(c *qt.C, url, statements string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	_, err = conn.ExecContext(c.Context(), statements)
	c.Assert(err, qt.IsNil)
	return conn
}

// TestDiffHeldSession compares two private in-memory SQLite databases: --from
// holds a, b and c, and --to holds a and b. Both sides are handed over, and a
// side whose connection the caller holds is read on it, and each connection
// is bound to its side, so two private databases opened with one URL are read
// as the two they are. A held connection opened with another URL than its side
// names is not read: the side is opened again by its URL, which reaches a new,
// empty database, and the plan drops every table. That row is the control that
// the first rows' answer came from the held connections.
func TestDiffHeldSession(t *testing.T) {
	const fromURL, toURL = "sqlite://from?mode=memory", "sqlite://to?mode=memory"
	tests := []struct {
		name string
		// fromURL and toURL open the two databases, and --from names fromURL.
		fromURL, toURL string
		// toSide is the URL --to names.
		toSide string
		want   []string
	}{
		{
			name: "both sides held", fromURL: fromURL, toURL: toURL, toSide: toURL,
			want: []string{`DROP TABLE "c"`},
		},
		{
			name:    "two private databases under one URL",
			fromURL: "sqlite://db?mode=memory", toURL: "sqlite://db?mode=memory", toSide: "sqlite://db?mode=memory",
			want: []string{`DROP TABLE "c"`},
		},
		{
			name:    "a connection opened with another URL is not read",
			fromURL: fromURL, toURL: toURL, toSide: "sqlite://elsewhere?mode=memory",
			want: []string{`DROP TABLE "a"`, `DROP TABLE "b"`, `DROP TABLE "c"`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			from := heldDatabase(c, test.fromURL, threeTables)
			to := heldDatabase(c, test.toURL, twoTables)

			statements := heldDiff(c, test.fromURL, test.toSide, from, to)

			c.Assert(statements, qt.ContentEquals, test.want)
		})
	}
}

// TestDiffHeldSession_ToSideNotHeld hands over the --from connection only. The
// --to side is opened again by its URL, which reaches a new, empty database,
// so the plan drops every table.
func TestDiffHeldSession_ToSideNotHeld(t *testing.T) {
	c := qt.New(t)
	const fromURL, toURL = "sqlite://from?mode=memory", "sqlite://to?mode=memory"
	from := heldDatabase(c, fromURL, threeTables)
	heldDatabase(c, toURL, twoTables)

	statements := heldDiff(c, fromURL, toURL, from, nil)

	c.Assert(statements, qt.ContentEquals, []string{`DROP TABLE "a"`, `DROP TABLE "b"`, `DROP TABLE "c"`})
}

// threeTables and twoTables are the --from and the --to schema of the held
// session tests.
const (
	threeTables = "CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (id INTEGER PRIMARY KEY); " +
		"CREATE TABLE c (id INTEGER PRIMARY KEY);"
	twoTables = "CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (id INTEGER PRIMARY KEY);"
)

// heldDiff compares fromURL with toSide, reading each side on the connection
// handed over for it, and returns the planned statements. A nil connection
// hands over none.
func heldDiff(c *qt.C, fromURL, toSide string, from, to *dbschema.DatabaseConnection) []string {
	c.Helper()
	report, err := atlasschema.Diff(c.Context(), atlasschema.DiffOptions{
		Runtime:  must.Must(builtin.New()),
		FromURLs: []string{fromURL},
		ToURLs:   []string{toSide},
		FromHeld: from,
		ToHeld:   to,
	})
	c.Assert(err, qt.IsNil)
	statements := make([]string, 0, len(report.Changes))
	for _, change := range report.Changes {
		statements = append(statements, change.Cmd)
	}
	return statements
}

// TestDiffHeldSession_FileIsOpenedAgain holds a connection to a database file
// and closes it before the comparison. A database a URL reaches from any
// connection is opened again by that URL, so the comparison never reads the
// held connection: had it done so, it would fail on the closed pool.
func TestDiffHeldSession_FileIsOpenedAgain(t *testing.T) {
	c := qt.New(t)
	fromURL := atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "from.db"))
	from := heldDatabase(c, fromURL, "CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (id INTEGER PRIMARY KEY);")
	const toURL = "sqlite://to?mode=memory"
	to := heldDatabase(c, toURL, "CREATE TABLE a (id INTEGER PRIMARY KEY);")
	c.Assert(from.Close(), qt.IsNil)

	report, err := atlasschema.Diff(c.Context(), atlasschema.DiffOptions{
		Runtime:  must.Must(builtin.New()),
		FromURLs: []string{fromURL},
		ToURLs:   []string{toURL},
		FromHeld: from,
		ToHeld:   to,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 1)
	c.Assert(report.Changes[0].Cmd, qt.Equals, `DROP TABLE "b"`)
}

// TestDiffHeldSession_LeavesTheConnectionOpen reads both held databases back
// after the comparison. A held connection is the caller's: closed by the
// comparison, the query fails on the closed pool, and an in-memory database
// is gone with its session.
func TestDiffHeldSession_LeavesTheConnectionOpen(t *testing.T) {
	c := qt.New(t)
	const fromURL, toURL = "sqlite://from?mode=memory", "sqlite://to?mode=memory"
	from := heldDatabase(c, fromURL, "CREATE TABLE a (id INTEGER PRIMARY KEY); CREATE TABLE b (id INTEGER PRIMARY KEY);")
	to := heldDatabase(c, toURL, "CREATE TABLE a (id INTEGER PRIMARY KEY);")

	_, err := atlasschema.Diff(c.Context(), atlasschema.DiffOptions{
		Runtime:  must.Must(builtin.New()),
		FromURLs: []string{fromURL},
		ToURLs:   []string{toURL},
		FromHeld: from,
		ToHeld:   to,
	})

	c.Assert(err, qt.IsNil)
	for conn, want := range map[*dbschema.DatabaseConnection]int{from: 2, to: 1} {
		var tables int
		c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM sqlite_master WHERE type = 'table'").Scan(&tables), qt.IsNil)
		c.Assert(tables, qt.Equals, want)
	}
}
