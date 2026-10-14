package sqliteconnect

// White-box testing required: the DSN the SQLite driver receives is built by unexported converters, and no exported result reports it.

import (
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/connector"
)

func TestConvertSQLiteURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "relative file host",
			input:    "sqlite://test.db",
			expected: "test.db?_pragma=foreign_keys%281%29",
		},
		{
			name:     "relative nested file",
			input:    "sqlite://data/app.db",
			expected: "data/app.db?_pragma=foreign_keys%281%29",
		},
		{
			name:     "absolute file path",
			input:    "sqlite:///tmp/app.db",
			expected: "/tmp/app.db?_pragma=foreign_keys%281%29",
		},
		{
			name:     "memory database",
			input:    "sqlite:///:memory:",
			expected: ":memory:?_pragma=foreign_keys%281%29",
		},
		{
			// The form this repository's own tests, help text and docs use.
			// Without the file: prefix SQLite never reads the query at all and
			// opens a FILE named `dev`, which then carries the previous run's
			// state into the next plan (stokaro/ptah#1819).
			name:     "named memory database becomes a uri",
			input:    "sqlite://dev?mode=memory",
			expected: "file:dev?_pragma=foreign_keys%281%29&mode=memory",
		},
		{
			name:     "named memory database keeps a shared cache",
			input:    "sqlite://dev?mode=memory&cache=shared",
			expected: "file:dev?_pragma=foreign_keys%281%29&cache=shared&mode=memory",
		},
		{
			// A plain path stays a plain path: file: there would change how a
			// name containing ? or # is read.
			name:     "a file database is not rewritten",
			input:    "sqlite://dev.db",
			expected: "dev.db?_pragma=foreign_keys%281%29",
		},
		{
			name:     "uri memory database",
			input:    "sqlite:file:memdb1?mode=memory&cache=shared",
			expected: "file:memdb1?_pragma=foreign_keys%281%29&cache=shared&mode=memory",
		},
		{
			name:     "preserves explicit foreign keys pragma",
			input:    "sqlite:///:memory:?_pragma=foreign_keys(0)",
			expected: ":memory:?_pragma=foreign_keys%280%29",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(convertSQLiteURL(tt.input), qt.Equals, tt.expected)
		})
	}
}

func TestIsSQLiteMemoryDSN(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want bool
	}{
		{name: "default empty dsn", dsn: "", want: true},
		{name: "memory path", dsn: ":memory:?_pragma=foreign_keys%281%29", want: true},
		{name: "anonymous uri memory path", dsn: "file::memory:?_pragma=foreign_keys%281%29&cache=shared", want: true},
		{name: "uri memory mode", dsn: "file:memdb1?_pragma=foreign_keys%281%29&cache=shared&mode=memory", want: true},
		{name: "file path", dsn: "test.db?_pragma=foreign_keys%281%29", want: false},
		{name: "absolute file path", dsn: "/tmp/app.db?_pragma=foreign_keys%281%29", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(isSQLiteMemoryDSN(tt.dsn), qt.Equals, tt.want)
		})
	}
}

// TestParseDatabaseURL_AcceptsAWindowsPath pins the shape that made ptah
// unusable on Windows.
//
// A drive letter's colon is not a port separator, but net/url reads it as one
// and refuses the whole URL, so every command that provisions a local database
// failed: 1014 tests across more than thirty packages, 1382 of them reporting
// "invalid database URL". The path is carried as opaque, which is the shape
// convertSQLiteURL reads first, so what the driver receives is the path itself.
//
// The rows run on every operating system because this is string handling, not
// a filesystem call -- which is the only reason the fix is testable from a
// machine that is not the one it is for.
func TestParseDatabaseURL_AcceptsAWindowsPath(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "a backslash path",
			url:  `sqlite://C:\Users\runner\AppData\Local\Temp\shadow.db`,
			want: `C:\Users\runner\AppData\Local\Temp\shadow.db`,
		},
		{
			name: "a forward-slash path with a drive",
			url:  "sqlite://D:/data/app.db",
			want: "D:/data/app.db",
		},
		{
			name: "a lowercase drive letter",
			url:  `sqlite://c:\tmp\app.db`,
			want: `c:\tmp\app.db`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := connector.ParseURL(test.url)

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Scheme, qt.Equals, "sqlite")
			// The DSN carries the pragma this package appends to every SQLite
			// address, so the path is matched as a prefix rather than whole.
			c.Assert(convertSQLiteURL(test.url), qt.Matches, regexp.QuoteMeta(test.want)+`(\?.*)?`)
		})
	}
}

// TestParseDatabaseURL_KeepsTheQueryOfAWindowsPath keeps the accommodation
// from eating the options.
//
// Carrying the whole remainder as opaque put the query inside the path, so
// convertSQLiteURL saw no query and appended its pragma with a second "?" --
// C:\tmp\app.db?mode=ro?_pragma=... The mode was silently not applied, which on
// a read-only target means the database opens with the wrong semantics rather
// than failing.
func TestParseDatabaseURL_KeepsTheQueryOfAWindowsPath(t *testing.T) {
	c := qt.New(t)

	parsed, err := connector.ParseURL(`sqlite://C:\tmp\app.db?mode=ro`)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Opaque, qt.Equals, `C:\tmp\app.db`)
	c.Assert(parsed.RawQuery, qt.Equals, "mode=ro")
	// One "?" and both parameters. Their order is the package's own and not
	// what this pins; that the requested one survives at all is.
	dsn := convertSQLiteURL(`sqlite://C:\tmp\app.db?mode=ro`)
	c.Assert(strings.Count(dsn, "?"), qt.Equals, 1)
	c.Assert(dsn, qt.Contains, `C:\tmp\app.db?`)
	c.Assert(dsn, qt.Contains, "mode=ro")
}
