package ydbacl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbacl"
)

// TestStatementPath_HappyPath pins the path a GRANT writes: relative to the
// database root where the line resolves it, which every line does for a path
// with a directory part and 26.1 and later for a single name, and absolute
// under the database's path otherwise. Measured on 25.1.4.7 to 26.2.1.14.
func TestStatementPath_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		objectType   string
		objectName   string
		databasePath string
		caps         capability.Capabilities
		want         string
	}{
		{name: "a table in a directory, on every line", objectType: "TABLE", objectName: "shop.orders",
			want: "shop/orders"},
		{name: "a table in a nested directory", objectType: "TABLE", objectName: "shop/eu.orders",
			want: "shop/eu/orders"},
		{name: "a table whose name holds a dot", objectType: "TABLE", objectName: `"a.b"`,
			caps: capability.YDB262(), want: "a.b"},
		{name: "a root table where a single name resolves", objectType: "TABLE", objectName: "events",
			caps: capability.YDB262(), want: "events"},
		{name: "a root table where it does not", objectType: "TABLE", objectName: "events",
			databasePath: "/local", want: "/local/events"},
		{name: "a nested directory on every line", objectType: "SCHEMA", objectName: "shop/eu/",
			want: "shop/eu"},
		{name: "a root directory where a single name does not resolve", objectType: "schema", objectName: "shop",
			databasePath: "/Root/db/", want: "/Root/db/shop"},
		{name: "the database", objectType: "DATABASE", databasePath: "/local", caps: capability.YDB262(),
			want: "/local"},
		{name: "an absolute path a plan already wrote", objectType: "TABLE", objectName: "/local/events",
			want: "/local/events"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbacl.StatementPath(test.objectType, test.objectName, test.databasePath, test.caps)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestStatementPath_FailurePath pins what has no path: an object only an
// absolute path reaches, with no database path to write it from, and the
// objects a YDB grant is not about.
func TestStatementPath_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		objectType string
		objectName string
		wantErr    string
	}{
		{name: "the database without its path", objectType: "DATABASE",
			wantErr: `YDB takes this object only by its absolute path, which begins with the database's own path`},
		{name: "a root table where a single name does not resolve", objectType: "TABLE", objectName: "events",
			wantErr: `YDB takes this object only by its absolute path, .*`},
		{name: "the database by a name", objectType: "DATABASE", objectName: "local",
			wantErr: `a grant on the database names the database "local", which is the database the connection opens`},
		{name: "a directory with no name", objectType: "SCHEMA", objectName: "/",
			wantErr: `a grant on a directory names no directory; the database root is the database itself`},
		{name: "a sequence", objectType: "SEQUENCE", objectName: "s",
			wantErr: `a YDB grant is on the database, a directory or a table, not a SEQUENCE`},
		{name: "a table with no name", objectType: "TABLE", objectName: "",
			wantErr: `a grant on a table names "", which is not a table`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbacl.StatementPath(test.objectType, test.objectName, "", capability.YDB251())
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestStatementPath_NeedsTheDatabasePath pins the sentinel a caller branches
// on to say where an absolute path comes from.
func TestStatementPath_NeedsTheDatabasePath(t *testing.T) {
	c := qt.New(t)
	_, err := ydbacl.StatementPath("DATABASE", "", "", capability.YDB262())
	c.Assert(err, qt.ErrorIs, ydbacl.ErrNeedsDatabasePath)
}

// TestDescribe pins how a message names each object a grant is about.
func TestDescribe(t *testing.T) {
	tests := []struct {
		objectType string
		objectName string
		want       string
	}{
		{objectType: "DATABASE", want: "the database"},
		{objectType: "schema", objectName: "shop/eu", want: "directory shop/eu"},
		{objectType: "TABLE", objectName: "shop.orders", want: "table shop.orders"},
	}

	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbacl.Describe(test.objectType, test.objectName), qt.Equals, test.want)
		})
	}
}
