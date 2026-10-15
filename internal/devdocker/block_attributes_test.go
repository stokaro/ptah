package devdocker_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
)

// TestBlockAttributes_HappyPath pins the atlas.hcl docker block types and the
// attributes each may carry: a PostgreSQL block names its database, and a
// MySQL-family block does not, since the schema is the database there.
func TestBlockAttributes_HappyPath(t *testing.T) {
	tests := []struct {
		typ  string
		want []string
	}{
		{typ: "postgres", want: []string{"image", "schema", "database", "baseline", "env", "timeout"}},
		{typ: "mysql", want: []string{"image", "schema", "baseline", "env", "timeout"}},
		{typ: "mariadb", want: []string{"image", "schema", "baseline", "env", "timeout"}},
	}
	for _, test := range tests {
		t.Run(test.typ, func(t *testing.T) {
			c := qt.New(t)

			got, ok := devdocker.BlockAttributes(test.typ)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestBlockAttributes_FailurePath pins the names that start a container from
// a docker URL and have no atlas.hcl block, and the names that start nothing.
func TestBlockAttributes_FailurePath(t *testing.T) {
	for _, typ := range []string{"maria", "pgvector", "postgis", "ydb", "sqlite", "Postgres", ""} {
		t.Run(typ, func(t *testing.T) {
			c := qt.New(t)

			got, ok := devdocker.BlockAttributes(typ)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestBlockAttributes_ReturnsACopy pins that a caller cannot change what the
// next caller is told.
func TestBlockAttributes_ReturnsACopy(t *testing.T) {
	c := qt.New(t)
	first, ok := devdocker.BlockAttributes("postgres")
	c.Assert(ok, qt.IsTrue)

	first[0] = "changed"

	second, _ := devdocker.BlockAttributes("postgres")
	c.Assert(second[0], qt.Equals, "image")
}
