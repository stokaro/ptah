package examplemigrations

import (
	"io/fs"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestExampleMigrations(t *testing.T) {
	c := qt.New(t)

	entries, err := fs.ReadDir(GetExampleMigrations(), "migrations")
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 6)
	c.Assert(entries[0].Name(), qt.Equals, "0000000001_initial_schema.down.sql")
	c.Assert(entries[5].Name(), qt.Equals, "0000000003_add_user_profile_fields.up.sql")
}
