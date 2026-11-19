package dialectlexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dialectlexer"
)

// A session's options are its dialect's, less backslash escapes where its
// sql_mode names NO_BACKSLASH_ESCAPES; every other rule stays.
func TestForSession(t *testing.T) {
	nbe := capability.ForDialect(platform.MySQL).With(capability.NoBackslashEscapes, true)
	c := qt.New(t)

	session := dialectlexer.ForSession(platform.MySQL, nbe)
	want := dialectlexer.Options(platform.MySQL)
	want.BackslashEscapes = false

	c.Assert(session, qt.DeepEquals, want)
	c.Assert(dialectlexer.ForSession(platform.MySQL, capability.ForDialect(platform.MySQL)), qt.DeepEquals, dialectlexer.Options(platform.MySQL))
	c.Assert(dialectlexer.ForSession(platform.Postgres, nbe), qt.DeepEquals, dialectlexer.Options(platform.Postgres))
}
