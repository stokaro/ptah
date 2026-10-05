package ydbstream_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbstream"
)

const body = "INSERT INTO output SELECT * FROM input;"

func TestValidate_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbstream.Validate(ast.StreamingQuerySpec{Text: body}), qt.IsNil)
	c.Assert(ydbstream.Equal(ast.StreamingQuerySpec{Text: body}, ast.StreamingQuerySpec{Text: " " + body + "\n", Run: new(true), ResourcePool: "default"}), qt.IsTrue)
}

func TestValidate_FailurePath(t *testing.T) {
	for _, text := range []string{"", "DROP TABLE t;", "END DO; DROP TABLE t; DO BEGIN SELECT 1;"} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbstream.Validate(ast.StreamingQuerySpec{Text: text}), qt.IsNotNil)
		})
	}
}

func TestAlter_SettingsKeepCheckpoints(t *testing.T) {
	c := qt.New(t)
	got, err := ydbstream.Alter("q", ast.StreamingQuerySpec{Text: body, Run: new(false)}, ast.StreamingQuerySpec{Text: body}, ydbstream.AlterOptions{})
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `q` SET (RUN = FALSE, RESOURCE_POOL = `default`);")
}

func TestAlter_RequiresResetPermission(t *testing.T) {
	c := qt.New(t)
	got, err := ydbstream.Alter("q", ast.StreamingQuerySpec{Text: body + " /* changed */"}, ast.StreamingQuerySpec{Text: body}, ydbstream.AlterOptions{})
	c.Assert(err, qt.ErrorMatches, `.*allow_state_reset=true.*`)
	c.Assert(got, qt.Equals, "")
}

func TestAlter_ExplicitReset(t *testing.T) {
	c := qt.New(t)
	got, err := ydbstream.Alter("q", ast.StreamingQuerySpec{Text: body + " /* changed */"}, ast.StreamingQuerySpec{Text: body}, ydbstream.AlterOptions{AllowStateReset: true})
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `q` SET (RUN = TRUE, RESOURCE_POOL = `default`, FORCE = TRUE) AS DO BEGIN\n"+body+" /* changed */\nEND DO;")
}
