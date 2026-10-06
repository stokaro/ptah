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
	got, err := ydbstream.Alter("q", ast.StreamingQuerySpec{Text: body + " SELECT 2;"}, ast.StreamingQuerySpec{Text: body}, ydbstream.AlterOptions{})
	c.Assert(err, qt.ErrorMatches, `.*allow_state_reset=true.*`)
	c.Assert(got, qt.Equals, "")
}

func TestAlter_ExplicitReset(t *testing.T) {
	c := qt.New(t)
	got, err := ydbstream.Alter("q", ast.StreamingQuerySpec{Text: body + " SELECT 2;"}, ast.StreamingQuerySpec{Text: body}, ydbstream.AlterOptions{AllowStateReset: true})
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `q` SET (RUN = TRUE, RESOURCE_POOL = `default`, FORCE = TRUE) AS DO BEGIN\n"+body+" SELECT 2;\nEND DO;")
}

func TestCreate_Guards(t *testing.T) {
	for _, test := range []struct {
		options ydbstream.CreateOptions
		prefix  string
	}{
		{prefix: "CREATE STREAMING QUERY "},
		{options: ydbstream.CreateOptions{OrReplace: true}, prefix: "CREATE OR REPLACE STREAMING QUERY "},
		{options: ydbstream.CreateOptions{IfNotExists: true}, prefix: "CREATE STREAMING QUERY IF NOT EXISTS "},
		{options: ydbstream.CreateOptions{OrReplace: true, IfNotExists: true}, prefix: "CREATE OR REPLACE STREAMING QUERY IF NOT EXISTS "},
	} {
		t.Run(test.prefix, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbstream.Create("copy", ast.StreamingQuerySpec{Text: body}, test.options), qt.Equals,
				test.prefix+"`copy` WITH (RUN = TRUE, RESOURCE_POOL = `default`) AS DO BEGIN\n"+body+"\nEND DO;")
		})
	}
}
