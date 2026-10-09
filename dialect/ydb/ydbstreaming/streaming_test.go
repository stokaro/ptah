package ydbstreaming_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbstreaming"
)

const body = "INSERT INTO output SELECT * FROM input;"

func TestValidate_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbstreaming.Validate(ydbstreaming.Spec{Text: body}), qt.IsNil)
	c.Assert(ydbstreaming.Equal(ydbstreaming.Spec{Text: body}, ydbstreaming.Spec{Text: " " + body + "\n", Run: new(true), ResourcePool: "default"}), qt.IsTrue)
}

func TestValidate_FailurePath(t *testing.T) {
	for _, text := range []string{"", "DROP TABLE t;", "END DO; DROP TABLE t; DO BEGIN SELECT 1;"} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbstreaming.Validate(ydbstreaming.Spec{Text: text}), qt.IsNotNil)
		})
	}
}

func TestAlter_SettingsKeepCheckpoints(t *testing.T) {
	c := qt.New(t)
	got, err := ydbstreaming.Alter("q", ydbstreaming.Spec{Text: body, Run: new(false)}, ydbstreaming.Spec{Text: body}, ydbstreaming.AlterOptions{})
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `q` SET (RUN = FALSE, RESOURCE_POOL = `default`);")
}

func TestAlter_RequiresResetPermission(t *testing.T) {
	c := qt.New(t)
	got, err := ydbstreaming.Alter("q", ydbstreaming.Spec{Text: body + " SELECT 2;"}, ydbstreaming.Spec{Text: body}, ydbstreaming.AlterOptions{})
	c.Assert(err, qt.ErrorMatches, `.*allow_state_reset=true.*`)
	c.Assert(got, qt.Equals, "")
}

func TestAlter_ExplicitReset(t *testing.T) {
	c := qt.New(t)
	got, err := ydbstreaming.Alter("q", ydbstreaming.Spec{Text: body + " SELECT 2;"}, ydbstreaming.Spec{Text: body}, ydbstreaming.AlterOptions{AllowStateReset: true})
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "ALTER STREAMING QUERY `q` SET (RUN = TRUE, RESOURCE_POOL = `default`, FORCE = TRUE) AS DO BEGIN\n"+body+" SELECT 2;\nEND DO;")
}

func TestCreate_Guards(t *testing.T) {
	for _, test := range []struct {
		options ydbstreaming.CreateOptions
		prefix  string
	}{
		{prefix: "CREATE STREAMING QUERY "},
		{options: ydbstreaming.CreateOptions{OrReplace: true}, prefix: "CREATE OR REPLACE STREAMING QUERY "},
		{options: ydbstreaming.CreateOptions{IfNotExists: true}, prefix: "CREATE STREAMING QUERY IF NOT EXISTS "},
		{options: ydbstreaming.CreateOptions{OrReplace: true, IfNotExists: true}, prefix: "CREATE OR REPLACE STREAMING QUERY IF NOT EXISTS "},
	} {
		t.Run(test.prefix, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbstreaming.Create("copy", ydbstreaming.Spec{Text: body}, test.options), qt.Equals,
				test.prefix+"`copy` WITH (RUN = TRUE, RESOURCE_POOL = `default`) AS DO BEGIN\n"+body+"\nEND DO;")
		})
	}
}
