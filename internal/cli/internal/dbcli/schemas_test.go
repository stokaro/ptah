package dbcli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/cli/internal/dbcli"
)

// A bare field is trimmed, so the common `a, b` spelling keeps working, and a
// quoted field keeps its bytes, which is the only way to name ` a`.
func TestParseSchemas_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, raw string
		want      []string
	}{
		{name: "nothing", raw: "", want: nil},
		{name: "bare fields are trimmed", raw: " a , b ", want: []string{"a", "b"}},
		{name: "a blank bare field is left out", raw: "a,,b, ,", want: []string{"a", "b"}},
		{name: "a field of only commas names nothing", raw: ",", want: make([]string, 0)},
		{name: "a quoted leading space", raw: `" a"`, want: []string{" a"}},
		{name: "a quoted trailing space", raw: `"a "`, want: []string{"a "}},
		{name: "a quoted name beside its bare spelling", raw: `" a",a," a"`, want: []string{" a", "a"}},
		{name: "a quoted comma", raw: `"a,b",c`, want: []string{"a,b", "c"}},
		{name: "a doubled quote is one quote", raw: `"a""b"`, want: []string{`a"b`}},
		{name: "a repeat is left out", raw: "b,a,b", want: []string{"b", "a"}},
		{name: "a quoted field after a space", raw: `a, " a"`, want: []string{"a", " a"}},
		{name: "a quoted field after a tab", raw: "a,\t\"a \"", want: []string{"a", "a "}},
		{name: "a quoted name loses its quotes", raw: `"Sales"`, want: []string{"Sales"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := dbcli.ParseSchemas(test.raw)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A malformed value is refused rather than read as some other list, and a
// quoted field that names no schema is refused rather than left out, since an
// empty selection widens the read.
func TestParseSchemas_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, raw, wantErr string
	}{
		{name: "a bare quote", raw: `a"b`, wantErr: `invalid schema list "a\\"b": parse error on line 1, column 2: bare " in non-quoted-field`},
		{name: "text after a closing quote", raw: `"a"b`, wantErr: `invalid schema list "\\"a\\"b": parse error on line 1, column 3: extraneous or missing " in quoted-field`},
		{name: "an unclosed quote", raw: `"a`, wantErr: `invalid schema list "\\"a": parse error on line 1, column 3: extraneous or missing " in quoted-field`},
		{name: "a line break", raw: "a\nb", wantErr: `invalid schema list "a\\nb": a schema list is one line`},
		{name: "a quoted empty field", raw: `a,""`, wantErr: `invalid schema list "a,\\"\\"": the quoted field "" names no schema`},
		{name: "a quoted blank field", raw: `" "`, wantErr: `invalid schema list "\\" \\"": the quoted field " " names no schema`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := dbcli.ParseSchemas(test.raw)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// JoinSchemas writes what ParseSchemas reads back as the same list, the
// spaces, commas and quotes of each name included, and quotes only the names
// that need it.
func TestJoinSchemas_RoundTrip(t *testing.T) {
	for _, test := range []struct {
		name   string
		list   []string
		joined string
	}{
		{name: "bare names", list: []string{"a", "b"}, joined: "a,b"},
		{name: "a leading space", list: []string{" a", "a"}, joined: `" a",a`},
		{name: "a trailing space", list: []string{"a "}, joined: `"a "`},
		{name: "a comma", list: []string{"a,b"}, joined: `"a,b"`},
		{name: "a quote", list: []string{`a"b`}, joined: `"a""b"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			joined := dbcli.JoinSchemas(test.list)
			got, err := dbcli.ParseSchemas(joined)

			c.Assert(joined, qt.Equals, test.joined)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.list)
		})
	}
}

// schemasCommand is a command with the --schemas flag, parsed from args.
func schemasCommand(c *qt.C, args ...string) (*cobra.Command, *string) {
	c.Helper()
	cmd := &cobra.Command{Use: "read"}
	raw := new(string)
	dbcli.RegisterSchemasFlag(cmd.Flags(), raw)
	c.Assert(cmd.ParseFlags(args), qt.IsNil)
	return cmd, raw
}

// A project's schema list reaches the command as the project wrote it, the
// spaces around a name, a comma and a quote in one included, and a --schemas
// value replaces it.
func TestEffectiveSchemas_HappyPath(t *testing.T) {
	project := projectconfig.Value[[]string]{Value: []string{" a", "a,b", `a"b`}, Present: true}
	for _, test := range []struct {
		name string
		args []string
		want []string
	}{
		{name: "the project's list", want: []string{" a", "a,b", `a"b`}},
		{name: "the flag replaces it", args: []string{"--schemas", `" b",c`}, want: []string{" b", "c"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd, raw := schemasCommand(c, test.args...)

			got, err := dbcli.EffectiveSchemas(cmd, *raw, project, "postgres://localhost/db")

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A malformed --schemas value is refused rather than replaced by the
// project's list.
func TestEffectiveSchemas_FailurePath(t *testing.T) {
	c := qt.New(t)
	cmd, raw := schemasCommand(c, "--schemas", `a"b`)

	got, err := dbcli.EffectiveSchemas(cmd, *raw, projectconfig.Value[[]string]{Value: []string{"a"}, Present: true}, "postgres://localhost/db")

	c.Assert(err, qt.ErrorMatches, `invalid schema list "a\\"b": parse error on line 1, column 2: bare " in non-quoted-field`)
	c.Assert(got, qt.IsNil)
}
