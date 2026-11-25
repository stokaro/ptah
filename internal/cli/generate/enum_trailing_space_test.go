package generate_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/generate"
)

// enumTrailingSpaceDocument is a YAML document whose column n declares the
// value 'ok ', with the platform group of the column.
func enumTrailingSpaceDocument(c *qt.C, platform string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(path, []byte(`tables:
  c:
    columns:
      id: {type: INTEGER, primary: true}
      n:
        enum: ['ok ', x]
`+platform), 0o600), qt.IsNil)
	return path
}

// The render validates a declaration before it renders it, so a value the
// server would not keep is refused here as it is in a plan, and nothing
// reaches stdout.
func TestGenerateCommand_EnumTrailingSpace_FailurePath(t *testing.T) {
	c := qt.New(t)
	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{"--schema-file", enumTrailingSpaceDocument(c, ""), "--dialect", "mysql"})

	stdout, _, err := executeGenerate(c, cmd)

	c.Assert(err, qt.ErrorMatches, `error rendering mysql schema: column "n" of table "c" declares the enum value "ok ", `+
		`and mysql removes the trailing spaces of an ENUM value, so the column would never match its declaration: declare "ok", `+
		`or name the binary character set on the column or its table; a binary schema default does not count \(stokaro/ptah#6665\)`)
	c.Assert(stdout, qt.Equals, "")
}

// A column of the binary character set keeps the space, so the same value
// renders.
func TestGenerateCommand_EnumTrailingSpace_HappyPath(t *testing.T) {
	c := qt.New(t)
	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{
		"--schema-file", enumTrailingSpaceDocument(c, "        platform:\n          mysql: {charset: binary}\n"),
		"--dialect", "mysql",
	})

	stdout, stderr, err := executeGenerate(c, cmd)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "ENUM('ok ', 'x') CHARACTER SET binary")
}
