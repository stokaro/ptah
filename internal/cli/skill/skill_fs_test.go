package skill_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/skill"
)

func run(c *qt.C, args ...string) (string, error) {
	command := skill.NewCommand()
	var out bytes.Buffer
	command.SetOut(&out)
	command.SetErr(&out)
	command.SetArgs(args)
	err := command.Execute()
	return out.String(), err
}

func TestList_HappyPath(t *testing.T) {
	c := qt.New(t)

	out, err := run(c, "list")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Matches, `(?s)ptah-schema\n  Change a database schema in a project that uses Ptah\. .*\n`)
}

func TestInstall_HappyPath(t *testing.T) {
	t.Run("into a named directory", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()

		out, err := run(c, "install", "ptah-schema", "--skills-dir", dir)

		c.Assert(err, qt.IsNil)
		c.Assert(out, qt.Equals, "written "+filepath.Join(dir, "ptah-schema", "SKILL.md")+"\n")
	})
	t.Run("for several agents under the home directory, each directory once", func(t *testing.T) {
		c := qt.New(t)
		home := t.TempDir()
		t.Setenv("HOME", home)
		t.Setenv("USERPROFILE", home)

		out, err := run(c, "install", "--agent", "claude", "--agent", "codex", "--agent", "cursor", "--user")

		c.Assert(err, qt.IsNil)
		c.Assert(out, qt.Equals,
			"written "+filepath.Join(home, ".claude", "skills", "ptah-schema", "SKILL.md")+"\n"+
				"written "+filepath.Join(home, ".agents", "skills", "ptah-schema", "SKILL.md")+"\n")
		for _, dir := range []string{".claude", ".agents"} {
			_, statErr := os.Stat(filepath.Join(home, dir, "skills", "ptah-schema", "SKILL.md"))
			c.Assert(statErr, qt.IsNil)
		}
	})
}

func TestInstall_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"no destination", []string{"install"}, `name the agent with --agent, or a skills directory with --skills-dir`},
		{"unknown agent", []string{"install", "--agent", "windsurf"}, `unknown agent "windsurf"; use one of claude, codex, cursor, copilot`},
		{"agent and directory", []string{"install", "--agent", "claude", "--skills-dir", "x"},
			`if any flags in the group \[agent skills-dir\] are set none of the others can be; \[agent skills-dir\] were all set`},
		{"unknown skill", []string{"install", "missing", "--skills-dir", "x"}, `unknown skill "missing"; Ptah ships ptah-schema`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())

			_, err := run(c, test.args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			left, readErr := os.ReadDir(".")
			c.Assert(readErr, qt.IsNil)
			c.Assert(left, qt.HasLen, 0)
		})
	}
}
