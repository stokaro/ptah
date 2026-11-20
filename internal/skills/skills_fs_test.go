package skills_test

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"go.yaml.in/yaml/v3"

	"ptah.run/docs"
	"ptah.run/internal/skills"
)

// skillName is the rule every agent that names its rules agrees on: lowercase
// letters, digits and single hyphens, at most 64 characters, and the same as
// the directory that holds the skill.
var skillName = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// skillFrontmatter is the part of a SKILL.md frontmatter the agents read.
type skillFrontmatter struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

// Every shipped skill parses as the agents parse it: YAML frontmatter, a name
// that matches its directory and the naming rule, and a description short
// enough for each agent's limit. A description holding an unquoted ": " is
// not YAML, and an agent skips the skill without saying so.
func TestShippedSkills_LoadAsAgentsReadThem(t *testing.T) {
	c := qt.New(t)
	entries, err := fs.ReadDir(docs.Skills, "skills")
	c.Assert(err, qt.IsNil)
	c.Assert(len(entries) >= 1, qt.IsTrue)
	for _, entry := range entries {
		t.Run(entry.Name(), func(t *testing.T) {
			c := qt.New(t)
			body, err := fs.ReadFile(docs.Skills, path.Join("skills", entry.Name(), "SKILL.md"))
			c.Assert(err, qt.IsNil)
			parts := strings.SplitN(string(body), "---\n", 3)
			c.Assert(parts, qt.HasLen, 3)
			c.Assert(parts[0], qt.Equals, "")

			var front skillFrontmatter
			c.Assert(yaml.Unmarshal([]byte(parts[1]), &front), qt.IsNil)
			c.Assert(front.Name, qt.Equals, entry.Name())
			c.Assert(front.Name, qt.Matches, skillName)
			c.Assert(len(front.Name) <= 64, qt.IsTrue)
			c.Assert(front.Description, qt.Not(qt.Equals), "")
			c.Assert(len(front.Description) <= 1024, qt.IsTrue, qt.Commentf("%d characters", len(front.Description)))
		})
	}
}

// List reads each description with its own small reader, and the YAML parse
// an agent makes is the reference it has to agree with.
func TestList_DescriptionsMatchTheYAMLParse(t *testing.T) {
	c := qt.New(t)
	shipped, err := skills.List()
	c.Assert(err, qt.IsNil)
	for _, skill := range shipped {
		body, err := fs.ReadFile(docs.Skills, path.Join("skills", skill.Name, "SKILL.md"))
		c.Assert(err, qt.IsNil)
		var front skillFrontmatter
		c.Assert(yaml.Unmarshal([]byte(strings.SplitN(string(body), "---\n", 3)[1]), &front), qt.IsNil)

		c.Assert(skill.Description, qt.Equals, front.Description)
	}
}

func TestList_HappyPath(t *testing.T) {
	c := qt.New(t)

	shipped, err := skills.List()

	c.Assert(err, qt.IsNil)
	c.Assert(shipped, qt.HasLen, 1)
	c.Assert(shipped[0].Name, qt.Equals, "ptah-schema")
	c.Assert(shipped[0].Description, qt.Matches, `^Change a database schema in a project that uses Ptah\. .*ptah\.sum\.$`)
}

func TestParseAgent_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		want skills.Agent
		dir  string
	}{
		{"claude", skills.Claude, filepath.Join(".claude", "skills")},
		{" Codex ", skills.Codex, filepath.Join(".agents", "skills")},
		{"CURSOR", skills.Cursor, filepath.Join(".agents", "skills")},
		{"copilot", skills.Copilot, filepath.Join(".agents", "skills")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			agent, err := skills.ParseAgent(test.name)

			c.Assert(err, qt.IsNil)
			c.Assert(agent, qt.Equals, test.want)
			c.Assert(skills.SkillsDir(agent), qt.Equals, test.dir)
		})
	}
}

func TestParseAgent_FailurePath(t *testing.T) {
	c := qt.New(t)

	agent, err := skills.ParseAgent("windsurf")

	c.Assert(err, qt.ErrorIs, skills.ErrUnknownAgent)
	c.Assert(err, qt.ErrorMatches, `unknown agent "windsurf"; use one of claude, codex, cursor, copilot`)
	c.Assert(agent, qt.Equals, skills.Agent(""))
}

func shippedSkill(c *qt.C) []byte {
	body, err := fs.ReadFile(docs.Skills, "skills/ptah-schema/SKILL.md")
	c.Assert(err, qt.IsNil)
	return body
}

func TestInstall_HappyPath(t *testing.T) {
	t.Run("writes the skill, then leaves it alone", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		target := filepath.Join(dir, "ptah-schema", "SKILL.md")

		first, err := skills.Install([]skills.Target{{Name: "ptah-schema", Dir: dir}}, skills.InstallOptions{})
		c.Assert(err, qt.IsNil)
		second, err := skills.Install([]skills.Target{{Name: "ptah-schema", Dir: dir}}, skills.InstallOptions{})
		c.Assert(err, qt.IsNil)

		c.Assert(first, qt.DeepEquals, []skills.InstalledFile{{Path: target, Status: skills.Written}})
		c.Assert(second, qt.DeepEquals, []skills.InstalledFile{{Path: target, Status: skills.Unchanged}})
		written, err := os.ReadFile(target)
		c.Assert(err, qt.IsNil)
		c.Assert(written, qt.DeepEquals, shippedSkill(c))
	})
	t.Run("every target, in order", func(t *testing.T) {
		c := qt.New(t)
		first, second := t.TempDir(), t.TempDir()

		installed, err := skills.Install([]skills.Target{
			{Name: "ptah-schema", Dir: first},
			{Name: "ptah-schema", Dir: second},
		}, skills.InstallOptions{})

		c.Assert(err, qt.IsNil)
		c.Assert(installed, qt.DeepEquals, []skills.InstalledFile{
			{Path: filepath.Join(first, "ptah-schema", "SKILL.md"), Status: skills.Written},
			{Path: filepath.Join(second, "ptah-schema", "SKILL.md"), Status: skills.Written},
		})
	})
	t.Run("replaces a changed file when asked", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		target := filepath.Join(dir, "ptah-schema", "SKILL.md")
		c.Assert(os.MkdirAll(filepath.Dir(target), 0o750), qt.IsNil)
		c.Assert(os.WriteFile(target, []byte("edited\n"), 0o600), qt.IsNil)

		installed, err := skills.Install([]skills.Target{{Name: "ptah-schema", Dir: dir}}, skills.InstallOptions{Overwrite: true})

		c.Assert(err, qt.IsNil)
		c.Assert(installed, qt.DeepEquals, []skills.InstalledFile{{Path: target, Status: skills.Written}})
		written, err := os.ReadFile(target)
		c.Assert(err, qt.IsNil)
		c.Assert(written, qt.DeepEquals, shippedSkill(c))
	})
}

func TestInstall_FailurePath(t *testing.T) {
	t.Run("a changed file in a later target keeps every target as it was", func(t *testing.T) {
		c := qt.New(t)
		first, second := t.TempDir(), t.TempDir()
		changed := filepath.Join(second, "ptah-schema", "SKILL.md")
		c.Assert(os.MkdirAll(filepath.Dir(changed), 0o750), qt.IsNil)
		c.Assert(os.WriteFile(changed, []byte("edited\n"), 0o600), qt.IsNil)

		installed, err := skills.Install([]skills.Target{
			{Name: "ptah-schema", Dir: first},
			{Name: "ptah-schema", Dir: second},
		}, skills.InstallOptions{})

		c.Assert(err, qt.ErrorIs, skills.ErrFileDiffers)
		c.Assert(installed, qt.IsNil)
		untouched, err := os.ReadDir(first)
		c.Assert(err, qt.IsNil)
		c.Assert(untouched, qt.HasLen, 0)
	})
	t.Run("a changed file is kept", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		target := filepath.Join(dir, "ptah-schema", "SKILL.md")
		c.Assert(os.MkdirAll(filepath.Dir(target), 0o750), qt.IsNil)
		c.Assert(os.WriteFile(target, []byte("edited\n"), 0o600), qt.IsNil)

		installed, err := skills.Install([]skills.Target{{Name: "ptah-schema", Dir: dir}}, skills.InstallOptions{})

		c.Assert(err, qt.ErrorIs, skills.ErrFileDiffers)
		c.Assert(installed, qt.IsNil)
		kept, err := os.ReadFile(target)
		c.Assert(err, qt.IsNil)
		c.Assert(string(kept), qt.Equals, "edited\n")
	})
	for _, name := range []string{"", ".", "..", "../skills/ptah-schema", "ptah-schema/", "ptah-schema/SKILL.md", "missing"} {
		t.Run("unknown skill "+name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()

			installed, err := skills.Install([]skills.Target{{Name: name, Dir: dir}}, skills.InstallOptions{})

			c.Assert(err, qt.ErrorIs, skills.ErrUnknownSkill)
			c.Assert(err, qt.ErrorMatches, `unknown skill ".*"; Ptah ships ptah-schema`)
			c.Assert(installed, qt.IsNil)
			left, err := os.ReadDir(dir)
			c.Assert(err, qt.IsNil)
			c.Assert(left, qt.HasLen, 0)
		})
	}
}
