package quickstart_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/quickstart"
)

// TestDiscover_HappyPath reads a tree and returns only the pages that opt in.
func TestDiscover_HappyPath(t *testing.T) {
	c := qt.New(t)

	pages, err := quickstart.Discover("testdata/pages")

	c.Assert(err, qt.IsNil)
	c.Assert(pages, qt.HasLen, 1)
	c.Assert(pages[0].Path, qt.Equals, optedInPage)
	c.Assert(pages[0].Title, qt.Equals, "A fixture quick start")
}

// TestCheckFloors_FailurePath is the guard against a run that discovers nothing
// and reports what a complete run reports.
//
// Each floor names the count it found, because "the pages moved" and "the
// extractor stopped matching" look the same from a bare failure.
func TestCheckFloors_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		shell   quickstart.Shell
		wantErr string
	}{
		{
			name:  "bash",
			shell: quickstart.Bash,
			wantErr: `(?s).*found 1 page\(s\) with quickstart: true in their frontmatter, expected at least 2.*` +
				`publishes 3 bash step\(s\), expected at least 6.*` +
				`asserts 2 bash output block\(s\), expected at least 4.*`,
		},
		{
			name:    "powershell",
			shell:   quickstart.PowerShell,
			wantErr: `(?s).*publishes 3 powershell step\(s\), expected at least 6.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			pages, err := quickstart.Discover("testdata/pages")
			c.Assert(err, qt.IsNil)

			c.Assert(quickstart.CheckFloors(pages, test.shell, quickstart.DefaultFloors()), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestCheckFloors_HappyPath is the control: the same pages clear floors they
// actually meet, so the failures above come from the counts and not from a
// check that always fails.
func TestCheckFloors_HappyPath(t *testing.T) {
	c := qt.New(t)

	pages, err := quickstart.Discover("testdata/pages")
	c.Assert(err, qt.IsNil)

	floors := quickstart.Floors{Pages: 1, Steps: 3, Expectations: 2}
	c.Assert(quickstart.CheckFloors(pages, quickstart.Bash, floors), qt.IsNil)
	c.Assert(quickstart.CheckFloors(pages, quickstart.PowerShell, floors), qt.IsNil)
}

// TestDiscoverSkills_HappyPath reads every SKILL.md under the root without an
// opt-in key, titles it with the skill's name, and reads no other file.
func TestDiscoverSkills_HappyPath(t *testing.T) {
	c := qt.New(t)

	skills, err := quickstart.DiscoverSkills("testdata/skills")

	c.Assert(err, qt.IsNil)
	c.Assert(skills, qt.HasLen, 1)
	c.Assert(skills[0].Path, qt.Equals, "testdata/skills/fixture-skill/SKILL.md")
	c.Assert(skills[0].Title, qt.Equals, "fixture-skill")
	program, ok := skills[0].Program(quickstart.Bash)
	c.Assert(ok, qt.IsTrue)
	c.Assert(program.Steps(), qt.Equals, 1)
	c.Assert(program.Expectations(), qt.Equals, 1)
}

// TestCheckSkillFloors_FailurePath holds the skills to a count of their own
// and to the per-page floors a quick start clears.
func TestCheckSkillFloors_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		root    string
		wantErr string
	}{
		{"no skills", "testdata/pages", `quickstart: found 0 agent skill\(s\) with a SKILL.md, expected at least 1`},
		{
			"too few steps", "testdata/skills",
			`(?s)quickstart: testdata/skills/fixture-skill/SKILL.md publishes 1 bash step\(s\), expected at least 6.*` +
				`asserts 1 bash output block\(s\), expected at least 4`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			skills, err := quickstart.DiscoverSkills(test.root)
			c.Assert(err, qt.IsNil)

			c.Assert(quickstart.CheckSkillFloors(skills, quickstart.Bash, quickstart.DefaultFloors()), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestCheckSkillFloors_HappyPath is the control: the fixture skill clears
// floors it meets.
func TestCheckSkillFloors_HappyPath(t *testing.T) {
	c := qt.New(t)
	skills, err := quickstart.DiscoverSkills("testdata/skills")
	c.Assert(err, qt.IsNil)

	err = quickstart.CheckSkillFloors(skills, quickstart.Bash, quickstart.Floors{Steps: 1, Expectations: 1, Skills: 1})

	c.Assert(err, qt.IsNil)
}
