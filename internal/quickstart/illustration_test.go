package quickstart_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/quickstart"
)

// illustratedPage carries one runnable step per shell and, beside each, a block
// of the same language marked as an illustration.
//
// A reference page has commands it teaches and cannot run — one against a
// database it never creates — and output quoted from a failure rather than
// captured from a step. Before the marker the only way to keep such a block out
// of a run was to drop its language, which `check-style` refuses and which costs
// the reader the highlighting (stokaro/ptah#3018).
const illustratedPage = `---
title: A page that shows more than it runs
description: One real step per shell, and three blocks the runner must leave alone.
quickstart: true
---

import { Tabs, TabItem } from '@astrojs/starlight/components';

## Run one thing

<Tabs syncKey="os">
<TabItem label="Linux and macOS">

` + "```bash" + `
echo ran
` + "```" + `

</TabItem>
<TabItem label="Windows PowerShell">

` + "```powershell" + `
Write-Output ran
` + "```" + `

</TabItem>
</Tabs>

Expected output on standard output:

` + "```text" + `
ran
` + "```" + `

## Show three things

` + "```bash illustration" + `
ptah migrations repair --db-url "$DATABASE_URL"
` + "```" + `

` + "```powershell illustration" + `
ptah migrations repair --db-url "$DATABASE_URL"
` + "```" + `

` + "```text illustration" + `
error: something this page quotes rather than produces
` + "```" + `
`

// TestExtract_LeavesAnIllustrationOutOfTheRun is the rule the marker exists for.
//
// Each shell keeps its one real step and its one assertion. Without the marker
// the two command blocks below become steps the runner executes — against a
// database no page created — and the quoted error becomes an assertion no
// command produces.
func TestExtract_LeavesAnIllustrationOutOfTheRun(t *testing.T) {
	tests := []struct {
		name  string
		shell quickstart.Shell
	}{
		{name: "bash", shell: quickstart.Bash},
		{name: "powershell", shell: quickstart.PowerShell},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			page, err := quickstart.Extract("testdata/illustrated.mdx", []byte(illustratedPage))
			c.Assert(err, qt.IsNil)
			c.Assert(page, qt.IsNotNil)

			found, ok := page.Program(test.shell)
			c.Assert(ok, qt.IsTrue)
			c.Assert(found.Steps(), qt.Equals, 1)
			c.Assert(found.Expectations(), qt.Equals, 1)
			c.Assert(found.Actions[0].Expectations[0].Lines, qt.DeepEquals, []string{"ran"})
		})
	}
}

// TestExtract_ReadsTheMarkerCaseInsensitivelyAndBesideOtherOptions keeps the
// marker readable where a fence already carries Starlight's own options.
//
// The word sits in the info string after the language, which is where a title
// or a frame setting goes, so it has to survive company and capitalization.
func TestExtract_ReadsTheMarkerCaseInsensitivelyAndBesideOtherOptions(t *testing.T) {
	tests := []struct {
		name  string
		fence string
	}{
		{name: "lower case", fence: "```bash illustration"},
		{name: "capitalized", fence: "```bash Illustration"},
		{name: "beside a title", fence: `` + "```bash title=\"never run\" illustration" + ``},
		{name: "before a title", fence: `` + "```bash illustration title=\"never run\"" + ``},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			source := "---\ntitle: t\ndescription: d\nquickstart: true\n---\n\n" +
				"```console\necho ran\n```\n\nExpected output on standard output:\n\n" +
				"```text\nran\n```\n\n" + test.fence + "\nnot a step\n```\n"

			page, err := quickstart.Extract("testdata/marked.mdx", []byte(source))
			c.Assert(err, qt.IsNil)

			found, ok := page.Program(quickstart.Bash)
			c.Assert(ok, qt.IsTrue)
			c.Assert(found.Steps(), qt.Equals, 1)
			c.Assert(found.Actions[0].Body, qt.Equals, "echo ran")
		})
	}
}

// TestExtract_AnUnmarkedFenceIsStillRead is the control.
//
// Every row above passes if the extractor stopped reading fences with an info
// string at all. The same page without the word keeps the second block as a
// step, which is what makes the marker a decision rather than a side effect.
func TestExtract_AnUnmarkedFenceIsStillRead(t *testing.T) {
	c := qt.New(t)

	source := "---\ntitle: t\ndescription: d\nquickstart: true\n---\n\n" +
		"```console\necho ran\n```\n\nExpected output on standard output:\n\n" +
		"```text\nran\n```\n\n" + "```bash title=\"run me\"" + "\necho second\n```\n"

	page, err := quickstart.Extract("testdata/unmarked.mdx", []byte(source))
	c.Assert(err, qt.IsNil)

	found, ok := page.Program(quickstart.Bash)
	c.Assert(ok, qt.IsTrue)
	c.Assert(found.Steps(), qt.Equals, 2)
	c.Assert(found.Actions[1].Body, qt.Equals, "echo second")
}
