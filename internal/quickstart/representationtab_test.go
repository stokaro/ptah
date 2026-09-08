package quickstart_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/quickstart"
)

// A tab set is not always a shell split. The adoption page tabs four
// representations of one schema -- SQL beside HCL, DBML and Go annotations --
// and a command inside such a panel has the same bytes in both shells
// (stokaro/ptah#3018).
//
// The pair below is one predicate seen from both sides: a `console` block is
// refused where the tab selects one shell, and admitted where it selects both.
// Testing only the refusal would keep passing after the rule stopped admitting
// anything at all.
const (
	shellTabPage = `---
quickstart: true
---

import { Tabs, TabItem } from '@astrojs/starlight/components';

<Tabs syncKey="os">
<TabItem label="Linux and macOS">

` + "```console\nptah version\n```" + `

</TabItem>
</Tabs>
`
	representationTabPage = `---
quickstart: true
---

import { Tabs, TabItem } from '@astrojs/starlight/components';

<Tabs syncKey="schema-source">
<TabItem label="SQL">

` + "```console\nptah version\n```" + `

</TabItem>
</Tabs>
`
)

// TestExtract_AConsoleBlockInARepresentationTab_HappyPath is the half that is
// easy to lose: the panel is not a shell, so the step belongs to both shells
// and the page keeps its layout.
func TestExtract_AConsoleBlockInARepresentationTab_HappyPath(t *testing.T) {
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

			page, err := quickstart.Extract("p.mdx", []byte(representationTabPage))

			c.Assert(err, qt.IsNil)
			c.Assert(page, qt.IsNotNil)
			found, ok := page.Program(test.shell)
			c.Assert(ok, qt.IsTrue)
			c.Assert(found.Actions, qt.HasLen, 1)
		})
	}
}

// TestExtract_AConsoleBlockInAShellTab_FailurePath keeps the refusal the tab
// set it was written for. A shell-neutral command inside a panel that already
// selects one shell says two contradictory things about which shell runs it.
func TestExtract_AConsoleBlockInAShellTab_FailurePath(t *testing.T) {
	c := qt.New(t)

	page, err := quickstart.Extract("p.mdx", []byte(shellTabPage))

	c.Assert(err, qt.ErrorMatches,
		`.*a console block is shell-neutral and must sit outside a tab that selects one shell.*`)
	c.Assert(page, qt.IsNil)
}
