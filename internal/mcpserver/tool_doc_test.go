package mcpserver_test

import (
	"context"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/mcpserver"
)

// The two pages that tell a reader which tools this server offers.
const (
	agentGuidePage       = "../../docs/site/src/content/docs/operate/ai-agents.md"
	commandReferencePage = "../../docs/site/src/content/docs/reference/native-commands.md"
)

// The headings whose first table is the one read here. The command reference
// has no heading of its own for the table, so its section heading is the bound.
const (
	readingToolsHeading  = "## The reading tools"
	artifactToolsHeading = "## The artifact tools"
	mcpSectionHeading    = "## An AI client, over MCP"
)

// TestToolDocs_TheGuideListsExactlyTheReadingTools holds the guide's reading
// table and the served surface to each other.
//
// Both pages said four reading tools and listed four, while the server had
// served five since describe_session landed. Nothing caught it: a page is read
// by people, and the person who adds a tool is not the person rereading the
// prose around it. So a tool added without a row written for it reddens here,
// and so does a row for a tool nobody serves.
func TestToolDocs_TheGuideListsExactlyTheReadingTools(t *testing.T) {
	c := qt.New(t)

	c.Assert(documentedTools(c, agentGuidePage, readingToolsHeading), qt.DeepEquals, readingTools(c),
		qt.Commentf("the reading table and the served tools disagree; update %s", agentGuidePage))
}

// TestToolDocs_TheGuideListsExactlyTheArtifactTools is the same check for the
// half a workspace adds.
func TestToolDocs_TheGuideListsExactlyTheArtifactTools(t *testing.T) {
	c := qt.New(t)

	c.Assert(documentedTools(c, agentGuidePage, artifactToolsHeading), qt.DeepEquals, artifactTools(c),
		qt.Commentf("the artifact table and the served tools disagree; update %s", agentGuidePage))
}

// TestToolDocs_TheCommandReferenceListsExactlyTheReadingTools holds the second
// page to the same surface.
//
// It is a separate page with its own copy of the list, which is why it drifted
// separately and has to be measured separately.
func TestToolDocs_TheCommandReferenceListsExactlyTheReadingTools(t *testing.T) {
	c := qt.New(t)

	c.Assert(documentedTools(c, commandReferencePage, mcpSectionHeading), qt.DeepEquals, readingTools(c),
		qt.Commentf("the reading table and the served tools disagree; update %s", commandReferencePage))
}

// TestToolDocs_TablesAreFound is the control on the three tests above.
//
// A parser that stopped finding a table would compare two lists it read as
// empty and report success, which is how a documentation check stops checking
// without saying so. This asserts each table yielded several names, and a name
// no default could have produced.
func TestToolDocs_TablesAreFound(t *testing.T) {
	c := qt.New(t)

	tables := map[string][]string{
		"guide reading":     documentedTools(c, agentGuidePage, readingToolsHeading),
		"guide artifact":    documentedTools(c, agentGuidePage, artifactToolsHeading),
		"reference reading": documentedTools(c, commandReferencePage, mcpSectionHeading),
	}
	for name, documented := range tables {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(len(documented) > 1, qt.IsTrue)
		})
	}
	c.Assert(tables["guide reading"], qt.Contains, "schema_lineage")
	c.Assert(tables["guide artifact"], qt.Contains, "preview_patch")
	c.Assert(tables["reference reading"], qt.Contains, "schema_lineage")
}

// TestToolDocs_TheDocsDoNotNameAToolTheServerDoesNotServe pins one name in
// particular.
//
// Both pages named describe_workspace, which no configuration of this server
// offers. A reader following it writes a call that cannot succeed, and the
// table check above cannot see it because it sat in prose. The ADRs may say the
// name -- recording that a tool was renamed is what a decision record is for --
// so the assertion is scoped to the pages a user reads.
func TestToolDocs_TheDocsDoNotNameAToolTheServerDoesNotServe(t *testing.T) {
	c := qt.New(t)

	for _, page := range []string{agentGuidePage, commandReferencePage} {
		t.Run(page, func(t *testing.T) {
			c := qt.New(t)
			body, err := os.ReadFile(page)
			c.Assert(err, qt.IsNil)
			c.Assert(string(body), qt.Not(qt.Contains), "describe_workspace")
		})
	}
	c.Assert(servedNames(c, readOnlyConfig(c)), qt.Not(qt.Contains), "describe_workspace")
}

// readingTools is what the server offers with no workspace, sorted.
func readingTools(c *qt.C) []string {
	c.Helper()
	return servedNames(c, readOnlyConfig(c))
}

// artifactTools is what a workspace adds, sorted: the served set with a
// workspace, minus the set without one.
//
// Derived rather than listed, so a tool moved between the two halves is a
// failure in the table it left rather than a test nobody updated.
func artifactTools(c *qt.C) []string {
	c.Helper()
	reading := readingTools(c)
	added := make([]string, 0, 3)
	for _, name := range servedNames(c, newWorkspace(c, agentpolicy.VerdictAllow, nil).config) {
		if slices.Contains(reading, name) {
			continue
		}
		added = append(added, name)
	}
	return added
}

// servedNames lists a configuration's tool names by driving a real client, so a
// registration that failed to take is absent here too.
func servedNames(c *qt.C, cfg mcpserver.Config) []string {
	c.Helper()
	session := connect(c, cfg, nil)
	result, err := session.ListTools(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(result.Tools))
	for _, tool := range result.Tools {
		names = append(names, tool.Name)
	}
	slices.Sort(names)
	return names
}

// toolCell matches one backticked token in a table's first column.
var toolCell = regexp.MustCompile("`([a-z_]+)`")

// documentedTools reads the first column of every table under a heading.
func documentedTools(c *qt.C, page, heading string) []string {
	c.Helper()

	body, err := os.ReadFile(page)
	c.Assert(err, qt.IsNil)
	_, after, found := strings.Cut(string(body), heading)
	c.Assert(found, qt.IsTrue, qt.Commentf("heading %q not found in %s", heading, page))
	// Bounded at the next heading of the same level, so a later table on the
	// same page cannot contribute names -- and so a table moved out of this
	// section is a failure here rather than a silent pass on rows read
	// elsewhere.
	section, _, _ := strings.Cut(after, "\n## ")

	names := make([]string, 0, 8)
	for line := range strings.SplitSeq(section, "\n") {
		row := strings.TrimSpace(line)
		if !strings.HasPrefix(row, "|") {
			continue
		}
		first := strings.TrimSpace(strings.Split(strings.Trim(row, "|"), "|")[0])
		for _, match := range toolCell.FindAllStringSubmatch(first, -1) {
			names = append(names, match[1])
		}
	}
	slices.Sort(names)
	return names
}
