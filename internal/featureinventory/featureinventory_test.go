package featureinventory_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentsurface"
	"go.5x5.cz/ptah/internal/featureinventory"
)

// The self-test is the gate's proof that it can still fail, so it runs here as
// well as from scripts/check-feature-inventory.sh: `go test ./...` is what a
// contributor runs before the gate exists in their head.
func TestSelfTest_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Assert(featureinventory.SelfTest(), qt.HasLen, 0)
}

func TestLedgerPackages_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		ledger string
		want   []string
	}{
		{
			name:   "a list item is a listing",
			ledger: "- `go.5x5.cz/ptah/core/renderer`\n",
			want:   []string{"go.5x5.cz/ptah/core/renderer"},
		},
		{
			name:   "trailing prose after the closing backtick is not part of the path",
			ledger: "- `go.5x5.cz/ptah/dbschema` -- reads a live database\n",
			want:   []string{"go.5x5.cz/ptah/dbschema"},
		},
		{
			name:   "the same package listed twice is one package",
			ledger: "- `go.5x5.cz/ptah/catalog`\n- `go.5x5.cz/ptah/catalog`\n",
			want:   []string{"go.5x5.cz/ptah/catalog"},
		},
		{
			name:   "the result is sorted regardless of file order",
			ledger: "- `go.5x5.cz/ptah/dbschema`\n- `go.5x5.cz/ptah/catalog`\n",
			want:   []string{"go.5x5.cz/ptah/catalog", "go.5x5.cz/ptah/dbschema"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(featureinventory.LedgerPackages([]byte(test.ledger)), qt.DeepEquals, test.want)
		})
	}
}

// A backticked path in a paragraph is a mention, not a listing. This is the
// distinction stokaro/ptah#2246 put a gate fixture on, and the recognition now
// exists once -- scripts/check-public-api.sh reads the ledger through this
// function -- so the two can no longer drift into different answers.
func TestLedgerPackages_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		ledger string
	}{
		{name: "a prose paragraph", ledger: "A paragraph mentioning `go.5x5.cz/ptah/gateselftest` is not a listing.\n"},
		{name: "an indented list item", ledger: "  - `go.5x5.cz/ptah/nested`\n"},
		{name: "a bullet with no backticks", ledger: "- go.5x5.cz/ptah/bare\n"},
		{name: "a package outside this module", ledger: "- `github.com/spf13/cobra`\n"},
		{name: "the module path with nothing after it", ledger: "- `go.5x5.cz/ptah`\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(featureinventory.LedgerPackages([]byte(test.ledger)), qt.HasLen, 0)
		})
	}
}

func TestReleaseBinaries_HappyPath(t *testing.T) {
	c := qt.New(t)

	binaries := featureinventory.ReleaseBinaries([]byte(
		"builds:\n" +
			"  - id: ptah\n    main: ./cmd/ptah\n    binary: ptah\n" +
			"  - id: ptah-ls\n    main: ./cmd/ptah-ls\n    binary: ptah-ls\n"))

	c.Assert(binaries, qt.DeepEquals, []string{"ptah", "ptah-ls"})
}

// A program is what the release configuration builds, and nothing else. A
// `go list` scan for main packages answers which programs are technically
// installable, which is not a statement that any of them is supported -- the
// inference that closed the attempt this replaces.
func TestReleaseBinaries_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		release string
	}{
		{name: "no builds block", release: "version: 2\nproject_name: ptah\n"},
		{name: "a build declaring no binary", release: "builds:\n  - id: ptah\n    main: ./cmd/ptah\n"},
		{name: "not yaml at all", release: "\tthis is not: [yaml\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(featureinventory.ReleaseBinaries([]byte(test.release)), qt.HasLen, 0)
		})
	}
}

func TestOwns_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		page string
		want []string
	}{
		{
			name: "a block sequence",
			page: "---\ntitle: Apply\nowns:\n  - cli-ptah-schema-apply\n  - cli-ptah-schema-plan\n---\n\nBody.\n",
			want: []string{"cli-ptah-schema-apply", "cli-ptah-schema-plan"},
		},
		{
			name: "a flow sequence means the same thing here as it does to the site",
			page: "---\ntitle: Apply\nowns: [cli-ptah-schema-apply]\n---\n",
			want: []string{"cli-ptah-schema-apply"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			owns, err := featureinventory.Owns([]byte(test.page))
			c.Assert(err, qt.IsNil)
			c.Assert(owns, qt.DeepEquals, test.want)
		})
	}
}

func TestOwns_ClaimlessPages(t *testing.T) {
	tests := []struct {
		name string
		page string
	}{
		{name: "no frontmatter", page: "# A page\n\nBody.\n"},
		{name: "frontmatter with no owns key", page: "---\ntitle: Apply\n---\n"},
		{name: "an unterminated frontmatter block", page: "---\ntitle: Apply\nowns:\n  - cli-ptah-schema-apply\n"},
		{name: "owns written in the body rather than the frontmatter", page: "---\ntitle: Apply\n---\n\nowns:\n  - cli-ptah-schema-apply\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			owns, err := featureinventory.Owns([]byte(test.page))
			c.Assert(err, qt.IsNil)
			c.Assert(owns, qt.HasLen, 0)
		})
	}
}

// Ownership is joined by string equality on a derived identifier and by nothing
// else. The check that closed the previous attempt computed identifying tokens
// from a feature and accepted a substring hit for any of them, so a page passed
// without documenting the feature it was credited with.
func TestDerive_HappyPath(t *testing.T) {
	c := qt.New(t)

	page := "docs/site/src/content/docs/direct/apply.md"
	doc, problems := featureinventory.Derive(featureinventory.Sources{
		// `schema apply-plan` derives an identifier that CONTAINS the claimed
		// one. Under the substring join that closed the previous attempt it
		// would be credited to the same page; here it stays unclaimed, because
		// the join is equality and there is no search step.
		NativeLeaves: []agentsurface.Leaf{{Name: "schema apply"}, {Name: "schema apply-plan"}},
		Ledger:       []byte("- `go.5x5.cz/ptah/core/renderer`\n"),
		Release:      []byte("builds:\n  - binary: ptah\n"),
		Pages:        []featureinventory.PageClaim{{Path: page, Owns: []string{"cli-ptah-schema-apply"}}},
		Examples:     []featureinventory.Example{{Page: "docs/site/src/content/docs/start/q.mdx"}},
	})

	c.Assert(problems, qt.HasLen, 0)
	c.Assert(doc.Owned, qt.Equals, 1)
	c.Assert(rowOwner(c, doc, "cli-ptah-schema-apply"), qt.Equals, page)
	c.Assert(rowSurface(c, doc, "cli-ptah-schema-apply"), qt.Equals, "ptah schema apply")
	c.Assert(rowByID(c, doc, "cli-ptah-schema-apply-plan").Owner, qt.IsNil)
	c.Assert(unclaimed(doc), qt.DeepEquals, []string{"cli-ptah-schema-apply-plan", "gopkg-core-renderer"})
}

// The identifiers a page may claim are the ones the derivation produces, and a
// claim naming anything else is reported rather than ignored. Every rule has
// its own fixture in SelfTest; this is the one a reader meets first.
func TestDerive_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, problems := featureinventory.Derive(featureinventory.Sources{
		NativeLeaves: []agentsurface.Leaf{{Name: "schema apply"}},
		Ledger:       []byte("- `go.5x5.cz/ptah/core/renderer`\n"),
		Release:      []byte("builds:\n  - binary: ptah\n"),
		Pages:        []featureinventory.PageClaim{{Path: "a.md", Owns: []string{"cli-ptah-schema-aplly"}}},
		Examples:     []featureinventory.Example{{Page: "q.mdx"}},
	})

	c.Assert(featureinventory.RulesOf(problems), qt.DeepEquals, []string{featureinventory.RuleUnknownClaim})
	c.Assert(problems[0].Message, qt.Equals,
		`a.md claims "cli-ptah-schema-aplly" in its owns: frontmatter, and the derivation produces no such feature`)
}

func TestRender_HappyPath(t *testing.T) {
	c := qt.New(t)

	doc, _ := featureinventory.Derive(featureinventory.Sources{
		NativeLeaves: []agentsurface.Leaf{{Name: "db read"}},
		Ledger:       []byte("- `go.5x5.cz/ptah/core/renderer`\n"),
		Release:      []byte("builds:\n  - binary: ptah\n"),
		Examples:     []featureinventory.Example{{Page: "q.mdx"}},
	})

	first, err := featureinventory.Render(doc)
	c.Assert(err, qt.IsNil)
	second, err := featureinventory.Render(doc)
	c.Assert(err, qt.IsNil)

	c.Assert(string(second), qt.Equals, string(first))
	c.Assert(strings.HasSuffix(string(first), "\n"), qt.IsTrue)
	c.Assert(featureinventory.CommittedFloor(first), qt.Equals, doc.OwnedFloor)
	// An unclaimed row carries null rather than an empty string, so a reader
	// cannot mistake "nobody documents this" for "documented at the repository
	// root".
	c.Assert(strings.Count(string(first), `"owner": null`), qt.Equals, len(doc.Rows))
}

// rowOwner is the claimed page of one row, and fails the test when no such row
// exists rather than answering for a row that is not there.
func rowOwner(c *qt.C, doc *featureinventory.Document, id string) string {
	row := rowByID(c, doc, id)
	c.Assert(row.Owner, qt.IsNotNil)
	return *row.Owner
}

func rowSurface(c *qt.C, doc *featureinventory.Document, id string) string {
	return rowByID(c, doc, id).Surface
}

func rowByID(c *qt.C, doc *featureinventory.Document, id string) featureinventory.Row {
	c.Helper()
	for _, row := range doc.Rows {
		if row.ID == id {
			return row
		}
	}
	c.Fatalf("no row carries the identifier %q", id)
	return featureinventory.Row{}
}

// unclaimed is the identifiers no page claims, capped so the assertion above
// reads as a list rather than as a wall.
func unclaimed(doc *featureinventory.Document) []string {
	var ids []string
	for _, row := range doc.Rows {
		if row.Owner == nil && len(ids) < 2 {
			ids = append(ids, row.ID)
		}
	}
	return ids
}
