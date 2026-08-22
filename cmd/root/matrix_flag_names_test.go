package root_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
)

// matrixRowsPath is the row data the published feature matrix is generated
// from. The generated page is not read instead: both carry the same sentence,
// so a check against the page would compare a claim with a copy of itself.
const matrixRowsPath = "../../docs/site/scripts/data/feature-matrix-rows.json"

// matrixFlagToken matches a long flag as the notes spell one.
//
// Long flags only. A short one is a single letter and the notes are full of
// em-dash-separated prose, so `-o` would match half the punctuation in the
// file; a rule that fires on prose is a rule people learn to silence.
var matrixFlagToken = regexp.MustCompile(`--[a-z][a-z0-9-]*`)

// matrixFlagsNotRegistered are flag names a note may carry although neither
// binary registers them, each with the reason it is allowed.
//
// Empty, and it is meant to stay that way. It exists so that a legitimate
// exception -- a note quoting a flag of the reference binary that Ptah
// deliberately does not have -- is written down with its reason rather than
// making somebody weaken the rule.
var matrixFlagsNotRegistered = make(map[string]string)

type matrixFlagRow struct {
	Feature string `json:"feature"`
	Note    string `json:"note"`
}

// TestMatrixNotesNameFlagsThatExist holds the most user-visible prose Ptah
// publishes to the command tree it describes.
//
// An audit of all 191 rows found seven notes saying something the tree does not
// do, and one of them was a flag under the wrong name: a row said the version
// is refined by a `--version` server string where the flag is
// `--server-version`. Nothing caught it. The generated page carries the same
// sentence as the row data, the verdict-prose check reads the verdict rather
// than the code, and the citation check opens paths rather than flags
// (stokaro/ptah#1924).
//
// What this catches is a flag name no binary has -- an invention, a
// misspelling, a flag renamed in code while the note kept the old spelling.
// What it does NOT catch is the row that prompted it: `--version` is a real
// flag, cobra's own, so a rule about existence cannot see that the row meant a
// different one. That case needs a rule about which VERB a flag belongs to, and
// the notes name a verb beside a flag seven times in 191 rows -- too thin a
// signal to build a gate on, and the three that do not resolve are prose shape
// rather than error. Recorded here so the next reader does not re-derive it.
func TestMatrixNotesNameFlagsThatExist(t *testing.T) {
	c := qt.New(t)

	rows := readMatrixRows(c)
	c.Assert(len(rows) > 0, qt.IsTrue,
		qt.Commentf("no matrix rows were read, so every assertion below would be vacuous"))

	registered := registeredFlagNames()
	// Both binaries, because a note may describe either surface and the
	// compatibility one carries flags the native tree does not: `--export`,
	// `--url` and `--baseline` are all real and all live there. Checking only
	// the native tree reports fifteen false failures.
	c.Assert(len(registered) > 100, qt.IsTrue,
		qt.Commentf("only %d flags were collected, which is too few to be the whole tree", len(registered)))

	c.Assert(unknownMatrixFlags(rows, registered), qt.DeepEquals, []string(nil))
}

// TestMatrixFlagCheckCanFail is the gate's own control.
//
// A check that reads the rows and asks a question nothing can answer no to
// reports success at exactly the moment it stops working. This asks the same
// question of a name no tree has.
func TestMatrixFlagCheckCanFail(t *testing.T) {
	c := qt.New(t)

	registered := registeredFlagNames()

	_, known := registered["--dev-url"]
	c.Assert(known, qt.IsTrue)
	_, invented := registered["--no-such-flag-anywhere"]
	c.Assert(invented, qt.IsFalse)
}

// unknownMatrixFlags names every flag a note carries that neither tree
// registers, rendered as "flag (row)" so a failure says which row to open
// rather than only which flag is wrong.
func unknownMatrixFlags(rows []matrixFlagRow, registered map[string]struct{}) []string {
	unknown := make(map[string]string)
	for _, row := range rows {
		for _, token := range matrixFlagToken.FindAllString(row.Note, -1) {
			if _, ok := registered[token]; ok {
				continue
			}
			if _, allowed := matrixFlagsNotRegistered[token]; allowed {
				continue
			}
			unknown[token] = row.Feature
		}
	}
	return sortedFlagReports(unknown)
}

func readMatrixRows(c *qt.C) []matrixFlagRow {
	c.Helper()
	raw, err := os.ReadFile(filepath.FromSlash(matrixRowsPath))
	c.Assert(err, qt.IsNil)
	var rows []matrixFlagRow
	c.Assert(json.Unmarshal(raw, &rows), qt.IsNil)
	return rows
}

// registeredFlagNames collects every long flag both command trees register, at
// any depth, local and persistent alike.
func registeredFlagNames() map[string]struct{} {
	names := make(map[string]struct{})
	for _, tree := range []*cobra.Command{
		root.NewRootCommand(),
		atlas.NewCompatCommandWithPolicy("ptah-compat", atlascompatpolicy.Policy{}),
	} {
		collectFlagNames(tree, names)
	}
	return names
}

func collectFlagNames(cmd *cobra.Command, into map[string]struct{}) {
	collect := func(flag *pflag.Flag) { into["--"+flag.Name] = struct{}{} }
	cmd.Flags().VisitAll(collect)
	cmd.PersistentFlags().VisitAll(collect)
	for _, sub := range cmd.Commands() {
		collectFlagNames(sub, into)
	}
}

// sortedFlagReports renders the failures as "flag (row)" so a failure names
// which row to open rather than only which flag is wrong.
func sortedFlagReports(unknown map[string]string) []string {
	if len(unknown) == 0 {
		return nil
	}
	reports := make([]string, 0, len(unknown))
	for flag, feature := range unknown {
		reports = append(reports, flag+" (row: "+feature+")")
	}
	sort.Strings(reports)
	return reports
}
