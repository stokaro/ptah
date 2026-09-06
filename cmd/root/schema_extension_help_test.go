package root_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/cmd/atlas"
	"ptah.run/cmd/root"
	"ptah.run/internal/schemaload"
)

// Help text that lists schema file extensions lists all of them --
// stokaro/ptah#2065.
//
// The set lived in five places: the loader's switch, the message beside it, and
// four command help strings, each spelled by hand. Adding `.dbml` reached three
// of them, so `ptah schema inspect --schema-file x.dbml` read the file while
// its own `--help` said the file had to be .hcl, .yaml, .yml or .sql.
//
// The loader is the authority now, and this walks both command trees rather
// than naming the four strings: a sixth place that lists extensions is found by
// the same rule that found the fifth.
func TestHelpTextListingExtensionsListsAllOfThem(t *testing.T) {
	trees := []struct {
		name string
		root *cobra.Command
	}{
		{name: "ptah", root: root.NewRootCommand()},
		{name: "ptah-compat", root: atlas.NewCompatCommand("ptah-compat")},
	}

	for _, tree := range trees {
		t.Run(tree.name, func(t *testing.T) {
			c := qt.New(t)

			listings := extensionListings(tree.root)

			c.Assert(len(listings) > 0, qt.IsTrue,
				qt.Commentf("no help text mentions a schema file extension; the rule below measures nothing"))
			for _, listing := range listings {
				assertListsEveryExtension(c, listing)
			}
		})
	}
}

func assertListsEveryExtension(c *qt.C, listing helpListing) {
	c.Helper()
	for _, extension := range schemaload.SupportedExtensions() {
		c.Assert(listing.text, qt.Contains, extension,
			qt.Commentf("%s does not name %s", listing.where, extension))
	}
}

type helpListing struct {
	where string
	text  string
}

// extensionListings collects every help string that names a schema file
// extension, from the whole command tree and every flag on it.
func extensionListings(command *cobra.Command) []helpListing {
	var found []helpListing
	for _, sub := range command.Commands() {
		found = append(found, extensionListings(sub)...)
	}
	found = appendListing(found, command.CommandPath()+" long", command.Long)
	found = appendListing(found, command.CommandPath()+" short", command.Short)
	command.Flags().VisitAll(func(flag *pflag.Flag) {
		found = appendListing(found, command.CommandPath()+" --"+flag.Name, flag.Usage)
	})
	return found
}

// listingProximity is how close two extensions have to be to be a listing
// rather than two sentences that each happen to mention one.
//
// `ptah-compat schema plan test` describes `.test.hcl` case files in one
// paragraph and `.sql` in another; that is prose about two different things and
// is not held to this rule. A listing puts them in one enumeration, which is
// always shorter than this.
const listingProximity = 80

// appendListing keeps a string only when it enumerates extensions.
func appendListing(found []helpListing, where, text string) []helpListing {
	if !enumeratesExtensions(text) {
		return found
	}
	return append(found, helpListing{where: where, text: text})
}

func enumeratesExtensions(text string) bool {
	hcl := strings.Index(text, ".hcl")
	sql := strings.Index(text, ".sql")
	if hcl < 0 || sql < 0 {
		return false
	}
	return max(hcl, sql)-min(hcl, sql) <= listingProximity
}
