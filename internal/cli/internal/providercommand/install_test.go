package providercommand_test

import (
	"errors"
	"io"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/providercommand"
)

// errOwnReport is the failure report of a verb that configured its own
// selection, which Install must leave in place.
var errOwnReport = errors.New("the verb's own report")

// installTree is a native tree with a verb of every kind Install tells apart.
func installTree() *cobra.Command {
	root := &cobra.Command{Use: "ptah"}
	run := func(*cobra.Command, []string) error { return nil }

	db := &cobra.Command{Use: "db"}
	read := &cobra.Command{Use: "read", RunE: run}
	read.Flags().String("db-url", "", "")
	db.AddCommand(read)

	render := &cobra.Command{Use: "render", RunE: run}
	render.Flags().String("dialect", "", "")
	adopt := &cobra.Command{Use: "adopt", RunE: run}
	adopt.Flags().String("env", "", "")
	validate := &cobra.Command{Use: "validate", RunE: run}
	validate.Flags().String("dev-url", "", "")
	version := &cobra.Command{Use: "version", RunE: run}

	assist := &cobra.Command{Use: "assist", RunE: run}
	assist.Flags().String("dialect", "", "")

	// A group that does its own work, as `ptah assist` does, is a verb too.
	group := &cobra.Command{Use: "group", RunE: run}
	group.Flags().String("db-url", "", "")
	group.AddCommand(&cobra.Command{Use: "child", RunE: run})

	own := &cobra.Command{Use: "own", RunE: run}
	own.Flags().String("db-url", "", "")
	providercommand.Configure(own, func(*cobra.Command, error) error { return errOwnReport })

	root.AddCommand(db, render, adopt, validate, version, assist, group, own)
	return root
}

func TestInstall_GivesEveryVerbThatNamesAnEngineTheProviderFlags(t *testing.T) {
	for _, row := range []struct {
		path     []string
		selected bool
	}{
		{[]string{"db", "read"}, true},
		{[]string{"render"}, true},
		{[]string{"adopt"}, true},
		{[]string{"validate"}, true},
		{[]string{"version"}, false},
		{[]string{"group"}, true},
		{[]string{"group", "child"}, false},
		{[]string{"assist"}, false},
	} {
		t.Run(row.path[len(row.path)-1], func(t *testing.T) {
			c := qt.New(t)
			root := installTree()

			providercommand.Install(root)

			verb, _, err := root.Find(row.path)
			c.Assert(err, qt.IsNil)
			c.Assert(verb.Flags().Lookup(providercommand.ConfigFlag) != nil, qt.Equals, row.selected)
			c.Assert(verb.Flags().Lookup(providercommand.EngineFlag) != nil, qt.Equals, row.selected)
		})
	}
}

// A verb that configured its own selection keeps its own failure report:
// Install does not wrap it a second time with the default one.
func TestInstall_KeepsTheReportOfAVerbThatSelectsAlready(t *testing.T) {
	c := qt.New(t)
	root := installTree()
	providercommand.Install(root)
	root.SetArgs([]string{"own", "--provider-config="})

	err := root.ExecuteContext(c.Context())

	c.Assert(err, qt.ErrorIs, errOwnReport)
}

// A verb Install configured reports a selection that fails before it runs, and
// the verb does not run.
func TestInstall_RefusesABlankManifestBeforeTheVerbRuns(t *testing.T) {
	c := qt.New(t)
	root := installTree()
	providercommand.Install(root)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"db", "read", "--provider-config="})

	err := root.ExecuteContext(c.Context())

	c.Assert(err, qt.ErrorMatches, `--provider-config requires a manifest path`)
}

// The exceptions are named by command path, each with its reason, and the
// path of an exception is what keeps a verb out of scope.
func TestSelectsNoProvider(t *testing.T) {
	c := qt.New(t)
	root := installTree()
	assist, _, err := root.Find([]string{"assist"})
	c.Assert(err, qt.IsNil)
	read, _, err := root.Find([]string{"db", "read"})
	c.Assert(err, qt.IsNil)

	c.Assert(providercommand.SelectsNoProvider()["ptah assist"], qt.Not(qt.Equals), "")
	c.Assert(providercommand.InScope(assist), qt.IsFalse)
	c.Assert(providercommand.InScope(read), qt.IsTrue)
}
