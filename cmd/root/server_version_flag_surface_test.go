package root_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/serverversion"
	"go.5x5.cz/ptah/cmd/root"
)

// This file is the LIVE ENUMERATION of the commands whose --dialect decides
// which SQL Ptah produces without a server to ask.
//
// stokaro/ptah#916 scope item 5 asks for the offline analogue of
// dbschema.TestLiveCapabilityPathsAvoidVersionBlindFactories: a command that
// takes a dialect and never connects has no banner to read, so whatever
// capability preset it plans against was chosen by a default nobody typed.
// `ptah schema render` was the one with no spelling for correcting it — it
// refused a foreign-key schema that MySQL 8.0 accepts, at exit 2, with no flag
// to say which MySQL.
//
// The set is derived by walking the built command tree rather than written
// down, because it has already moved twice while the issue was open:
// `migrations checkpoint` gained --shadow-db and `migrations lint` gained
// --dev-url, and each time a census taken by hand went stale in silence.
//
// The identification of the version flag is the other half, and it is the half
// that decides whether this file measures anything. Two commands in the walk
// carry a --version flag that has nothing to do with a server:
// `migrations checkpoint --version` names the checkpoint it writes and
// `schema push --version` names the artifact tag it publishes. A predicate
// asking "is there a flag called version?" would score both as covered and the
// whole gate would read green while proving nothing. serverversion.Lookup
// answers from an annotation the registrar sets instead, and
// TestDialectVerbs_ExemptOnesCarryNoServerVersionFlag asserts it finds NOTHING
// on those two — the control that fails if the predicate is ever weakened back
// to a name comparison.

// dialectVerb is one command in the walk, with the reason it does or does not
// need a way to name the server.
type dialectVerb struct {
	// verb is the space-joined command path, matching what the walk prints.
	verb string
	// why records what --dialect decides on this command. The exempt rows are
	// the interesting ones: each names something other than a capability
	// preset, and that is the whole reason the exemption is defensible.
	why string
	// carriesAnUnrelatedVersionFlag marks an exempt command whose --version
	// means something that is not a server. It exists so the exemption
	// assertion cannot pass by measuring a flag that is simply absent.
	carriesAnUnrelatedVersionFlag bool
}

// offlineDialectVerbs are the commands whose --dialect selects a capability
// preset with no live server to read a version from. Each must offer a way to
// name that server.
func offlineDialectVerbs() []dialectVerb {
	return []dialectVerb{
		{
			verb: "schema render",
			why: "--dialect selects the capability preset the DDL is rendered against, " +
				"and nothing here connects to a server",
		},
		{
			verb: "sql lint",
			why: "--dialect selects the capability preset the CAP rules are evaluated against, " +
				"and nothing here connects to a server",
		},
	}
}

// exemptDialectVerbs are the rest of the walk: commands where --dialect
// decides something a server version cannot refine. Each row says what.
func exemptDialectVerbs() []dialectVerb {
	return []dialectVerb{
		{
			verb: "migrations lint",
			why: "--dialect gates dialect-specific rules by NAME; migration/lint imports no " +
				"capability package, so there is no preset for a version to refine. " +
				"Gating those rules by capability is stokaro/ptah#929, not this",
		},
		{
			verb:                          "migrations checkpoint",
			carriesAnUnrelatedVersionFlag: true,
			why: "the directory is replayed into the required --shadow-db, a live server whose " +
				"banner is the version; --dialect only names what that URL would have said, " +
				"and --version names the checkpoint being written",
		},
		{
			verb:                          "schema push",
			carriesAnUnrelatedVersionFlag: true,
			why: "--dialect is a parser hint for SQL schema files; the artifact published is " +
				"canonical HCL and no dialect SQL is rendered, and --version is the " +
				"write-once artifact tag",
		},
	}
}

// accountedDialectVerbs is the union the walk is checked against.
func accountedDialectVerbs() []dialectVerb {
	return append(slices.Clone(offlineDialectVerbs()), exemptDialectVerbs()...)
}

// TestDialectVerbs_AreAllAccountedFor requires the walk and the two tables to
// agree in both directions, so a command added later cannot land without
// saying which of the two it is.
func TestDialectVerbs_AreAllAccountedFor(t *testing.T) {
	c := qt.New(t)

	walked := nativeVerbsRegisteringFlag(root.NewRootCommand(), "dialect")
	c.Assert(len(walked) > 0, qt.IsTrue,
		qt.Commentf("the walk found no --dialect at all, so it is measuring nothing"))

	accounted := make([]string, 0, len(accountedDialectVerbs()))
	for _, row := range accountedDialectVerbs() {
		accounted = append(accounted, row.verb)
	}
	slices.Sort(accounted)

	for _, verb := range walked {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(accounted, qt.Contains, verb,
				qt.Commentf("%q registers --dialect but no row states whether it needs a server version", verb))
		})
	}
	for _, verb := range accounted {
		t.Run("still registered: "+verb, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(walked, qt.Contains, verb,
				qt.Commentf("%q is accounted for but no longer registers --dialect", verb))
		})
	}
}

// TestDialectVerbs_OfflineOnesTakeAServerVersion is the gate stokaro/ptah#916
// scope item 5 asks for.
func TestDialectVerbs_OfflineOnesTakeAServerVersion(t *testing.T) {
	c := qt.New(t)

	commands := nativeLeafCommands(root.NewRootCommand())
	c.Assert(len(offlineDialectVerbs()) > 0, qt.IsTrue,
		qt.Commentf("no row requires a server version, so this gate asserts nothing"))

	for _, row := range offlineDialectVerbs() {
		t.Run(row.verb, func(t *testing.T) {
			c := qt.New(t)

			cmd := commands[row.verb]
			c.Assert(cmd, qt.IsNotNil,
				qt.Commentf("%q is not a leaf command in the built tree", row.verb))
			c.Assert(serverversion.Lookup(cmd), qt.IsNotNil,
				qt.Commentf("%q has no flag carrying the server-version contract: %s", row.verb, row.why))
		})
	}
}

// TestDialectVerbs_ExemptOnesCarryNoServerVersionFlag is the control that stops
// the gate above from passing on a predicate that reads flag names.
//
// `migrations checkpoint --version` and `schema push --version` both exist and
// both mean something else. If serverversion.Lookup ever went back to matching
// on the name, this test — not the one above — is what goes red.
func TestDialectVerbs_ExemptOnesCarryNoServerVersionFlag(t *testing.T) {
	commands := nativeLeafCommands(root.NewRootCommand())

	for _, row := range exemptDialectVerbs() {
		t.Run(row.verb, func(t *testing.T) {
			c := qt.New(t)

			cmd := commands[row.verb]
			c.Assert(cmd, qt.IsNotNil)
			c.Assert(serverversion.Lookup(cmd), qt.IsNil,
				qt.Commentf("%q is exempt but carries the server-version contract: %s", row.verb, row.why))
		})
	}

	// The unrelated --version flags have to actually be there, or the
	// assertion above is measuring an absence rather than a distinction.
	for _, row := range unrelatedVersionFlagVerbs() {
		t.Run("unrelated --version still present on "+row.verb, func(t *testing.T) {
			c := qt.New(t)

			cmd := commands[row.verb]
			c.Assert(cmd, qt.IsNotNil)
			c.Assert(cmd.Flags().Lookup("version"), qt.IsNotNil,
				qt.Commentf("%q no longer has the --version this control tells apart from a server version", row.verb))
		})
	}
}

// TestServerVersion_EveryRegistrationSharesOneHelpString pins the other half of
// the registrar.
//
// The flag is spelled --server-version on `schema render` and --version on
// `sql lint`, which predates the name. Two spellings are already one more than
// anybody wants; two spellings AND two descriptions would leave an operator
// reading both help pages unable to tell whether they mean the same thing.
func TestServerVersion_EveryRegistrationSharesOneHelpString(t *testing.T) {
	c := qt.New(t)

	commands := nativeLeafCommands(root.NewRootCommand())
	c.Assert(len(offlineDialectVerbs()) > 1, qt.IsTrue,
		qt.Commentf("fewer than two registrations accounted for, so sharing is not being measured"))

	for _, row := range offlineDialectVerbs() {
		t.Run(row.verb, func(t *testing.T) {
			c := qt.New(t)

			flag := serverversion.Lookup(commands[row.verb])
			c.Assert(flag, qt.IsNotNil)
			// Contains rather than Equals: cmdflags.InstallEnvBinding appends
			// " [env: PTAH_...]" to every registered usage string, so the
			// shared wording is never the last bytes.
			c.Assert(flag.Usage, qt.Contains, serverversion.Usage,
				qt.Commentf("the server-version help on %q is not the shared one", row.verb))
		})
	}
}

// unrelatedVersionFlagVerbs are the exempt rows that carry a --version meaning
// something other than a server. It is a function rather than a filter inside a
// test body because the repository's test style keeps branching in the table.
func unrelatedVersionFlagVerbs() []dialectVerb {
	rows := make([]dialectVerb, 0, len(exemptDialectVerbs()))
	for _, row := range exemptDialectVerbs() {
		if !row.carriesAnUnrelatedVersionFlag {
			continue
		}
		rows = append(rows, row)
	}
	return rows
}

// nativeLeafCommands maps every runnable command path below root to its
// command. "Runnable" means a leaf, the same definition nativeFlagUsages uses,
// so the two walks cannot drift apart about what a command is.
func nativeLeafCommands(tree *cobra.Command) map[string]*cobra.Command {
	leaves := make(map[string]*cobra.Command)
	var walk func(cmd *cobra.Command, path []string)
	walk = func(cmd *cobra.Command, path []string) {
		children := cmd.Commands()
		for _, child := range children {
			walk(child, append(slices.Clone(path), child.Name()))
		}
		if len(children) > 0 {
			return
		}
		leaves[strings.Join(path, " ")] = cmd
	}
	for _, child := range tree.Commands() {
		walk(child, []string{child.Name()})
	}
	return leaves
}
