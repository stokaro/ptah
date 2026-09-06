package agentsurface_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/root"
	"ptah.run/internal/agentsurface"
)

// TestClassification_NamesEveryVerbTheBinaryHas is the guard ADR 0002's
// inventory needed and did not have.
//
// That table was enumerated from the built binary once and then could not be
// edited, because a record of a decision is never rewritten. The surface grew
// by nine verbs and nothing noticed: `schema approve`, `schema security`,
// `schema verify-approval`, `migrations tag`, `migrations test`,
// `migrations up`, `migrations validate`, `assist` and `mcp` were all in the
// binary and in no inventory (stokaro/ptah#1484).
//
// The walk is the measurement and this map is the answer to it. Neither is
// allowed to name something the other does not.
func TestClassification_NamesEveryVerbTheBinaryHas(t *testing.T) {
	c := qt.New(t)

	c.Assert(agentsurface.NamesOf(agentsurface.Walk(root.NewRootCommand())),
		qt.DeepEquals, agentsurface.Names())
}

// TestClassification_AgreesWithTheFlagsEachVerbRegisters is what makes the
// hand-written half checkable.
//
// What a verb DOES to a database cannot be read off its flag set — that is why
// the classification is written down. Which databases it can be POINTED AT can:
// a verb that takes no `--db-url` cannot touch a target, and one that takes no
// `--dev-url` or `--shadow-db` has no second database to rewrite. So the two
// have to agree in both directions, and a classification that drifts from the
// command it describes fails here rather than in a document nobody re-reads.
func TestClassification_AgreesWithTheFlagsEachVerbRegisters(t *testing.T) {
	for _, leaf := range agentsurface.Walk(root.NewRootCommand()) {
		t.Run(leaf.Name, func(t *testing.T) {
			c := qt.New(t)
			verb, known := agentsurface.Lookup(leaf.Name)
			c.Assert(known, qt.IsTrue)

			// A verb can be pointed at a database two ways: by a flag, which
			// Walk can see, or by the project file it was given, which it
			// cannot. The second is enumerated rather than inferred, so the
			// check stays bidirectional for everything else.
			pointable := len(leaf.TargetFlags) > 0 || agentsurface.TargetFromProject(leaf.Name)
			c.Check(verb.Target != agentsurface.TargetNone, qt.Equals, pointable,
				qt.Commentf("target flags %v against target class %q", leaf.TargetFlags, verb.Target))
			c.Check(verb.Scratch != agentsurface.ScratchNone, qt.Equals, len(leaf.ScratchFlags) > 0,
				qt.Commentf("scratch flags %v against scratch class %q", leaf.ScratchFlags, verb.Scratch))
		})
	}
}

// TestClassification_EveryReasonSaysWhat pins the shape that makes a
// classification an answer rather than an assertion.
func TestClassification_EveryReasonSaysWhat(t *testing.T) {
	for _, name := range agentsurface.Names() {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			verb, known := agentsurface.Lookup(name)

			c.Assert(known, qt.IsTrue)
			c.Assert(len(verb.Reason) > 40, qt.IsTrue,
				qt.Commentf("%s: a classification has to say what the verb does, not just that", name))
		})
	}
}

// TestDatabaseSafe_ExcludesWhatCanDestroySomething pins the property the agent
// surface is chosen by, and the two verbs that make it worth having.
//
// `schema inspect` reads its target and is still not exposable, because its dev
// database "is reset destructively" — a destructive capability wearing a
// read-only label, which is the finding ADR 0002 §1.2 was written for.
// `schema test` reads nothing and writes everything: measured on PostgreSQL
// 17.11, a case with an `apply_schema` step created a table in the database
// `--db-url` named and an `exec` step inserted into it. ADR 0002 listed it as a
// pure reader.
func TestDatabaseSafe_ExcludesWhatCanDestroySomething(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{name: "db read", want: true},
		{name: "schema render", want: true},
		{name: "schema compare", want: true},
		{name: "schema inspect", want: false},
		{name: "schema test", want: false},
		{name: "migrations validate", want: false},
		{name: "db drop-all", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			verb, known := agentsurface.Lookup(test.name)

			c.Assert(known, qt.IsTrue)
			c.Assert(verb.DatabaseSafe(), qt.Equals, test.want)
		})
	}
}

// TestClassification_TheMaintenanceVerbsShareOneAnswer pins that three names
// for one code path carry one class.
//
// `migrations edit`, `migrations rebase` and `migrations rm` each rewrite the
// migration directory through internal/migrateops, which opens no database, and
// each reaches one only through migratemaint.Options.Guard, which reads the
// applied set and refuses an already-applied version unless --force. They were
// classified reads, writes and writes, and the two writers said so in their
// reasons: "updates the target's tracking table", for commands that never write
// a row. The reference started printing those reasons in a user-facing column,
// which is how the disagreement became visible.
//
// `migrations repair` is the control. It is the neighbouring verb that really
// does rewrite revision metadata, so a change that made every migrations verb a
// reader would fail here rather than agree with itself.
func TestClassification_TheMaintenanceVerbsShareOneAnswer(t *testing.T) {
	tests := []struct {
		name        string
		verb        string
		wantTarget  agentsurface.Target
		wantScratch agentsurface.Scratch
		wantSafe    bool
	}{
		{name: "edit", verb: "migrations edit", wantTarget: agentsurface.TargetReads, wantScratch: agentsurface.ScratchNone, wantSafe: true},
		{name: "rebase", verb: "migrations rebase", wantTarget: agentsurface.TargetReads, wantScratch: agentsurface.ScratchNone, wantSafe: true},
		{name: "rm", verb: "migrations rm", wantTarget: agentsurface.TargetReads, wantScratch: agentsurface.ScratchNone, wantSafe: true},
		{name: "repair, which really writes", verb: "migrations repair", wantTarget: agentsurface.TargetWrites, wantScratch: agentsurface.ScratchNone, wantSafe: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			verb, known := agentsurface.Lookup(test.verb)

			c.Assert(known, qt.IsTrue)
			c.Assert(verb.Target, qt.Equals, test.wantTarget)
			c.Assert(verb.Scratch, qt.Equals, test.wantScratch)
			c.Assert(verb.DatabaseSafe(), qt.Equals, test.wantSafe)
		})
	}
}

// TestClassification_TheMaintenanceReasonsDescribeTheReadTheyDo is the same
// finding read off the prose rather than off the class.
//
// A class is four values and a reason is a sentence, so a class can be
// corrected while the sentence that argued for the wrong one stays. The three
// share a clause because they share the function that does the work.
func TestClassification_TheMaintenanceReasonsDescribeTheReadTheyDo(t *testing.T) {
	const guarded = "the target is read to check whether the migration has been applied"

	for _, name := range []string{"migrations edit", "migrations rebase", "migrations rm"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			verb, known := agentsurface.Lookup(name)

			c.Assert(known, qt.IsTrue)
			c.Assert(verb.Reason, qt.Contains, guarded)
			c.Assert(verb.Reason, qt.Not(qt.Contains), "updates the target")
		})
	}
}

// TestNodes_AgreesWithWalkAboutWhatIsALeaf keeps the two views of the one
// traversal from becoming two traversals.
//
// [agentsurface.Walk] is expressed over [agentsurface.Nodes], and this is what
// says so out loud: if a second walk were ever introduced for the reference,
// the two would agree on the day it was written and would come apart at the
// next change to how cobra finishes building a tree.
func TestNodes_AgreesWithWalkAboutWhatIsALeaf(t *testing.T) {
	c := qt.New(t)

	walked := make(map[string]bool)
	for _, leaf := range agentsurface.Walk(root.NewRootCommand()) {
		walked[leaf.Name] = true
	}
	nodes := agentsurface.Nodes(root.NewRootCommand())

	// A walk that reached nothing would agree with a node list that reached
	// nothing, and the subtests below would all be absent rather than failing.
	c.Assert(len(walked) > 0, qt.IsTrue)
	c.Assert(len(nodes) > len(walked), qt.IsTrue)

	for _, node := range nodes {
		t.Run(node.Name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(walked[node.Name], qt.Equals, node.Leaf)
		})
	}
}

// TestNodes_RecordsWhatAFlagCarries pins the four facts a command reference
// needs and the two the walk previously threw away.
//
// The environment variable is the one worth naming here. It is asked of the
// COMMAND's own installed binding, not derived from a prefix written down
// here, and not read back out of the `[env: PTAH_X]` suffix the installer
// appends to the usage string. Both shortcuts answer for the four
// `ptah completion` shells, which cobra supplies after the binding was
// installed and which therefore read no variable at all.
func TestNodes_RecordsWhatAFlagCarries(t *testing.T) {
	tests := []struct {
		name        string
		command     string
		flag        string
		wantType    string
		wantDefault string
		wantEnv     string
	}{
		{
			name: "a bound string flag", command: "migrations up", flag: "db-url",
			wantType: "string", wantDefault: "", wantEnv: "PTAH_DB_URL",
		},
		{
			name: "a flag no variable may set", command: "schema apply", flag: "auto-approve",
			wantType: "bool", wantDefault: "false", wantEnv: "",
		},
		{
			name: "a flag on a command cobra supplied afterwards", command: "completion zsh", flag: "no-descriptions",
			wantType: "bool", wantDefault: "false", wantEnv: "",
		},
		{
			name: "a variable whose name loses a separator", command: "migrations up", flag: "migration-lock-timeout",
			wantType: "string", wantDefault: "", wantEnv: "PTAH_MIGRATION_LOCK_TIMEOUT",
		},
	}

	registered := make(map[string]map[string]agentsurface.Flag)
	for _, node := range agentsurface.Nodes(root.NewRootCommand()) {
		registered[node.Name] = make(map[string]agentsurface.Flag)
		for _, flag := range node.Flags {
			registered[node.Name][flag.Name] = flag
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			flag, found := registered[test.command][test.flag]

			c.Assert(found, qt.IsTrue)
			c.Assert(flag.Type, qt.Equals, test.wantType)
			c.Assert(flag.Default, qt.Equals, test.wantDefault)
			c.Assert(flag.Environment, qt.Equals, test.wantEnv)
		})
	}
}
