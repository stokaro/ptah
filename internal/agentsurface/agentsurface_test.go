package agentsurface_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/agentsurface"
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

			c.Check(verb.Target != agentsurface.TargetNone, qt.Equals, len(leaf.TargetFlags) > 0,
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
