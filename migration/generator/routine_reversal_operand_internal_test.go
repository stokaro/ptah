package generator

// White-box testing required: what this pins is which definition the reversal
// resolves for a modified function, sequence or synonym, and the reversal is
// unexported. Through the public API a wrong operand and a missing one are both
// just SQL that does not say what it should.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestReverseSchemaDiff_RoutineOperandsComeFromThePriorSchema pins all three
// reversals to the pre-change database.
//
// Each object here exists under one name in both schemas and differs only in
// the value the rollback has to restore, so a reversal carrying the forward
// operand through renders a statement that is well-formed, names the right
// object, and re-applies the change it is undoing.
func TestReverseSchemaDiff_RoutineOperandsComeFromThePriorSchema(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect(platform.Postgres)
	oldIncrement, newIncrement := int64(1), int64(5)

	prior := &schemamodel.Database{
		Functions: []schemamodel.Function{
			{Name: "audit", Returns: "VOID", Language: "plpgsql", Body: "BEGIN END;"},
		},
		Sequences: []schemamodel.Sequence{
			{Name: "order_seq", Increment: &oldIncrement},
		},
		Synonyms: []schemamodel.Synonym{
			{Name: "orders", Target: "dbo.orders_v1"},
		},
	}

	functions := reverseFunctionDiffs([]difftypes.FunctionDiff{{
		FunctionName: "audit",
		Changes:      map[string]string{"body": "BEGIN END; -> BEGIN RAISE NOTICE 'x'; END;"},
		Desired: schemamodel.Function{
			Name: "audit", Returns: "VOID", Language: "plpgsql",
			Body: "BEGIN RAISE NOTICE 'x'; END;",
		},
	}}, prior)
	c.Assert(functions, qt.HasLen, 1)
	c.Assert(functions[0].Desired.Body, qt.Equals, "BEGIN END;",
		qt.Commentf("the rollback replaces the function with the body the database held"))

	sequences := reverseSequenceDiffs([]difftypes.SequenceDiff{{
		SequenceName: "order_seq",
		Changes:      map[string]string{"increment": "1 -> 5"},
		Desired:      schemamodel.Sequence{Name: "order_seq", Increment: &newIncrement},
	}}, prior, semantics)
	c.Assert(sequences, qt.HasLen, 1)
	c.Assert(sequences[0].Desired.Increment, qt.IsNotNil)
	c.Assert(*sequences[0].Desired.Increment, qt.Equals, int64(1),
		qt.Commentf("the rollback alters the sequence back to the increment the database held"))

	synonyms := reverseSynonymDiffs([]difftypes.SynonymDiff{{
		SynonymName: "orders",
		OldTarget:   "dbo.orders_v1",
		NewTarget:   "dbo.orders_v2",
		Desired:     schemamodel.Synonym{Name: "orders", Target: "dbo.orders_v2"},
	}}, prior)
	c.Assert(synonyms, qt.HasLen, 1)
	c.Assert(synonyms[0].Desired.Target, qt.Equals, "dbo.orders_v1",
		qt.Commentf("the rollback recreates the synonym pointing where the database pointed"))
}

// TestReverseSchemaDiff_SequenceOperandResolvesAcrossSchemaSpellings is the
// lookup the retired planner-side control used to pin.
//
// The change spells a name the declaration produced and the schema it resolves
// against comes from a database read, so the two do not have to agree on
// whether the schema is written down. Resolving by string equality finds
// nothing, and a rollback with no operand emits no ALTER SEQUENCE at all --
// which reads, in the plan, as a sequence that needed nothing undone.
func TestReverseSchemaDiff_SequenceOperandResolvesAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name           string
		diffName       string
		sequenceSchema string
	}{
		{name: "both sides spell it the same way", diffName: "order_seq", sequenceSchema: ""},
		{name: "the change qualifies public and the read does not", diffName: "public.order_seq", sequenceSchema: ""},
		{name: "the read qualifies public and the change does not", diffName: "order_seq", sequenceSchema: "public"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForDialect(platform.Postgres)
			increment := int64(1)

			prior := &schemamodel.Database{Sequences: []schemamodel.Sequence{{
				Name: "order_seq", Schema: test.sequenceSchema, Increment: &increment,
			}}}

			reversed := reverseSequenceDiffs([]difftypes.SequenceDiff{{
				SequenceName: test.diffName,
				Changes:      map[string]string{"increment": "1 -> 5"},
			}}, prior, semantics)

			c.Assert(reversed, qt.HasLen, 1)
			c.Assert(reversed[0].Desired.Name, qt.Equals, "order_seq",
				qt.Commentf("the rollback found the sequence the database reported"))
		})
	}
}
