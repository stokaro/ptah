package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// A declared sequence that a column of the database owns is not added, whether
// the declaration names its schema or leaves it out: pg_dump writes
// `public.t_id_seq`, and the reader reports a table of the default schema with
// no schema. A sequence of the same name in another schema is another
// sequence, and is added.
func TestCompareOwnedSequence_SchemaOfTheDeclaration(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		wantAdded []string
	}{
		{name: "no schema", schema: ""},
		{name: "the default schema", schema: "public"},
		{name: "another schema", schema: "app", wantAdded: []string{"app.t_id_seq"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := plainDeclaration(schemamodel.Field{Type: "BIGINT", DefaultExpr: "nextval('t_id_seq'::regclass)"})
			declared.Sequences = []schemamodel.Sequence{{Name: "t_id_seq", Schema: test.schema, OwnedBy: "t.id", Increment: new(int64(10))}}

			diff, err := schemadiff.CompareWithDialect(c.Context(), declared, heldSerialColumn("t_id_seq"), "postgres", builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.SequencesAdded.Names(), qt.CmpEquals(cmpopts.EquateEmpty()), test.wantAdded)
		})
	}
}

// A declaration that writes the column plain and declares the sequence the
// column owns keeps the sequence: it leaves with the default only when nothing
// declares it, whether the declaration names its schema or leaves it out. The
// control in another schema declares another sequence, so the column's leaves.
func TestCompareOwnedSequence_DeclaredBesideAPlainColumn(t *testing.T) {
	tests := []struct {
		name        string
		schema      string
		wantRemoved []string
	}{
		{name: "no schema", schema: ""},
		{name: "the default schema", schema: "public"},
		{name: "another schema", schema: "app", wantRemoved: []string{"t_id_seq"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := plainDeclaration(schemamodel.Field{Type: "BIGINT"})
			declared.Sequences = []schemamodel.Sequence{{Name: "t_id_seq", Schema: test.schema}}

			diff, err := schemadiff.CompareWithDialect(c.Context(), declared, heldSerialColumn("t_id_seq"), "postgres", builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.SequencesRemoved.Names(), qt.CmpEquals(cmpopts.EquateEmpty()), test.wantRemoved)
		})
	}
}
