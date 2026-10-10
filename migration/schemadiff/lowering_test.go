package schemadiff_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

// tableAddingLowering records the request a comparison sends it and declares
// one more table, which the comparison then reports as added.
type tableAddingLowering struct {
	received *schemapreparation.LoweringRequest
}

func (l tableAddingLowering) LowerDesired(_ context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	*l.received = request
	lowered := *request.Desired
	lowered.Tables = append(slices.Clone(request.Desired.Tables), schemamodel.Table{StructName: "Lowered", Name: "lowered"})
	lowered.Fields = append(slices.Clone(request.Desired.Fields), schemamodel.Field{StructName: "Lowered", Name: "id", Type: "BIGINT", Primary: true})
	return &lowered, nil
}

// loweringComparisonInputs are a declaration and a database that already
// agree, so any table the comparison adds came from the lowering.
func loweringComparisonInputs() (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events"}},
		Fields: []schemamodel.Field{{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true}},
	}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "events", Type: "TABLE", Columns: []catalog.Column{
		{Name: "id", DataType: "BIGINT", ColumnType: "BIGINT", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
	}}}}
	return desired, current
}

// TestCompare_ReachesTheTargetsLowering shows that a comparison sends the
// desired schema through the lowering the selected target registered, under
// the target's canonical name and beside the database's read, and compares
// what the lowering returns. The control registers no lowering and compares
// the declaration as written.
func TestCompare_ReachesTheTargetsLowering(t *testing.T) {
	var received schemapreparation.LoweringRequest
	lowering := tableAddingLowering{received: &received}
	cases := []struct {
		name      string
		lowering  schemapreparation.Lowering
		wantAdded []string
		wantSeen  string
	}{
		{name: "registered", lowering: lowering, wantAdded: []string{"lowered"}, wantSeen: "custom"},
		{name: "absent", lowering: nil, wantAdded: nil, wantSeen: ""},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			received = schemapreparation.LoweringRequest{}
			runtime := must.Must(engine.New(engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{
				Name: "custom", Aliases: []string{"alternate"}, Preparation: schemapreparation.Identity{}, Lowering: tt.lowering,
			}}}))
			desired, current := loweringComparisonInputs()

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, tt.wantAdded)
			c.Assert(received.Target, qt.Equals, tt.wantSeen)
			c.Assert(desired.Tables, qt.HasLen, 1)
		})
	}
}

// TestCompare_LoweringReceivesTheComparisonInputs pins what a lowering is
// given: the declaration and the read the comparison scoped, and the
// identifier rules it pairs names by.
func TestCompare_LoweringReceivesTheComparisonInputs(t *testing.T) {
	c := qt.New(t)
	var received schemapreparation.LoweringRequest
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{
		Name: "custom", Preparation: schemapreparation.Identity{}, Lowering: tableAddingLowering{received: &received},
	}}}))
	desired, current := loweringComparisonInputs()

	_, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "custom", runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(received.Desired.Tables, qt.DeepEquals, desired.Tables)
	c.Assert(received.Current.Tables, qt.DeepEquals, current.Tables)
	c.Assert(received.Semantics.Equal(identifier.ForDialect("custom")), qt.IsTrue)
}
