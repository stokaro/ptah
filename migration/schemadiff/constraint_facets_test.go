package schemadiff_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

const constraintLabelKind schemaext.Kind = "example.org/constraint-label"

type constraintLabel struct {
	Text string `json:"text"`
}

func (*constraintLabel) Kind() schemaext.Kind     { return constraintLabelKind }
func (v *constraintLabel) Clone() schemaext.Value { return new(*v) }
func (v *constraintLabel) Equal(other schemaext.Value) bool {
	w, ok := other.(*constraintLabel)
	return ok && *v == *w
}

func constraintLabelFacets(text string) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&constraintLabel{Text: text}))
}

type constraintComparison struct {
	request schemaext.FacetComparisonRequest
}

func (s *constraintComparison) CompareFacets(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	s.request = request
	return schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}, nil
}

func constraintComparisonRuntime(service *constraintComparison) *engine.Runtime {
	var codecs []schemaext.Codec
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		codecs = append(codecs, schemaext.ModelCodec[*constraintLabel]{Prototype: &constraintLabel{}, Representation: representation, Version: 1,
			Definition: json.RawMessage(`{"type":"object","required":["text"],"additionalProperties":false,"properties":{"text":{"type":"string"}}}`),
			Shape: func(data json.RawMessage) error {
				_, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "constraint label", Allowed: []string{"text"}, Required: []string{"text"}})
				return err
			}, Validate: func(*constraintLabel) error { return nil },
		}.Codec())
	}
	return must.Must(engine.New(engine.Provider{ID: "example.org/constraints", Codecs: codecs,
		Targets:          []engine.Target{{Name: "postgres", Preparation: schemapreparation.Identity{}}},
		FacetComparisons: []engine.FacetComparison{{Target: "postgres", OwnerKinds: []objectidentity.Kind{objectidentity.KindConstraint}, Kinds: []schemaext.Kind{constraintLabelKind}, Service: service}},
	}))
}

// The public comparison dispatches common constraints by table-scoped identity.
// Their captured state survives the effective-schema reconstruction.
func TestComparisonCapturesNamedConstraintFacets(t *testing.T) {
	for _, kind := range []string{"CHECK", "UNIQUE", "FOREIGN KEY"} {
		t.Run(kind, func(t *testing.T) {
			c := qt.New(t)
			service := &constraintComparison{}
			runtime := constraintComparisonRuntime(service)
			desired := &schemamodel.Database{}
			current := &catalog.Database{}
			builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
			var owners []schemaext.ParentState
			for _, table := range []string{"events", "events.archive"} {
				desired.Tables = append(desired.Tables, schemamodel.Table{Name: table, Schema: "tenant", StructName: table, Comment: "declared comment"})
				desired.Constraints = append(desired.Constraints, schemamodel.Constraint{Name: "rule", StructName: table, Type: kind,
					Columns: []string{"id"}, CheckExpression: "id > 0", ForeignTable: "parent", ForeignColumns: []string{"id"}, Facets: constraintLabelFacets(table)})
				current.Tables = append(current.Tables, catalog.Table{Name: table, Schema: "tenant"})
				current.Constraints = append(current.Constraints, catalog.Constraint{Name: "rule", TableName: table, Schema: "tenant", Type: kind,
					ColumnNames: []string{"id"}, CheckClause: new("id > 0"), ForeignTable: new("parent"), ForeignColumns: []string{"id"}, Facets: constraintLabelFacets(table)})
				owners = append(owners, schemaext.ParentState{Subject: builder.ConstraintParts("tenant", table, "rule"), Desired: true, Current: true})
			}
			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(service.request.Owners, qt.DeepEquals, owners)
			c.Assert(service.request.Desired.Records, qt.HasLen, 2)
			c.Assert(service.request.Current.Records, qt.HasLen, 2)
			c.Assert(service.request.DesiredConstraints, qt.HasLen, 2)
			c.Assert(service.request.CurrentConstraints, qt.HasLen, 2)
			c.Assert(diff.TablesModified, qt.HasLen, 2)
			for i, table := range diff.TablesModified {
				c.Assert(table.Desired.Constraints, qt.HasLen, 1)
				c.Assert(table.Current.Constraints, qt.HasLen, 1)
				c.Assert(table.Desired.Constraints[0].Facets.Equal(desired.Constraints[i].Facets), qt.IsTrue)
				c.Assert(table.Current.Constraints[0].Facets.Equal(current.Constraints[i].Facets), qt.IsTrue)
				c.Assert(service.request.Desired.Records[i].Subject, qt.Equals, owners[i].Subject)
				c.Assert(service.request.DesiredConstraints[i], qt.DeepEquals, schemaext.ConstraintContext{Subject: owners[i].Subject, Type: kind, Columns: []string{"id"}})
				c.Assert(service.request.CurrentConstraints[i], qt.DeepEquals, service.request.DesiredConstraints[i])
			}
			service.request.DesiredConstraints[0].Columns[0] = "changed"
			service.request.CurrentConstraints[0].Columns[0] = "changed"
			c.Assert(desired.Constraints[0].Columns, qt.DeepEquals, []string{"id"})
			c.Assert(current.Constraints[0].ColumnNames, qt.DeepEquals, []string{"id"})
		})
	}
}

func TestComparisonRefusesFacetsWithoutAConstraintIdentity(t *testing.T) {
	c := qt.New(t)
	service := &constraintComparison{}
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", StructName: "Event"}},
		Constraints: []schemamodel.Constraint{{StructName: "Event", Type: "CHECK", CheckExpression: "id > 0", Facets: constraintLabelFacets("declared")}}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, &catalog.Database{}, "postgres", constraintComparisonRuntime(service))
	c.Assert(err, qt.IsNotNil)
	c.Assert(diff, qt.IsNil)
	c.Assert(service.request.Owners, qt.HasLen, 0)
}

// A setting may be present on only one side. Its comparer still needs both
// common definitions to decide whether the missing setting means the same key.
func TestConstraintContextIncludesTheSideWithoutFacetValues(t *testing.T) {
	for _, test := range []struct {
		name     string
		declared schemaext.Facets
		observed schemaext.Facets
	}{
		{name: "declared setting", declared: constraintLabelFacets("selected")},
		{name: "observed setting", observed: constraintLabelFacets("selected")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := &constraintComparison{}
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "orders", StructName: "Order"}},
				Constraints: []schemamodel.Constraint{{Name: "customer_fk", StructName: "Order", Type: "FOREIGN KEY",
					Columns: []string{"tenant", "customer"}, ForeignTable: "customers", ForeignColumns: []string{"tenant", "id"}, Facets: test.declared}},
			}
			current := &catalog.Database{
				Tables: []catalog.Table{{Name: "orders"}},
				Constraints: []catalog.Constraint{{Name: "customer_fk", TableName: "orders", Type: "FOREIGN KEY",
					ColumnNames: []string{"tenant", "customer"}, ForeignTable: new("customers"), ForeignColumns: []string{"tenant", "id"}, Facets: test.observed}},
			}
			_, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", constraintComparisonRuntime(service))
			c.Assert(err, qt.IsNil)
			subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Constraint("orders", "customer_fk")
			want := []schemaext.ConstraintContext{{Subject: subject, Type: "FOREIGN KEY", Columns: []string{"tenant", "customer"}}}
			c.Assert(service.request.DesiredConstraints, qt.DeepEquals, want)
			c.Assert(service.request.CurrentConstraints, qt.DeepEquals, want)
		})
	}
}
