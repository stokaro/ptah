package schemapreparation_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
)

type labels struct{ names []string }

func (*labels) Kind() schemaext.Kind { return "example.org/labels" }
func (v *labels) Clone() schemaext.Value {
	return &labels{names: slices.Clone(v.names)}
}
func (v *labels) Equal(other schemaext.Value) bool {
	w, ok := other.(*labels)
	return ok && slices.Equal(slices.Sorted(slices.Values(v.names)), slices.Sorted(slices.Values(w.names)))
}

func labelFacets(names ...string) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&labels{names: names}))
}

func TestAcceptUsesOwnerEqualityAndRetainsOriginalRepresentations(t *testing.T) {
	c := qt.New(t)
	request := schemapreparation.Request{Tables: []schemapreparation.Table{{
		Desired: schemacapture.TableDeclaration{
			Table:  schemamodel.Table{Name: "events", Facets: labelFacets("a", "b")},
			Fields: []schemamodel.Field{{Name: "id", Facets: labelFacets("a", "b")}},
		},
		Current:          schemacapture.TableObservation{Table: catalog.Table{Name: "events", Columns: []catalog.Column{{Name: "id", Facets: labelFacets("a", "b")}}}},
		CurrentKnowledge: schemaext.Knowledge{State: schemaext.Complete},
	}}}
	reply := request.Clone()
	reply.Tables[0].Desired.Table.Facets = labelFacets("b", "a")
	reply.Tables[0].Desired.Fields[0].Facets = labelFacets("b", "a")
	reply.Tables[0].Current.Table.Columns[0].Facets = labelFacets("b", "a")
	capture, err := schemapreparation.Accept(request, schemapreparation.Result{Complete: true, Tables: reply.Tables})
	c.Assert(err, qt.IsNil)
	source, _, err := schemaext.FacetAs[*labels](capture.Source[0].Desired.Table.Facets, "example.org/labels")
	c.Assert(err, qt.IsNil)
	prepared, _, err := schemaext.FacetAs[*labels](capture.Prepared[0].Desired.Table.Facets, "example.org/labels")
	c.Assert(err, qt.IsNil)
	c.Assert(source.names, qt.DeepEquals, []string{"a", "b"})
	c.Assert(prepared.names, qt.DeepEquals, []string{"b", "a"})
}

func TestAcceptRejectsFeatureChanges(t *testing.T) {
	tests := []struct {
		name   string
		facets schemaext.Facets
	}{
		{name: "dropped", facets: schemaext.Facets{}},
		{name: "changed", facets: labelFacets("changed")},
		{name: "scope", facets: must.Must(labelFacets("a", "b").WithTargetScope("example.org/labels", "postgres"))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemapreparation.Request{Tables: []schemapreparation.Table{{Desired: schemacapture.TableDeclaration{
				Table: schemamodel.Table{Name: "events", Facets: labelFacets("a", "b")},
			}}}}
			reply := request.Clone()
			reply.Tables[0].Desired.Table.Facets = test.facets
			capture, err := schemapreparation.Accept(request, schemapreparation.Result{Complete: true, Tables: reply.Tables})
			c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
			c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
		})
	}
}

// indexKindRequest is one table declaring index by_status, and its reply.
func indexKindRequest() (schemapreparation.Request, schemapreparation.Result) {
	request := schemapreparation.Request{Tables: []schemapreparation.Table{{Desired: schemacapture.TableDeclaration{
		Table:   schemamodel.Table{Name: "events"},
		Indexes: []schemamodel.Index{{Name: "by_status", Fields: []string{"status"}, Type: "GLOBAL ASYNC"}},
	}}}}
	reply := request.Clone()
	reply.Tables[0].IndexMethods = map[string]string{"by_status": "async"}
	return request, schemapreparation.Result{Complete: true, Tables: reply.Tables}
}

func TestAcceptIndexMethods_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := indexKindRequest()
	capture, err := schemapreparation.Accept(request, result)
	c.Assert(err, qt.IsNil)
	c.Assert(capture.Prepared[0].IndexMethods, qt.DeepEquals, map[string]string{"by_status": "async"})
	c.Assert(capture.Source[0].IndexMethods, qt.IsNil)
	// The declaration keeps the type the source wrote, and the capture its
	// own copy of the kinds.
	c.Assert(capture.Prepared[0].Desired.Indexes[0].Type, qt.Equals, "GLOBAL ASYNC")
	result.Tables[0].IndexMethods["by_status"] = "changed"
	c.Assert(capture.Prepared[0].IndexMethods, qt.DeepEquals, map[string]string{"by_status": "async"})
	clone := capture.Clone()
	clone.Prepared[0].IndexMethods["by_status"] = "changed"
	c.Assert(capture.Prepared[0].IndexMethods, qt.DeepEquals, map[string]string{"by_status": "async"})
}

func TestAcceptIndexMethods_FailurePath(t *testing.T) {
	t.Run("a kind for an index the table does not declare", func(t *testing.T) {
		c := qt.New(t)
		request, result := indexKindRequest()
		result.Tables[0].IndexMethods["by_other"] = "async"
		capture, err := schemapreparation.Accept(request, result)
		c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
		c.Assert(err, qt.ErrorMatches, `.*states the kind of index "by_other", which the table does not declare`)
		c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
	})
	t.Run("an input that already states kinds", func(t *testing.T) {
		c := qt.New(t)
		request, result := indexKindRequest()
		request.Tables[0].IndexMethods = map[string]string{"by_status": "async"}
		capture, err := schemapreparation.Accept(request, result)
		c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
		c.Assert(err, qt.ErrorMatches, `.*preparation input already states index kinds`)
		c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
	})
}

// indexNullsRequest is one table declaring index by_status over two parts, and
// its reply stating the NULLS order of each.
func indexNullsRequest() (schemapreparation.Request, schemapreparation.Result) {
	request := schemapreparation.Request{Tables: []schemapreparation.Table{{Desired: schemacapture.TableDeclaration{
		Table: schemamodel.Table{Name: "events"},
		Indexes: []schemamodel.Index{{Name: "by_status", Parts: []schemamodel.IndexPart{{Name: "status", NullsOrder: schemamodel.NullsOrderFirst},
			{Name: "id", Desc: true}}}},
	}}}}
	reply := request.Clone()
	reply.Tables[0].IndexNulls = map[string][]string{"by_status": {"", schemamodel.NullsOrderFirst}}
	return request, schemapreparation.Result{Complete: true, Tables: reply.Tables}
}

func TestAcceptIndexNulls_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := indexNullsRequest()
	capture, err := schemapreparation.Accept(request, result)
	c.Assert(err, qt.IsNil)
	c.Assert(capture.Prepared[0].IndexNulls, qt.DeepEquals, map[string][]string{"by_status": {"", schemamodel.NullsOrderFirst}})
	c.Assert(capture.Source[0].IndexNulls, qt.IsNil)
	// The declaration keeps the orders the source wrote, and the capture its
	// own copy of the stated ones.
	c.Assert(capture.Prepared[0].Desired.Indexes[0].Parts[0].NullsOrder, qt.Equals, schemamodel.NullsOrderFirst)
	result.Tables[0].IndexNulls["by_status"][1] = "changed"
	c.Assert(capture.Prepared[0].IndexNulls["by_status"][1], qt.Equals, schemamodel.NullsOrderFirst)
	clone := capture.Clone()
	clone.Prepared[0].IndexNulls["by_status"][1] = "changed"
	c.Assert(capture.Prepared[0].IndexNulls["by_status"][1], qt.Equals, schemamodel.NullsOrderFirst)
}

func TestAcceptIndexNulls_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		wantErr string
		nulls   map[string][]string
		stated  map[string][]string
	}{
		{name: "orders of an index the table does not declare", wantErr: `.*states the NULLS orders of index "by_other", which the table does not declare`,
			nulls: map[string][]string{"by_other": {"", ""}}},
		{name: "fewer orders than key parts", wantErr: `.*states 1 NULLS orders for the 2 key parts of index "by_status"`,
			nulls: map[string][]string{"by_status": {""}}},
		{name: "an order in another spelling", wantErr: `.*states NULLS order "nulls first" for index "by_status"`,
			nulls: map[string][]string{"by_status": {"", "nulls first"}}},
		{name: "an input that already states orders", wantErr: `.*preparation input already states index NULLS orders`,
			nulls: map[string][]string{"by_status": {"", ""}}, stated: map[string][]string{"by_status": {"", ""}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := indexNullsRequest()
			result.Tables[0].IndexNulls = row.nulls
			request.Tables[0].IndexNulls = row.stated
			capture, err := schemapreparation.Accept(request, result)
			c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
		})
	}
}

// indexConditionsRequest is one table declaring the partial index by_status
// and the full index by_id, and its reply stating each predicate as the
// target holds it.
func indexConditionsRequest() (schemapreparation.Request, schemapreparation.Result) {
	request := schemapreparation.Request{Tables: []schemapreparation.Table{{Desired: schemacapture.TableDeclaration{
		Table: schemamodel.Table{Name: "events"},
		Indexes: []schemamodel.Index{{Name: "by_status", Fields: []string{"status"}, Condition: " status > 0 "},
			{Name: "by_id", Fields: []string{"id"}}},
	}}}}
	reply := request.Clone()
	reply.Tables[0].IndexConditions = map[string]string{"by_status": "(status > '0'::bigint)", "by_id": ""}
	return request, schemapreparation.Result{Complete: true, Tables: reply.Tables}
}

func TestAcceptIndexConditions_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := indexConditionsRequest()
	capture, err := schemapreparation.Accept(request, result)
	c.Assert(err, qt.IsNil)
	c.Assert(capture.Prepared[0].IndexConditions, qt.DeepEquals, map[string]string{"by_status": "(status > '0'::bigint)", "by_id": ""})
	c.Assert(capture.Source[0].IndexConditions, qt.IsNil)
	// The declaration keeps the predicate the source wrote, and the capture
	// its own copy of the stated ones.
	c.Assert(capture.Prepared[0].Desired.Indexes[0].Condition, qt.Equals, " status > 0 ")
	result.Tables[0].IndexConditions["by_status"] = "changed"
	c.Assert(capture.Prepared[0].IndexConditions["by_status"], qt.Equals, "(status > '0'::bigint)")
	clone := capture.Clone()
	clone.Prepared[0].IndexConditions["by_status"] = "changed"
	c.Assert(capture.Prepared[0].IndexConditions["by_status"], qt.Equals, "(status > '0'::bigint)")
}

func TestAcceptIndexConditions_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name       string
		wantErr    string
		conditions map[string]string
		stated     map[string]string
	}{
		{name: "a predicate of an index the table does not declare", wantErr: `.*states the predicate of index "by_other", which the table does not declare`,
			conditions: map[string]string{"by_other": "a > 0"}},
		{name: "no predicate for a partial index", wantErr: `.*states a predicate for index "by_status" exactly when the index declares one`,
			conditions: map[string]string{"by_status": ""}},
		{name: "a predicate for a full index", wantErr: `.*states a predicate for index "by_id" exactly when the index declares one`,
			conditions: map[string]string{"by_status": "status > 0", "by_id": "id > 0"}},
		{name: "a predicate with surrounding white space", wantErr: `.*states the predicate of index "by_status" with surrounding white space`,
			conditions: map[string]string{"by_status": "status > 0 "}},
		{name: "an input that already states predicates", wantErr: `.*preparation input already states index predicates`,
			conditions: map[string]string{"by_status": "status > 0"}, stated: map[string]string{"by_status": "status > 0"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := indexConditionsRequest()
			result.Tables[0].IndexConditions = row.conditions
			request.Tables[0].IndexConditions = row.stated
			capture, err := schemapreparation.Accept(request, result)
			c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
		})
	}
}
