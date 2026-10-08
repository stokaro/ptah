package schemacapture_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

type labels struct{ Values []string }

func (*labels) Kind() schemaext.Kind { return "example.org/labels" }
func (v *labels) Clone() schemaext.Value {
	return &labels{Values: slices.Clone(v.Values)}
}
func (v *labels) Equal(other schemaext.Value) bool {
	w, ok := other.(*labels)
	return ok && slices.Equal(v.Values, w.Values)
}

func TestCaptureCloneRetainsFeatureState(t *testing.T) {
	cases := []struct {
		representation schemaext.Representation
		clone          func(schemaext.Facets, schemaext.Objects, schemaext.Coverage) (schemaext.Facets, schemaext.Objects, schemaext.Coverage)
	}{
		{schemaext.Desired, func(f schemaext.Facets, o schemaext.Objects, k schemaext.Coverage) (schemaext.Facets, schemaext.Objects, schemaext.Coverage) {
			v := schemacapture.TableDeclaration{Table: schemamodel.Table{Facets: f}, OwnedObjects: o, FeatureCoverage: k}.Clone()
			return v.Table.Facets, v.OwnedObjects, v.FeatureCoverage
		}},
		{schemaext.Observed, func(f schemaext.Facets, o schemaext.Objects, k schemaext.Coverage) (schemaext.Facets, schemaext.Objects, schemaext.Coverage) {
			v := schemacapture.TableObservation{Table: catalog.Table{Facets: f}, OwnedObjects: o, FeatureCoverage: k}.Clone()
			return v.Table.Facets, v.OwnedObjects, v.FeatureCoverage
		}},
	}
	for _, test := range cases {
		t.Run(string(test.representation), func(t *testing.T) {
			c := qt.New(t)
			parent := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("app", "orders.pending")
			ref := objectidentity.ID{Kind: "example.org/labels", Schema: parent.Schema, Parent: parent.Name, Name: objectidentity.Part{Source: "retained", Normalized: "retained"}}
			facets, err := schemaext.NewFacets(&labels{Values: []string{"source"}})
			c.Assert(err, qt.IsNil)
			objects, err := schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &labels{Values: []string{"source"}}})
			c.Assert(err, qt.IsNil)
			coverage := featureCoverage(c, test.representation, parent)
			f, o, k := test.clone(facets, objects, coverage)
			c.Assert(o.Refs(), qt.DeepEquals, []objectidentity.ID{ref})
			c.Assert(k.KindRecords(), qt.DeepEquals, coverage.KindRecords())
			c.Assert(k.Lookup("example.org/labels", parent), qt.DeepEquals, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "siblings were not read"})
			c.Assert(k.Lookup("example.org/new-kind", parent).State, qt.Equals, schemaext.Uninspected)
			value, found, err := schemaext.FacetAs[*labels](f, "example.org/labels")
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			value.Values[0] = "changed"
			object, found, err := o.Get(ref)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			object.Value.(*labels).Values[0] = "changed"
			retained, _, err := objects.Get(ref)
			c.Assert(err, qt.IsNil)
			c.Assert(retained.Value.(*labels).Values, qt.DeepEquals, []string{"source"})
			retainedFacet, _, err := schemaext.FacetAs[*labels](facets, "example.org/labels")
			c.Assert(err, qt.IsNil)
			c.Assert(retainedFacet.Values, qt.DeepEquals, []string{"source"})
		})
	}
}

func featureCoverage(c *qt.C, representation schemaext.Representation, parent objectidentity.ID) schemaext.Coverage {
	coverage, err := schemaext.NewCoverage(representation, []schemaext.KindCoverage{{
		Model:     schemaext.CodecIdentity{Owner: "example.org/provider", Kind: "example.org/labels", Representation: representation, Version: 1, Definition: "fixture-definition"},
		Knowledge: schemaext.Knowledge{State: schemaext.Complete},
	}}, []schemaext.SubjectCoverage{{Kind: "example.org/labels", Subject: parent, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "siblings were not read"}}})
	c.Assert(err, qt.IsNil)
	return coverage
}
