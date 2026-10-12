package atlasfilter_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/engine"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/featureselect"
)

const plaqueKind schemaext.Kind = "example.org/plaques/plaque"

// plaque is an object of the owner example.org/plaques. It lives in a schema
// or on the database, and one with a table is attached to that table, the
// shape of a SQL Server extended property.
type plaque struct {
	On string `json:"on,omitempty"`
}

func (*plaque) Kind() schemaext.Kind { return plaqueKind }

func (p *plaque) Clone() schemaext.Value { return new(*p) }

func (p *plaque) Equal(other schemaext.Value) bool {
	o, ok := other.(*plaque)
	return ok && *o == *p
}

// plaqueRelations reports the table a plaque is on as its attachment.
type plaqueRelations struct{}

func (plaqueRelations) DescribeRelations(_ context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
	builder := objectidentity.NewBuilder(request.Identifiers)
	result := schemaext.RelationResult{Complete: true}
	for _, value := range request.Values {
		record := schemaext.ValueRelations{Subject: value.Subject, Complete: true}
		if on := value.Value.(*plaque).On; on != "" {
			record.Attachment = builder.TableParts(value.Subject.Subject.Schema.Source, on)
		}
		result.Values = append(result.Values, record)
	}
	return result, nil
}

// plaqueRuntime registers the plaque owner on target "plaques": a selector
// type that names a plaque in its schema or on the database, and relation
// discovery on both sides.
func plaqueRuntime() *engine.Runtime {
	var codecs []schemaext.Codec
	var relations []engine.RelationDiscovery
	for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		codecs = append(codecs, schemaext.ModelCodec[*plaque]{Prototype: &plaque{}, Representation: representation, Version: 1,
			Definition: json.RawMessage(`{"type":"object"}`)}.Codec())
		relations = append(relations, engine.RelationDiscovery{Target: "plaques", Representation: representation,
			Kinds: []schemaext.Kind{plaqueKind}, Service: plaqueRelations{}})
	}
	return must.Must(engine.New(engine.Provider{
		ID:         "example.org/plaques",
		Targets:    []engine.Target{{Name: "plaques"}},
		Codecs:     codecs,
		Relations:  relations,
		Selections: []selectext.Extension{{Owner: "example.org/plaques", Selections: []selectext.Selection{{Kind: plaqueKind, Type: "plaque", Naming: selectext.SchemaOrDatabase}}}},
	}))
}

// plaqueObjects are p1 on app.a, p2 on app.b, p3 in schema app, and p4 on
// the database.
func plaqueObjects() schemaext.Objects {
	return must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: customRef(plaqueKind, "app", "p1"), Value: &plaque{On: "a"}},
		schemaext.Object{Ref: customRef(plaqueKind, "app", "p2"), Value: &plaque{On: "b"}},
		schemaext.Object{Ref: customRef(plaqueKind, "app", "p3"), Value: &plaque{}},
		schemaext.Object{Ref: customRef(plaqueKind, "", "p4"), Value: &plaque{}},
	))
}

// plaqueSides are the two sides of a comparison holding tables app.a and
// app.b and every plaque, and the bindings a command captures over them.
func plaqueSides() (*catalog.Database, *schemamodel.Database, atlasfilter.Scope) {
	runtime := plaqueRuntime()
	current := &catalog.Database{Schemas: []catalog.Schema{{Name: "app"}}, FeatureObjects: plaqueObjects(),
		Tables: []catalog.Table{{Schema: "app", Name: "a"}, {Schema: "app", Name: "b"}}}
	desired := &schemamodel.Database{FeatureObjects: plaqueObjects(),
		Tables: []schemamodel.Table{{StructName: "A", Schema: "app", Name: "a"}, {StructName: "B", Schema: "app", Name: "b"}}}
	bindings := must.Must(featureselect.CaptureBindings(context.Background(), runtime, "plaques", identifier.ForDialect("postgres"),
		featureselect.Side{Representation: schemaext.Desired, Objects: desired.FeatureObjects},
		featureselect.Side{Representation: schemaext.Observed, Objects: current.FeatureObjects}))
	return current, desired, atlasfilter.Scope{Bindings: bindings, Selections: runtime.Selections(), DefaultSchema: "app"}
}

// plaqueNames lists the names of the plaques in objects.
func plaqueNames(objects schemaext.Objects) []string {
	var names []string
	for _, ref := range objects.Refs() {
		names = append(names, ref.Name.Source)
	}
	slices.Sort(names)
	return names
}

// TestScope_AnAttachedObjectFollowsItsTable pins how a scope decides an
// owner's objects, the same on both sides: one attached to a table follows
// the table and is not narrowed by name, one in a schema is named in it, and
// one on the database is named alone, outside every schema.
func TestScope_AnAttachedObjectFollowsItsTable(t *testing.T) {
	tests := []struct {
		name    string
		schemas []string
		include []string
		exclude []string
		want    []string
	}{
		{name: "nothing selected", want: []string{"p1", "p2", "p3", "p4"}},
		{name: "a table included", include: []string{"a"}, want: []string{"p1"}},
		{name: "a table excluded", exclude: []string{"b"}, want: []string{"p1", "p3", "p4"}},
		{name: "an attached object excluded by name", exclude: []string{"p1[type=plaque]"}, want: []string{"p2", "p3", "p4"}},
		{name: "a database object included by name", include: []string{"p4[type=plaque]"}, want: []string{"p4"}},
		{name: "a schema object included by name", include: []string{"app.p3[type=plaque]"}, want: []string{"p3"}},
		{name: "a schema outside the universe", schemas: []string{"other"}, want: []string{"p4"}},
		{name: "a schema excluded", exclude: []string{"app.*"}, want: []string{"p4"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current, desired, scope := plaqueSides()
			scope.Schemas, scope.Include, scope.Exclude = test.schemas, test.include, test.exclude

			scopedCurrent, err := atlasfilter.ScopeDatabase(current, scope)
			c.Assert(err, qt.IsNil)
			scopedDesired, err := atlasfilter.ScopeGenerated(desired, scope)
			c.Assert(err, qt.IsNil)

			c.Assert(plaqueNames(scopedCurrent.FeatureObjects), qt.DeepEquals, test.want)
			c.Assert(plaqueNames(scopedDesired.FeatureObjects), qt.DeepEquals, test.want)
		})
	}
}
