package atlasfilter_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/engine"
	"ptah.run/internal/atlasfilter"
)

const (
	gadgetKind schemaext.Kind = "example.org/custom/gadget"
	depotKind  schemaext.Kind = "example.org/custom/depot"
)

// gadget is an object of the owner example.org/custom: named in a schema,
// with a label an exclusion may subtract, routed to a depot.
type gadget struct {
	Label string `json:"label,omitempty"`
	Depot string `json:"depot,omitempty"`
}

func (*gadget) Kind() schemaext.Kind { return gadgetKind }

func (g *gadget) Clone() schemaext.Value { return new(*g) }

func (g *gadget) Equal(other schemaext.Value) bool {
	o, ok := other.(*gadget)
	return ok && *o == *g
}

// depot is a database-wide object of the owner.
type depot struct{}

func (*depot) Kind() schemaext.Kind { return depotKind }

func (*depot) Clone() schemaext.Value { return &depot{} }

func (*depot) Equal(other schemaext.Value) bool {
	_, ok := other.(*depot)
	return ok
}

func customCodec(prototype schemaext.Value) schemaext.Codec {
	encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) }
	return schemaext.Codec{Prototype: prototype, Representation: schemaext.Desired, Version: 1,
		Definition: json.RawMessage(`{"type":"object"}`),
		Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(schemaext.Value).Clone(), nil },
		Encode:     encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			value := prototype.Clone()
			return value, json.Unmarshal(data, value)
		},
	}
}

func customRef(kind schemaext.Kind, schema, name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(kind), Schema: objectidentity.Part{Source: schema, Normalized: schema},
		Name: objectidentity.Part{Source: name, Normalized: name}}
}

// customService clears a gadget's label, and requires the depot each kept
// gadget routes to.
type customService struct{}

func (customService) SelectObjects(_ context.Context, request selectext.Request) (selectext.Result, error) {
	var result selectext.Result
	for _, removal := range request.Clears {
		object, _, err := request.Kept.Get(removal.Object)
		if err != nil {
			return selectext.Result{}, err
		}
		cleared := *object.Value.(*gadget)
		cleared.Label = ""
		object.Value = &cleared
		result.Cleared = append(result.Cleared, object)
	}
	kept, err := request.Kept.All()
	if err != nil {
		return selectext.Result{}, err
	}
	for _, object := range kept {
		if value, ok := object.Value.(*gadget); ok && value.Depot != "" {
			result.Requires = append(result.Requires, customRef(depotKind, "", value.Depot))
		}
	}
	return result, nil
}

// customSelections registers the owner example.org/custom through the
// runtime: a gadget named in its schema, with a label field, and a depot
// with a database-wide name.
func customSelections() selectext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:     "example.org/custom",
		Codecs: []schemaext.Codec{customCodec(&gadget{}), customCodec(&depot{})},
		Selections: []selectext.Extension{{Owner: "example.org/custom", Service: customService{}, Selections: []selectext.Selection{
			{Kind: gadgetKind, Type: "gadget", Naming: selectext.InSchema, Fields: []string{"label"}},
			{Kind: depotKind, Type: "depot", Naming: selectext.DatabaseWide},
		}}},
	})).Selections()
}

// customDatabase holds gadgets in two schemas and three depots, one with a
// dot in its name, which is part of the name.
func customDatabase() *catalog.Database {
	return &catalog.Database{Schemas: []catalog.Schema{{Name: "app"}, {Name: "other"}}, FeatureObjects: must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: customRef(gadgetKind, "app", "g1"), Value: &gadget{Label: "one", Depot: "app.d1"}},
		schemaext.Object{Ref: customRef(gadgetKind, "other", "g2"), Value: &gadget{Label: "two", Depot: "d2"}},
		schemaext.Object{Ref: customRef(depotKind, "", "app.d1"), Value: &depot{}},
		schemaext.Object{Ref: customRef(depotKind, "", "d2"), Value: &depot{}},
		schemaext.Object{Ref: customRef(depotKind, "", "d3"), Value: &depot{}},
	))}
}

// customObjects lists what a database holds, each gadget with its label.
func customObjects(c *qt.C, db *catalog.Database) []string {
	c.Helper()
	all := must.Must(db.FeatureObjects.All())
	names := make([]string, 0, len(all))
	for _, object := range all {
		name := object.Ref.Schema.Source + "|" + object.Ref.Name.Source
		if value, ok := object.Value.(*gadget); ok {
			name += "=" + value.Label
		}
		names = append(names, name)
	}
	return names
}

// TestScopeDatabase_SelectsAnOwnersObjectsByItsSelections selects an owner's
// objects by the selector types it registers through the runtime: a gadget
// in its schema, a depot by its database-wide name, which no schema scopes,
// and the depot each kept gadget requires, though no selector names it.
func TestScopeDatabase_SelectsAnOwnersObjectsByItsSelections(t *testing.T) {
	tests := []struct {
		name  string
		scope atlasfilter.Scope
		want  []string
	}{
		{name: "a gadget and the depot it requires", scope: atlasfilter.Scope{Include: []string{"app.g1"}},
			want: []string{"app|g1=one", "|app.d1"}},
		{name: "every gadget and the depots they require", scope: atlasfilter.Scope{Include: []string{"*[type=gadget]"}},
			want: []string{"app|g1=one", "other|g2=two", "|app.d1", "|d2"}},
		{name: "a depot by a name holding a dot", scope: atlasfilter.Scope{Include: []string{"app.d1[type=depot]"}},
			want: []string{"|app.d1"}},
		{name: "a schema keeps its gadgets, and depots are in none", scope: atlasfilter.Scope{Schemas: []string{"other"}},
			want: []string{"other|g2=two", "|app.d1", "|d2", "|d3"}},
		{name: "an exclusion of a required depot stays", scope: atlasfilter.Scope{Include: []string{"*[type=gadget]"}, Exclude: []string{"d2"}},
			want: []string{"app|g1=one", "other|g2=two", "|app.d1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.scope.Selections = customSelections()

			got, err := atlasfilter.ScopeDatabase(customDatabase(), test.scope)

			c.Assert(err, qt.IsNil)
			c.Assert(customObjects(c, got), qt.ContentEquals, test.want)
		})
	}
}

// TestExcludeDatabase_ExcludesAnOwnersObjectsBySelector drops an owner's
// object an exclusion names under its type, in its schema or by a schema it
// is in, a depot by its whole name, and subtracts the field a field selector
// names through the owner, keeping the object.
func TestExcludeDatabase_ExcludesAnOwnersObjectsBySelector(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		want    []string
	}{
		{name: "a gadget", exclude: []string{"app.g1"}, want: []string{"other|g2=two", "|app.d1", "|d2", "|d3"}},
		{name: "a gadget by type", exclude: []string{"*[type=gadget]"}, want: []string{"|app.d1", "|d2", "|d3"}},
		{name: "a schema", exclude: []string{"other"}, want: []string{"app|g1=one", "|app.d1", "|d2", "|d3"}},
		{name: "a depot by a name holding a dot", exclude: []string{"app.d1"},
			want: []string{"app|g1=one", "other|g2=two", "|d2", "|d3"}},
		{name: "a gadget's label", exclude: []string{"app.g1[type=gadget].label"},
			want: []string{"app|g1=", "other|g2=two", "|app.d1", "|d2", "|d3"}},
		{name: "every gadget's every field", exclude: []string{"*[type=gadget].*"},
			want: []string{"app|g1=", "other|g2=", "|app.d1", "|d2", "|d3"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, report, err := atlasfilter.ExcludeDatabaseScopeReport(customDatabase(),
				atlasfilter.Scope{Exclude: test.exclude, Selections: customSelections()})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Unmatched, qt.HasLen, 0)
			c.Assert(customObjects(c, got), qt.ContentEquals, test.want)
		})
	}
}

// TestValidateSelectors_ReadAnOwnersTypesAndFields accepts an owner's
// selector type and field only with the owner selected, and refuses a set
// nobody built.
func TestValidateSelectors_ReadAnOwnersTypesAndFields(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlasfilter.ValidateIncludeSelectors([]string{"*[type=gadget]"}, customSelections()), qt.IsNil)
	c.Assert(atlasfilter.ValidateExcludeSelectors([]string{"*[type=gadget].label"}, customSelections()), qt.IsNil)
	c.Assert(atlasfilter.ValidateIncludeSelectors([]string{"*[type=gadget]"}, selectext.None()), qt.ErrorMatches,
		`unsupported Atlas include resource type "gadget" in selector "\*\[type=gadget\]"`)
	c.Assert(atlasfilter.ValidateExcludeSelectors([]string{"*[type=gadget].label"}, selectext.None()), qt.ErrorMatches,
		`unsupported Atlas exclude field selector "\.label": .*`)
	c.Assert(atlasfilter.ValidateIncludeSelectors([]string{"app.t"}, selectext.Set{}), qt.ErrorIs, selectext.ErrUnselected)
	c.Assert(atlasfilter.ValidateExcludeSelectors([]string{"app.t"}, selectext.Set{}), qt.ErrorIs, selectext.ErrUnselected)
	c.Assert(atlasfilter.ValidateResourceIncludeSelectors([]string{"app.t"}, selectext.Set{}), qt.ErrorIs, selectext.ErrUnselected)
}

// TestScope_RefusesAnUnselectedSet_FailurePath refuses a scope whose
// selections nobody built, on every entry point, so a caller that forgot the
// owners is told rather than left with their objects unnamed.
func TestScope_RefusesAnUnselectedSet_FailurePath(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{Include: []string{"app.g1"}, Exclude: []string{"d3"}}

	scoped, err := atlasfilter.ScopeDatabase(customDatabase(), scope)
	c.Assert(err, qt.ErrorIs, selectext.ErrUnselected)
	c.Assert(scoped, qt.IsNil)
	excluded, _, err := atlasfilter.ExcludeDatabaseScopeReport(customDatabase(), scope)
	c.Assert(err, qt.ErrorIs, selectext.ErrUnselected)
	c.Assert(excluded, qt.IsNil)
	resources, err := atlasfilter.ScopeResources(nil, scope)
	c.Assert(err, qt.ErrorIs, selectext.ErrUnselected)
	c.Assert(resources, qt.IsNil)
	nothing, err := atlasfilter.ScopeDatabase(nil, scope)
	c.Assert(err, qt.ErrorIs, selectext.ErrUnselected)
	c.Assert(nothing, qt.IsNil)
}

// TestExcludeDatabase_LeavesADatabaseWideNameOutOfEverySchema keeps every
// depot when an exclusion names the schema unqualified objects default to:
// a database-wide name is in no schema, so no schema exclusion reaches it.
func TestExcludeDatabase_LeavesADatabaseWideNameOutOfEverySchema(t *testing.T) {
	c := qt.New(t)

	got, _, err := atlasfilter.ExcludeDatabaseScopeReport(customDatabase(),
		atlasfilter.Scope{Exclude: []string{"app"}, DefaultSchema: "app", RealmRelativePatterns: true, Selections: customSelections()})

	c.Assert(err, qt.IsNil)
	c.Assert(customObjects(c, got), qt.ContentEquals, []string{"other|g2=two", "|app.d1", "|d2", "|d3"})
}
