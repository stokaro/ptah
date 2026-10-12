package selectext_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
)

// gizmo is a value of any kind.
type gizmo struct {
	kind  schemaext.Kind
	Label string
}

func (g *gizmo) Kind() schemaext.Kind { return g.kind }

func (g *gizmo) Clone() schemaext.Value { return new(*g) }

func (g *gizmo) Equal(other schemaext.Value) bool {
	o, ok := other.(*gizmo)
	return ok && *o == *g
}

func ref(kind schemaext.Kind, name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.Kind(kind), Name: objectidentity.Part{Source: name, Normalized: name}}
}

func object(kind schemaext.Kind, name, label string) schemaext.Object {
	return schemaext.Object{Ref: ref(kind, name), Value: &gizmo{kind: kind, Label: label}}
}

// answering answers every request with result and err, and records the
// requests.
type answering struct {
	result   selectext.Result
	err      error
	requests *[]selectext.Request
}

func (a answering) SelectObjects(_ context.Context, request selectext.Request) (selectext.Result, error) {
	if a.requests != nil {
		*a.requests = append(*a.requests, request)
	}
	return a.result, a.err
}

// TestNewSet_HappyPath freezes the selections, ordered by selector type
// whatever the registration order, and finds one by its type.
func TestNewSet_HappyPath(t *testing.T) {
	c := qt.New(t)
	fields := []string{"comment"}

	set, err := selectext.NewSet(
		selectext.Extension{Owner: "example.org/b", Selections: []selectext.Selection{{Kind: "example.org/b/x", Type: "widget", Naming: selectext.InSchema}}},
		selectext.Extension{Owner: "example.org/a", Service: answering{}, Selections: []selectext.Selection{
			{Kind: "example.org/a/x", Type: "gadget", Naming: selectext.DatabaseWide, Relation: true, Fields: fields}}},
	)
	fields[0] = "changed"

	c.Assert(err, qt.IsNil)
	c.Assert(set.Selected(), qt.IsTrue)
	c.Assert(set.Selections(), qt.DeepEquals, []selectext.Selection{
		{Kind: "example.org/a/x", Type: "gadget", Naming: selectext.DatabaseWide, Relation: true, Fields: []string{"comment"}},
		{Kind: "example.org/b/x", Type: "widget", Naming: selectext.InSchema},
	})
	widget, found := set.Selection("widget")
	c.Assert(found, qt.IsTrue)
	c.Assert(widget.Kind, qt.Equals, schemaext.Kind("example.org/b/x"))
	_, found = set.Selection("nothing")
	c.Assert(found, qt.IsFalse)
	c.Assert(selectext.None().Selected(), qt.IsTrue)
	c.Assert(selectext.None().Selections(), qt.HasLen, 0)
	c.Assert(selectext.Set{}.Selected(), qt.IsFalse)
}

// TestNewSet_FailurePath refuses an extension without an owner, a selection
// a scope cannot apply, a kind or a type two selections claim, and fields
// with no service to clear them.
func TestNewSet_FailurePath(t *testing.T) {
	selection := func(kind schemaext.Kind, selectorType string) selectext.Selection {
		return selectext.Selection{Kind: kind, Type: selectorType, Naming: selectext.InSchema}
	}
	tests := []struct {
		name       string
		extensions []selectext.Extension
		wantErr    string
	}{
		{name: "no owner", extensions: []selectext.Extension{{Selections: []selectext.Selection{selection("example.org/a/x", "gadget")}}},
			wantErr: `scope selection extension 0 names no owner`},
		{name: "no kind", extensions: []selectext.Extension{{Owner: "example.org/a", Selections: []selectext.Selection{selection("", "gadget")}}},
			wantErr: `scope selection of example.org/a names no kind or no selector type`},
		{name: "no type", extensions: []selectext.Extension{{Owner: "example.org/a", Selections: []selectext.Selection{selection("example.org/a/x", " ")}}},
			wantErr: `scope selection of example.org/a names no kind or no selector type`},
		{name: "an unknown naming", extensions: []selectext.Extension{{Owner: "example.org/a", Selections: []selectext.Selection{
			{Kind: "example.org/a/x", Type: "gadget", Naming: "global"}}}},
			wantErr: `scope selection "gadget" of example.org/a has unknown naming "global"`},
		{name: "fields without a service", extensions: []selectext.Extension{{Owner: "example.org/a", Selections: []selectext.Selection{
			{Kind: "example.org/a/x", Type: "gadget", Naming: selectext.InSchema, Fields: []string{"comment"}}}}},
			wantErr: `scope selection "gadget" of example.org/a names fields without a service to clear them`},
		{name: "a field twice", extensions: []selectext.Extension{{Owner: "example.org/a", Service: answering{}, Selections: []selectext.Selection{
			{Kind: "example.org/a/x", Type: "gadget", Naming: selectext.InSchema, Fields: []string{"comment", "comment"}}}}},
			wantErr: `scope selection "gadget" of example.org/a names field "comment" empty or twice`},
		{name: "a kind twice", extensions: []selectext.Extension{
			{Owner: "example.org/a", Selections: []selectext.Selection{selection("example.org/x", "gadget")}},
			{Owner: "example.org/b", Selections: []selectext.Selection{selection("example.org/x", "widget")}}},
			wantErr: `duplicate feature identity: model "example.org/x" is selected by example.org/a and example.org/b`},
		{name: "a type twice", extensions: []selectext.Extension{
			{Owner: "example.org/a", Selections: []selectext.Selection{selection("example.org/a/x", "gadget")}},
			{Owner: "example.org/b", Selections: []selectext.Selection{selection("example.org/b/x", "gadget")}}},
			wantErr: `duplicate feature identity: selector type "gadget" is claimed by example.org/a and example.org/b`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := selectext.NewSet(test.extensions...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

// TestSet_SelectObjects_HappyPath hands each extension the kept objects of
// its kinds and their clears, and returns what every extension answers. An
// extension the side keeps nothing of and clears nothing of is not called.
func TestSet_SelectObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	var first, idle []selectext.Request
	cleared := object("example.org/a/x", "g", "")
	required := ref("example.org/a/y", "pool")
	set := must.Must(selectext.NewSet(
		selectext.Extension{Owner: "example.org/a", Service: answering{requests: &first,
			result: selectext.Result{Cleared: []schemaext.Object{cleared}, Requires: []objectidentity.ID{required}}},
			Selections: []selectext.Selection{
				{Kind: "example.org/a/x", Type: "gadget", Naming: selectext.InSchema, Fields: []string{"comment"}},
				{Kind: "example.org/a/y", Type: "pool", Naming: selectext.DatabaseWide}}},
		selectext.Extension{Owner: "example.org/b", Service: answering{requests: &idle},
			Selections: []selectext.Selection{{Kind: "example.org/b/x", Type: "widget", Naming: selectext.InSchema}}},
	))
	kept := must.Must(schemaext.NewObjects(object("example.org/a/x", "g", "labelled"), object("example.org/c/x", "other", "")))
	removal := selectext.Clear{Object: ref("example.org/a/x", "g"), Field: "comment"}

	result, err := set.SelectObjects(c.Context(), selectext.Request{Kept: kept, Clears: []selectext.Clear{removal}})

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, selectext.Result{Cleared: []schemaext.Object{cleared}, Requires: []objectidentity.ID{required}})
	c.Assert(idle, qt.HasLen, 0)
	c.Assert(first, qt.HasLen, 1)
	c.Assert(first[0].Kept.Refs(), qt.DeepEquals, []objectidentity.ID{ref("example.org/a/x", "g")})
	c.Assert(first[0].Clears, qt.DeepEquals, []selectext.Clear{removal})
}

// TestSet_SelectObjects_FailurePath refuses the unselected set, a clear of a
// field no selection names, an answer the extension cannot give, and passes
// an extension's own error on as it is.
func TestSet_SelectObjects_FailurePath(t *testing.T) {
	refused := errors.New("refused")
	kept := must.Must(schemaext.NewObjects(object("example.org/a/x", "g", "")))
	extension := func(service selectext.Service) selectext.Set {
		return must.Must(selectext.NewSet(selectext.Extension{Owner: "example.org/a", Service: service, Selections: []selectext.Selection{
			{Kind: "example.org/a/x", Type: "gadget", Naming: selectext.InSchema, Fields: []string{"comment"}}}}))
	}
	tests := []struct {
		name    string
		set     selectext.Set
		clears  []selectext.Clear
		wantErr string
	}{
		{name: "the unselected set", wantErr: `no scope selection owners were selected`},
		{name: "a field no selection names", set: extension(answering{}),
			clears:  []selectext.Clear{{Object: ref("example.org/a/x", "g"), Field: "version"}},
			wantErr: `invalid feature value: no scope selection of example.org/a/x names field "version"`},
		{name: "a clear of a kind no selection names", set: extension(answering{}),
			clears:  []selectext.Clear{{Object: ref("example.org/b/x", "g"), Field: "comment"}},
			wantErr: `invalid feature value: no scope selection of example.org/b/x names field "comment"`},
		{name: "an object it was not handed", set: extension(answering{result: selectext.Result{Cleared: []schemaext.Object{object("example.org/a/x", "h", "")}}}),
			wantErr: `invalid feature value: scope selection service of example.org/a cleared .*, which it was not handed or cleared twice`},
		{name: "an object twice", set: extension(answering{result: selectext.Result{Cleared: []schemaext.Object{object("example.org/a/x", "g", ""), object("example.org/a/x", "g", "")}}}),
			wantErr: `invalid feature value: scope selection service of example.org/a cleared .*, which it was not handed or cleared twice`},
		{name: "a requirement of a kind it does not select", set: extension(answering{result: selectext.Result{Requires: []objectidentity.ID{ref("example.org/b/x", "p")}}}),
			wantErr: `invalid feature value: scope selection service of example.org/a requires .*, of a kind it does not select`},
		{name: "the extension's own error", set: extension(answering{err: refused}), wantErr: `refused`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := test.set.SelectObjects(c.Context(), selectext.Request{Kept: kept, Clears: test.clears})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, selectext.Result{})
		})
	}
}

// TestSelection_ByNameAlone pins which objects a selection names by their
// name alone: every object of a database-wide selection, none of an in-schema
// one, and only the one with no schema of a schema-or-database selection.
func TestSelection_ByNameAlone(t *testing.T) {
	tests := []struct {
		naming selectext.Naming
		schema string
		want   bool
	}{
		{naming: selectext.DatabaseWide, schema: "app", want: true},
		{naming: selectext.DatabaseWide, schema: "", want: true},
		{naming: selectext.InSchema, schema: "app", want: false},
		{naming: selectext.InSchema, schema: "", want: false},
		{naming: selectext.SchemaOrDatabase, schema: "app", want: false},
		{naming: selectext.SchemaOrDatabase, schema: "", want: true},
	}
	for _, test := range tests {
		t.Run(string(test.naming)+"/"+test.schema, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(selectext.Selection{Naming: test.naming}.ByNameAlone(test.schema), qt.Equals, test.want)
		})
	}
}
