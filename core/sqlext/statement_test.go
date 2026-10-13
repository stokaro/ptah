package sqlext_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
)

const gadgetKind schemaext.Kind = "example.org/a/gadget"

// gadget is an object of the kind it names: example.org/a's gadget, or an
// object of another owner.
type gadget struct {
	kind schemaext.Kind
}

func (g *gadget) Kind() schemaext.Kind { return g.kind }

func (g *gadget) Clone() schemaext.Value { return new(*g) }

func (g *gadget) Equal(other schemaext.Value) bool {
	o, ok := other.(*gadget)
	return ok && *o == *g
}

func gadgetObject(kind schemaext.Kind, name string) schemaext.Object {
	return schemaext.Object{Ref: objectidentity.ID{Kind: objectidentity.Kind(kind), Name: objectidentity.Part{Source: name, Normalized: name}},
		Value: &gadget{kind: kind}}
}

// gadgetStatement is the owner's payload of a statement.
type gadgetStatement struct{}

func (*gadgetStatement) Kind() schemaext.Kind { return "example.org/a/gadget-statement" }

func (*gadgetStatement) CloneExtension() ast.ExtensionPayload { return &gadgetStatement{} }

// answeringParser answers every claim with value and records its requests.
type answeringParser struct {
	value    sqlext.Value
	requests *[]sqlext.Request
}

func (p answeringParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	*p.requests = append(*p.requests, request)
	values := make([]sqlext.Value, len(request.Claims))
	for i := range values {
		values[i] = p.value
	}
	return sqlext.Result{Values: values}, nil
}

// label is a facet value of any kind.
type label struct {
	kind schemaext.Kind
}

func (l *label) Kind() schemaext.Kind { return l.kind }

func (l *label) Clone() schemaext.Value { return new(*l) }

func (l *label) Equal(other schemaext.Value) bool {
	o, ok := other.(*label)
	return ok && *o == *l
}

func gadgetSet(site sqlext.Site, parser sqlext.Parser) sqlext.Set {
	return must.Must(sqlext.NewSet(sqlext.Extension{Owner: "example.org/a", Targets: []string{"x"},
		Kinds:    []schemaext.Kind{gadgetKind},
		Facets:   []schemaext.Kind{labelKind},
		Patterns: []sqlext.Pattern{{Name: "gadget", Site: site, Words: []string{"CREATE", "GADGET"}}},
		Parser:   parser}))
}

// TestSet_Parse_HandsAStatementTheObjectsOfItsKinds hands an owner the objects
// of its own kinds that earlier documents declared, and none of another
// owner's, and returns the statement with the objects it declares.
func TestSet_Parse_HandsAStatementTheObjectsOfItsKinds(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	value := sqlext.Value{Statement: &gadgetStatement{}, Objects: []schemaext.Object{gadgetObject(gadgetKind, "new")}}
	set := gadgetSet(sqlext.Statement, answeringParser{value: value, requests: &requests})
	declared := must.Must(schemaext.NewObjects(gadgetObject(gadgetKind, "earlier"), gadgetObject("example.org/b/other", "foreign")))
	matches := []sqlext.Match{{Candidate: set.Candidates("x", sqlext.Statement)[0],
		Claim: sqlext.Claim{Pattern: "gadget", Site: sqlext.Statement, Text: "CREATE GADGET new"}}}

	values, err := set.Parse(t.Context(), "x", matches, sqlext.Document{Declared: declared})

	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].Objects, qt.HasLen, 1)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Declared, qt.HasLen, 1)
	c.Assert(requests[0].Declared[0].Ref.Name.Source, qt.Equals, "earlier")
}

// TestSet_Parse_ReturnsATableAStatementDeclares returns a statement's table,
// which declares nothing beside it.
func TestSet_Parse_ReturnsATableAStatementDeclares(t *testing.T) {
	c := qt.New(t)
	table := &sqlext.TableDeclaration{Name: "main.docs", IfNotExists: true}
	set := gadgetSet(sqlext.Statement, answeringParser{value: sqlext.Value{Table: table}, requests: new([]sqlext.Request)})
	matches := []sqlext.Match{{Candidate: set.Candidates("x", sqlext.Statement)[0],
		Claim: sqlext.Claim{Pattern: "gadget", Site: sqlext.Statement, Text: "CREATE GADGET main.docs"}}}

	values, err := set.Parse(t.Context(), "x", matches, sqlext.Document{})

	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].Table, qt.Equals, table)
	c.Assert(values[0].Statement, qt.IsNil)
}

// TestSet_Parse_RefusesAValueItsSiteDoesNotTake refuses a statement answered
// without the owner's statement or a table, or with both, a table without a
// name or with objects or table facets beside it, table facets without a
// table name or any facet, a value of another site that declares objects, a table or table
// facets, an index or an operation at a site that takes facets alone, a facet
// or an object of a kind the extension does not declare.
func TestSet_Parse_RefusesAValueItsSiteDoesNotTake(t *testing.T) {
	tests := []struct {
		name    string
		site    sqlext.Site
		value   sqlext.Value
		wantErr string
	}{
		{name: "a blank table reference", site: sqlext.Statement, value: sqlext.Value{Statement: &gadgetStatement{}, TableReferences: []string{" "}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with a blank table reference`},
		{name: "table references at another site", site: sqlext.TableOption, value: sqlext.Value{TableReferences: []string{"t"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its table-option claim "gadget" with a statement's value`},
		{name: "refusal at another site", site: sqlext.TableOption, value: sqlext.Value{UnmodeledReason: "not a declaration"},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its table-option claim "gadget" with a statement's value`},
		{name: "table with statement references", site: sqlext.Statement, value: sqlext.Value{Table: &sqlext.TableDeclaration{Name: "t"}, TableReferences: []string{"u"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with a table and statement requirements`},
		{name: "table with statement refusal", site: sqlext.Statement, value: sqlext.Value{Table: &sqlext.TableDeclaration{Name: "t"}, UnmodeledReason: "not a declaration"},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with a table and statement requirements`},
		{name: "a statement without the owner's statement", site: sqlext.Statement, value: sqlext.Value{},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with no statement`},
		{name: "a statement and a table", site: sqlext.Statement,
			value:   sqlext.Value{Statement: &gadgetStatement{}, Table: &sqlext.TableDeclaration{Name: "t"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with both a statement and a table`},
		{name: "table facets at another site", site: sqlext.TableOption,
			value:   sqlext.Value{TableFacets: &sqlext.TableFacets{Name: "t"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its table-option claim "gadget" with a statement's value`},
		{name: "table facets without a table name", site: sqlext.Statement,
			value:   sqlext.Value{Statement: &gadgetStatement{}, TableFacets: &sqlext.TableFacets{Name: " "}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with table facets without a table name`},
		{name: "table facets without a facet", site: sqlext.Statement,
			value:   sqlext.Value{Statement: &gadgetStatement{}, TableFacets: &sqlext.TableFacets{Name: "t"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with table facets without a facet`},
		{name: "a table and another table's facets", site: sqlext.Statement,
			value:   sqlext.Value{Table: &sqlext.TableDeclaration{Name: "t"}, TableFacets: &sqlext.TableFacets{Name: "u"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with a table and another table's facets`},
		{name: "a table facet of a kind the extension does not declare", site: sqlext.Statement,
			value: sqlext.Value{Statement: &gadgetStatement{},
				TableFacets: &sqlext.TableFacets{Name: "t", Facets: must.Must(schemaext.NewFacets(&label{kind: "example.org/a/other"}))}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered a facet of kind "example.org/a/other", which its extension does not declare`},
		{name: "a table without a name", site: sqlext.Statement, value: sqlext.Value{Table: &sqlext.TableDeclaration{Name: " "}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with a table without a name`},
		{name: "a table and an object", site: sqlext.Statement,
			value:   sqlext.Value{Table: &sqlext.TableDeclaration{Name: "t"}, Objects: []schemaext.Object{gadgetObject(gadgetKind, "new")}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with a table and objects beside it`},
		{name: "a table and an amendment", site: sqlext.Statement,
			value:   sqlext.Value{Table: &sqlext.TableDeclaration{Name: "t"}, Replaces: []schemaext.Object{gadgetObject(gadgetKind, "old")}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its statement claim "gadget" with a table and objects beside it`},
		{name: "a table at another site", site: sqlext.TableElement,
			value:   sqlext.Value{Index: &sqlext.Index{Name: "i", Expression: "a"}, Table: &sqlext.TableDeclaration{Name: "t"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its table-element claim "gadget" with a statement's value`},
		{name: "an index at a table prefix", site: sqlext.TablePrefix, value: sqlext.Value{Index: &sqlext.Index{Name: "i", Expression: "a"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its table-prefix claim "gadget" with an index or an operation, which the site does not take`},
		{name: "an index at a table option", site: sqlext.TableOption, value: sqlext.Value{Index: &sqlext.Index{Name: "i", Expression: "a"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its table-option claim "gadget" with an index or an operation, which the site does not take`},
		{name: "an operation at a table option", site: sqlext.TableOption, value: sqlext.Value{Operation: &gadgetStatement{}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its table-option claim "gadget" with an index or an operation, which the site does not take`},
		{name: "an index at a view clause", site: sqlext.ViewClause, value: sqlext.Value{Index: &sqlext.Index{Name: "i", Expression: "a"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its view-clause claim "gadget" with an index or an operation, which the site does not take`},
		{name: "a table at a table option", site: sqlext.TableOption, value: sqlext.Value{Table: &sqlext.TableDeclaration{Name: "t"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its table-option claim "gadget" with a statement's value`},
		{name: "an index at a column option", site: sqlext.ColumnOption, value: sqlext.Value{Index: &sqlext.Index{Name: "i", Expression: "a"}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its column-option claim "gadget" with an index or an operation, which the site does not take`},
		{name: "an operation at an index option", site: sqlext.IndexOption, value: sqlext.Value{Operation: &gadgetStatement{}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its index-option claim "gadget" with an index or an operation, which the site does not take`},
		{name: "a facet of a kind the extension does not declare", site: sqlext.ColumnOption,
			value:   sqlext.Value{Facets: must.Must(schemaext.NewFacets(&label{kind: "example.org/a/other"}))},
			wantErr: `invalid feature value: SQL parser of example.org/a answered a facet of kind "example.org/a/other", which its extension does not declare`},
		{name: "objects at another site", site: sqlext.AlterOperation,
			value:   sqlext.Value{Operation: &gadgetStatement{}, Objects: []schemaext.Object{gadgetObject(gadgetKind, "new")}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered its alter-operation claim "gadget" with a statement's value`},
		{name: "an object of another kind", site: sqlext.Statement,
			value:   sqlext.Value{Statement: &gadgetStatement{}, Replaces: []schemaext.Object{gadgetObject("example.org/b/other", "foreign")}},
			wantErr: `invalid feature value: SQL parser of example.org/a answered an object of kind "example.org/b/other", which its extension does not declare`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []sqlext.Request
			set := gadgetSet(test.site, answeringParser{value: test.value, requests: &requests})
			matches := []sqlext.Match{{Candidate: set.Candidates("x", test.site)[0],
				Claim: sqlext.Claim{Pattern: "gadget", Site: test.site, Text: "CREATE GADGET new"}}}

			values, err := set.Parse(t.Context(), "x", matches, sqlext.Document{})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}

// TestNewSet_RefusesAnInvalidObjectKind refuses an extension whose statements
// would declare objects of a kind no value can carry.
func TestNewSet_RefusesAnInvalidObjectKind(t *testing.T) {
	c := qt.New(t)

	set, err := sqlext.NewSet(sqlext.Extension{Owner: "example.org/a", Targets: []string{"x"}, Kinds: []schemaext.Kind{"gadget"}})

	c.Assert(err, qt.ErrorMatches, `SQL extension of example.org/a declares invalid object kind "gadget"`)
	c.Assert(set.Selected(), qt.IsFalse)
}

// TestSet_Parse_DocumentFacts copies source-ordered table removals into the
// owner's request and returns statement requirements without interpreting them.
func TestSet_Parse_DocumentFacts(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	value := sqlext.Value{Statement: &gadgetStatement{}, TableReferences: []string{"t"}, UnmodeledReason: "requires a declaration"}
	set := gadgetSet(sqlext.Statement, answeringParser{value: value, requests: &requests})
	drops := []sqlext.DroppedTable{{Name: "t", Offset: 20}, {Name: "app.u", Offset: 40}}
	matches := []sqlext.Match{{Candidate: set.Candidates("x", sqlext.Statement)[0], Claim: sqlext.Claim{Pattern: "gadget", Site: sqlext.Statement, Text: "CREATE GADGET x"}}}
	values, err := set.Parse(t.Context(), "x", matches, sqlext.Document{DroppedTables: drops})
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []sqlext.Value{value})
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].DroppedTables, qt.DeepEquals, drops)
	requests[0].DroppedTables[0].Name = "changed by owner"
	c.Assert(drops[0].Name, qt.Equals, "t")
}

// combinedParser returns a fixed batch to exercise declaration references.
type combinedParser struct{ values []sqlext.Value }

func (p combinedParser) ParseSQL(context.Context, sqlext.Request) (sqlext.Result, error) {
	return sqlext.Result{Values: p.values}, nil
}

func parseCombined(c *qt.C, values []sqlext.Value) ([]sqlext.Value, error) {
	c.Helper()
	set := gadgetSet(sqlext.Statement, combinedParser{values: values})
	matches := make([]sqlext.Match, len(values))
	for i := range matches {
		matches[i] = sqlext.Match{Candidate: set.Candidates("x", sqlext.Statement)[0], Claim: sqlext.Claim{Pattern: "gadget", Site: sqlext.Statement}}
	}
	return set.Parse(c.Context(), "x", matches, sqlext.Document{})
}

// TestSet_Parse_CombinedDeclaration accepts one final model for earlier
// operations without replacing their separate syntax payloads.
func TestSet_Parse_CombinedDeclaration(t *testing.T) {
	c := qt.New(t)
	values := []sqlext.Value{{Statement: &gadgetStatement{}, DescribedBy: 2}, {Statement: &gadgetStatement{}, Objects: []schemaext.Object{gadgetObject(gadgetKind, "new")}}}
	got, err := parseCombined(c, values)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, values)
}

// TestSet_Parse_CombinedDeclaration_FailurePath refuses dangling, cyclic
// and chained references, and a target carrying no model declaration.
func TestSet_Parse_CombinedDeclaration_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		values []sqlext.Value
		want   string
	}{
		{name: "negative", values: []sqlext.Value{{Statement: &gadgetStatement{}, DescribedBy: -1}}, want: "must be described by a later statement claim"},
		{name: "self", values: []sqlext.Value{{Statement: &gadgetStatement{}, DescribedBy: 1}}, want: "must be described by a later statement claim"},
		{name: "missing", values: []sqlext.Value{{Statement: &gadgetStatement{}, DescribedBy: 2}}, want: "must be described by a later statement claim"},
		{name: "backward", values: []sqlext.Value{{Statement: &gadgetStatement{}}, {Statement: &gadgetStatement{}, DescribedBy: 1}}, want: "must be described by a later statement claim"},
		{name: "no declaration", values: []sqlext.Value{{Statement: &gadgetStatement{}, DescribedBy: 2}, {Statement: &gadgetStatement{}}}, want: "without its own model declaration"},
		{name: "chain", values: []sqlext.Value{{Statement: &gadgetStatement{}, DescribedBy: 2}, {Statement: &gadgetStatement{}, DescribedBy: 3}, {Statement: &gadgetStatement{}, Objects: []schemaext.Object{gadgetObject(gadgetKind, "new")}}}, want: "without its own model declaration"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := parseCombined(c, test.values)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}
