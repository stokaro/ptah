package hclext_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// widget is an object of the kind it names.
type widget struct {
	kind schemaext.Kind
}

func (w *widget) Kind() schemaext.Kind { return w.kind }

func (w *widget) Clone() schemaext.Value { return new(*w) }

func (w *widget) Equal(other schemaext.Value) bool {
	o, ok := other.(*widget)
	return ok && *o == *w
}

func object(kind schemaext.Kind, name string) schemaext.Object {
	return schemaext.Object{Ref: objectidentity.ID{Kind: objectidentity.Kind(kind), Name: objectidentity.Part{Source: name, Normalized: name}},
		Value: &widget{kind: kind}}
}

// recordingDecoder declares one object of its kind for each claim, named
// after the claim, and records the requests it is given.
type recordingDecoder struct {
	kind     schemaext.Kind
	requests *[]hclext.Request
}

func (d recordingDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*d.requests = append(*d.requests, request)
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	for _, claim := range request.Claims {
		declarations = append(declarations, hclext.Declaration{Objects: []schemaext.Object{object(d.kind, claim.Name)}})
	}
	return hclext.Result{Declarations: declarations}, nil
}

// answeringDecoder answers every request with result and err.
type answeringDecoder struct {
	result hclext.Result
	err    error
}

func (d answeringDecoder) DecodeHCL(context.Context, hclext.Request) (hclext.Result, error) {
	return d.result, d.err
}

func block(name string) hclext.Block {
	return hclext.Block{Site: hclext.TopLevel, Type: name, Naming: hclext.SchemaAndName,
		Attributes: []hclext.Attribute{{Name: "size", Kind: hclext.String}, {Name: "note", Kind: hclext.Text}}}
}

func extension(owner string, kind schemaext.Kind, decoder hclext.Decoder, blocks ...hclext.Block) hclext.Extension {
	return hclext.Extension{Owner: owner, Kinds: []schemaext.Kind{kind}, Blocks: blocks, Decoder: decoder}
}

func TestNewSet_FailurePath(t *testing.T) {
	decoder := answeringDecoder{}
	tests := []struct {
		name       string
		extensions []hclext.Extension
		wantErr    string
	}{
		{name: "no owner", extensions: []hclext.Extension{extension("", "example.org/a/x", decoder, block("gadget"))},
			wantErr: `HCL extension 0 names no owner`},
		{name: "a model claimed twice", extensions: []hclext.Extension{
			extension("example.org/a", "example.org/a/x", decoder), extension("example.org/b", "example.org/a/x", decoder)},
			wantErr: `duplicate.*: model "example.org/a/x" is claimed by example.org/a and example.org/b`},
		{name: "blocks without a decoder", extensions: []hclext.Extension{extension("example.org/a", "example.org/a/x", nil, block("gadget"))},
			wantErr: `HCL extension of example.org/a declares blocks without a decoder`},
		{name: "an unknown site", extensions: []hclext.Extension{extension("example.org/a", "example.org/a/x", decoder,
			hclext.Block{Site: "table", Type: "gadget", Naming: hclext.SchemaAndName})},
			wantErr: `HCL block "gadget" of example.org/a names unknown site "table"`},
		{name: "no type", extensions: []hclext.Extension{extension("example.org/a", "example.org/a/x", decoder,
			hclext.Block{Site: hclext.TopLevel, Naming: hclext.SchemaAndName})},
			wantErr: `HCL extension of example.org/a declares a block without a type`},
		{name: "an unknown naming", extensions: []hclext.Extension{extension("example.org/a", "example.org/a/x", decoder,
			hclext.Block{Site: hclext.TopLevel, Type: "gadget", Naming: "labels"})},
			wantErr: `HCL block "gadget" of example.org/a names unknown naming "labels"`},
		{name: "an attribute without a name", extensions: []hclext.Extension{extension("example.org/a", "example.org/a/x", decoder,
			hclext.Block{Site: hclext.TopLevel, Type: "gadget", Naming: hclext.SchemaAndName, Attributes: []hclext.Attribute{{Kind: hclext.String}}})},
			wantErr: `HCL block "gadget" of example.org/a declares an attribute without a name`},
		{name: "an attribute named twice", extensions: []hclext.Extension{extension("example.org/a", "example.org/a/x", decoder,
			hclext.Block{Site: hclext.TopLevel, Type: "gadget", Naming: hclext.SchemaAndName,
				Attributes: []hclext.Attribute{{Name: "size", Kind: hclext.String}, {Name: "size", Kind: hclext.Text}}})},
			wantErr: `duplicate.*: HCL block "gadget" of example.org/a declares attribute "size" twice or as its naming's own`},
		{name: "the naming's own attribute", extensions: []hclext.Extension{extension("example.org/a", "example.org/a/x", decoder,
			hclext.Block{Site: hclext.TopLevel, Type: "gadget", Naming: hclext.SchemaAndName,
				Attributes: []hclext.Attribute{{Name: "schema", Kind: hclext.String}}})},
			wantErr: `duplicate.*: HCL block "gadget" of example.org/a declares attribute "schema" twice or as its naming's own`},
		{name: "an unknown kind", extensions: []hclext.Extension{extension("example.org/a", "example.org/a/x", decoder,
			hclext.Block{Site: hclext.TopLevel, Type: "gadget", Naming: hclext.SchemaAndName,
				Attributes: []hclext.Attribute{{Name: "size", Kind: "expression"}}})},
			wantErr: `HCL attribute "size" of example.org/a block "gadget" names unknown kind "expression"`},
		{name: "index attributes of another block", extensions: []hclext.Extension{extension("example.org/a", "example.org/a/x", decoder,
			hclext.Block{Site: hclext.IndexAttributes, Type: "table"})},
			wantErr: `HCL index-attributes block of example.org/a must be of type "index" with no naming`},
		{name: "index attributes with a naming", extensions: []hclext.Extension{extension("example.org/a", "example.org/a/x", decoder,
			hclext.Block{Site: hclext.IndexAttributes, Type: "index", Naming: hclext.SchemaAndName})},
			wantErr: `HCL index-attributes block of example.org/a must be of type "index" with no naming`},
		{name: "one block read twice", extensions: []hclext.Extension{
			extension("example.org/a", "example.org/a/x", decoder, block("gadget")),
			extension("example.org/b", "example.org/b/x", decoder, block("gadget"))},
			wantErr: `duplicate.*: HCL top-level block "gadget" is read by example.org/a and example.org/b`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			set, err := hclext.NewSet(test.extensions...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(set.Selected(), qt.IsFalse)
		})
	}
}

// TestNewSet_HappyPath accepts every naming and every attribute kind.
func TestNewSet_HappyPath(t *testing.T) {
	c := qt.New(t)

	set, err := hclext.NewSet(extension("example.org/a", "example.org/a/x", answeringDecoder{},
		hclext.Block{Site: hclext.TopLevel, Type: "gadget", Naming: hclext.SchemaAndName,
			Attributes: []hclext.Attribute{{Name: "size", Kind: hclext.String}, {Name: "note", Kind: hclext.Text}, {Name: "on", Kind: hclext.Bool}}},
		hclext.Block{Site: hclext.TopLevel, Type: "widget", Naming: hclext.SchemaAndQualifiedName}))

	c.Assert(err, qt.IsNil)
	c.Assert(set.Selected(), qt.IsTrue)
}

// TestSet_Lookup finds the shape an extension registered for a block type at
// its site, and none for another type.
func TestSet_Lookup(t *testing.T) {
	c := qt.New(t)
	set := must.Must(hclext.NewSet(extension("example.org/a", "example.org/a/x", answeringDecoder{}, block("gadget"))))

	shape, found := set.Lookup(hclext.TopLevel, "gadget")
	_, other := set.Lookup(hclext.TopLevel, "widget")

	c.Assert(found, qt.IsTrue)
	c.Assert(shape.Block, qt.DeepEquals, block("gadget"))
	c.Assert(other, qt.IsFalse)
}

// TestSet_Decode_HappyPath hands each extension its own claims in one call,
// in the order of the matches, and returns the declarations in that order.
func TestSet_Decode_HappyPath(t *testing.T) {
	c := qt.New(t)
	var first, second []hclext.Request
	set := must.Must(hclext.NewSet(
		extension("example.org/a", "example.org/a/x", recordingDecoder{kind: "example.org/a/x", requests: &first}, block("gadget")),
		extension("example.org/b", "example.org/b/x", recordingDecoder{kind: "example.org/b/x", requests: &second}, block("widget")),
	))
	gadget, _ := set.Lookup(hclext.TopLevel, "gadget")
	widget, _ := set.Lookup(hclext.TopLevel, "widget")
	matches := []hclext.Match{
		{Shape: widget, Claim: hclext.Claim{Block: "widget", Name: "w1"}},
		{Shape: gadget, Claim: hclext.Claim{Block: "gadget", Name: "g1"}},
		{Shape: widget, Claim: hclext.Claim{Block: "widget", Name: "w2"}},
	}

	declarations, err := set.Decode(t.Context(), matches)

	c.Assert(err, qt.IsNil)
	c.Assert(declaredNames(declarations), qt.DeepEquals, []string{"w1", "g1", "w2"})
	c.Assert(first, qt.HasLen, 1)
	c.Assert(second, qt.HasLen, 1)
	c.Assert(claimNames(second[0]), qt.DeepEquals, []string{"w1", "w2"})
}

// TestSet_Decode_FailurePath refuses a decoder's failure, an answer for
// another number of claims, an object of a kind the extension does not claim,
// and names a refused claim by its place among the matches.
func TestSet_Decode_FailurePath(t *testing.T) {
	refused := errors.New("refused")
	tests := []struct {
		name    string
		decoder hclext.Decoder
		wantErr string
	}{
		{name: "a failure", decoder: answeringDecoder{err: refused}, wantErr: `refused`},
		{name: "too few declarations", decoder: answeringDecoder{},
			wantErr: `invalid feature value: HCL decoder of example.org/b answered 2 claims with 0 declarations`},
		{name: "an object of another kind", decoder: answeringDecoder{result: hclext.Result{Declarations: []hclext.Declaration{
			{Objects: []schemaext.Object{object("example.org/a/x", "g")}}, {}}}},
			wantErr: `invalid feature value: HCL decoder of example.org/b answered a model of kind "example.org/a/x", which its extension does not declare`},
		{name: "a refused claim", decoder: answeringDecoder{err: &hclext.ClaimError{Claim: 1, Err: refused}},
			wantErr: `claim 2: refused`},
		{name: "a facet of another kind", decoder: answeringDecoder{result: hclext.Result{Declarations: []hclext.Declaration{
			{Facets: must.Must(schemaext.NewFacets(&widget{kind: "example.org/a/x"}))}, {}}}},
			wantErr: `invalid feature value: HCL decoder of example.org/b answered a model of kind "example.org/a/x", which its extension does not declare`},
		{name: "a table setting of another kind", decoder: answeringDecoder{result: hclext.Result{Declarations: []hclext.Declaration{
			{TableSetting: &widget{kind: "example.org/a/x"}}, {}}}},
			wantErr: `invalid feature value: HCL decoder of example.org/b answered a model of kind "example.org/a/x", which its extension does not declare`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			set := must.Must(hclext.NewSet(
				extension("example.org/a", "example.org/a/x", recordingDecoder{kind: "example.org/a/x", requests: &requests}, block("gadget")),
				extension("example.org/b", "example.org/b/x", test.decoder, block("widget")),
			))
			gadget, _ := set.Lookup(hclext.TopLevel, "gadget")
			widget, _ := set.Lookup(hclext.TopLevel, "widget")
			matches := []hclext.Match{
				{Shape: gadget, Claim: hclext.Claim{Block: "gadget", Name: "g"}},
				{Shape: widget, Claim: hclext.Claim{Block: "widget", Name: "w1"}},
				{Shape: widget, Claim: hclext.Claim{Block: "widget", Name: "w2"}},
			}

			declarations, err := set.Decode(t.Context(), matches)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(declarations, qt.IsNil)
		})
	}
}

// TestSet_Decode_ReturnsTheEarliestRefusal returns the refusal of the claim
// earliest among the matches, whichever owner refused it and in whichever
// order the owners were called.
func TestSet_Decode_ReturnsTheEarliestRefusal(t *testing.T) {
	refused := errors.New("refused")
	c := qt.New(t)
	set := must.Must(hclext.NewSet(
		extension("example.org/a", "example.org/a/x", answeringDecoder{err: &hclext.ClaimError{Claim: 1, Err: errors.New("later")}}, block("gadget")),
		extension("example.org/b", "example.org/b/x", answeringDecoder{err: &hclext.ClaimError{Claim: 0, Err: refused}}, block("widget")),
	))
	gadget, _ := set.Lookup(hclext.TopLevel, "gadget")
	widget, _ := set.Lookup(hclext.TopLevel, "widget")
	matches := []hclext.Match{
		{Shape: gadget, Claim: hclext.Claim{Block: "gadget", Name: "g1"}},
		{Shape: widget, Claim: hclext.Claim{Block: "widget", Name: "w1"}},
		{Shape: gadget, Claim: hclext.Claim{Block: "gadget", Name: "g2"}},
	}

	declarations, err := set.Decode(t.Context(), matches)

	c.Assert(err, qt.ErrorMatches, `claim 1: refused`)
	c.Assert(declarations, qt.IsNil)
}

// TestSet_Coverage_HandsEachOwnerItsHeader hands each owner the header
// entries of its own kinds, and the captured header only to the owner that
// reads one.
func TestSet_Coverage_HandsEachOwnerItsHeader(t *testing.T) {
	c := qt.New(t)
	var first, second hclext.CoverageRequest
	recording := func(into *hclext.CoverageRequest) func(hclext.CoverageRequest) (schemaext.Coverage, error) {
		return func(request hclext.CoverageRequest) (schemaext.Coverage, error) {
			*into = request
			return schemaext.NewCoverage(schemaext.Desired, nil, nil)
		}
	}
	set := must.Must(hclext.NewSet(
		hclext.Extension{Owner: "example.org/a", Header: []coverage.Kind{"gizmo"}, CapturedCoverage: true, Coverage: recording(&first)},
		hclext.Extension{Owner: "example.org/b", Header: []coverage.Kind{"widget"}, Coverage: recording(&second)},
	))
	header := []coverage.Object{{Kind: "Gizmo", Name: "g"}, {Kind: "widget", Name: "w"}}

	_, err := set.Coverage(header, []string{"ptah:feature-coverage {}"})

	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.DeepEquals, hclext.CoverageRequest{Limits: []coverage.Object{{Kind: "Gizmo", Name: "g"}}, Captured: []string{"ptah:feature-coverage {}"}})
	c.Assert(second, qt.DeepEquals, hclext.CoverageRequest{Limits: []coverage.Object{{Kind: "widget", Name: "w"}}})
	c.Assert(set.ReadsHeader(" GIZMO "), qt.IsTrue)
	c.Assert(set.ReadsHeader("\u017fecret"), qt.IsFalse)
	c.Assert(set.ReadsCapturedCoverage(), qt.IsTrue)
	c.Assert(hclext.None().ReadsCapturedCoverage(), qt.IsFalse)
}

// TestSet_Coverage combines the claims of the extensions that make one.
func TestSet_Coverage(t *testing.T) {
	c := qt.New(t)
	claimed := hclext.Extension{Owner: "example.org/a", Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) {
		return schemaext.NewCoverage(schemaext.Desired, nil, nil)
	}}
	silent := hclext.Extension{Owner: "example.org/b"}

	claims, err := must.Must(hclext.NewSet(claimed, silent)).Coverage(nil, nil)
	none, noneErr := must.Must(hclext.NewSet(silent)).Coverage(nil, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(claims.Representation(), qt.Equals, schemaext.Desired)
	c.Assert(noneErr, qt.IsNil)
	c.Assert(none.IsZero(), qt.IsTrue)
}

// TestNone_IsSelectedWithoutOwners selects no owner, where the zero value
// selects nothing at all.
func TestNone_IsSelectedWithoutOwners(t *testing.T) {
	c := qt.New(t)
	_, found := hclext.None().Lookup(hclext.TopLevel, "synonym")

	c.Assert(hclext.None().Selected(), qt.IsTrue)
	c.Assert(found, qt.IsFalse)
	c.Assert(hclext.Set{}.Selected(), qt.IsFalse)
	c.Assert(hclext.ErrUnselected.Error(), qt.Contains, "HCL")
}

// TestClaimError names the claim and unwraps to the reason.
func TestClaimError(t *testing.T) {
	c := qt.New(t)
	reason := errors.New("refused")

	err := error(&hclext.ClaimError{Claim: 3, Err: reason})

	c.Assert(err, qt.ErrorMatches, `claim 3: refused`)
	c.Assert(err, qt.ErrorIs, reason)
}

func declaredNames(declarations []hclext.Declaration) []string {
	names := make([]string, 0, len(declarations))
	for _, declaration := range declarations {
		names = append(names, declaration.Objects[0].Ref.Name.Source)
	}
	return names
}

func claimNames(request hclext.Request) []string {
	names := make([]string, 0, len(request.Claims))
	for _, claim := range request.Claims {
		names = append(names, claim.Name)
	}
	return names
}
