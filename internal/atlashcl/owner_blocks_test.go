package atlashcl_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
)

const gadgetKind schemaext.Kind = "example.org/custom/gadget"

// gadget is an object of the owner example.org/custom, which a `gadget`
// block declares.
type gadget struct {
	Size string
}

func (*gadget) Kind() schemaext.Kind { return gadgetKind }

func (g *gadget) Clone() schemaext.Value { return new(*g) }

func (g *gadget) Equal(other schemaext.Value) bool {
	o, ok := other.(*gadget)
	return ok && *o == *g
}

func gadgetCodec() schemaext.Codec {
	encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value.(*gadget).Size) }
	return schemaext.Codec{Prototype: &gadget{}, Representation: schemaext.Desired, Version: 1,
		Definition: json.RawMessage(`{"type":"string"}`),
		Clone:      func(value schemaext.Payload) (schemaext.Payload, error) { return value.(*gadget).Clone(), nil },
		Encode:     encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			size, err := schemaext.DecodeJSON[string](data)
			return &gadget{Size: size}, err
		},
	}
}

// gadgetDecoder declares a gadget for each claim, refuses the one named
// "refused", and records the requests it is given and the claims made.
type gadgetDecoder struct {
	requests *[]hclext.Request
}

func (d gadgetDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*d.requests = append(*d.requests, request)
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	for i, claim := range request.Claims {
		if claim.Name == "refused" {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: fmt.Errorf("gadget %q is refused", claim.Name)}
		}
		ref := objectidentity.ID{Kind: objectidentity.Kind(gadgetKind), Name: objectidentity.Part{Source: claim.Name, Normalized: claim.Name}}
		declarations = append(declarations, hclext.Declaration{Objects: []schemaext.Object{
			{Ref: ref, Value: &gadget{Size: claim.Attributes["size"].Text}}}})
	}
	return hclext.Result{Declarations: declarations}, nil
}

// gadgetOwners registers the owner example.org/custom, whose `gadget` block
// takes a string `size` and a `note` read as text, and whose claim counts the
// documents that ask for it.
func gadgetOwners(requests *[]hclext.Request, claims *int) hclext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:     "example.org/custom",
		Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{
			Owner: "example.org/custom",
			Kinds: []schemaext.Kind{gadgetKind},
			Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "gadget", Naming: hclext.SchemaAndName,
				Attributes: []hclext.Attribute{{Name: "size", Kind: hclext.String}, {Name: "note", Kind: hclext.Text}}}},
			Decoder: gadgetDecoder{requests: requests},
			Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) {
				*claims++
				return schemaext.Coverage{}, nil
			},
		}},
	})).HCL()
}

const gadgetDocument = `variable "note" {
  type    = string
  default = "from a variable"
}

schema "app" {}

gadget "app" "small" {
  size = "s"
  note = var.note
}

gadget "large" {
  schema = schema.app
  size   = "l"
  note   = 5
}
`

// TestParseWithOptions_ReachesTheOwnersBlocks hands an owner registered
// through the runtime every block of its in one call, with the labels read as
// a schema and a name and each attribute evaluated as its kind -- a variable
// and a number become text -- and adds the objects it declares. The owner's
// claim is asked for once.
func TestParseWithOptions_ReachesTheOwnersBlocks(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	claims := 0

	db, err := atlashcl.ParseWithOptions([]byte(gadgetDocument), "schema.hcl", atlashcl.Options{HCL: gadgetOwners(&requests, &claims)})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []hclext.Request{{Claims: []hclext.Claim{
		{Block: "gadget", Schema: "app", Name: "small", Attributes: map[string]hclext.Value{
			"size": {Kind: hclext.String, Text: "s"}, "note": {Kind: hclext.Text, Text: "from a variable"}}, At: "schema.hcl:8"},
		{Block: "gadget", Schema: "app", Name: "large", Attributes: map[string]hclext.Value{
			"size": {Kind: hclext.String, Text: "l"}, "note": {Kind: hclext.Text, Text: "5"}}, At: "schema.hcl:13"},
	}}})
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 2)
	c.Assert(claims, qt.Equals, 1)
}

// TestParseWithOptions_RefusesAnOwnersBlockAsItsOwn refuses an owner's block
// the way a block of the reader's own is refused, positioned at the block:
// its labels, a nested block, an unknown attribute, an attribute of the wrong
// kind, a second declaration of one object, and the owner's own refusal.
func TestParseWithOptions_RefusesAnOwnersBlockAsItsOwn(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "three labels", document: "gadget \"a\" \"b\" \"c\" {}\n",
			wantErr: `parse HCL schema at schema.hcl:1,1-7: gadget block requires one or two name labels`},
		{name: "a nested block", document: "gadget \"g\" {\n  inner {}\n}\n",
			wantErr: `parse HCL schema at schema.hcl:2,3-8: unsupported gadget block "inner"`},
		{name: "an unknown attribute", document: "gadget \"g\" {\n  color = \"red\"\n}\n",
			wantErr: `parse HCL schema at schema.hcl:1,1-7: unsupported gadget attribute "color"`},
		{name: "a string attribute of another kind", document: "gadget \"g\" {\n  size = 5\n}\n",
			wantErr: `parse HCL schema at schema.hcl:1,1-7: gadget attribute "size" must be a string`},
		{name: "one object declared twice", document: "gadget \"g\" {}\ngadget \"g\" {}\n",
			wantErr: `parse HCL schema at schema.hcl:2,1-7: gadget g is declared twice: duplicate feature identity: .*`},
		{name: "the owner's refusal", document: "gadget \"ok\" {}\ngadget \"refused\" {}\n",
			wantErr: `parse HCL schema at schema.hcl:2,1-7: gadget "refused" is refused`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			claims := 0

			db, err := atlashcl.ParseWithOptions([]byte(test.document), "schema.hcl", atlashcl.Options{HCL: gadgetOwners(&requests, &claims)})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseWithOptions_DropsAnOwnersUnknownNameUnderTheTolerance drops an
// unknown attribute of an owner's block, and records it, as it drops one of
// the reader's own; the owner never sees it.
func TestParseWithOptions_DropsAnOwnersUnknownNameUnderTheTolerance(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	var ignored []atlashcl.IgnoredName
	claims := 0

	db, err := atlashcl.ParseWithOptions([]byte("gadget \"g\" {\n  color = \"red\"\n}\n"), "schema.hcl", atlashcl.Options{
		HCL: gadgetOwners(&requests, &claims), IgnoreUnknownNames: true,
		RecordIgnored: func(name atlashcl.IgnoredName) { ignored = append(ignored, name) },
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 1)
	c.Assert(ignored, qt.DeepEquals, []atlashcl.IgnoredName{{Name: "color", Kind: "attribute", Scope: "gadget", Filename: "schema.hcl", Line: 2}})
	c.Assert(requests[0].Claims[0].Attributes, qt.HasLen, 0)
}

// TestParseWithOptions_WithoutTheOwnerRefusesItsBlock is the control: without
// the owner a gadget is a block the reader does not model.
func TestParseWithOptions_WithoutTheOwnerRefusesItsBlock(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.ParseWithOptions([]byte("gadget \"g\" {}\n"), "schema.hcl", atlashcl.Options{HCL: hclext.None()})

	c.Assert(err, qt.ErrorMatches, `parse HCL schema at schema.hcl:1,1-7: unsupported top-level block "gadget"`)
	c.Assert(db, qt.IsNil)
}

// TestParseWithOptions_RefusesAnUnselectedOwnerSet refuses the zero set, so a
// caller that forgot to select owners is told rather than read without them.
func TestParseWithOptions_RefusesAnUnselectedOwnerSet(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.ParseWithOptions([]byte("schema \"app\" {}\n"), "schema.hcl", atlashcl.Options{})

	c.Assert(err, qt.ErrorIs, hclext.ErrUnselected)
	c.Assert(db, qt.IsNil)
}

// dialDecoder sets a gadget on the table each `dial` block names, sized by
// whether the block turned it `on`, declares nothing for a `knob` block, and
// records the requests it is given.
type dialDecoder struct {
	requests *[]hclext.Request
}

func (d dialDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*d.requests = append(*d.requests, request)
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	for _, claim := range request.Claims {
		if claim.Block == "knob" {
			declarations = append(declarations, hclext.Declaration{})
			continue
		}
		size := "off"
		if claim.Attributes["on"].Bool {
			size = "on"
		}
		declarations = append(declarations, hclext.Declaration{TableSetting: &gadget{Size: size}})
	}
	return hclext.Result{Declarations: declarations}, nil
}

// dialOwners registers the owner example.org/custom, whose `dial` block sets
// a gadget on the table it names and whose `knob` block, named with a
// qualified name, declares nothing.
func dialOwners(requests *[]hclext.Request) hclext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:     "example.org/custom",
		Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{
			Owner: "example.org/custom",
			Kinds: []schemaext.Kind{gadgetKind},
			Blocks: []hclext.Block{
				{Site: hclext.TopLevel, Type: "dial", Naming: hclext.SchemaAndName, Attributes: []hclext.Attribute{{Name: "on", Kind: hclext.Bool}}},
				{Site: hclext.TopLevel, Type: "knob", Naming: hclext.SchemaAndQualifiedName},
			},
			Decoder: dialDecoder{requests: requests},
		}},
	})).HCL()
}

// TestParseWithOptions_AttachesAnOwnersTableSetting attaches the setting a
// block declares to the table its labels name, though the table block comes
// later, and reads a bool attribute written as null as absent.
func TestParseWithOptions_AttachesAnOwnersTableSetting(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	document := `dial "app" "first" {
  on = true
}

dial "app" "second" {
  on = null
}

schema "app" {}

table "first" {
  schema = schema.app
  column "id" {
    type = int
  }
}

table "second" {
  schema = schema.app
  column "id" {
    type = int
  }
}
`

	db, err := atlashcl.ParseWithOptions([]byte(document), "schema.hcl", atlashcl.Options{HCL: dialOwners(&requests)})

	c.Assert(err, qt.IsNil)
	c.Assert(requests[0].Claims[0].Attributes, qt.DeepEquals, map[string]hclext.Value{"on": {Kind: hclext.Bool, Bool: true}})
	c.Assert(requests[0].Claims[1].Attributes, qt.HasLen, 0)
	c.Assert(tableGadgets(c, db), qt.DeepEquals, map[string]string{"first": "on", "second": "off"})
}

// TestParseWithOptions_RefusesABoolAttributeThatIsNoBool refuses a value of a
// bool attribute that is not a bool, positioned at the attribute, rather than
// dropping what the author wrote. A string spelling a bool is refused too, as
// the pinned community binary refuses one in its own blocks.
func TestParseWithOptions_RefusesABoolAttributeThatIsNoBool(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "a word", value: `"yes"`},
		{name: "a string spelling true", value: `"true"`},
		{name: "a number", value: `1`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request

			db, err := atlashcl.ParseWithOptions([]byte("dial \"app\" \"t\" {\n  on = "+test.value+"\n}\n"), "schema.hcl",
				atlashcl.Options{HCL: dialOwners(&requests)})

			c.Assert(err, qt.ErrorMatches, `parse HCL schema at schema\.hcl:2,3-5: dial attribute "on" must be a bool`)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseWithOptions_RefusesASettingOfAnUndeclaredTable refuses a setting
// whose table the document does not declare, positioned at its block.
func TestParseWithOptions_RefusesASettingOfAnUndeclaredTable(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request

	db, err := atlashcl.ParseWithOptions([]byte("dial \"app\" \"missing\" {}\n"), "schema.hcl", atlashcl.Options{HCL: dialOwners(&requests)})

	c.Assert(err, qt.ErrorMatches, `.*schema\.hcl:1,1-5.*dial "app\.missing" names a table this schema does not declare`)
	c.Assert(db, qt.IsNil)
}

// TestParseWithOptions_ReadsAQualifiedName splits a one-label name written
// `schema.name` into its schema and name for a block named that way, and
// keeps a dotted name whole where the block names a schema.
func TestParseWithOptions_ReadsAQualifiedName(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request

	_, err := atlashcl.ParseWithOptions([]byte("knob \"app.volume\" {}\nknob \"app\" \"bass.boost\" {}\n"), "schema.hcl",
		atlashcl.Options{HCL: dialOwners(&requests)})

	c.Assert(err, qt.IsNil)
	c.Assert(requests[0].Claims, qt.DeepEquals, []hclext.Claim{
		{Block: "knob", Schema: "app", Name: "volume", Attributes: make(map[string]hclext.Value), At: "schema.hcl:1"},
		{Block: "knob", Schema: "app", Name: "bass.boost", Attributes: make(map[string]hclext.Value), At: "schema.hcl:2"},
	})
}

// tableGadgets maps each table to the size of the gadget set on it.
func tableGadgets(c *qt.C, db *schemamodel.Database) map[string]string {
	c.Helper()
	sizes := make(map[string]string, len(db.Tables))
	for _, table := range db.Tables {
		set, _, err := schemaext.FacetAs[*gadget](table.Facets, gadgetKind)
		c.Assert(err, qt.IsNil)
		sizes[table.Name] = set.Size
	}
	return sizes
}

// tunerDecoder sets a gadget, sized by the index's `tune` and type, on each
// claimed index that writes `tune`, refuses a gadget block named "refused",
// and records the requests it is given.
type tunerDecoder struct {
	requests *[]hclext.Request
}

func (d tunerDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*d.requests = append(*d.requests, request)
	declarations := make([]hclext.Declaration, 0, len(request.Claims))
	for i, claim := range request.Claims {
		if claim.Name == "refused" {
			return hclext.Result{}, &hclext.ClaimError{Claim: i, Err: fmt.Errorf("gadget %q is refused", claim.Name)}
		}
		tune, written := claim.Attributes["tune"]
		if claim.Index == nil || !written {
			declarations = append(declarations, hclext.Declaration{})
			continue
		}
		facets := must.Must(schemaext.NewFacets(&gadget{Size: tune.Text + "/" + claim.Index.Type}))
		declarations = append(declarations, hclext.Declaration{Facets: facets})
	}
	return hclext.Result{Declarations: declarations}, nil
}

// tunerOwners registers the owner example.org/custom, which reads a `tune`
// attribute of every index block and a `gadget` block, a `gizmo` header entry
// and a captured header, and records the coverage requests it is given.
func tunerOwners(requests *[]hclext.Request, coverageRequests *[]hclext.CoverageRequest, captured bool) hclext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:     "example.org/custom",
		Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{
			Owner: "example.org/custom",
			Kinds: []schemaext.Kind{gadgetKind},
			Blocks: []hclext.Block{
				{Site: hclext.IndexAttributes, Type: "index", Attributes: []hclext.Attribute{{Name: "tune", Kind: hclext.Text}}},
				{Site: hclext.TopLevel, Type: "gadget", Naming: hclext.SchemaAndName},
			},
			Decoder:          tunerDecoder{requests: requests},
			Header:           []coverage.Kind{"gizmo"},
			CapturedCoverage: captured,
			Coverage: func(request hclext.CoverageRequest) (schemaext.Coverage, error) {
				*coverageRequests = append(*coverageRequests, request)
				return schemaext.Coverage{}, nil
			},
		}},
	})).HCL()
}

const tunedDocument = `table "t" {
  column "a" {
    type = int
  }
  index "fast" {
    columns = [column.a]
    type    = "btree"
    tune    = "fast"
  }
  index "plain" {
    columns = [column.a]
  }
}
`

// TestParseWithOptions_ClaimsEveryIndexForTheOwner hands the owner that reads
// index attributes every index block, with what the reader knows of the
// index, and adds the facets it returns to that index alone.
func TestParseWithOptions_ClaimsEveryIndexForTheOwner(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	var coverageRequests []hclext.CoverageRequest

	db, err := atlashcl.ParseWithOptions([]byte(tunedDocument), "schema.hcl",
		atlashcl.Options{HCL: tunerOwners(&requests, &coverageRequests, false)})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []hclext.Request{{Claims: []hclext.Claim{
		{Block: "index", Name: "fast", Attributes: map[string]hclext.Value{"tune": {Kind: hclext.Text, Text: "fast"}},
			Index: &hclext.Index{Table: "t", Type: "btree"}, At: "schema.hcl:5"},
		{Block: "index", Name: "plain", Attributes: make(map[string]hclext.Value), Index: &hclext.Index{Table: "t"}, At: "schema.hcl:10"},
	}}})
	c.Assert(db.Indexes, qt.HasLen, 2)
	tuned, _, err := schemaext.FacetAs[*gadget](db.Indexes[0].Facets, gadgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(tuned.Size, qt.Equals, "fast/btree")
	c.Assert(db.Indexes[1].Facets.Len(), qt.Equals, 0)
}

// TestParseWithOptions_AdmitsAnIndexAttributeOnlyWithItsOwner is the
// control: without the owner, `tune` is no attribute of an index block.
func TestParseWithOptions_AdmitsAnIndexAttributeOnlyWithItsOwner(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.ParseWithOptions([]byte(tunedDocument), "schema.hcl", atlashcl.Options{HCL: hclext.None()})

	c.Assert(err, qt.ErrorMatches, `parse HCL schema at schema\.hcl:5,3-8: unsupported index attribute "tune"`)
	c.Assert(db, qt.IsNil)
}

// TestParseWithOptions_HandsTheOwnerItsHeader hands the owner the header
// entries of its kind and the captured header, and keeps the other entries
// in the common record.
func TestParseWithOptions_HandsTheOwnerItsHeader(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	var coverageRequests []hclext.CoverageRequest
	document := "// ptah:not-described gizmo \"g1\"\n// ptah:not-described view\n// ptah:feature-coverage {\"exact\":true}\nschema \"app\" {}\n"

	db, err := atlashcl.ParseWithOptions([]byte(document), "schema.hcl",
		atlashcl.Options{HCL: tunerOwners(&requests, &coverageRequests, true)})

	c.Assert(err, qt.IsNil)
	c.Assert(coverageRequests, qt.HasLen, 1)
	c.Assert(coverageRequests[0].Limits, qt.HasLen, 1)
	c.Assert(coverageRequests[0].Limits[0].Name, qt.Equals, "g1")
	c.Assert(coverageRequests[0].Captured, qt.DeepEquals, []string{`ptah:feature-coverage {"exact":true}`})
	c.Assert(db.NotDescribed.Describes(coverage.View), qt.IsFalse)
}

// TestParseWithOptions_RefusesACapturedHeaderNoOwnerReads refuses a captured
// header no selected owner reads rather than reading the document as
// claiming nothing.
func TestParseWithOptions_RefusesACapturedHeaderNoOwnerReads(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	var coverageRequests []hclext.CoverageRequest

	db, err := atlashcl.ParseWithOptions([]byte("// ptah:feature-coverage {}\nschema \"app\" {}\n"), "schema.hcl",
		atlashcl.Options{HCL: tunerOwners(&requests, &coverageRequests, false)})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*the document carries a ptah:feature-coverage header, and none of the selected owners reads one`)
	c.Assert(db, qt.IsNil)
}

// TestParseWithOptions_ReportsTheEarliestRefusal refuses a document where it
// is first wrong, whether an owner or the reader found that place: an owner's
// refusal of an early block is reported before the reader's refusal of a
// later one, though the reader found its own first.
func TestParseWithOptions_ReportsTheEarliestRefusal(t *testing.T) {
	laterTable := "table \"t\" {\n  column \"a\" {\n    type = int\n    null = \"x\"\n  }\n}\n"
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "the owner's refusal comes first", document: "gadget \"refused\" {}\n" + laterTable,
			wantErr: `parse HCL schema at schema\.hcl:1,1-7: gadget "refused" is refused`},
		{name: "the reader's refusal comes first", document: laterTable + "gadget \"refused\" {}\n",
			wantErr: `parse HCL schema at schema\.hcl:4,5-9: column attribute "null" must be a bool`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			var coverageRequests []hclext.CoverageRequest

			db, err := atlashcl.ParseWithOptions([]byte(test.document), "schema.hcl",
				atlashcl.Options{HCL: tunerOwners(&requests, &coverageRequests, false)})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
