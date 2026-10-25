package provider_test

import (
	"errors"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func encodedNumber(number string) *protocol.SourceFragment_Value {
	return &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Value: &protocol.SourceValue_Number{Number: number}}}
}

// encodeExchange writes a table facet under the YAML table key that reads it,
// and reports a native object YAML cannot say.
func encodeExchange() (*protocol.Description, *protocol.EncodeSourceRequest, *protocol.EncodedSource) {
	d, decodeRequest, decoded := sourceExchange()
	d.Features = append(d.Features, provider.EncodeSourceFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.EncodeSourceFeature)
	facet := decoded.Documents[0].Fragments[0].Values[0].Value
	native := proto.CloneOf(facet)
	native.Id = "second"
	native.Value.Owner.Kind, native.NormalizedOwner.Kind = native.Value.Kind, native.Value.Kind
	request := &protocol.EncodeSourceRequest{Engine: "fixture", TargetFeatures: decodeRequest.TargetFeatures, Format: protocol.SourceDocument_YAML,
		Bindings: []*protocol.SourceBinding{{Id: "table", Object: proto.CloneOf(facet.Value.Owner), StructName: "Record", EntryKey: "record"}},
		Values: []*protocol.EncodeSourceValue{{Placement: protocol.SourceModelValue_FACET, Value: facet, BindingId: "table"},
			{Placement: protocol.SourceModelValue_NATIVE_OBJECT, Value: native}},
		Coverage: decoded.Documents[0].Coverage}
	fragment := &protocol.SourceFragment{Id: "settings", Selector: "retention", BindingId: "table",
		YamlKey: proto.CloneOf(d.Engines[0].YamlSourceKeys[0]), Content: encodedNumber("60")}
	result := &protocol.EncodedSource{Complete: true, Fragments: []*protocol.EncodedFragment{{Fragment: fragment, ValueIds: []string{facet.Id}}},
		Losses: []*protocol.EncodeLoss{{ValueId: "second", Message: "a retention object has no YAML key"}},
		Limits: []*protocol.SchemaCoverageLimit{{Kind: "retention", Name: "old"}}}
	return d, request, result
}

func TestEncodeSource_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := encodeExchange()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	empty := &protocol.EncodeSourceRequest{Engine: "fixture", TargetFeatures: request.TargetFeatures, Format: protocol.SourceDocument_HCL}
	c.Assert(provider.ValidateEncodedSource(d, empty, &protocol.EncodedSource{Complete: true}), qt.IsNil)
}

func TestEncodeSourceRequest_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.EncodeSourceRequest)
	}{
		{"the provider does not encode", "encoding is unavailable", func(d *protocol.Description, _ *protocol.EncodeSourceRequest) {
			d.Features = d.Features[:len(d.Features)-1]
		}},
		{"the engine does not encode", "the engine does not encode", func(d *protocol.Description, _ *protocol.EncodeSourceRequest) {
			d.Engines[0].Features = d.Engines[0].Features[:1]
		}},
		{"another engine", "value engine is not owned by this provider", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) { r.Engine = "other" }},
		{"a SQL document", "a format the host writes is YAML, HCL or Go annotations", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Format = protocol.SourceDocument_SQL
		}},
		{"no format", "a format the host writes is YAML, HCL or Go annotations", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Format = protocol.SourceDocument_FORMAT_UNSPECIFIED
		}},
		{"a binding without an ID", "an invalid or repeated binding", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) { r.Bindings[0].Id = "" }},
		{"a repeated binding", "an invalid or repeated binding", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Bindings = append(r.Bindings, proto.CloneOf(r.Bindings[0]))
		}},
		{"a binding of another engine", "an invalid or repeated binding", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			other := proto.CloneOf(r.Bindings[0])
			other.Id, other.Object.Engine = "other", "other"
			r.Bindings = append(r.Bindings, other)
		}},
		{"a second binding without an ID", "an invalid or repeated binding", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			other := proto.CloneOf(r.Bindings[0])
			other.Id = ""
			r.Bindings = append(r.Bindings, other)
		}},
		{"a value without an ID", "missing or duplicate value ID", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) { r.Values[1].Value.Id = "" }},
		{"a repeated value ID", "a value needs a unique ID", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Values[1].Value.Id = r.Values[0].Value.Id
		}},
		{"a value of an unknown model", "value does not match the negotiated model definition", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Values[0].Value.Value.Kind = "example.org/other"
		}},
		{"a native object bound to a table", "a native object has its model kind as identity kind and no binding", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Values[1].BindingId = "table"
		}},
		{"a native object of another kind", "a native object has its model kind as identity kind and no binding", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Values[1].Placement = protocol.SourceModelValue_NATIVE_OBJECT
			r.Values[0].Placement, r.Values[0].BindingId = protocol.SourceModelValue_NATIVE_OBJECT, ""
		}},
		{"a facet without a binding", "a table facet belongs to its binding's table", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Values[0].BindingId = ""
		}},
		{"a facet of another table", "a table facet belongs to its binding's table", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Bindings[0].Object.Name = "other"
		}},
		{"a facet of an object that is no table, column or index", "a facet belongs to a table, a column or an index", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Values[0].Value.Value.Owner.Kind, r.Values[0].Value.NormalizedOwner.Kind = "view", "view"
		}},
		{"a column facet without its table", "a column or index facet belongs to a column or index of its binding's table", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			owner := r.Values[0].Value.Value.Owner
			owner.Kind, owner.Path, owner.Name = "column", nil, "id"
			r.Values[0].Value.NormalizedOwner = proto.CloneOf(owner)
		}},
		{"a column facet in another schema", "a column or index facet belongs to a column or index of its binding's table", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			owner := r.Values[0].Value.Value.Owner
			owner.Kind, owner.Path, owner.Name, owner.Schema = "column", []string{r.Bindings[0].Object.Name}, "id", "other"
			r.Values[0].Value.NormalizedOwner = proto.CloneOf(owner)
		}},
		{"a column facet of another table", "a column or index facet belongs to a column or index of its binding's table", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			owner := r.Values[0].Value.Value.Owner
			owner.Kind, owner.Path, owner.Name = "column", []string{"other"}, "id"
			r.Values[0].Value.NormalizedOwner = proto.CloneOf(owner)
		}},
		{"an unknown placement", "unknown value placement", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) { r.Values[0].Placement = 9 }},
		{"observed coverage", "coverage is DESIRED", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Coverage.Representation = protocol.ValueInput_OBSERVED
		}},
		{"coverage of an unknown model", "unknown, changed, or duplicate coverage model", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Coverage.Kinds[0].Model.Kind = "example.org/other"
		}},
		{"columns on a YAML document", "columns travel only on a Go export", func(_ *protocol.Description, r *protocol.EncodeSourceRequest) {
			r.Columns = []*protocol.EncodeSourceColumn{{BindingId: "table", Name: "id", Type: "Uint64"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := encodeExchange()
			test.mutate(d, request)
			err := provider.ValidateEncodeSourceRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

func TestEncodedSource_FailurePath(t *testing.T) {
	fragment := func(r *protocol.EncodedSource) *protocol.SourceFragment { return r.Fragments[0].Fragment }
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.EncodedSource)
	}{
		{"an incomplete reply", "incomplete encoding", func(r *protocol.EncodedSource) { r.Complete = false }},
		{"a fragment without its claim", "an encoded fragment carries its format's claim", func(r *protocol.EncodedSource) { fragment(r).YamlKey = nil }},
		{"a missing fragment", "an encoded fragment carries its format's claim", func(r *protocol.EncodedSource) { r.Fragments[0].Fragment = nil }},
		{"a claim the engine does not declare", "YAML fragment changed its negotiated grammar claim", func(r *protocol.EncodedSource) { fragment(r).YamlKey.Key = "other" }},
		{"a changed selector", "YAML fragment changed its negotiated grammar claim", func(r *protocol.EncodedSource) { fragment(r).Selector = "other" }},
		{"a fragment with a position", "an encoded fragment carries a position", func(r *protocol.EncodedSource) { fragment(r).Span = sourceSpan() }},
		{"a value with a position", "an encoded fragment carries a position", func(r *protocol.EncodedSource) { fragment(r).GetValue().Span = sourceSpan() }},
		{"a fragment without an ID", "missing or duplicate fragment identity", func(r *protocol.EncodedSource) { fragment(r).Id = "" }},
		{"a table key without its table", "table YAML key requires one frontend binding", func(r *protocol.EncodedSource) { fragment(r).BindingId = "" }},
		{"a fragment that writes nothing", "a fragment writes no value", func(r *protocol.EncodedSource) { r.Fragments[0].ValueIds = nil }},
		{"a fragment that writes an unknown value", "a fragment writes a value the request lacks, or of a model its claim does not name", func(r *protocol.EncodedSource) {
			r.Fragments[0].ValueIds = []string{"third"}
		}},
		{"a value written twice", "a fragment writes a value written already", func(r *protocol.EncodedSource) { r.Fragments[0].ValueIds = []string{"first", "first"} }},
		{"a value written and lost", "a loss names a value of the request once, in one line", func(r *protocol.EncodedSource) { r.Losses[0].ValueId = "first" }},
		{"a value neither written nor lost", "a value is neither written nor lost", func(r *protocol.EncodedSource) { r.Losses = nil }},
		{"a loss of an unknown value", "a loss names a value of the request once, in one line", func(r *protocol.EncodedSource) { r.Losses[0].ValueId = "third" }},
		{"a loss without a message", "a loss names a value of the request once, in one line", func(r *protocol.EncodedSource) { r.Losses[0].Message = "" }},
		{"a loss of two lines", "a loss names a value of the request once, in one line", func(r *protocol.EncodedSource) { r.Losses[0].Message = "no\nkey" }},
		{"a placement on a YAML fragment", "an HCL block of the owner's says where it stands, and no other fragment does", func(r *protocol.EncodedSource) {
			r.Fragments[0].Placement = protocol.EncodedFragment_AFTER_TABLES
		}},
		{"an unknown placement", "unknown placement", func(r *protocol.EncodedSource) { r.Fragments[0].Placement = 99 }},
		{"a limit without a kind", "a limit needs a kind", func(r *protocol.EncodedSource) { r.Limits[0].Kind = "" }},
		{"field types on a YAML document", "field types belong to a Go export", func(r *protocol.EncodedSource) {
			r.FieldTypes = []*protocol.EncodedFieldType{{BindingId: "table", Column: "id", GoType: "uint64"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := encodeExchange()
			test.mutate(result)
			err := provider.ValidateEncodedSource(d, request, result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

// A reply is checked against a valid request.
func TestEncodedSourceChecksTheRequest(t *testing.T) {
	c := qt.New(t)
	d, request, result := encodeExchange()
	request.Format = protocol.SourceDocument_SQL
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorMatches, `.*a format the host writes is YAML, HCL or Go annotations`)
}

// A value is written under a claim that names its model: the native object
// cannot borrow the table key of another model.
func TestEncodedValueStaysWithinItsClaim(t *testing.T) {
	c := qt.New(t)
	d, request, result := encodeExchange()
	second := proto.CloneOf(d.Engines[0].ValueModels[0])
	second.Kind = "example.org/fixture/another"
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, second)
	d.Engines[0].YamlSourceKeys = append(d.Engines[0].YamlSourceKeys, &protocol.YAMLSourceKey{Scope: protocol.YAMLSourceKey_DOCUMENT, Key: "others",
		Selector: "others", Models: []string{second.Kind}})
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
	native := request.Values[1].Value
	native.Value.Kind, native.Value.Owner.Kind, native.NormalizedOwner.Kind = second.Kind, second.Kind, second.Kind
	native.Value.DefinitionSha256 = second.DefinitionSha256
	result.Fragments[0].ValueIds = []string{"first", "second"}
	result.Losses = nil
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorMatches, `.*of a model its claim does not name`)
}

// A value of a model whose source formats lack the document's format has no
// claim to be written under, so it is a loss.
func TestEncodedValueOfAnotherFormatIsALoss(t *testing.T) {
	c := qt.New(t)
	d, request, result := encodeExchange()
	request.Format = protocol.SourceDocument_GO_ANNOTATIONS
	result.Fragments[0].Fragment = &protocol.SourceFragment{Id: "settings", Selector: "retention", BindingId: "table",
		GoDirective: &protocol.GoSourceDirective{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:fixture:retention", Selector: "retention",
			Models: []string{d.Engines[0].ValueModels[0].Kind}}, Content: encodedNumber("60")}
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorMatches, `.*Go fragment changed its negotiated directive claim`)
	result.Fragments = nil
	result.Losses = append(result.Losses, &protocol.EncodeLoss{ValueId: "first", Message: "no Go directive reads retention"})
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
}

// goEncodeExchange writes the facet under a Go table directive and types the
// table's key column.
func goEncodeExchange() (*protocol.Description, *protocol.EncodeSourceRequest, *protocol.EncodedSource) {
	d, request, result := encodeExchange()
	model := d.Engines[0].ValueModels[0]
	model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_GO_ANNOTATIONS}
	d.Engines[0].YamlSourceKeys = nil
	claim := &protocol.GoSourceDirective{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:fixture:retention", Selector: "retention",
		Attributes: []string{"seconds"}, Models: []string{model.Kind}}
	d.Engines[0].GoSourceDirectives = []*protocol.GoSourceDirective{claim}
	request.Format = protocol.SourceDocument_GO_ANNOTATIONS
	request.Columns = []*protocol.EncodeSourceColumn{{BindingId: "table", Name: "id", Type: "Uint64", PrimaryKey: true}}
	request.Coverage.Kinds[0].Model = proto.CloneOf(model)
	result.Fragments[0].Fragment = &protocol.SourceFragment{Id: "settings", Selector: "retention", BindingId: "table", GoDirective: proto.CloneOf(claim),
		Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Value: &protocol.SourceValue_Mapping{Mapping: &protocol.SourceMapping{
			Entries: []*protocol.SourceEntry{{Key: "seconds", Value: &protocol.SourceValue{Value: &protocol.SourceValue_Number{Number: "60"}}}}}}}}}
	result.FieldTypes = []*protocol.EncodedFieldType{{BindingId: "table", Column: "id", GoType: "uint64"}}
	return d, request, result
}

func TestGoEncoding_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := goEncodeExchange()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
}

func TestGoEncoding_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.EncodeSourceRequest, *protocol.EncodedSource)
	}{
		{"a column without a binding", func(q *protocol.EncodeSourceRequest, _ *protocol.EncodedSource) { q.Columns[0].BindingId = "other" }},
		{"a column without a name", func(q *protocol.EncodeSourceRequest, _ *protocol.EncodedSource) { q.Columns[0].Name = "" }},
		{"a column without a type", func(q *protocol.EncodeSourceRequest, _ *protocol.EncodedSource) { q.Columns[0].Type = " " }},
		{"a repeated column", func(q *protocol.EncodeSourceRequest, _ *protocol.EncodedSource) {
			q.Columns = append(q.Columns, proto.CloneOf(q.Columns[0]))
		}},
		{"a field type of an unknown column", func(_ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) { r.FieldTypes[0].Column = "other" }},
		{"a field typed twice", func(_ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.FieldTypes = append(r.FieldTypes, proto.CloneOf(r.FieldTypes[0]))
		}},
		{"a field without a type", func(_ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) { r.FieldTypes[0].GoType = "" }},
		{"an import of two lines", func(_ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.FieldTypes[0].GoImport = "time\nos"
		}},
		{"an attribute outside the directive", func(_ *protocol.EncodeSourceRequest, r *protocol.EncodedSource) {
			r.Fragments[0].Fragment.GetValue().GetMapping().Entries[0].Key = "minutes"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := goEncodeExchange()
			test.mutate(request, result)
			// A request row is refused by the request alone.
			c.Assert(errors.Join(provider.ValidateEncodeSourceRequest(d, request), provider.ValidateEncodedSource(d, request, result)), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// The column rows of a Go export are refused before any reply is read.
func TestGoEncodingColumns_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.EncodeSourceRequest)
	}{
		{"a column without a binding", func(q *protocol.EncodeSourceRequest) { q.Columns[0].BindingId = "other" }},
		{"a column without a name", func(q *protocol.EncodeSourceRequest) { q.Columns[0].Name = " " }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := goEncodeExchange()
			c.Assert(provider.ValidateEncodeSourceRequest(d, request), qt.IsNil)
			test.mutate(request)
			c.Assert(provider.ValidateEncodeSourceRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A Go fragment needs its directive claim even on an engine that declares no
// Go claim, where a decoded fragment may lack one.
func TestGoEncodingNeedsAClaim(t *testing.T) {
	c := qt.New(t)
	d, request, result := goEncodeExchange()
	d.Engines[0].GoSourceDirectives = nil
	result.Fragments[0].Fragment.GoDirective = nil
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorMatches, `.*an encoded fragment carries its format's claim`)
}

// hclEncodeExchange writes a native object as a top-level block.
func hclEncodeExchange() (*protocol.Description, *protocol.EncodeSourceRequest, *protocol.EncodedSource) {
	d, _, _ := hclExchange()
	_, request, result := encodeExchange()
	d.Features = append(d.Features, provider.EncodeSourceFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.EncodeSourceFeature)
	request.Format = protocol.SourceDocument_HCL
	request.Values = request.Values[1:]
	request.Coverage.Kinds[0].Model = proto.CloneOf(d.Engines[0].ValueModels[0])
	block := d.Engines[0].HclSource.Blocks[0]
	result.Fragments[0] = &protocol.EncodedFragment{ValueIds: []string{"second"}, Placement: protocol.EncodedFragment_AFTER_TABLES,
		Fragment: &protocol.SourceFragment{Id: "keep", Selector: "retention", HclBlock: proto.CloneOf(block),
			Content: &protocol.SourceFragment_Block{Block: &protocol.SourceBlock{Type: "retention", Labels: []*protocol.SourceLabel{{Value: "keep"}},
				Attributes: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: "seconds", Value: &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: "60"}}}}}}}}}
	result.Losses = nil
	return d, request, result
}

func TestHCLEncoding_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := hclEncodeExchange()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
}

func TestHCLEncoding_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.EncodedSource)
	}{
		{"a block that does not say where it stands", func(r *protocol.EncodedSource) {
			r.Fragments[0].Placement = protocol.EncodedFragment_PLACEMENT_UNSPECIFIED
		}},
		{"a limit of an undeclared header kind", func(r *protocol.EncodedSource) { r.Limits[0].Kind = "topic" }},
		{"a label with a position", func(r *protocol.EncodedSource) {
			r.Fragments[0].Fragment.GetBlock().Labels[0].Span = sourceSpan()
		}},
		{"a block of another type", func(r *protocol.EncodedSource) { r.Fragments[0].Fragment.GetBlock().Type = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := hclEncodeExchange()
			test.mutate(result)
			c.Assert(provider.ValidateEncodedSource(d, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestEncodeSourceFeatureNeedsDecoding(t *testing.T) {
	c := qt.New(t)
	d := description()
	d.Features = append(d.Features, provider.EncodeSourceFeature)
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*source\.encode requires source decoding`)
	d.Features = []string{provider.InspectFeature}
	d.Engines[0].Features = []string{provider.EncodeSourceFeature}
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*an engine lists the service without the provider's service and source decoding`)
}
