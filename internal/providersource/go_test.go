package providersource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/internal/providersource"
)

const retentionModel = "example.org/test/retention"

// goCapture is the file decoder of a test owner that keeps what the frontend
// hands it, so a test sees real declarations with real positions.
type goCapture struct {
	declarations *[]annotation.Declaration
	tables       *annotation.Tables
}

func (f goCapture) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	*f.declarations = append(*f.declarations, declaration)
	return nil, nil
}

func (f goCapture) Finish(tables annotation.Tables) ([]annotation.Contribution, error) {
	*f.tables = tables
	return nil, nil
}

func captureGo(c *qt.C, source string) (annotation.Tables, []annotation.Declaration) {
	c.Helper()
	var (
		declarations []annotation.Declaration
		tables       annotation.Tables
	)
	owners, err := annotation.NewSet(annotation.Extension{Owner: "example.org/test",
		Directives: []annotation.Directive{
			{Name: "ptah:test:retention", Scopes: []annotation.Scope{annotation.ScopeStruct}, Attributes: []annotation.Attribute{{Name: "seconds", Value: "string"}, {Name: "minutes", Value: "string"}}},
			{Name: "ptah:test:node", Scopes: []annotation.Scope{annotation.ScopeStruct}, Attributes: []annotation.Attribute{{Name: "name", Value: "string"}}},
		},
		Kinds:    []schemaext.Kind{retentionModel},
		File:     func() annotation.FileDecoder { return goCapture{declarations: &declarations, tables: &tables} },
		Coverage: func([]coverage.Object) (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }})
	c.Assert(err, qt.IsNil)
	_, err = goschema.ParseSource(owners, "records.go", source)
	c.Assert(err, qt.IsNil)
	return tables, declarations
}

func goEngine() *protocol.Engine {
	return &protocol.Engine{Id: "example", GoSourceDirectives: []*protocol.GoSourceDirective{
		{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:test:retention", Selector: "retention", Attributes: []string{"seconds", "minutes"}, Models: []string{retentionModel}},
		{Scope: protocol.GoSourceDirective_DOCUMENT, Directive: "ptah:test:node", Selector: "node", Attributes: []string{"name"}, Models: []string{retentionModel}},
	}, ValueModels: []*protocol.ValueModel{
		{Kind: retentionModel, Representation: protocol.ValueInput_DESIRED, SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_GO_ANNOTATIONS}},
		{Kind: "example.org/test/yaml-only", Representation: protocol.ValueInput_DESIRED, SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}},
		{Kind: "example.org/test/observed", Representation: protocol.ValueInput_OBSERVED, SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_GO_ANNOTATIONS}},
	}}
}

const goSource = "package p\n\n//ptah:test:retention seconds=60 minutes=1\n//ptah:schema:table name=\"records\" schema=\"app\"\ntype Record struct{}\n\n//ptah:schema:table name=\"other\"\ntype Other struct{}\n"

func TestGoAnnotations_HappyPath(t *testing.T) {
	c := qt.New(t)
	tables, declarations := captureGo(c, goSource)
	request, err := providersource.GoAnnotations(c.Context(), goEngine(), tables, declarations)
	c.Assert(err, qt.IsNil)
	c.Assert(request.GetEngine(), qt.Equals, "example")
	document := request.GetDocuments()[0]
	c.Assert(document.GetFormat(), qt.Equals, protocol.SourceDocument_GO_ANNOTATIONS)
	c.Assert(document.GetFile(), qt.Equals, "records.go")
	c.Assert(document.GetByteLength(), qt.Equals, uint64(len(goSource)))
	c.Assert(document.GetModels(), qt.HasLen, 1)
	c.Assert(document.GetModels()[0].GetKind(), qt.Equals, retentionModel)
	c.Assert(document.GetBindings(), qt.HasLen, 2)
	c.Assert(document.GetBindings()[0].GetObject().GetSchema(), qt.Equals, "app")
	c.Assert(document.GetBindings()[0].GetObject().GetName(), qt.Equals, "records")
	c.Assert(document.GetBindings()[0].GetStructName(), qt.Equals, "Record")
	c.Assert(document.GetFragments(), qt.HasLen, 1)
	fragment := document.GetFragments()[0]
	c.Assert(proto.Equal(fragment.GetGoDirective(), goEngine().GetGoSourceDirectives()[0]), qt.IsTrue)
	c.Assert(fragment.GetSelector(), qt.Equals, "retention")
	c.Assert(fragment.GetBindingId(), qt.Equals, document.GetBindings()[0].GetId())
	entries := fragment.GetValue().GetMapping().GetEntries()
	c.Assert(entries, qt.HasLen, 2)
	c.Assert(entries[0].GetKey(), qt.Equals, "minutes")
	c.Assert(entries[0].GetValue().GetText(), qt.Equals, "1")
	c.Assert(entries[1].GetKey(), qt.Equals, "seconds")
	c.Assert(entries[1].GetValue().GetText(), qt.Equals, "60")
}

func TestGoAnnotations_SpanIsTheComment(t *testing.T) {
	c := qt.New(t)
	tables, declarations := captureGo(c, goSource)
	request, err := providersource.GoAnnotations(c.Context(), goEngine(), tables, declarations)
	c.Assert(err, qt.IsNil)
	span := request.GetDocuments()[0].GetFragments()[0].GetSpan()
	c.Assert(span.GetStart().GetByteOffset(), qt.Equals, uint64(11))
	c.Assert(span.GetEnd().GetByteOffset(), qt.Equals, uint64(53))
	c.Assert(span.GetStart().GetLine(), qt.Equals, uint32(3))
	c.Assert(span.GetStart().GetColumn(), qt.Equals, uint32(1))
}

func TestGoAnnotations_DocumentScopeHasNoBinding(t *testing.T) {
	c := qt.New(t)
	tables, declarations := captureGo(c, "package p\n\n//ptah:test:node name=\"n\"\ntype Record struct{}\n")
	request, err := providersource.GoAnnotations(c.Context(), goEngine(), tables, declarations)
	c.Assert(err, qt.IsNil)
	c.Assert(request.GetDocuments()[0].GetFragments(), qt.HasLen, 1)
	c.Assert(request.GetDocuments()[0].GetFragments()[0].GetBindingId(), qt.Equals, "")
}

func TestGoAnnotations_WithoutDeclarationsEnrollsModels(t *testing.T) {
	c := qt.New(t)
	request, err := providersource.GoAnnotations(c.Context(), goEngine(), nil, nil)
	c.Assert(err, qt.IsNil)
	document := request.GetDocuments()[0]
	c.Assert(document.GetModels(), qt.HasLen, 1)
	c.Assert(document.GetFragments(), qt.HasLen, 0)
	c.Assert(document.GetFile(), qt.Equals, "<unnamed Go>")
}

func TestGoAnnotations_FailurePath(t *testing.T) {
	t.Run("struct without a table", func(t *testing.T) {
		c := qt.New(t)
		tables, declarations := captureGo(c, "package p\n\n//ptah:test:retention seconds=60\ntype Record struct{}\n")
		request, err := providersource.GoAnnotations(c.Context(), goEngine(), tables, declarations)
		var refusal *annotation.DeclarationError
		c.Assert(err, qt.ErrorAs, &refusal)
		c.Assert(refusal.Declaration.Directive, qt.Equals, "ptah:test:retention")
		c.Assert(err, qt.ErrorMatches, `struct Record maps to no table in this file.*`)
		c.Assert(request, qt.IsNil)
	})
	t.Run("directive the engine does not claim", func(t *testing.T) {
		c := qt.New(t)
		tables, declarations := captureGo(c, goSource)
		declarations[0].Directive = "ptah:test:unclaimed"
		request, err := providersource.GoAnnotations(c.Context(), goEngine(), tables, declarations)
		c.Assert(err, qt.ErrorMatches, `engine "example" claims no Go directive "ptah:test:unclaimed"`)
		c.Assert(request, qt.IsNil)
	})
	t.Run("declaration without a position", func(t *testing.T) {
		c := qt.New(t)
		tables, declarations := captureGo(c, goSource)
		declarations[0].Start, declarations[0].End = annotation.SourcePosition{}, annotation.SourcePosition{}
		request, err := providersource.GoAnnotations(c.Context(), goEngine(), tables, declarations)
		c.Assert(err, qt.ErrorMatches, `Go declaration "ptah:test:retention" carries no usable source position`)
		c.Assert(request, qt.IsNil)
	})
	t.Run("position outside the file", func(t *testing.T) {
		c := qt.New(t)
		tables, declarations := captureGo(c, goSource)
		declarations[0].FileSize = declarations[0].End.Offset - 1
		request, err := providersource.GoAnnotations(c.Context(), goEngine(), tables, declarations)
		c.Assert(err, qt.ErrorMatches, `Go declaration "ptah:test:retention" carries no usable source position`)
		c.Assert(request, qt.IsNil)
	})
	t.Run("declarations of two files", func(t *testing.T) {
		c := qt.New(t)
		tables, declarations := captureGo(c, goSource)
		other := declarations[0]
		other.File = "other.go"
		request, err := providersource.GoAnnotations(c.Context(), goEngine(), tables, []annotation.Declaration{declarations[0], other})
		c.Assert(err, qt.ErrorMatches, `Go declarations of "records.go" and "other.go" cannot share a document`)
		c.Assert(request, qt.IsNil)
	})
	t.Run("canceled", func(t *testing.T) {
		c := qt.New(t)
		tables, declarations := captureGo(c, goSource)
		ctx, cancel := context.WithCancel(c.Context())
		cancel()
		request, err := providersource.GoAnnotations(ctx, goEngine(), tables, declarations)
		c.Assert(err, qt.ErrorIs, context.Canceled)
		c.Assert(request, qt.IsNil)
	})
}

func TestGoClaims_HappyPath(t *testing.T) {
	c := qt.New(t)
	claims, err := providersource.GoClaims([]*protocol.Engine{goEngine(), {Id: "plain"}})
	c.Assert(err, qt.IsNil)
	c.Assert(claims, qt.HasLen, 2)
	c.Assert(claims["ptah:test:retention"].Engine.GetId(), qt.Equals, "example")
	c.Assert(claims["ptah:test:node"].Directive.GetScope(), qt.Equals, protocol.GoSourceDirective_DOCUMENT)
}

func TestGoClaims_FailurePath(t *testing.T) {
	second := proto.CloneOf(goEngine())
	second.Id = "second"
	column := proto.CloneOf(goEngine())
	column.GoSourceDirectives[0].Scope = protocol.GoSourceDirective_COLUMN
	index := proto.CloneOf(goEngine())
	index.GoSourceDirectives[0].Scope = protocol.GoSourceDirective_INDEX
	for _, row := range []struct {
		name    string
		engines []*protocol.Engine
		want    string
	}{
		{"one name in two engines", []*protocol.Engine{goEngine(), second}, `Go directive "ptah:test.*" is claimed by engines "example" and "second"; a directive name has one owner`},
		{"column scope", []*protocol.Engine{column}, `Go directive "ptah:test:retention" of engine "example" has scope COLUMN, which the host does not read.*`},
		{"index scope", []*protocol.Engine{index}, `Go directive "ptah:test:retention" of engine "example" has scope INDEX, which the host does not read.*`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			claims, err := providersource.GoClaims(row.engines)
			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(claims, qt.IsNil)
		})
	}
}
