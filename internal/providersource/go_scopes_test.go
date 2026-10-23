package providersource_test

import (
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

// scopedGoSource declares one table, one column and one index, each carrying a
// directive of the owner and an attribute the owner adds to the frontend's own
// directive.
const scopedGoSource = `package p

//ptah:schema:table name="records" schema="app" ttl=30
type Record struct {
	//ptah:schema:field name="id" type="INTEGER" family=hot
	//ptah:test:column codec=zstd
	ID int

	//ptah:schema:index name="by_id" fields="id" bloom=on
	//ptah:test:index depth=2
	_ int
}

//ptah:schema:table name="other"
type Other struct {
	//ptah:schema:index name="by_other" fields="id" table="records"
	//ptah:test:index depth=3
	_ int
}
`

func scopedEngine() *protocol.Engine {
	models := []string{retentionModel}
	engine := goEngine()
	engine.GoSourceDirectives = []*protocol.GoSourceDirective{
		{Scope: protocol.GoSourceDirective_COLUMN, Directive: "ptah:test:column", Selector: "column", Attributes: []string{"codec"}, Models: models},
		{Scope: protocol.GoSourceDirective_INDEX, Directive: "ptah:test:index", Selector: "index", Attributes: []string{"depth"}, Models: models},
		{Scope: protocol.GoSourceDirective_TABLE, Directive: "ptah:schema:table", Selector: "table", Attributes: []string{"ttl"}, Models: models},
		{Scope: protocol.GoSourceDirective_COLUMN, Directive: "ptah:schema:field", Selector: "field", Attributes: []string{"family"}, Models: models},
		{Scope: protocol.GoSourceDirective_INDEX, Directive: "ptah:schema:index", Selector: "index", Attributes: []string{"bloom"}, Models: models},
	}
	return engine
}

func captureScopedGo(c *qt.C, source string) (annotation.Tables, []annotation.Declaration) {
	c.Helper()
	var (
		declarations []annotation.Declaration
		tables       annotation.Tables
	)
	capture := func() annotation.FileDecoder { return goCapture{declarations: &declarations, tables: &tables} }
	attribute := func(name string) []annotation.Attribute { return []annotation.Attribute{{Name: name, Value: "string"}} }
	owners, err := annotation.NewSet(annotation.Extension{Owner: "example.org/test",
		Directives: []annotation.Directive{
			{Name: "ptah:test:column", Scopes: []annotation.Scope{annotation.ScopeField}, Describes: annotation.SubjectColumn, Attributes: attribute("codec")},
			{Name: "ptah:test:index", Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField}, Describes: annotation.SubjectIndex, Attributes: attribute("depth")},
		},
		Attributes: []annotation.DirectiveAttributes{
			{Directive: "ptah:schema:table", Attributes: attribute("ttl"), PerFile: true},
			{Directive: "ptah:schema:field", Attributes: attribute("family"), PerFile: true},
			{Directive: "ptah:schema:index", Attributes: attribute("bloom"), PerFile: true},
		},
		Kinds:    []schemaext.Kind{retentionModel},
		File:     capture,
		Coverage: func([]coverage.Object) (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }})
	c.Assert(err, qt.IsNil)
	_, err = goschema.ParseSource(owners, "records.go", source)
	c.Assert(err, qt.IsNil)
	return tables, declarations
}

func TestGoAnnotations_ScopedFragments(t *testing.T) {
	c := qt.New(t)
	tables, declarations := captureScopedGo(c, scopedGoSource)
	request, err := providersource.GoAnnotations(c.Context(), scopedEngine(), tables, declarations)
	c.Assert(err, qt.IsNil)
	document := request.GetDocuments()[0]
	c.Assert(document.GetBindings(), qt.HasLen, 2)
	// The table Other declares no attribute of the owner, and neither does its
	// index directive, so only the owner's own index directive reaches the
	// provider, bound to the table the index names.
	c.Assert(document.GetFragments(), qt.HasLen, 6)
	records := document.GetBindings()[0].GetId()
	for i, row := range []struct {
		directive, attribute, value, kind, name string
		path                                    []string
	}{
		{"ptah:schema:table", "ttl", "30", "", "", nil},
		{"ptah:schema:field", "family", "hot", "column", "id", []string{"records"}},
		{"ptah:test:column", "codec", "zstd", "column", "id", []string{"records"}},
		{"ptah:schema:index", "bloom", "on", "index", "by_id", []string{"records"}},
		{"ptah:test:index", "depth", "2", "index", "by_id", []string{"records"}},
		{"ptah:test:index", "depth", "3", "index", "by_other", []string{"records"}},
	} {
		fragment := document.GetFragments()[i]
		entry := fragment.GetValue().GetMapping().GetEntries()[0]
		note := qt.Commentf("fragment %d", i)
		c.Assert(fragment.GetGoDirective().GetDirective(), qt.Equals, row.directive, note)
		c.Assert(fragment.GetBindingId(), qt.Equals, records, note)
		c.Assert(fragment.GetSubject().GetKind(), qt.Equals, row.kind, note)
		c.Assert(fragment.GetSubject().GetName(), qt.Equals, row.name, note)
		c.Assert(fragment.GetSubject().GetPath(), qt.DeepEquals, row.path, note)
		c.Assert(fragment.GetSubject().GetEngine(), qt.Equals, map[bool]string{true: "example", false: ""}[row.kind != ""], note)
		c.Assert([]string{entry.GetKey(), entry.GetValue().GetText()}, qt.DeepEquals, []string{row.attribute, row.value}, note)
	}
	c.Assert(document.GetFragments()[2].GetSubject().GetSchema(), qt.Equals, "app")
}

func TestGoAnnotations_ScopedFragmentsOnlyCarryTheOwnersAttributes(t *testing.T) {
	c := qt.New(t)
	tables, declarations := captureScopedGo(c, scopedGoSource)
	c.Assert(declarations[1].Directive, qt.Equals, "ptah:schema:field")
	c.Assert(declarations[1].Attributes, qt.DeepEquals, map[string]string{"family": "hot"})
	c.Assert(declarations[1].Subject, qt.Equals, annotation.Subject{Kind: annotation.SubjectColumn, Struct: "Record", Name: "id"})
	c.Assert(declarations[5].Subject, qt.Equals, annotation.Subject{Kind: annotation.SubjectIndex, Struct: "Other", Name: "by_other", Table: "records"})
	c.Assert(tables, qt.HasLen, 2)
}

func TestGoSubject_FailurePath(t *testing.T) {
	tables, declarations := captureScopedGo(qt.New(t), scopedGoSource)
	engine := scopedEngine()
	column := engine.GetGoSourceDirectives()[0]
	index := engine.GetGoSourceDirectives()[1]
	for _, row := range []struct {
		name   string
		claim  *protocol.GoSourceDirective
		mutate func(*annotation.Declaration)
		want   string
	}{
		{"column directive without a column", column, func(d *annotation.Declaration) { d.Subject = annotation.Subject{} },
			`Go declaration "ptah:test:column" carries no column it describes`},
		{"index directive describing a column", index, func(d *annotation.Declaration) { d.Subject.Kind = annotation.SubjectColumn },
			`Go declaration "ptah:test:column" carries no index it describes`},
		{"column of a struct without a table", column, func(d *annotation.Declaration) { d.Struct = "Missing" },
			`struct Missing maps to no table in this file.*`},
		{"index naming a table the file lacks", index, func(d *annotation.Declaration) { d.Subject.Kind, d.Subject.Table = annotation.SubjectIndex, "absent" },
			`table "absent" is not declared in this file.*`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			declaration := declarations[2]
			row.mutate(&declaration)
			table, subject, err := providersource.GoSubject("example", tables, row.claim, declaration)
			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(table, qt.Equals, -1)
			c.Assert(subject, qt.IsNil)
		})
	}
}

func TestSameSubject(t *testing.T) {
	c := qt.New(t)
	want := &protocol.Reference{Engine: "example", Kind: "column", Schema: "app", Path: []string{"records"}, Name: "id"}
	moved := proto.CloneOf(want)
	moved.Name = "other"
	deeper := proto.CloneOf(want)
	deeper.Path = []string{"records", "x"}
	foreign := proto.CloneOf(want)
	foreign.Kind, foreign.Engine, foreign.DisplayName = "index", "other", "shown"
	renamed := proto.CloneOf(want)
	renamed.Engine, renamed.DisplayName = "elsewhere", "shown"
	c.Assert(providersource.SameSubject(renamed, want), qt.IsTrue, qt.Commentf("the engine and display name are not identity"))
	c.Assert(providersource.SameSubject(moved, want), qt.IsFalse)
	c.Assert(providersource.SameSubject(deeper, want), qt.IsFalse)
	c.Assert(providersource.SameSubject(foreign, want), qt.IsFalse)
	c.Assert(providersource.SameSubject(nil, want), qt.IsFalse)
	c.Assert(providersource.SameSubject(nil, nil), qt.IsTrue)
}

func TestGoClaims_ScopedHappyPath(t *testing.T) {
	c := qt.New(t)
	claims, err := providersource.GoClaims([]*protocol.Engine{scopedEngine()})
	c.Assert(err, qt.IsNil)
	c.Assert(claims, qt.HasLen, 5)
	c.Assert(claims["ptah:test:column"].Attributes, qt.IsFalse)
	c.Assert(claims["ptah:schema:field"].Attributes, qt.IsTrue)
	c.Assert(claims["ptah:schema:index"].Directive.GetScope(), qt.Equals, protocol.GoSourceDirective_INDEX)
}
