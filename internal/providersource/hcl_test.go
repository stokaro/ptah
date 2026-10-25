package providersource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/internal/providersource"
)

func hclEngine() *protocol.Engine {
	text := []*protocol.HCLSourceAttribute{{Name: "seconds", Kind: protocol.HCLSourceAttribute_TEXT}}
	models := []string{retentionModel}
	return &protocol.Engine{Id: "example", ValueModels: []*protocol.ValueModel{{Kind: retentionModel, Representation: protocol.ValueInput_DESIRED,
		SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_HCL}}},
		HclSource: &protocol.HCLSourceDeclaration{HeaderKinds: []string{"node"}, Blocks: []*protocol.HCLSourceBlock{
			{Site: protocol.HCLSourceBlock_TOP_LEVEL, Type: "node", Naming: protocol.HCLSourceBlock_SCHEMA_AND_NAME, Selector: "node", Models: models,
				Attributes: []*protocol.HCLSourceAttribute{{Name: "seconds", Kind: protocol.HCLSourceAttribute_TEXT}, {Name: "on", Kind: protocol.HCLSourceAttribute_REFERENCE, Refers: "table"},
					{Name: "tags", Kind: protocol.HCLSourceAttribute_TEXT_LIST}, {Name: "enabled", Kind: protocol.HCLSourceAttribute_BOOL}, {Name: "check", Kind: protocol.HCLSourceAttribute_SQL}},
				Children: []*protocol.HCLSourceChild{{Type: "part", Attributes: text}}},
			{Site: protocol.HCLSourceBlock_IN_TABLE, Type: "retention", Selector: "retention", Models: models, Attributes: text},
			{Site: protocol.HCLSourceBlock_INDEX_ATTRIBUTES, Type: "index", Selector: "index", Models: models, Attributes: text, Inherited: []string{"type"}},
		}}}
}

func hclRange(start, end int) hclext.Range {
	return hclext.Range{Filename: "schema.hcl", Start: hclext.Position{Byte: start, Line: 1, Column: start + 1}, End: hclext.Position{Byte: end, Line: 1, Column: end + 1}}
}

// TestHCL_HappyPath sends each claim as its site needs: a top-level block with
// its labels, children and every value kind, a block in a table bound to the
// table, and index attributes bound to the table with the index as the
// subject and its type inherited, and the header entries of the engine's
// kinds as limits.
func TestHCL_HappyPath(t *testing.T) {
	c := qt.New(t)
	claims := []hclext.Claim{
		{Block: "node", Schema: "app", Name: "n1", Range: hclRange(0, 40), Attributes: map[string]hclext.Value{
			"seconds": {Kind: hclext.Text, Text: "5"}, "on": {Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "table", Schema: "app", Name: "items"}},
			"tags": {Kind: hclext.TextList, Texts: []string{"a", "b"}}, "enabled": {Kind: hclext.Bool, Bool: true}, "check": {Kind: hclext.SQL, Text: "x > 0"},
		}, Children: []hclext.ClaimChild{{Type: "part", Name: "p1", Range: hclRange(20, 30), Attributes: map[string]hclext.Value{"seconds": {Kind: hclext.Text, Text: "1"}}}}},
		{Block: "retention", Range: hclRange(50, 60), Table: &hclext.Table{Schema: "app", Name: "items"}, Attributes: map[string]hclext.Value{"seconds": {Kind: hclext.Text, Text: "9"}}},
		{Block: "index", Name: "by_id", Range: hclRange(70, 90), Index: &hclext.Index{Table: "app.items", Type: "BTREE"}},
	}
	limits := []coverage.Object{{Kind: "node", Name: "n2", Reason: coverage.NotInspected, Provenance: coverage.Declared}, {Kind: "view"}}

	request, err := providersource.HCL(c.Context(), hclEngine(), claims, limits)

	c.Assert(err, qt.IsNil)
	document := request.GetDocuments()[0]
	c.Assert(document.GetFile(), qt.Equals, "schema.hcl")
	c.Assert(document.GetByteLength(), qt.Equals, uint64(90))
	c.Assert(document.GetLimits(), qt.CmpEquals(protocmp.Transform()), []*protocol.SchemaCoverageLimit{{Kind: "node", Name: "n2", Reason: string(coverage.NotInspected), Provenance: string(coverage.Declared)}})
	c.Assert(document.GetBindings(), qt.CmpEquals(protocmp.Transform()), []*protocol.SourceBinding{{Id: "table-0", Object: &protocol.Reference{Engine: "example", Kind: "table", Schema: "app", Name: "items"}}})
	top := document.GetFragments()[0].GetBlock()
	c.Assert(top.GetLabels()[0].GetValue(), qt.Equals, "app")
	c.Assert(top.GetLabels()[1].GetValue(), qt.Equals, "n1")
	keys := make([]string, 0, 5)
	for _, entry := range top.GetAttributes().GetEntries() {
		keys = append(keys, entry.GetKey())
	}
	c.Assert(keys, qt.DeepEquals, []string{"seconds", "on", "tags", "enabled", "check"})
	c.Assert(top.GetAttributes().GetEntries()[1].GetValue().GetReference(), qt.CmpEquals(protocmp.Transform()), &protocol.Reference{Engine: "example", Kind: "table", Schema: "app", Name: "items"})
	c.Assert(top.GetAttributes().GetEntries()[2].GetValue().GetSequence().GetValues(), qt.HasLen, 2)
	c.Assert(top.GetAttributes().GetEntries()[3].GetValue().GetBoolean(), qt.IsTrue)
	c.Assert(top.GetAttributes().GetEntries()[4].GetValue().GetSql(), qt.Equals, "x > 0")
	c.Assert(top.GetChildren()[0].GetLabels()[0].GetValue(), qt.Equals, "p1")
	c.Assert(top.GetChildren()[0].GetSpan().GetStart().GetByteOffset(), qt.Equals, uint64(20))
	c.Assert(document.GetFragments()[0].GetBindingId(), qt.Equals, "")
	c.Assert(document.GetFragments()[1].GetBindingId(), qt.Equals, "table-0")
	index := document.GetFragments()[2]
	c.Assert(index.GetBindingId(), qt.Equals, "table-0")
	c.Assert(index.GetSubject(), qt.CmpEquals(protocmp.Transform()), &protocol.Reference{Engine: "example", Kind: "index", Schema: "app", Path: []string{"items"}, Name: "by_id"})
	c.Assert(index.GetInheritedAttributes().GetEntries()[0].GetValue().GetText(), qt.Equals, "BTREE")
	c.Assert(index.GetValue().GetMapping().GetEntries(), qt.HasLen, 0)
}

// TestHCL_FailurePath refuses a claim of a block the engine does not claim,
// an attribute outside the claim, and a block without a usable position.
func TestHCL_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name  string
		claim hclext.Claim
		want  string
	}{
		{"a block the engine does not claim", hclext.Claim{Block: "other", At: "schema.hcl:1", Range: hclRange(0, 10)},
			`HCL block "other" at schema.hcl:1: engine "example" claims no such block`},
		{"an attribute outside the claim", hclext.Claim{Block: "retention", At: "schema.hcl:1", Range: hclRange(0, 10), Table: &hclext.Table{Name: "items"},
			Attributes: map[string]hclext.Value{"minutes": {Kind: hclext.Text, Text: "1"}}},
			`HCL block "retention" at schema.hcl:1: attribute "minutes" is not in the claim`},
		{"no position", hclext.Claim{Block: "retention", At: "schema.hcl:1", Table: &hclext.Table{Name: "items"}},
			`HCL block "retention" at schema.hcl:1: HCL block carries no usable source position`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			request, err := providersource.HCL(c.Context(), hclEngine(), []hclext.Claim{row.claim}, nil)

			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(request, qt.IsNil)
		})
	}
}

// TestCheckHCLSource_FailurePath refuses a claim the host cannot read: an
// unknown site, naming or attribute kind, and an inherited attribute the
// frontend does not hand an owner.
func TestCheckHCLSource_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name  string
		block *protocol.HCLSourceBlock
		want  string
	}{
		{"an unknown site", &protocol.HCLSourceBlock{Type: "node", Site: protocol.HCLSourceBlock_SITE_UNSPECIFIED},
			`HCL block "node" of engine "example" stands at site SITE_UNSPECIFIED, which the host does not read`},
		{"an unknown naming", &protocol.HCLSourceBlock{Type: "node", Site: protocol.HCLSourceBlock_TOP_LEVEL, Naming: protocol.HCLSourceBlock_Naming(99)},
			`HCL block "node" of engine "example" has naming 99, which the host does not read`},
		{"an unknown attribute kind", &protocol.HCLSourceBlock{Type: "node", Site: protocol.HCLSourceBlock_TOP_LEVEL, Naming: protocol.HCLSourceBlock_UNLABELED,
			Attributes: []*protocol.HCLSourceAttribute{{Name: "x"}}}, `HCL attribute "x" of block "node" of engine "example" has kind KIND_UNSPECIFIED, which the host does not read`},
		{"an inherited attribute of a table", &protocol.HCLSourceBlock{Type: "table", Site: protocol.HCLSourceBlock_TABLE_ATTRIBUTES, Inherited: []string{"comment"}},
			`HCL block "table" of engine "example" inherits "comment", which the frontend does not hand an owner at site TABLE_ATTRIBUTES`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			engine := hclEngine()
			engine.HclSource.Blocks = []*protocol.HCLSourceBlock{row.block}

			err := providersource.CheckHCLSource(engine)

			c.Assert(err, qt.ErrorMatches, row.want)
		})
	}
}
