package providerhost

// White-box testing required: the HCL encoder turns the provider's fragments
// into the writer's blocks in private functions. The fixture writes one block
// and one attribute through the CLI; each value kind, label shape and loss
// placement is only observable here.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/hclext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

func TestHCLEncodedValue_HappyPath(t *testing.T) {
	text := func(value string) *protocol.SourceValue {
		return &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: value}}
	}
	reference := func(name string) *protocol.SourceValue {
		return &protocol.SourceValue{Value: &protocol.SourceValue_Reference{Reference: &protocol.Reference{Kind: "table", Schema: "app", Name: name}}}
	}
	sequence := func(values ...*protocol.SourceValue) *protocol.SourceValue {
		return &protocol.SourceValue{Value: &protocol.SourceValue_Sequence{Sequence: &protocol.SourceSequence{Values: values}}}
	}
	for _, row := range []struct {
		name  string
		kind  hclext.Kind
		value *protocol.SourceValue
		want  hclext.Value
	}{
		{"text as a string", hclext.Text, text("5"), hclext.Value{Kind: hclext.String, Text: "5"}},
		{"a type name as a string", hclext.TypeName, text("uint64"), hclext.Value{Kind: hclext.String, Text: "uint64"}},
		{"sql", hclext.SQL, &protocol.SourceValue{Value: &protocol.SourceValue_Sql{Sql: "x > 0"}}, hclext.Value{Kind: hclext.SQL, Text: "x > 0"}},
		{"a bool", hclext.Bool, &protocol.SourceValue{Value: &protocol.SourceValue_Boolean{Boolean: true}}, hclext.Value{Kind: hclext.Bool, Bool: true}},
		{"a reference", hclext.Ref, reference("items"), hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: "table", Schema: "app", Name: "items"}}},
		{"a text list", hclext.TextList, sequence(text("a"), text("b")), hclext.Value{Kind: hclext.TextList, Texts: []string{"a", "b"}}},
		{"references", hclext.Refs, sequence(reference("items")), hclext.Value{Kind: hclext.Refs, References: []hclext.Reference{{Kind: "table", Schema: "app", Name: "items"}}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := hclEncodedValue(row.kind, row.value)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, row.want)
		})
	}
}

func groupedNativeHCL() (*hclEncoding, *protocol.EncodedSource) {
	const kind = "example.org/fixture/node"
	encoding := &hclEncoding{batch: &encodeBatch{request: &protocol.EncodeSourceRequest{}}, objects: make(map[string]schemaext.Object)}
	group := &protocol.EncodedSourceGroup{}
	for _, name := range []string{"first", "second"} {
		subject := &protocol.Reference{Engine: "fixture", Kind: kind, Schema: "app", Name: name}
		encoding.batch.request.Values = append(encoding.batch.request.Values, &protocol.EncodeSourceValue{Value: &protocol.NormalizedValue{Id: name,
			Value: &protocol.Envelope{Kind: kind, Owner: proto.CloneOf(subject)}, NormalizedOwner: proto.CloneOf(subject)}})
		encoding.objects[name] = schemaext.Object{Ref: objectidentity.ID{Kind: kind, Schema: objectidentity.Part{Source: "app", Normalized: "app"},
			Name: objectidentity.Part{Source: name, Normalized: name}}, Targets: []string{name + "-engine"}}
		group.ValueIds = append(group.ValueIds, name)
		group.Parts = append(group.Parts, &protocol.EncodedSourcePart{Placement: protocol.EncodedFragment_AFTER_TABLES, Fragment: &protocol.SourceFragment{
			Subject: proto.CloneOf(subject), HclBlock: &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_TOP_LEVEL, Type: "node",
				Naming: protocol.HCLSourceBlock_SCHEMA_AND_NAME, Models: []string{kind}},
			Content: &protocol.SourceFragment_Block{Block: &protocol.SourceBlock{Type: "node", Labels: []*protocol.SourceLabel{{Value: "app"}, {Value: name}}}}}})
	}
	return encoding, &protocol.EncodedSource{Complete: true, Groups: []*protocol.EncodedSourceGroup{group}}
}

func TestHCLGroupsKeepEachNativeIdentityAndTargetScope(t *testing.T) {
	c := qt.New(t)
	encoding, reply := groupedNativeHCL()
	parts := encoding.parts(reply)
	c.Assert(parts, qt.HasLen, 2)
	for i, name := range []string{"first", "second"} {
		c.Assert(parts[i].GetValueIds(), qt.DeepEquals, []string{name})
		block, err := encoding.block(parts[i])
		c.Assert(err, qt.IsNil)
		c.Assert(block.Object, qt.DeepEquals, encoding.objects[name].Ref)
		c.Assert(block.Targets, qt.DeepEquals, encoding.objects[name].Targets)
	}
}

func TestHCLGroupRefusesOneBlockWithSeveralNativeIdentities(t *testing.T) {
	c := qt.New(t)
	encoding, reply := groupedNativeHCL()
	reply.Groups[0].Parts[0].Fragment.Subject = nil
	parts := encoding.parts(reply)
	block, err := encoding.block(parts[0])
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(block, qt.DeepEquals, hclext.EncodedBlock{})
}

func TestHCLGroupRefusesConflictingTargetsForOneIdentity(t *testing.T) {
	c := qt.New(t)
	encoding, reply := groupedNativeHCL()
	second := encoding.objects["second"]
	second.Ref = encoding.objects["first"].Ref
	encoding.objects["second"] = second
	reply.Groups[0].Parts[0].Fragment.Subject = nil
	parts := encoding.parts(reply)
	block, err := encoding.block(parts[0])
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(block, qt.DeepEquals, hclext.EncodedBlock{})
	second.Targets = encoding.objects["first"].Targets
	encoding.objects["second"] = second
	block, err = encoding.block(parts[0])
	c.Assert(err, qt.IsNil)
	c.Assert(block.Object, qt.DeepEquals, second.Ref)
	c.Assert(block.Targets, qt.DeepEquals, second.Targets)
}

// TestHCLEncodedValue_FailurePath refuses a value the claim does not read as
// its attribute's kind.
func TestHCLEncodedValue_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name  string
		kind  hclext.Kind
		value *protocol.SourceValue
	}{
		{"text for a bool", hclext.Bool, &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: "true"}}},
		{"a number", hclext.Text, &protocol.SourceValue{Value: &protocol.SourceValue_Number{Number: "5"}}},
		{"a reference in a text list", hclext.TextList, &protocol.SourceValue{Value: &protocol.SourceValue_Sequence{Sequence: &protocol.SourceSequence{
			Values: []*protocol.SourceValue{{Value: &protocol.SourceValue_Reference{Reference: &protocol.Reference{Name: "x"}}}}}}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := hclEncodedValue(row.kind, row.value)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got, qt.DeepEquals, hclext.Value{})
		})
	}
}

// TestHCLEncodingLosses reports a facet's loss at the writer's block that
// holds it and an object's as an unwritten block at the object.
func TestHCLEncodingLosses(t *testing.T) {
	c := qt.New(t)
	index := hclext.Slot{Kind: hclext.SlotIndex, Table: hclext.Table{Name: "items"}, Name: "by_id"}
	node := objectidentity.ID{Kind: "node", Name: objectidentity.Part{Source: "n1", Normalized: "n1"}}
	encoding := &hclEncoding{slots: map[string]hclext.Slot{"value-0": index}, objects: map[string]schemaext.Object{"value-1": {Ref: node, Targets: []string{"fixture"}}},
		losses: make(map[hclext.Slot][]string)}

	encoding.loss("value-0", "a hash index keeps no retention")
	encoding.loss("value-1", "a node has no HCL spelling")
	result := encoding.finish()

	c.Assert(result.Attributes, qt.DeepEquals, []hclext.SlotAttributes{{Slot: index, Losses: []string{"a hash index keeps no retention"}}})
	c.Assert(result.Blocks, qt.DeepEquals, []hclext.EncodedBlock{{Type: "node", Placement: hclext.AfterTables, Name: "n1", Path: "node.n1",
		Losses: []string{"a node has no HCL spelling"}, Unwritten: true, Object: node, Targets: []string{"fixture"}}})
}

// TestHCLEncodingBlock names a top-level block by its labels and places a
// block in a table by the table, without a label.
func TestHCLEncodingBlock(t *testing.T) {
	seconds := []*protocol.HCLSourceAttribute{{Name: "seconds", Kind: protocol.HCLSourceAttribute_TEXT}}
	attributes := &protocol.SourceMapping{Entries: []*protocol.SourceEntry{{Key: "seconds", Value: &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: "5"}}}}}
	table := hclext.Table{Schema: "app", Name: "items"}
	for _, row := range []struct {
		name    string
		encoded *protocol.EncodedFragment
		want    hclext.EncodedBlock
	}{
		{"a top-level block", &protocol.EncodedFragment{Placement: protocol.EncodedFragment_AFTER_TABLES, Fragment: &protocol.SourceFragment{
			HclBlock: &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_TOP_LEVEL, Type: "node", Naming: protocol.HCLSourceBlock_SCHEMA_AND_NAME, Attributes: seconds},
			Content: &protocol.SourceFragment_Block{Block: &protocol.SourceBlock{Type: "node", Attributes: attributes,
				Labels: []*protocol.SourceLabel{{Value: "app"}, {Value: "n1"}}}}}},
			hclext.EncodedBlock{Type: "node", Placement: hclext.AfterTables, Schema: "app", Name: "n1", Path: "node.n1",
				Attributes: []hclext.EncodedAttribute{{Name: "seconds", Value: hclext.Value{Kind: hclext.String, Text: "5"}}}}},
		{"a block in a table", &protocol.EncodedFragment{Placement: protocol.EncodedFragment_WITHIN_TABLE, Fragment: &protocol.SourceFragment{BindingId: "table-0",
			HclBlock: &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_IN_TABLE, Type: "retention", Attributes: seconds},
			Content:  &protocol.SourceFragment_Block{Block: &protocol.SourceBlock{Type: "retention", Attributes: attributes}}}},
			hclext.EncodedBlock{Type: "retention", Placement: hclext.WithinTable, Table: &table, Path: "tables.items.retention",
				Attributes: []hclext.EncodedAttribute{{Name: "seconds", Value: hclext.Value{Kind: hclext.String, Text: "5"}}}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			encoding := &hclEncoding{tables: map[string]hclext.Table{"table-0": table}, objects: make(map[string]schemaext.Object)}

			block, err := encoding.block(row.encoded)

			c.Assert(err, qt.IsNil)
			c.Assert(block, qt.DeepEquals, row.want)
		})
	}
}
