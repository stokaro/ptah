package providersource

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/internal/tableref"
)

// HCLSites maps each site of a provider's HCL claim onto the frontend's.
var HCLSites = map[protocol.HCLSourceBlock_Site]hclext.Site{
	protocol.HCLSourceBlock_TOP_LEVEL:                    hclext.TopLevel,
	protocol.HCLSourceBlock_IN_TABLE:                     hclext.InTable,
	protocol.HCLSourceBlock_TABLE_ATTRIBUTES:             hclext.TableAttributes,
	protocol.HCLSourceBlock_INDEX_ATTRIBUTES:             hclext.IndexAttributes,
	protocol.HCLSourceBlock_ROUTINE_ATTRIBUTES:           hclext.RoutineAttributes,
	protocol.HCLSourceBlock_MATERIALIZED_VIEW_ATTRIBUTES: hclext.MaterializedViewAttributes,
}

// HCLNamings maps each naming of a provider's top-level block onto the
// frontend's.
var HCLNamings = map[protocol.HCLSourceBlock_Naming]hclext.Naming{
	protocol.HCLSourceBlock_UNLABELED:                 hclext.Unlabeled,
	protocol.HCLSourceBlock_SCHEMA_AND_NAME:           hclext.SchemaAndName,
	protocol.HCLSourceBlock_SCHEMA_AND_QUALIFIED_NAME: hclext.SchemaAndQualifiedName,
	protocol.HCLSourceBlock_JOINED_NAME:               hclext.JoinedName,
}

// HCLKinds maps each attribute kind of a provider's claim onto the
// frontend's.
var HCLKinds = map[protocol.HCLSourceAttribute_Kind]hclext.Kind{
	protocol.HCLSourceAttribute_STRING:     hclext.String,
	protocol.HCLSourceAttribute_TEXT:       hclext.Text,
	protocol.HCLSourceAttribute_TEXT_LIST:  hclext.TextList,
	protocol.HCLSourceAttribute_BOOL:       hclext.Bool,
	protocol.HCLSourceAttribute_REFERENCE:  hclext.Ref,
	protocol.HCLSourceAttribute_REFERENCES: hclext.Refs,
	protocol.HCLSourceAttribute_SQL:        hclext.SQL,
	protocol.HCLSourceAttribute_TYPE_NAME:  hclext.TypeName,
}

// HCLInherited are the attributes of the frontend's own block a claim at an
// attribute site can inherit, by site: what the frontend hands an owner of
// the block it read. An index's operator class is its ops.
var HCLInherited = map[protocol.HCLSourceBlock_Site][]string{
	protocol.HCLSourceBlock_INDEX_ATTRIBUTES: {"type", "ops"},
}

// CheckHCLSource refuses an HCL claim the host cannot read: a site, a naming
// or an attribute kind it has no name for, and an inherited attribute the
// frontend does not hand an owner at the claim's site.
func CheckHCLSource(engine *protocol.Engine) error {
	for _, block := range engine.GetHclSource().GetBlocks() {
		if _, known := HCLSites[block.GetSite()]; !known {
			return fmt.Errorf("HCL block %q of engine %q stands at site %s, which the host does not read", block.GetType(), engine.GetId(), block.GetSite())
		}
		if _, known := HCLNamings[block.GetNaming()]; !known && block.GetNaming() != protocol.HCLSourceBlock_NAMING_UNSPECIFIED {
			return fmt.Errorf("HCL block %q of engine %q has naming %s, which the host does not read", block.GetType(), engine.GetId(), block.GetNaming())
		}
		attributes := slices.Clone(block.GetAttributes())
		for _, child := range block.GetChildren() {
			attributes = append(attributes, child.GetAttributes()...)
		}
		for _, attribute := range attributes {
			if _, known := HCLKinds[attribute.GetKind()]; !known {
				return fmt.Errorf("HCL attribute %q of block %q of engine %q has kind %s, which the host does not read", attribute.GetName(), block.GetType(), engine.GetId(), attribute.GetKind())
			}
		}
		for _, name := range block.GetInherited() {
			if !slices.Contains(HCLInherited[block.GetSite()], name) {
				return fmt.Errorf("HCL block %q of engine %q inherits %q, which the frontend does not hand an owner at site %s", block.GetType(), engine.GetId(), name, block.GetSite())
			}
		}
	}
	return nil
}

// HCLClaimSite is the site of a claim the frontend made: an index's
// attributes, a table's, a block in a table, a routine's attributes, a
// materialized view's, or a top-level block. A provider's own block never
// takes the type of one of the frontend's, so the type and what the claim is
// about tell them apart.
func HCLClaimSite(claim hclext.Claim) protocol.HCLSourceBlock_Site {
	switch {
	case claim.Index != nil:
		return protocol.HCLSourceBlock_INDEX_ATTRIBUTES
	case claim.Table != nil && claim.Block == "table":
		return protocol.HCLSourceBlock_TABLE_ATTRIBUTES
	case claim.Table != nil:
		return protocol.HCLSourceBlock_IN_TABLE
	case claim.Block == "function" || claim.Block == "procedure":
		return protocol.HCLSourceBlock_ROUTINE_ATTRIBUTES
	case claim.Block == "materialized":
		return protocol.HCLSourceBlock_MATERIALIZED_VIEW_ATTRIBUTES
	}
	return protocol.HCLSourceBlock_TOP_LEVEL
}

// HCL captures the claims of one engine's blocks in one HCL document as a
// document of that format, with the header entries of the engine's header
// kinds as its limits. A fragment carries the frozen claim, its content as the
// frontend evaluated it, a block at the sites that read the owner's own block
// and an attribute mapping at the others, and what its site attaches to: the
// table of a block in a table and of table attributes, the table and the index
// of index attributes, the routine or the view of their attributes, and
// nothing at the top level. The frontend records where a block stands, not
// each attribute, so every span is the block's. The document's length is the
// end of its last claimed block, which every span lies within. The caller
// validates the request against the negotiated description.
func HCL(ctx context.Context, engine *protocol.Engine, claims []hclext.Claim, limits []coverage.Object) (*protocol.DecodeSourceRequest, error) {
	document := &protocol.SourceDocument{Id: "document", Format: protocol.SourceDocument_HCL, File: "<unnamed HCL>"}
	for _, model := range engine.GetValueModels() {
		if model.GetRepresentation() == protocol.ValueInput_DESIRED && slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_HCL) {
			document.Models = append(document.Models, proto.CloneOf(model))
		}
	}
	for _, limit := range limits {
		if slices.Contains(engine.GetHclSource().GetHeaderKinds(), string(limit.Kind)) {
			document.Limits = append(document.Limits, &protocol.SchemaCoverageLimit{Kind: string(limit.Kind), Name: limit.Name, Reason: string(limit.Reason),
				Provenance: string(limit.Provenance)})
		}
	}
	bindings := make(map[[2]string]string)
	bind := func(schema, name string) string {
		key := [2]string{schema, name}
		if id, bound := bindings[key]; bound {
			return id
		}
		id := "table-" + strconv.Itoa(len(bindings))
		bindings[key] = id
		document.Bindings = append(document.Bindings, &protocol.SourceBinding{Id: id, Object: &protocol.Reference{Engine: engine.GetId(), Kind: "table", Schema: schema, Name: name}})
		return id
	}
	for i, claim := range claims {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fragment, err := hclFragment(engine, claim, bind)
		if err != nil {
			return nil, fmt.Errorf("HCL block %q at %s: %w", claim.Block, claim.At, err)
		}
		fragment.Id = strconv.Itoa(i)
		document.Fragments = append(document.Fragments, fragment)
		if claim.Range.Filename != "" {
			document.File = claim.Range.Filename
		}
		document.ByteLength = max(document.ByteLength, uint64(max(claim.Range.End.Byte, 0)))
	}
	request := &protocol.DecodeSourceRequest{Engine: engine.GetId(), Documents: []*protocol.SourceDocument{document}}
	if err := provider.ValidateSourceDocuments(engine.GetId(), request.GetDocuments()); err != nil {
		return nil, err
	}
	return request, nil
}

// HCLBlockClaim is the engine's claim of the block a claim matched, or nil.
func HCLBlockClaim(engine *protocol.Engine, claim hclext.Claim) *protocol.HCLSourceBlock {
	site := HCLClaimSite(claim)
	for _, block := range engine.GetHclSource().GetBlocks() {
		if block.GetSite() == site && block.GetType() == claim.Block {
			return block
		}
	}
	return nil
}

func hclFragment(engine *protocol.Engine, claim hclext.Claim, bind func(schema, name string) string) (*protocol.SourceFragment, error) {
	shape := HCLBlockClaim(engine, claim)
	if shape == nil {
		return nil, fmt.Errorf("engine %q claims no such block", engine.GetId())
	}
	span, err := hclSpan(claim.Range)
	if err != nil {
		return nil, err
	}
	attributes, err := hclMapping(engine.GetId(), shape.GetAttributes(), claim.Attributes, span)
	if err != nil {
		return nil, err
	}
	fragment := &protocol.SourceFragment{HclBlock: proto.CloneOf(shape), Selector: shape.GetSelector(), Span: span}
	switch shape.GetSite() {
	case protocol.HCLSourceBlock_TOP_LEVEL, protocol.HCLSourceBlock_IN_TABLE:
		block := &protocol.SourceBlock{Type: claim.Block, Attributes: attributes, Span: span}
		if claim.Schema != "" && shape.GetSite() == protocol.HCLSourceBlock_TOP_LEVEL {
			block.Labels = append(block.Labels, &protocol.SourceLabel{Value: claim.Schema, Span: span})
		}
		if claim.Name != "" {
			block.Labels = append(block.Labels, &protocol.SourceLabel{Value: claim.Name, Span: span})
		}
		for _, child := range claim.Children {
			childSpan, err := hclSpan(child.Range)
			if err != nil {
				return nil, err
			}
			index := slices.IndexFunc(shape.GetChildren(), func(declared *protocol.HCLSourceChild) bool { return declared.GetType() == child.Type })
			if index < 0 {
				return nil, fmt.Errorf("child block %q is not in the claim", child.Type)
			}
			values, err := hclMapping(engine.GetId(), shape.GetChildren()[index].GetAttributes(), child.Attributes, childSpan)
			if err != nil {
				return nil, err
			}
			nested := &protocol.SourceBlock{Type: child.Type, Attributes: values, Span: childSpan}
			if child.Name != "" {
				nested.Labels = []*protocol.SourceLabel{{Value: child.Name, Span: childSpan}}
			}
			block.Children = append(block.Children, nested)
		}
		fragment.Content = &protocol.SourceFragment_Block{Block: block}
	default:
		fragment.Content = &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Mapping{Mapping: attributes}}}
	}
	if err := hclAttachment(engine.GetId(), shape, claim, fragment, bind); err != nil {
		return nil, err
	}
	return fragment, nil
}

// hclAttachment gives a fragment the binding and the subject its site needs,
// and an index fragment the inherited attributes of its index.
func hclAttachment(engine string, shape *protocol.HCLSourceBlock, claim hclext.Claim, fragment *protocol.SourceFragment, bind func(schema, name string) string) error {
	switch shape.GetSite() {
	case protocol.HCLSourceBlock_IN_TABLE, protocol.HCLSourceBlock_TABLE_ATTRIBUTES:
		fragment.BindingId = bind(claim.Table.Schema, claim.Table.Name)
	case protocol.HCLSourceBlock_INDEX_ATTRIBUTES:
		table, valid := tableref.Parse(claim.Index.Table)
		if !valid {
			return fmt.Errorf("index %q names no table", claim.Name)
		}
		fragment.BindingId = bind(table.Schema, table.Name)
		fragment.Subject = &protocol.Reference{Engine: engine, Kind: "index", Schema: table.Schema, Path: []string{table.Name}, Name: claim.Name}
		inherited := map[string]string{"type": claim.Index.Type, "ops": claim.Index.Operator}
		for _, name := range shape.GetInherited() {
			if value := inherited[name]; value != "" {
				if fragment.InheritedAttributes == nil {
					fragment.InheritedAttributes = &protocol.SourceMapping{}
				}
				fragment.InheritedAttributes.Entries = append(fragment.InheritedAttributes.Entries, &protocol.SourceEntry{Key: name,
					Value: &protocol.SourceValue{Span: fragment.GetSpan(), Value: &protocol.SourceValue_Text{Text: value}}})
			}
		}
	case protocol.HCLSourceBlock_ROUTINE_ATTRIBUTES:
		fragment.Subject = &protocol.Reference{Engine: engine, Kind: claim.Block, Schema: claim.Schema, Name: claim.Name}
	case protocol.HCLSourceBlock_MATERIALIZED_VIEW_ATTRIBUTES:
		view, valid := tableref.Parse(claim.Name)
		if !valid {
			return fmt.Errorf("materialized view %q is not a structural name", claim.Name)
		}
		fragment.Subject = &protocol.Reference{Engine: engine, Kind: "materialized_view", Schema: view.Schema, Name: view.Name}
	}
	return nil
}

// hclMapping is the mapping of the attributes a block writes, in the order
// its claim lists them.
func hclMapping(engine string, shapes []*protocol.HCLSourceAttribute, written map[string]hclext.Value, span *protocol.SourceSpan) (*protocol.SourceMapping, error) {
	mapping := &protocol.SourceMapping{}
	for _, shape := range shapes {
		value, found := written[shape.GetName()]
		if !found {
			continue
		}
		converted, err := hclValue(engine, value, span)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", shape.GetName(), err)
		}
		mapping.Entries = append(mapping.Entries, &protocol.SourceEntry{Key: shape.GetName(), KeySpan: span, Value: converted})
	}
	for name := range written {
		if !slices.ContainsFunc(shapes, func(shape *protocol.HCLSourceAttribute) bool { return shape.GetName() == name }) {
			return nil, fmt.Errorf("attribute %q is not in the claim", name)
		}
	}
	return mapping, nil
}

// hclValue converts a value as the frontend evaluated it: text stays text, an
// sql() call its SQL, and a reference the object it resolved to.
func hclValue(engine string, value hclext.Value, span *protocol.SourceSpan) (*protocol.SourceValue, error) {
	reference := func(ref hclext.Reference) *protocol.SourceValue {
		return &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Reference{Reference: &protocol.Reference{Engine: engine, Kind: ref.Kind,
			Schema: ref.Schema, Name: ref.Name}}}
	}
	switch value.Kind {
	case hclext.String, hclext.Text, hclext.TypeName:
		return &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Text{Text: value.Text}}, nil
	case hclext.SQL:
		return &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Sql{Sql: value.Text}}, nil
	case hclext.Bool:
		return &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Boolean{Boolean: value.Bool}}, nil
	case hclext.TextList:
		sequence := &protocol.SourceSequence{}
		for _, text := range value.Texts {
			sequence.Values = append(sequence.Values, &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Text{Text: text}})
		}
		return &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Sequence{Sequence: sequence}}, nil
	case hclext.Ref:
		if value.Reference == nil {
			return nil, fmt.Errorf("a reference resolved to nothing")
		}
		return reference(*value.Reference), nil
	case hclext.Refs:
		sequence := &protocol.SourceSequence{}
		for _, ref := range value.References {
			sequence.Values = append(sequence.Values, reference(ref))
		}
		return &protocol.SourceValue{Span: span, Value: &protocol.SourceValue_Sequence{Sequence: sequence}}, nil
	}
	return nil, fmt.Errorf("a value of kind %q, which the provider contract does not carry", value.Kind)
}

// hclSpan converts where a block stands.
func hclSpan(at hclext.Range) (*protocol.SourceSpan, error) {
	position := func(position hclext.Position) (*protocol.SourcePosition, error) {
		if position.Byte < 0 || position.Line <= 0 || position.Column <= 0 || int64(position.Line) > math.MaxUint32 || int64(position.Column) > math.MaxUint32 {
			return nil, fmt.Errorf("HCL block carries no usable source position")
		}
		return &protocol.SourcePosition{ByteOffset: uint64(position.Byte), Line: uint32(position.Line), Column: uint32(position.Column)}, nil
	}
	start, err := position(at.Start)
	if err != nil {
		return nil, err
	}
	end, err := position(at.End)
	if err != nil {
		return nil, err
	}
	return &protocol.SourceSpan{Start: start, End: end}, nil
}
