package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/providersource"
)

// encodingEngine is the engine whose provider writes a document's values: the
// one the document is written for, or, without a target, the only engine that
// encodes HCL.
func (o *hclOwner) encodingEngine(written string) (*target, error) {
	var encoding []*target
	for _, engine := range o.engines {
		selected := o.host.targets[engine.GetId()]
		if !selected.encodes() {
			continue
		}
		if written != "" && (engine.GetId() == written || slices.Contains(engine.GetAliases(), written)) {
			return selected, nil
		}
		encoding = append(encoding, selected)
	}
	if written != "" || len(encoding) != 1 {
		return nil, fmt.Errorf("%w: provider %s writes HCL for %d engines, and the document is written for %q, which names none of them", ptaherr.ErrUnsupportedFeature,
			o.host.description.GetProviderId(), len(encoding), written)
	}
	return encoding[0], nil
}

// hclEncoding is where the host reports each value of one encoding batch it
// cannot have the provider write, and what the writer's blocks are.
type hclEncoding struct {
	batch *encodeBatch
	// slots maps a facet's value ID to the writer's block that holds it.
	slots map[string]hclext.Slot
	// objects maps an object's value ID to its identity.
	objects map[string]schemaext.Object
	// handed are the writer's blocks the request hands this owner, by their
	// binding and subject.
	tables  map[string]hclext.Table
	indexes map[[2]string]hclext.Slot
	result  hclext.EncodeResult
	losses  map[hclext.Slot][]string
}

// EncodeHCL has the provider write the document's values of its models: its
// objects, and the facets of tables, columns and indexes. The provider answers
// with the blocks and attributes its HCL claims read, which the host turns
// into the writer's blocks and the attributes of its own; a value it cannot
// write is a loss reported where the writer writes the value's block. A
// column's facet goes to the provider bound to the column's table, as a Go or
// YAML export sends it, so the provider can say that a facet the column holds
// by holding nothing is no loss. A facet of a key, a routine or a materialized
// view has no place in an encoding request and is a loss.
//
// The host writes the document's account of the provider's models itself, as
// the captured feature coverage header coverageHeader returns, and sends the
// provider no coverage: read back, the header is the document's exact claim.
// An HCL document without a header the provider reads claims nothing of its
// models (see coverage), so an export that wrote nothing would lose the claim
// of a read, and the provider's ptah:not-described entries can only say less
// than complete. A provider that answers with such entries anyway is refused:
// read back beside the header, the document would hold two accounts.
func (o *hclOwner) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	selected, err := o.encodingEngine(request.Target)
	if err != nil {
		return hclext.EncodeResult{}, err
	}
	encoding := &hclEncoding{batch: selected.encodeBatch(protocol.SourceDocument_HCL), slots: make(map[string]hclext.Slot), objects: make(map[string]schemaext.Object),
		tables: make(map[string]hclext.Table), indexes: make(map[[2]string]hclext.Slot), losses: make(map[hclext.Slot][]string)}
	if err := encoding.capture(request); err != nil {
		return hclext.EncodeResult{}, err
	}
	if encoding.result.Header, err = o.coverageHeader(ctx, encoding.batch, request.Coverage); err != nil {
		return hclext.EncodeResult{}, err
	}
	if len(encoding.batch.request.GetValues()) == 0 {
		return encoding.finish(), nil
	}
	encoded, err := o.host.client.EncodeSource(ctx, encoding.batch.request)
	if err != nil {
		return hclext.EncodeResult{}, err
	}
	if len(encoded.GetLimits()) != 0 {
		return hclext.EncodeResult{}, fmt.Errorf("%w: provider %s wrote %s entries in an HCL document, whose account of the provider's models the host writes as its %s header",
			schemaext.ErrInvalidValue, o.host.description.GetProviderId(), coverage.DirectiveMarker, schemaext.CoverageHeaderMarker)
	}
	for _, part := range encoding.parts(encoded) {
		if err := encoding.fragment(part); err != nil {
			return hclext.EncodeResult{}, err
		}
	}
	for _, loss := range encoded.GetLosses() {
		encoding.loss(loss.GetValueId(), loss.GetMessage())
	}
	return encoding.finish(), ctx.Err()
}

// coverageHeader is the captured feature coverage header line that states
// what the document knows of the provider's models, written with the
// provider's desired codecs under the identities the provider describes (see
// headerCodecs), or none for a document that knows nothing of them. A kind a
// read knows completely in every region it reports is written as complete, as
// the encoding request of every other format sends it (see completeRead): the
// document describes the namespaces the read listed, and a later plan applies
// it to the selection that plan names. Any other knowledge is written as the
// document holds it.
//
// A model HCL states only at a table's platform group is left out, kind-wide
// and for each subject: the groups the document writes are its whole account
// of it. Written as a claim, it would make a table whose group an edit removes
// lose what it holds, where a table that never wrote one keeps it.
func (o *hclOwner) coverageHeader(ctx context.Context, batch *encodeBatch, known schemaext.Coverage) ([]string, error) {
	if known.IsZero() {
		return nil, nil
	}
	kinds := slices.DeleteFunc(known.KindRecords(), func(record schemaext.KindCoverage) bool { return o.platformOnly(record.Model.Kind) })
	for i, record := range kinds {
		if sources := record.Knowledge.Sources(); len(sources) == 1 && batch.completeRead(sources[0]) {
			kinds[i].Knowledge = schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})
		}
	}
	subjects := slices.DeleteFunc(known.SubjectRecords(), func(record schemaext.SubjectCoverage) bool { return o.platformOnly(record.Kind) })
	written, err := schemaext.NewCoverage(known.Representation(), kinds, subjects)
	if err != nil || written.IsZero() {
		return nil, err
	}
	headers, err := o.codecs()
	if err != nil {
		return nil, err
	}
	line, err := headers.line(ctx, written)
	if err != nil {
		return nil, err
	}
	return []string{line}, nil
}

// platformOnly reports whether an engine of the extension states kind in HCL
// only at a table's platform group (see Host.hclPlatformOnly).
func (o *hclOwner) platformOnly(kind schemaext.Kind) bool {
	contract := o.host.models[modelKey{kind: kind, representation: schemaext.Desired}]
	return contract != nil && slices.ContainsFunc(o.engines, func(engine *protocol.Engine) bool { return o.host.hclPlatformOnly(engine.GetId(), contract) })
}

// parts retains value accounting for the writer's object identity and target
// scope. A group may contain several subjects; each part receives only the
// matching subjects of its own claim and binding.
func (e *hclEncoding) parts(result *protocol.EncodedSource) []*protocol.EncodedFragment {
	parts := slices.Clone(result.GetFragments())
	for _, group := range result.GetGroups() {
		for _, part := range group.GetParts() {
			fragment := part.GetFragment()
			encoded := &protocol.EncodedFragment{Fragment: fragment, Placement: part.GetPlacement()}
			models := fragment.GetHclBlock().GetModels()
			if fragment.GetIndexKind() != nil {
				models = fragment.GetIndexKind().GetModels()
			}
			for _, value := range e.batch.request.GetValues() {
				if !slices.Contains(group.GetValueIds(), value.GetValue().GetId()) || !slices.Contains(models, value.GetValue().GetValue().GetKind()) ||
					fragment.GetBindingId() != value.GetBindingId() {
					continue
				}
				// An index-kind part writes a facet of the table its subject
				// is an index of.
				if fragment.GetSubject() == nil || fragment.GetIndexKind() != nil || proto.Equal(fragment.GetSubject(), value.GetValue().GetNormalizedOwner()) {
					encoded.ValueIds = append(encoded.ValueIds, value.GetValue().GetId())
				}
			}
			parts = append(parts, encoded)
		}
	}
	return parts
}

// capture adds the document's values to the batch: objects as native objects,
// table, column and index facets bound to their table, and the facets the
// request has no place for as losses at their block. The document's coverage
// stays out of the batch; EncodeHCL writes it.
func (e *hclEncoding) capture(request hclext.EncodeRequest) error {
	for _, object := range request.Objects {
		id, err := e.batch.add(protocol.SourceModelValue_NATIVE_OBJECT, "", object.Value)
		if err != nil {
			return err
		}
		e.objects[id] = object
	}
	for _, table := range request.Tables {
		slot := hclext.Slot{Kind: hclext.SlotTable, Table: table.Table}
		binding := e.batch.bind(table.Table.Schema, table.Table.Name)
		e.tables[binding] = table.Table
		if err := e.facets(slot, binding, table.Facets); err != nil {
			return err
		}
	}
	for _, index := range request.Indexes {
		binding := e.batch.bind(index.Slot.Table.Schema, index.Slot.Table.Name)
		e.indexes[[2]string{binding, index.Slot.Name}] = index.Slot
		if err := e.facets(index.Slot, binding, index.Facets); err != nil {
			return err
		}
	}
	for _, column := range request.Columns {
		if err := e.facets(column.Slot, e.batch.bind(column.Slot.Table.Schema, column.Slot.Table.Name), column.Facets); err != nil {
			return err
		}
	}
	for _, slots := range [][]hclext.SlotFacets{request.Keys, request.Routines, request.MaterializedViews} {
		for _, slot := range slots {
			for _, kind := range slot.Facets.Kinds() {
				e.losses[slot.Slot] = append(e.losses[slot.Slot], fmt.Sprintf("%s of %s has no HCL spelling a provider writes", kind, slot.Slot))
			}
		}
	}
	return nil
}

func (e *hclEncoding) facets(slot hclext.Slot, binding string, facets schemaext.Facets) error {
	for _, kind := range facets.Kinds() {
		value, found, err := facets.Get(kind)
		if err != nil || !found {
			return err
		}
		id, err := e.batch.add(protocol.SourceModelValue_FACET, binding, value)
		if err != nil {
			return err
		}
		e.slots[id] = slot
	}
	return nil
}

// loss reports a value the provider did not write where the writer writes its
// block: a facet's at the writer's own block, an object's as an unwritten
// block at its identity.
func (e *hclEncoding) loss(id, message string) {
	if slot, facet := e.slots[id]; facet {
		e.losses[slot] = append(e.losses[slot], message)
		return
	}
	object := e.objects[id]
	e.result.Blocks = append(e.result.Blocks, hclext.EncodedBlock{Type: string(object.Ref.Kind), Placement: hclext.AfterTables, Name: object.Ref.Name.Source,
		Path: string(object.Ref.Kind) + "." + object.Ref.Name.Source, Losses: []string{message}, Unwritten: true, Object: object.Ref, Targets: slices.Clone(object.Targets)})
}

// slot is the entry of the writer's block slot in the result, added the first
// time a fragment or a loss writes there: the writer takes one entry for each
// of its blocks.
func (e *hclEncoding) slot(slot hclext.Slot) *hclext.SlotAttributes {
	index := slices.IndexFunc(e.result.Attributes, func(written hclext.SlotAttributes) bool { return written.Slot == slot })
	if index < 0 {
		e.result.Attributes = append(e.result.Attributes, hclext.SlotAttributes{Slot: slot})
		index = len(e.result.Attributes) - 1
	}
	return &e.result.Attributes[index]
}

// finish joins the losses of the writer's blocks to the attributes written
// there.
func (e *hclEncoding) finish() hclext.EncodeResult {
	for slot, losses := range e.losses {
		written := e.slot(slot)
		written.Losses = append(written.Losses, losses...)
	}
	slices.SortStableFunc(e.result.Attributes, func(a, b hclext.SlotAttributes) int { return strings.Compare(a.Slot.String(), b.Slot.String()) })
	return e.result
}

// hclPlacements are the writer's placements of the provider's blocks.
var hclPlacements = map[protocol.EncodedFragment_Placement]hclext.Placement{
	protocol.EncodedFragment_BEFORE_TYPES:   hclext.BeforeTypes,
	protocol.EncodedFragment_USER_TYPES:     hclext.UserTypes,
	protocol.EncodedFragment_AFTER_TABLES:   hclext.AfterTables,
	protocol.EncodedFragment_AFTER_TRIGGERS: hclext.AfterTriggers,
	protocol.EncodedFragment_WITHIN_TABLE:   hclext.WithinTable,
}

// fragment turns one encoded fragment into a block of the writer's or the
// attributes of one of the writer's own blocks.
func (e *hclEncoding) fragment(encoded *protocol.EncodedFragment) error {
	fragment := encoded.GetFragment()
	if fragment.GetIndexKind() != nil {
		return e.index(fragment)
	}
	claim := fragment.GetHclBlock()
	switch claim.GetSite() {
	case protocol.HCLSourceBlock_TOP_LEVEL, protocol.HCLSourceBlock_IN_TABLE:
		block, err := e.block(encoded)
		if err != nil {
			return err
		}
		e.result.Blocks = append(e.result.Blocks, block)
		return nil
	case protocol.HCLSourceBlock_TABLE_ATTRIBUTES, protocol.HCLSourceBlock_INDEX_ATTRIBUTES:
		slot, found := hclext.Slot{Kind: hclext.SlotTable, Table: e.tables[fragment.GetBindingId()]}, true
		if claim.GetSite() == protocol.HCLSourceBlock_INDEX_ATTRIBUTES {
			slot, found = e.indexes[[2]string{fragment.GetBindingId(), fragment.GetSubject().GetName()}]
		}
		attributes, err := hclEncodedAttributes(claim.GetAttributes(), fragment.GetValue().GetMapping())
		if err != nil || !found {
			return cmpErr(err, fmt.Errorf("%w: the provider wrote attributes of a block the document does not hand it", schemaext.ErrInvalidValue))
		}
		written := e.slot(slot)
		written.Attributes = append(written.Attributes, attributes...)
		return nil
	case protocol.HCLSourceBlock_TABLE_PLATFORM:
		table, found := e.tables[fragment.GetBindingId()]
		if !found {
			return fmt.Errorf("%w: the provider wrote the platform group of a table the document does not hand it", schemaext.ErrInvalidValue)
		}
		overrides := make([]hclext.PlatformOverride, 0, len(fragment.GetValue().GetMapping().GetEntries()))
		for _, entry := range fragment.GetValue().GetMapping().GetEntries() {
			text, isText := entry.GetValue().GetValue().(*protocol.SourceValue_Text)
			if !isText {
				return fmt.Errorf("%w: the provider wrote platform %q override %q as no string", schemaext.ErrInvalidValue, claim.GetType(), entry.GetKey())
			}
			overrides = append(overrides, hclext.PlatformOverride{Platform: claim.GetType(), Key: entry.GetKey(), Value: text.Text})
		}
		written := e.slot(hclext.Slot{Kind: hclext.SlotTable, Table: table})
		written.Overrides = append(written.Overrides, overrides...)
		return nil
	}
	return fmt.Errorf("%w: the provider wrote HCL at site %s, which the host writes no provider value at", ptaherr.ErrUnsupportedFeature, claim.GetSite())
}

// index turns an index-kind part into an index the writer writes as an
// `index` block of the part's table: what the frontend writes from the part's
// inherited part, and the provider's attributes from its content, each as the
// kind the engine's claim of the index block reads it as.
func (e *hclEncoding) index(fragment *protocol.SourceFragment) error {
	table, found := e.tables[fragment.GetBindingId()]
	if !found {
		return fmt.Errorf("%w: the provider wrote index %q of a table the document does not hand it", schemaext.ErrInvalidValue, fragment.GetSubject().GetName())
	}
	declaration, err := providersource.ReadIndexDeclaration(fragment)
	if err != nil {
		return err
	}
	var shapes []*protocol.HCLSourceAttribute
	for _, block := range e.batch.target.engine.GetHclSource().GetBlocks() {
		if block.GetSite() == protocol.HCLSourceBlock_INDEX_ATTRIBUTES {
			shapes = block.GetAttributes()
		}
	}
	attributes, err := hclEncodedAttributes(shapes, fragment.GetValue().GetMapping())
	if err != nil {
		return err
	}
	e.result.Indexes = append(e.result.Indexes, hclext.EncodedIndex{Table: table, Name: declaration.Name, Type: declaration.Type, Columns: declaration.Columns,
		Include: declaration.Include, Unique: declaration.Unique, Comment: declaration.Comment, Ops: declaration.Ops, Attributes: attributes})
	return nil
}

func cmpErr(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}

// block is the writer's block for a top-level or in-table fragment.
func (e *hclEncoding) block(encoded *protocol.EncodedFragment) (hclext.EncodedBlock, error) {
	fragment := encoded.GetFragment()
	claim, source := fragment.GetHclBlock(), fragment.GetBlock()
	placement, known := hclPlacements[encoded.GetPlacement()]
	if !known {
		return hclext.EncodedBlock{}, fmt.Errorf("%w: the provider placed block %q nowhere the writer writes", schemaext.ErrInvalidValue, source.GetType())
	}
	attributes, err := hclEncodedAttributes(claim.GetAttributes(), source.GetAttributes())
	if err != nil {
		return hclext.EncodedBlock{}, err
	}
	block := hclext.EncodedBlock{Type: source.GetType(), Placement: placement, Attributes: attributes, Path: source.GetType()}
	labels := source.GetLabels()
	switch {
	case claim.GetSite() == protocol.HCLSourceBlock_IN_TABLE:
		// A block in a table takes no label; the writer places it by its
		// table.
		table := e.tables[fragment.GetBindingId()]
		block.Table, block.Path = &table, "tables."+table.Name+"."+source.GetType()
	case claim.GetNaming() == protocol.HCLSourceBlock_UNLABELED:
		block.Unlabeled = true
	case len(labels) == 2:
		block.Schema, block.Name = labels[0].GetValue(), labels[1].GetValue()
	case len(labels) == 1:
		block.Name = labels[0].GetValue()
	}
	if block.Name != "" {
		block.Path += "." + block.Name
	}
	for _, child := range source.GetChildren() {
		index := slices.IndexFunc(claim.GetChildren(), func(shape *protocol.HCLSourceChild) bool { return shape.GetType() == child.GetType() })
		if index < 0 {
			return hclext.EncodedBlock{}, fmt.Errorf("%w: the provider wrote child block %q outside its claim", schemaext.ErrInvalidValue, child.GetType())
		}
		values, err := hclEncodedAttributes(claim.GetChildren()[index].GetAttributes(), child.GetAttributes())
		if err != nil {
			return hclext.EncodedBlock{}, err
		}
		nested := hclext.EncodedChild{Type: child.GetType(), Unlabeled: len(child.GetLabels()) == 0, Attributes: values}
		if len(child.GetLabels()) == 1 {
			nested.Name = child.GetLabels()[0].GetValue()
		}
		block.Children = append(block.Children, nested)
	}
	for _, id := range encoded.GetValueIds() {
		if object, written := e.objects[id]; written {
			if block.Object.Kind != "" && (block.Object != object.Ref || !slices.Equal(block.Targets, object.Targets)) {
				return hclext.EncodedBlock{}, unsupported("one HCL block representing several native object identities or target scopes")
			}
			block.Object, block.Targets = object.Ref, slices.Clone(object.Targets)
		}
	}
	return block, nil
}

// hclEncodedAttributes is what the writer writes for the attributes of a
// mapping, in the order the provider wrote them, each as the kind its claim
// reads it as.
func hclEncodedAttributes(shapes []*protocol.HCLSourceAttribute, mapping *protocol.SourceMapping) ([]hclext.EncodedAttribute, error) {
	attributes := make([]hclext.EncodedAttribute, 0, len(mapping.GetEntries()))
	for _, entry := range mapping.GetEntries() {
		index := slices.IndexFunc(shapes, func(shape *protocol.HCLSourceAttribute) bool { return shape.GetName() == entry.GetKey() })
		if index < 0 {
			return nil, fmt.Errorf("%w: the provider wrote attribute %q outside its claim", schemaext.ErrInvalidValue, entry.GetKey())
		}
		value, err := hclEncodedValue(providersource.HCLKinds[shapes[index].GetKind()], entry.GetValue())
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", entry.GetKey(), err)
		}
		attributes = append(attributes, hclext.EncodedAttribute{Name: entry.GetKey(), Value: value})
	}
	return attributes, nil
}

// hclEncodedValue is the value the writer writes for an attribute the claim
// reads as kind: text as a quoted string, a whole number for a text attribute
// bare, SQL in an sql() call, a bool bare, and references as references. The
// reader reads a bare number back as its text.
func hclEncodedValue(kind hclext.Kind, value *protocol.SourceValue) (hclext.Value, error) {
	switch content := value.GetValue().(type) {
	case *protocol.SourceValue_Text:
		if kind == hclext.String || kind == hclext.Text || kind == hclext.TypeName {
			return hclext.Value{Kind: hclext.String, Text: content.Text}, nil
		}
	case *protocol.SourceValue_Number:
		if kind == hclext.Text && content.Number != "" && strings.Trim(content.Number, "0123456789") == "" {
			return hclext.Value{Kind: hclext.Number, Text: content.Number}, nil
		}
	case *protocol.SourceValue_Sql:
		if kind == hclext.SQL {
			return hclext.Value{Kind: hclext.SQL, Text: content.Sql}, nil
		}
	case *protocol.SourceValue_Boolean:
		if kind == hclext.Bool {
			return hclext.Value{Kind: hclext.Bool, Bool: content.Boolean}, nil
		}
	case *protocol.SourceValue_Reference:
		if kind == hclext.Ref {
			return hclext.Value{Kind: hclext.Ref, Reference: &hclext.Reference{Kind: content.Reference.GetKind(), Schema: content.Reference.GetSchema(), Name: content.Reference.GetName()}}, nil
		}
	case *protocol.SourceValue_Sequence:
		return hclEncodedSequence(kind, content.Sequence)
	}
	return hclext.Value{}, fmt.Errorf("%w: a value the claim does not read as %s", schemaext.ErrInvalidValue, kind)
}

func hclEncodedSequence(kind hclext.Kind, sequence *protocol.SourceSequence) (hclext.Value, error) {
	result := hclext.Value{Kind: kind}
	for _, element := range sequence.GetValues() {
		switch {
		case kind == hclext.TextList && element.GetValue() != nil && isText(element):
			result.Texts = append(result.Texts, element.GetText())
		case kind == hclext.Refs && element.GetReference() != nil:
			reference := element.GetReference()
			result.References = append(result.References, hclext.Reference{Kind: reference.GetKind(), Schema: reference.GetSchema(), Name: reference.GetName()})
		default:
			return hclext.Value{}, fmt.Errorf("%w: a list the claim does not read as %s", schemaext.ErrInvalidValue, kind)
		}
	}
	return result, nil
}

func isText(value *protocol.SourceValue) bool {
	_, text := value.GetValue().(*protocol.SourceValue_Text)
	return text
}
