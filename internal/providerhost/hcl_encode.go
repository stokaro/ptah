package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/providersource"
)

// encodingEngine is the engine whose provider writes a document's values: the
// one the document is written for, or the only engine that encodes HCL.
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
	if len(encoding) != 1 {
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
// objects, and the facets of tables and indexes. The provider answers with the
// blocks and attributes its HCL claims read, which the host turns into the
// writer's blocks and the attributes of its own; a value it cannot write is a
// loss reported where the writer writes the value's block. A facet of a column,
// a key, a routine or a materialized view has no place in an encoding request
// and is a loss too. The provider's ptah:not-described entries become header
// lines.
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
	if len(encoding.batch.request.GetValues()) == 0 && encoding.batch.request.GetCoverage() == nil {
		return encoding.finish(), nil
	}
	encoded, err := o.host.client.EncodeSource(ctx, encoding.batch.request)
	if err != nil {
		return hclext.EncodeResult{}, err
	}
	for _, fragment := range encoded.GetFragments() {
		if err := encoding.fragment(fragment); err != nil {
			return hclext.EncodeResult{}, err
		}
	}
	for _, loss := range encoded.GetLosses() {
		encoding.loss(loss.GetValueId(), loss.GetMessage())
	}
	for _, limit := range encoded.GetLimits() {
		encoding.result.Header = append(encoding.result.Header, coverage.Object{Kind: coverage.Kind(limit.GetKind()), Name: limit.GetName(),
			Reason: coverage.Reason(limit.GetReason()), Provenance: coverage.Provenance(limit.GetProvenance())}.Directive())
	}
	return encoding.finish(), ctx.Err()
}

// capture adds the document's values to the batch: objects as native objects,
// table and index facets bound to their table, and the facets the request has
// no place for as losses at their block.
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
	for _, slots := range [][]hclext.SlotFacets{request.Columns, request.Keys, request.Routines, request.MaterializedViews} {
		for _, slot := range slots {
			for _, kind := range slot.Facets.Kinds() {
				e.losses[slot.Slot] = append(e.losses[slot.Slot], fmt.Sprintf("%s of %s has no HCL spelling a provider writes", kind, slot.Slot))
			}
		}
	}
	return e.batch.coverage(request.Coverage)
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

// finish joins the losses of the writer's blocks to the attributes written
// there.
func (e *hclEncoding) finish() hclext.EncodeResult {
	for slot, losses := range e.losses {
		index := slices.IndexFunc(e.result.Attributes, func(written hclext.SlotAttributes) bool { return written.Slot == slot })
		if index < 0 {
			e.result.Attributes = append(e.result.Attributes, hclext.SlotAttributes{Slot: slot})
			index = len(e.result.Attributes) - 1
		}
		e.result.Attributes[index].Losses = append(e.result.Attributes[index].Losses, losses...)
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
		e.result.Attributes = append(e.result.Attributes, hclext.SlotAttributes{Slot: slot, Attributes: attributes})
		return nil
	}
	return fmt.Errorf("%w: the provider wrote HCL at site %s, which the host writes no provider value at", ptaherr.ErrUnsupportedFeature, claim.GetSite())
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
// reads as kind: text as a quoted string, SQL in an sql() call, a bool bare,
// and references as references.
func hclEncodedValue(kind hclext.Kind, value *protocol.SourceValue) (hclext.Value, error) {
	switch content := value.GetValue().(type) {
	case *protocol.SourceValue_Text:
		if kind == hclext.String || kind == hclext.Text || kind == hclext.TypeName {
			return hclext.Value{Kind: hclext.String, Text: content.Text}, nil
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
