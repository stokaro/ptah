package providerhost

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// goEncoder writes a provider's values as Go annotations through
// EncodeSource.
type goEncoder struct{ host *Host }

// goEncodingEngine is the engine whose provider writes a source's values: the
// one the source is written for, or, without a target, the only engine that
// encodes and claims Go directives.
func (g goEncoder) goEncodingEngine(written string) (*target, error) {
	var encoding []*target
	for _, engine := range g.host.description.GetEngines() {
		selected := g.host.targets[engine.GetId()]
		if !selected.encodes() || len(engine.GetGoSourceDirectives()) == 0 {
			continue
		}
		if written != "" && (engine.GetId() == written || slices.Contains(engine.GetAliases(), written)) {
			return selected, nil
		}
		encoding = append(encoding, selected)
	}
	if written != "" || len(encoding) != 1 {
		return nil, fmt.Errorf("%w: provider %s writes Go annotations for %d engines, and the source is written for %q, which names none of them",
			ptaherr.ErrUnsupportedFeature, g.host.description.GetProviderId(), len(encoding), written)
	}
	return encoding[0], nil
}

// goEncoding is one Go encoding batch and what its value IDs stand for.
type goEncoding struct {
	batch *encodeBatch
	// tables are the bound tables by binding, and fields the declaration of
	// each column by its binding and name.
	tables map[string]annotation.Table
	fields map[[2]string]annotation.Slot
	result annotation.EncodeResult
}

// EncodeAnnotations has the provider write the schema's values of its models:
// its objects and the facets of the declarations of the frontend's own
// directives, with every field so the provider can give a field the Go type of
// its engine type. A fragment of a directive of the provider's own becomes a
// directive at its table, or among the schema's standalone objects; one of the
// frontend's directive becomes attributes of that declaration. A Go export
// takes no loss, so a value the provider cannot write refuses the export,
// naming it, and so does a not-described entry, which a Go source of a
// provider cannot carry.
func (g goEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	selected, err := g.goEncodingEngine(request.Target)
	if err != nil {
		return annotation.EncodeResult{}, err
	}
	encoding := &goEncoding{batch: selected.encodeBatch(protocol.SourceDocument_GO_ANNOTATIONS), tables: make(map[string]annotation.Table),
		fields: make(map[[2]string]annotation.Slot)}
	if err := encoding.capture(request); err != nil {
		return annotation.EncodeResult{}, err
	}
	encoded, err := g.host.client.EncodeSource(ctx, encoding.batch.request)
	if err != nil {
		return annotation.EncodeResult{}, err
	}
	if len(encoded.GetLosses()) > 0 {
		loss := encoded.GetLosses()[0]
		return annotation.EncodeResult{}, fmt.Errorf("%w: Go annotations cannot represent a value of provider %s: %s", ptaherr.ErrUnsupportedFeature,
			g.host.description.GetProviderId(), loss.GetMessage())
	}
	if len(encoded.GetLimits()) > 0 {
		return annotation.EncodeResult{}, fmt.Errorf("%w: a Go source of provider %s cannot carry its not-described %s entries", ptaherr.ErrUnsupportedFeature,
			g.host.description.GetProviderId(), encoded.GetLimits()[0].GetKind())
	}
	for _, fragment := range encoded.GetFragments() {
		if err := encoding.fragment(fragment.GetFragment()); err != nil {
			return annotation.EncodeResult{}, err
		}
	}
	for _, typed := range encoded.GetFieldTypes() {
		slot, found := encoding.fields[[2]string{typed.GetBindingId(), typed.GetColumn()}]
		if !found {
			return annotation.EncodeResult{}, fmt.Errorf("%w: provider %s typed column %q, which the source does not write", schemaext.ErrInvalidValue,
				g.host.description.GetProviderId(), typed.GetColumn())
		}
		encoding.result.FieldTypes = append(encoding.result.FieldTypes, annotation.FieldType{Slot: slot, Name: typed.GetGoType(), Import: typed.GetGoImport()})
	}
	return encoding.result, ctx.Err()
}

// capture adds the schema's objects, the facets of its declarations and its
// fields to the batch.
func (e *goEncoding) capture(request annotation.EncodeRequest) error {
	for _, table := range request.Tables {
		e.tables[e.batch.bind(table.Schema, table.Name)] = table
	}
	for _, object := range request.Objects {
		if _, err := e.batch.add(protocol.SourceModelValue_NATIVE_OBJECT, "", object.Value); err != nil {
			return err
		}
	}
	for _, slot := range request.Slots {
		binding := e.batch.bind(slot.Slot.Table.Schema, slot.Slot.Table.Name)
		for _, kind := range slot.Facets.Kinds() {
			value, found, err := slot.Facets.Get(kind)
			if err != nil || !found {
				return err
			}
			if _, err := e.batch.add(protocol.SourceModelValue_FACET, binding, value); err != nil {
				return err
			}
		}
	}
	for _, field := range request.Fields {
		binding := e.batch.bind(field.Slot.Table.Schema, field.Slot.Table.Name)
		e.fields[[2]string{binding, field.Slot.Name}] = field.Slot
		e.batch.request.Columns = append(e.batch.request.Columns, &protocol.EncodeSourceColumn{BindingId: binding, Name: field.Slot.Name, Type: field.Type,
			Nullable: field.Nullable, PrimaryKey: field.Key})
	}
	return e.batch.coverage(request.Coverage)
}

// goDirectiveSlots maps each of the frontend's directives a claim adds
// attributes to onto the declaration it names.
var goDirectiveSlots = map[protocol.GoSourceDirective_Scope]string{
	protocol.GoSourceDirective_TABLE:  "ptah:schema:table",
	protocol.GoSourceDirective_COLUMN: "ptah:schema:field",
	protocol.GoSourceDirective_INDEX:  "ptah:schema:index",
}

// fragment turns one fragment into a directive of the provider's own or the
// attributes of one of the frontend's declarations.
func (e *goEncoding) fragment(fragment *protocol.SourceFragment) error {
	claim := fragment.GetGoDirective()
	attributes := make([]annotation.EncodedAttribute, 0, len(fragment.GetValue().GetMapping().GetEntries()))
	for _, entry := range fragment.GetValue().GetMapping().GetEntries() {
		text, isText := entry.GetValue().GetValue().(*protocol.SourceValue_Text)
		if !isText {
			return fmt.Errorf("%w: the provider wrote Go attribute %q of %s as other than text", schemaext.ErrInvalidValue, entry.GetKey(), claim.GetDirective())
		}
		attributes = append(attributes, annotation.EncodedAttribute{Name: entry.GetKey(), Value: text.Text})
	}
	if claim.GetDirective() == goDirectiveSlots[claim.GetScope()] {
		slot, err := e.declaration(claim, fragment)
		if err != nil {
			return err
		}
		e.result.Attributes = append(e.result.Attributes, annotation.SlotAttributes{Slot: slot, Attributes: attributes})
		return nil
	}
	directive := annotation.EncodedDirective{Directive: claim.GetDirective(), Attributes: attributes}
	switch claim.GetScope() {
	case protocol.GoSourceDirective_DOCUMENT:
		directive.Placement = annotation.AfterViews
	case protocol.GoSourceDirective_TABLE:
		table, bound := e.tables[fragment.GetBindingId()]
		if !bound {
			return fmt.Errorf("%w: the provider wrote %s for a table the source does not write", schemaext.ErrInvalidValue, claim.GetDirective())
		}
		directive.Placement, directive.Table = annotation.AfterTable, &table
	default:
		return fmt.Errorf("%w: the provider wrote its %s directive %s, which the Go writer has no place for beside a field or an index",
			ptaherr.ErrUnsupportedFeature, claim.GetScope(), claim.GetDirective())
	}
	e.result.Directives = append(e.result.Directives, directive)
	return nil
}

// declaration is the declaration of the frontend's directive a fragment adds
// attributes to: the table of its binding, or the field or index its subject
// names.
func (e *goEncoding) declaration(claim *protocol.GoSourceDirective, fragment *protocol.SourceFragment) (annotation.Slot, error) {
	table, bound := e.tables[fragment.GetBindingId()]
	if !bound {
		return annotation.Slot{}, fmt.Errorf("%w: the provider wrote %s attributes for a table the source does not write", schemaext.ErrInvalidValue, claim.GetDirective())
	}
	slot := annotation.Slot{Directive: claim.GetDirective(), Table: table}
	if claim.GetScope() != protocol.GoSourceDirective_TABLE {
		slot.Name = fragment.GetSubject().GetName()
	}
	return slot, nil
}
