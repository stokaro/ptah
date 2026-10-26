package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// EncodeSourceFeature advertises EncodeSource, the mirror of DecodeSource: the
// owner returns its DESIRED values as the fragments its claims read, and the
// host writes the syntax of the format. A provider lists it, and so does each
// supporting engine, beside DecodeSourceFeature. It adds no field to an
// existing message, so it is not two-way. Contract 25.3.
const EncodeSourceFeature = "source.encode"

// EncodeGroupsFeature enables ordered syntax parts that account for a value
// together. It is negotiated by both peers beside EncodeSourceFeature.
const EncodeGroupsFeature = "source.encode-groups"

// SourceEncoder writes a batch of values as source fragments offline. Every
// value is written by one fragment or reported as one loss. It never writes
// text of a format, and a failure exposes no prefix.
type SourceEncoder interface {
	EncodeSource(context.Context, *protocol.EncodeSourceRequest) (*protocol.EncodedSource, error)
}

func encodeError(reason string) error {
	return fmt.Errorf("%w: encode source: %s", ErrProtocol, reason)
}

// validateEncodeEngine requires an engine that lists the service to have the
// provider's service and source decoding.
func validateEncodeEngine(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(engine.GetFeatures(), EncodeGroupsFeature) &&
		(!slices.Contains(description.GetFeatures(), EncodeGroupsFeature) || !slices.Contains(engine.GetFeatures(), EncodeSourceFeature)) {
		return encodeError("an engine lists encoding groups without the provider's feature and source encoding")
	}
	if slices.Contains(engine.GetFeatures(), EncodeSourceFeature) &&
		(!slices.Contains(description.GetFeatures(), EncodeSourceFeature) || !slices.Contains(description.GetFeatures(), DecodeSourceFeature)) {
		return encodeError("an engine lists the service without the provider's service and source decoding")
	}
	return nil
}

// ValidateEncodeSourceRequest checks an encoding batch before owner code runs:
// a format the host writes, bindings and columns of the engine, and DESIRED
// values of the engine's models, a facet bound to its table.
func ValidateEncodeSourceRequest(description *protocol.Description, request *protocol.EncodeSourceRequest) error {
	if !slices.Contains(description.GetFeatures(), EncodeSourceFeature) {
		return encodeError("encoding is unavailable")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), EncodeSourceFeature) {
		return encodeError("the engine does not encode")
	}
	switch request.GetFormat() {
	case protocol.SourceDocument_YAML, protocol.SourceDocument_HCL, protocol.SourceDocument_GO_ANNOTATIONS:
	default:
		return encodeError("a format the host writes is YAML, HCL or Go annotations")
	}
	bindings := make(map[string]*protocol.SourceBinding)
	for _, binding := range request.GetBindings() {
		if binding.GetId() == "" || bindings[binding.GetId()] != nil || !validValueReference(binding.GetObject(), engine.GetId()) {
			return encodeError("an invalid or repeated binding")
		}
		bindings[binding.GetId()] = binding
	}
	ids := make(map[string]bool)
	for _, value := range request.GetValues() {
		// The envelope check below refuses an empty ID.
		id := value.GetValue().GetId()
		if ids[id] {
			return encodeError("a value needs a unique ID")
		}
		ids[id] = true
		if err := validateEncodeValue(description, request, bindings, value); err != nil {
			return err
		}
	}
	if coverage := request.GetCoverage(); coverage != nil {
		if coverage.GetRepresentation() != protocol.ValueInput_DESIRED {
			return encodeError("coverage is DESIRED")
		}
		if err := ValidateModelCoverage(description, engine.GetId(), coverage); err != nil {
			return err
		}
	}
	return validateEncodeColumns(request, bindings)
}

func validateEncodeValue(description *protocol.Description, request *protocol.EncodeSourceRequest, bindings map[string]*protocol.SourceBinding, value *protocol.EncodeSourceValue) error {
	normalized := value.GetValue()
	envelope := normalized.GetValue()
	normalization := &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(),
		Values: []*protocol.ValueInput{{Id: normalized.GetId(), Representation: protocol.ValueInput_DESIRED, Value: envelope}}}
	if err := ValidateNormalizedValues(description, normalization, &protocol.NormalizedValues{Complete: true, Values: []*protocol.NormalizedValue{normalized}}); err != nil {
		return err
	}
	owner := envelope.GetOwner()
	switch value.GetPlacement() {
	case protocol.SourceModelValue_NATIVE_OBJECT:
		if envelope.GetKind() != owner.GetKind() || value.GetBindingId() != "" {
			return encodeError("a native object has its model kind as identity kind and no binding")
		}
	case protocol.SourceModelValue_FACET:
		binding := bindings[value.GetBindingId()]
		table := binding.GetObject()
		switch owner.GetKind() {
		case "table":
			if compareReferences(owner, table) != 0 {
				return encodeError("a table facet belongs to its binding's table")
			}
		case "column", "index":
			if len(owner.GetPath()) != 1 || owner.GetPath()[0] != table.GetName() || owner.GetSchema() != table.GetSchema() {
				return encodeError("a column or index facet belongs to a column or index of its binding's table")
			}
		default:
			return encodeError("a facet belongs to a table, a column or an index")
		}
	default:
		return encodeError("unknown value placement")
	}
	return nil
}

func validateEncodeColumns(request *protocol.EncodeSourceRequest, bindings map[string]*protocol.SourceBinding) error {
	if len(request.GetColumns()) != 0 && request.GetFormat() != protocol.SourceDocument_GO_ANNOTATIONS {
		return encodeError("columns travel only on a Go export")
	}
	seen := make(map[[2]string]bool)
	for _, column := range request.GetColumns() {
		key := [2]string{column.GetBindingId(), column.GetName()}
		if bindings[column.GetBindingId()] == nil || strings.TrimSpace(column.GetName()) == "" || strings.TrimSpace(column.GetType()) == "" || seen[key] {
			return encodeError("a column needs a binding, a unique name and a type")
		}
		seen[key] = true
	}
	return nil
}

// ValidateEncodedSource checks a reply against its request: complete, every
// value written by one group or flat fragment, or lost once, each part a negotiated claim
// of the format with the binding, subject and content its scope or site
// needs and no position, and limits and field types the request allows.
func ValidateEncodedSource(description *protocol.Description, request *protocol.EncodeSourceRequest, result *protocol.EncodedSource) error {
	if err := ValidateEncodeSourceRequest(description, request); err != nil {
		return err
	}
	if !result.GetComplete() {
		return encodeError("incomplete encoding")
	}
	engine, _, _ := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	grouped := slices.Contains(engine.GetFeatures(), EncodeGroupsFeature)
	if grouped && len(result.GetFragments()) != 0 {
		return encodeError("a grouped encoding carries no flat fragments")
	}
	if !grouped && len(result.GetGroups()) != 0 {
		return encodeError("encoding groups were not negotiated")
	}
	document, kinds, err := encodedDocument(engine, request, result)
	if err != nil {
		return err
	}
	walk := sourceWalk{engine: engine.GetId(), values: make(map[*protocol.SourceValue]bool), blocks: make(map[*protocol.SourceBlock]bool), encoded: true}
	if err := walk.document(document); err != nil {
		return err
	}
	for _, check := range []func(string, *protocol.SourceDocument, map[string]bool) error{
		func(engine string, document *protocol.SourceDocument, kinds map[string]bool) error {
			return validateYAMLSourceFragments(description, engine, document, kinds)
		},
		func(engine string, document *protocol.SourceDocument, kinds map[string]bool) error {
			return validateGoSourceFragments(description, engine, document, kinds)
		},
		func(engine string, document *protocol.SourceDocument, kinds map[string]bool) error {
			return validateHCLSourceFragments(description, engine, document, kinds)
		},
	} {
		if err := check(engine.GetId(), document, kinds); err != nil {
			return err
		}
	}
	if err := validateEncodedAccount(request, result); err != nil {
		return err
	}
	return validateEncodedExtras(engine, request, result)
}

// encodedDocument wraps the reply's fragments in the document a later decode
// would read, and requires each fragment to carry its format's claim. Every
// model of the engine is in scope: a claim names only models that take its
// format.
func encodedDocument(engine *protocol.Engine, request *protocol.EncodeSourceRequest, result *protocol.EncodedSource) (*protocol.SourceDocument, map[string]bool, error) {
	kinds := make(map[string]bool)
	for _, model := range engine.GetValueModels() {
		kinds[model.GetKind()] = true
	}
	document := &protocol.SourceDocument{Id: "encoded", Format: request.GetFormat(), File: "encoded", Bindings: request.GetBindings()}
	for _, encoded := range EncodedSourceParts(result) {
		fragment := encoded.GetFragment()
		claimed := map[protocol.SourceDocument_Format]bool{
			protocol.SourceDocument_YAML:           fragment.GetYamlKey() != nil,
			protocol.SourceDocument_GO_ANNOTATIONS: fragment.GetGoDirective() != nil,
			protocol.SourceDocument_HCL:            fragment.GetHclBlock() != nil,
		}[request.GetFormat()]
		if !claimed {
			return nil, nil, encodeError("an encoded fragment carries its format's claim")
		}
		document.Fragments = append(document.Fragments, fragment)
	}
	return document, kinds, nil
}

// fragmentModels returns the models a fragment's claim names.
func fragmentModels(fragment *protocol.SourceFragment) []string {
	switch {
	case fragment.GetYamlKey() != nil:
		return fragment.GetYamlKey().GetModels()
	case fragment.GetGoDirective() != nil:
		return fragment.GetGoDirective().GetModels()
	default:
		return fragment.GetHclBlock().GetModels()
	}
}

// validateEncodedAccount requires every value to be written once or lost once,
// a written value to be of a model its fragment's claim names, and an HCL
// block to say where it stands. A claim names only models whose source
// formats include its format, so a value of any other model is a loss.
func validateEncodedAccount(request *protocol.EncodeSourceRequest, result *protocol.EncodedSource) error {
	values := make(map[string]*protocol.EncodeSourceValue, len(request.GetValues()))
	for _, value := range request.GetValues() {
		values[value.GetValue().GetId()] = value
	}
	accounted := make(map[string]bool, len(values))
	for _, encoded := range result.GetFragments() {
		if len(encoded.GetValueIds()) == 0 {
			return encodeError("a fragment writes no value")
		}
		if err := validateEncodedPlacement(encoded); err != nil {
			return err
		}
		for _, id := range encoded.GetValueIds() {
			if accounted[id] {
				return encodeError("a fragment writes a value written already")
			}
			accounted[id] = true
			// A value the request lacks has no model, so its claim names none.
			if !slices.Contains(fragmentModels(encoded.GetFragment()), values[id].GetValue().GetValue().GetKind()) {
				return encodeError("a fragment writes a value the request lacks, or of a model its claim does not name")
			}
		}
	}
	for _, group := range result.GetGroups() {
		if err := validateEncodedGroup(group, values, accounted); err != nil {
			return err
		}
	}
	for _, loss := range result.GetLosses() {
		if values[loss.GetValueId()] == nil || accounted[loss.GetValueId()] || !diagnosticLine(loss.GetMessage()) {
			return encodeError("a loss names a value of the request once, in one line")
		}
		accounted[loss.GetValueId()] = true
	}
	if len(accounted) != len(values) {
		return encodeError("a value is neither written nor lost")
	}
	return nil
}

// EncodedSourceParts returns the syntax parts of a validated reply in order.
// Accounting stays in the reply's groups or flat fragments. Callers validate
// the reply before consuming these parts, including its negotiated shape.
func EncodedSourceParts(result *protocol.EncodedSource) []*protocol.EncodedSourcePart {
	var parts []*protocol.EncodedSourcePart
	for _, encoded := range result.GetFragments() {
		parts = append(parts, &protocol.EncodedSourcePart{Fragment: encoded.GetFragment(), Placement: encoded.GetPlacement()})
	}
	for _, group := range result.GetGroups() {
		parts = append(parts, group.GetParts()...)
	}
	return parts
}

func validateEncodedGroup(group *protocol.EncodedSourceGroup, values map[string]*protocol.EncodeSourceValue, accounted map[string]bool) error {
	if len(group.GetValueIds()) == 0 || len(group.GetParts()) == 0 {
		return encodeError("an encoding group needs values and syntax parts")
	}
	covered := make(map[string]bool)
	for _, id := range group.GetValueIds() {
		if values[id] == nil || accounted[id] {
			return encodeError("a group names each requested value once")
		}
		accounted[id] = true
	}
	for _, part := range group.GetParts() {
		if err := validateEncodedPlacement(&protocol.EncodedFragment{Fragment: part.GetFragment(), Placement: part.GetPlacement()}); err != nil {
			return err
		}
		matched := false
		for _, id := range group.GetValueIds() {
			if encodedPartOwns(part.GetFragment(), values[id]) {
				covered[id], matched = true, true
			}
		}
		if !matched {
			return encodeError("an encoding part names no group value with its model, binding and subject")
		}
	}
	if len(covered) != len(group.GetValueIds()) {
		return encodeError("an encoding group leaves a value outside its parts' models, bindings or subjects")
	}
	return nil
}

func encodedPartOwns(fragment *protocol.SourceFragment, value *protocol.EncodeSourceValue) bool {
	if !slices.Contains(fragmentModels(fragment), value.GetValue().GetValue().GetKind()) || fragment.GetBindingId() != value.GetBindingId() {
		return false
	}
	if subject := fragment.GetSubject(); subject != nil {
		return compareReferences(subject, value.GetValue().GetNormalizedOwner()) == 0
	}
	return true
}

func validateEncodedPlacement(encoded *protocol.EncodedFragment) error {
	block := encoded.GetFragment().GetHclBlock()
	placement := encoded.GetPlacement()
	if _, known := protocol.EncodedFragment_Placement_name[int32(placement)]; !known {
		return encodeError("unknown placement")
	}
	if (block != nil && hclOwnBlock(block.GetSite())) != (placement != protocol.EncodedFragment_PLACEMENT_UNSPECIFIED) {
		return encodeError("an HCL block of the owner's says where it stands, and no other fragment does")
	}
	return nil
}

// validateEncodedExtras checks the header limits and the Go field types.
func validateEncodedExtras(engine *protocol.Engine, request *protocol.EncodeSourceRequest, result *protocol.EncodedSource) error {
	if slices.Contains(engine.GetFeatures(), SourceLimitsFeature) {
		if sourceCoverageRequiresLimits(request.GetCoverage()) && len(result.GetLimits()) == 0 {
			return encodeError("narrowed coverage requires a claimed source limit")
		}
		var kinds []string
		switch request.GetFormat() {
		case protocol.SourceDocument_GO_ANNOTATIONS:
			kinds = engine.GetGoHeaderKinds()
		case protocol.SourceDocument_HCL:
			kinds = engine.GetHclSource().GetHeaderKinds()
		}
		if err := validateHeaderLimits(kinds, result.GetLimits()); err != nil {
			return err
		}
	}
	for _, limit := range result.GetLimits() {
		if !comparisonReason(limit.GetKind()) {
			return encodeError("a limit needs a kind")
		}
		if request.GetFormat() == protocol.SourceDocument_HCL && !slices.Contains(engine.GetHclSource().GetHeaderKinds(), limit.GetKind()) {
			return encodeError("an HCL limit names a declared header kind")
		}
	}
	if len(result.GetFieldTypes()) != 0 && request.GetFormat() != protocol.SourceDocument_GO_ANNOTATIONS {
		return encodeError("field types belong to a Go export")
	}
	typed := make(map[[2]string]bool)
	for _, field := range result.GetFieldTypes() {
		key := [2]string{field.GetBindingId(), field.GetColumn()}
		known := slices.ContainsFunc(request.GetColumns(), func(column *protocol.EncodeSourceColumn) bool {
			return column.GetBindingId() == field.GetBindingId() && column.GetName() == field.GetColumn()
		})
		if !known || typed[key] || !comparisonReason(field.GetGoType()) || (field.GetGoImport() != "" && !comparisonReason(field.GetGoImport())) {
			return encodeError("a field type names a column of the request once, with a type")
		}
		typed[key] = true
	}
	return nil
}
