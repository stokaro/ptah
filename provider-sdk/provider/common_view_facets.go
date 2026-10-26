package provider

import (
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// CommonViewFacetsFeature carries native settings on common views and actual
// observed operands on replacements. Both peers negotiate it with common objects.
const CommonViewFacetsFeature = "schema.common-view-facets"

func validateViewFacetFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), CommonViewFacetsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), CommonViewFacetsFeature) ||
		!slices.Contains(engine.GetFeatures(), CommonObjectsFeature) || !slices.Contains(description.GetFeatures(), NormalizeValuesFeature) {
		return commonObjectError("view facets require negotiated common objects and value normalization")
	}
	return nil
}

func validateDeclaredViewFacets(description *protocol.Description, engine *protocol.Engine, features []string, identifiers *protocol.ComparisonIdentifiers, views []*protocol.SQLView) error {
	for _, view := range views {
		captured := view.GetFeatures()
		if captured == nil {
			continue
		}
		if !EngineOffers(description, engine.GetId(), CommonViewFacetsFeature) || !slices.Contains(features, CommonViewFacetsFeature) {
			return commonObjectError("desired view facets were not negotiated")
		}
		if len(captured.GetObjects().GetRecords()) != 0 {
			return commonObjectError("a common view holds facets, not standalone objects")
		}
		request := &protocol.PlanFeaturesRequest{Engine: engine.GetId(), TargetFeatures: features, Identifiers: identifiers}
		if err := validateCapturedFeatures(description, request, engine, protocol.ValueInput_DESIRED, captured); err != nil {
			return err
		}
		for _, record := range captured.GetFacets().GetRecords() {
			if !viewFacetSubject(view, identifiers, record.GetSubject()) {
				return commonObjectError("desired facet belongs to another view")
			}
			if record.GetValue() != nil && !declarationIdentityMatches(&protocol.PlanDeclarationsRequest{Engine: engine.GetId(), Identifiers: identifiers}, record) {
				return commonObjectError("desired view facet ownership differs from its subject")
			}
		}
	}
	return nil
}

func viewFacetSubject(view *protocol.SQLView, identifiers *protocol.ComparisonIdentifiers, subject *protocol.ComparisonIdentity) bool {
	if subject.GetKind() != declaredViewKind(view) || subject.GetDatabase().GetSource() != "" || subject.GetDatabase().GetNormalized() != "" ||
		subject.GetCatalog().GetSource() != "" || subject.GetCatalog().GetNormalized() != "" || len(subject.GetPath()) != 0 ||
		subject.GetParent().GetSource() != "" || subject.GetParent().GetNormalized() != "" || len(subject.GetSignature()) != 0 {
		return false
	}
	schema := view.GetSchema()
	if schema == "" && subject.GetSchema().GetDefaulted() {
		schema = identifiers.GetDefaultSchema()
	}
	fold := identifiers.GetRules().GetTableNames()
	return subject.GetName().GetSource() == view.GetName() && subject.GetName().GetNormalized() == sqlFold(fold, view.GetName()) &&
		subject.GetSchema().GetSource() == schema && subject.GetSchema().GetNormalized() == sqlFold(fold, schema)
}

func validateObservedViewFacets(description *protocol.Description, engine *protocol.Engine, features []string, views []*protocol.ObservedView) error {
	request := &protocol.NormalizeValuesRequest{Engine: engine.GetId(), TargetFeatures: features}
	for _, view := range views {
		if len(view.GetFacets()) != 0 && (!EngineOffers(description, engine.GetId(), CommonViewFacetsFeature) || !slices.Contains(features, CommonViewFacetsFeature)) {
			return commonObjectError("observed view facets were not negotiated")
		}
		seen := make(map[string]bool)
		for _, facet := range view.GetFacets() {
			if facet == nil || compareReferences(facet.GetOwner(), view.GetIdentity()) != 0 || seen[facet.GetKind()] {
				return commonObjectError("observed view facet has foreign or duplicate ownership")
			}
			seen[facet.GetKind()] = true
			request.Values = append(request.Values, &protocol.ValueInput{Id: strconv.Itoa(len(request.Values)), Representation: protocol.ValueInput_OBSERVED, Value: facet})
		}
	}
	if len(request.GetValues()) == 0 {
		return nil
	}
	return ValidateValueInputs(description, request)
}

func validateViewCreationFacets(description *protocol.Description, engine *protocol.Engine, features []string, identifiers *protocol.ComparisonIdentifiers, creation *protocol.ViewCreation) error {
	if creation == nil {
		return nil
	}
	if err := validateDeclaredViewFacets(description, engine, features, identifiers, []*protocol.SQLView{creation.GetView()}); err != nil {
		return err
	}
	current := creation.GetCurrent()
	if current == nil {
		return nil
	}
	if !EngineOffers(description, engine.GetId(), CommonViewFacetsFeature) || !slices.Contains(features, CommonViewFacetsFeature) || !creation.GetReplace() {
		return commonObjectError("an observed view operand requires a negotiated replacement")
	}
	view, ref := creation.GetView(), current.GetIdentity()
	if !validValueReference(ref, engine.GetId()) || ref.GetKind() != declaredViewKind(view) || ref.GetSchema() != view.GetSchema() || ref.GetName() != view.GetName() || !comparisonReason(current.GetBody()) {
		return commonObjectError("replacement observation differs from its declared view")
	}
	return validateObservedViewFacets(description, engine, features, []*protocol.ObservedView{current})
}

func viewIdentifiers(engine *protocol.Engine) *protocol.ComparisonIdentifiers {
	return &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(engine.GetIdentifiers()), DefaultSchema: engine.GetDefaultSchema()}
}
