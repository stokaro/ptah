package provider

import (
	"fmt"
	"regexp"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ScopeSelectionsFeature enables Engine.scope_selections: how the --include,
// --exclude and --schema selectors of a command's scope name an engine's
// objects, which grants name them, and what a kept object keeps. The provider
// lists it in its features and in each engine that declares selections, and
// the host lists it in DescribeRequest.host_features.
const ScopeSelectionsFeature = "scope.selections"

var (
	selectorTypePattern    = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	grantObjectTypePattern = regexp.MustCompile(`^[A-Z]+( [A-Z]+)*$`)
)

// commonGrantTargets are the object types a grant names on the common objects
// the host models. An engine's selection never claims one.
var commonGrantTargets = []string{"TABLE", "VIEW", "MATERIALIZED VIEW", "SEQUENCE", "SCHEMA", "DATABASE", "FUNCTION", "PROCEDURE", "ROUTINE"}

// validateScopeSelections checks an engine's scope selections: each names a
// DESIRED and OBSERVED model kind of the engine once, with a known naming and
// a selector type of the declared spelling; selections that share a selector
// type agree on its naming and relation; only an IN_SCHEMA selection names a
// relation; a grant object type is of the declared spelling, is no common
// target and names one selection; and keeps_required names other selections'
// kinds, each once.
func validateScopeSelections(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), ScopeSelectionsFeature)
	if listed && !slices.Contains(description.GetFeatures(), ScopeSelectionsFeature) {
		return scopeSelectionError("an engine lists the feature without the provider")
	}
	selections := engine.GetScopeSelections()
	if len(selections) != 0 && !listed {
		return scopeSelectionError("an engine declares selections without listing the feature")
	}
	kinds := make(map[string]bool, len(selections))
	types := make(map[string]*protocol.ScopeSelection, len(selections))
	grants := make(map[string]bool, len(selections))
	for _, selection := range selections {
		if err := validateScopeSelection(engine, selection, kinds, types, grants); err != nil {
			return err
		}
	}
	for _, selection := range selections {
		for i, kind := range selection.GetKeepsRequired() {
			if !kinds[kind] || kind == selection.GetKind() || slices.Contains(selection.GetKeepsRequired()[:i], kind) {
				return scopeSelectionError("a kept requirement names no other selection, or names one twice")
			}
		}
	}
	return nil
}

// validateScopeSelection checks one selection against those before it and
// records it.
func validateScopeSelection(engine *protocol.Engine, selection *protocol.ScopeSelection, kinds map[string]bool,
	types map[string]*protocol.ScopeSelection, grants map[string]bool,
) error {
	kind := selection.GetKind()
	if !engineModel(engine, kind, protocol.ValueInput_DESIRED) || !engineModel(engine, kind, protocol.ValueInput_OBSERVED) {
		return scopeSelectionError("a selection's kind is not a desired and observed model of the engine")
	}
	if kinds[kind] {
		return scopeSelectionError("a kind is selected twice")
	}
	kinds[kind] = true
	switch selection.GetNaming() {
	case protocol.ScopeSelection_IN_SCHEMA, protocol.ScopeSelection_DATABASE_WIDE, protocol.ScopeSelection_SCHEMA_OR_DATABASE:
	default:
		return scopeSelectionError("a selection needs a known naming")
	}
	if !selectorTypePattern.MatchString(selection.GetSelectorType()) {
		return scopeSelectionError("a selector type is not lower-case letters, digits and underscores starting with a letter")
	}
	if existing, defined := types[selection.GetSelectorType()]; defined &&
		(existing.GetNaming() != selection.GetNaming() || existing.GetRelation() != selection.GetRelation()) {
		return scopeSelectionError("a selector type is defined with two namings or relations")
	}
	types[selection.GetSelectorType()] = selection
	if selection.GetRelation() && selection.GetNaming() != protocol.ScopeSelection_IN_SCHEMA {
		return scopeSelectionError("only an in-schema selection names a relation")
	}
	grant := selection.GetGrantObjectType()
	if grant == "" {
		return nil
	}
	if !grantObjectTypePattern.MatchString(grant) || slices.Contains(commonGrantTargets, grant) {
		return scopeSelectionError("a grant object type is not upper-case words, or is a common target")
	}
	if grants[grant] {
		return scopeSelectionError("a grant object type names two selections")
	}
	grants[grant] = true
	return nil
}

// engineModel reports whether the engine declares a model of kind in the
// representation.
func engineModel(engine *protocol.Engine, kind string, representation protocol.ValueInput_Representation) bool {
	return slices.ContainsFunc(engine.GetValueModels(), func(model *protocol.ValueModel) bool {
		return model.GetKind() == kind && model.GetRepresentation() == representation
	})
}

func scopeSelectionError(reason string) error {
	return fmt.Errorf("%w: scope selections: %s", ErrProtocol, reason)
}
