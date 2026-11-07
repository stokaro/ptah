package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ViewQueryEquivalenceFeature enables CompareViewQueries: the owner of an
// engine decides whether the query a declared view states and the query the
// held view keeps are one query, in place of the host's comparison of their
// texts. An engine keeps a view's query in a form of its own, and only its
// owner knows which differences matter: a host that folds case would call two
// queries one where the engine's identifiers keep case. A host that asks
// compares no view query of the engine by its text. The call is new and adds
// nothing to an existing message, so the feature is the provider's alone: the
// provider lists it, and so does each engine whose view queries it compares.
// Contract 25.33.
const ViewQueryEquivalenceFeature = "schema.view-query-equivalence"

// ViewQueryComparer decides view query equivalence offline.
type ViewQueryComparer interface {
	// CompareViewQueries answers each pair of the request: whether the
	// declared query and the query of the held view are one query on the
	// engine. It reads the request alone; it opens nothing and runs nothing.
	// The reply has one answer for each pair, in the request's order.
	CompareViewQueries(context.Context, *protocol.CompareViewQueriesRequest) (*protocol.ComparedViewQueries, error)
}

func viewQueryError(reason string) error {
	return fmt.Errorf("%w: view query equivalence: %s", ErrProtocol, reason)
}

// validateViewQueryEquivalenceEngine checks that an engine that lists
// ViewQueryEquivalenceFeature has the provider's.
func validateViewQueryEquivalenceEngine(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(engine.GetFeatures(), ViewQueryEquivalenceFeature) && !slices.Contains(description.GetFeatures(), ViewQueryEquivalenceFeature) {
		return fmt.Errorf("%w: engine %s requires the provider's", ErrProtocol, ViewQueryEquivalenceFeature)
	}
	return nil
}

// ValidateCompareViewQueriesRequest refuses a request for an engine the
// provider does not own or that does not list ViewQueryEquivalenceFeature,
// and invalid identifier facts. Each pair holds a view of the engine, of a
// kind the engine carries as a common object, as a read reports it: with its
// query and without captured facets; and a declared query. No two pairs name
// one view, a view and a materialized view sharing their names. Declared
// columns need ViewQueryContextFeature on both sides (see
// validateViewQueryColumns).
func ValidateCompareViewQueriesRequest(description *protocol.Description, request *protocol.CompareViewQueriesRequest) error {
	if request == nil {
		return viewQueryError("the request is missing")
	}
	if !slices.Contains(description.GetFeatures(), ViewQueryEquivalenceFeature) {
		return viewQueryError("the provider does not compare view queries")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), ViewQueryEquivalenceFeature) {
		return viewQueryError("the engine does not compare view queries")
	}
	facts := &protocol.CompareModelsRequest{Identifiers: request.GetIdentifiers(), Capabilities: request.GetCapabilities()}
	if err := validateComparisonContext(facts, &protocol.ModelComparison{Placement: protocol.ModelComparison_OBJECTS}); err != nil {
		return err
	}
	kinds := engineCommonKinds(engine)
	names := request.GetIdentifiers().GetRules().GetTableNames()
	seen := make(map[string]bool, len(request.GetPairs()))
	for i, pair := range request.GetPairs() {
		identity := pair.GetCurrent().GetIdentity()
		if err := validateHeldView(engine, kinds, pair.GetCurrent(), i); err != nil {
			return err
		}
		if strings.TrimSpace(pair.GetDeclared()) == "" {
			return viewQueryError(fmt.Sprintf("the pair of view %q has no declared query", identity.GetName()))
		}
		key := strings.Join([]string{identity.GetDatabase(), identity.GetCatalog(), sqlFold(names, identity.GetSchema()), sqlFold(names, identity.GetName())}, "\x00")
		if seen[key] {
			return viewQueryError(fmt.Sprintf("view %q is named twice", identity.GetName()))
		}
		seen[key] = true
	}
	return validateViewQueryColumns(description, request)
}

// validateHeldView checks the held view of the pair at index: a view or a
// materialized view of the engine, of a kind the engine carries, named
// without a path or an overload, with a query and without facets.
func validateHeldView(engine *protocol.Engine, kinds commonKinds, current *protocol.ObservedView, index int) error {
	if current == nil {
		return viewQueryError(fmt.Sprintf("pair %d requires its held view", index))
	}
	identity := current.GetIdentity()
	kind, known := viewKinds[identity.GetKind()]
	if !validValueReference(identity, engine.GetId()) || !known || len(identity.GetPath()) != 0 || len(identity.GetOverload()) != 0 {
		return viewQueryError(fmt.Sprintf("pair %d holds no view or materialized view of the engine", index))
	}
	if !kinds[kind] {
		return viewQueryError(fmt.Sprintf("the engine does not carry the %s objects pair %d holds", strings.ToLower(kind.String()), index))
	}
	if strings.TrimSpace(current.GetBody()) == "" {
		return viewQueryError(fmt.Sprintf("held view %q has no query", identity.GetName()))
	}
	if len(current.GetFacets()) != 0 {
		return viewQueryError(fmt.Sprintf("held view %q carries no facets", identity.GetName()))
	}
	return nil
}

// ValidateComparedViewQueries checks a reply against its request: it is
// complete, and it answers each pair of the request in order, by the same
// view.
func ValidateComparedViewQueries(description *protocol.Description, request *protocol.CompareViewQueriesRequest, result *protocol.ComparedViewQueries) error {
	if err := ValidateCompareViewQueriesRequest(description, request); err != nil {
		return err
	}
	return ValidateComparedViewQueriesReply(description, request, result)
}

// ValidateComparedViewQueriesReply is [ValidateComparedViewQueries] for a
// request that [ValidateCompareViewQueriesRequest] accepted with description
// and that has not changed since: it checks the reply and not the request
// again. The SDK's client and server validate each request once and call it
// for the reply. An undecided answer needs ViewQueryContextFeature on both
// sides and is not equivalent.
func ValidateComparedViewQueriesReply(description *protocol.Description, request *protocol.CompareViewQueriesRequest, result *protocol.ComparedViewQueries) error {
	if result == nil || !result.GetComplete() || len(result.GetQueries()) != len(request.GetPairs()) {
		return viewQueryError("the reply does not answer every pair")
	}
	for i, pair := range request.GetPairs() {
		held, answered := pair.GetCurrent().GetIdentity(), result.GetQueries()[i].GetIdentity()
		if !sameReference(held, answered) {
			return viewQueryError(fmt.Sprintf("answer %d names %q where the request names %q", i, answered.GetName(), held.GetName()))
		}
	}
	return validateUndecidedViewQueries(description, request, result)
}

// sameReference reports whether two references name one object in the same
// words: engine, database, catalog, schema, path, kind, name and overload.
// The display name is presentation and is not compared.
func sameReference(a, b *protocol.Reference) bool {
	return a.GetEngine() == b.GetEngine() && a.GetDatabase() == b.GetDatabase() && a.GetCatalog() == b.GetCatalog() && a.GetSchema() == b.GetSchema() &&
		slices.Equal(a.GetPath(), b.GetPath()) && a.GetKind() == b.GetKind() && a.GetName() == b.GetName() && slices.Equal(a.GetOverload(), b.GetOverload())
}
