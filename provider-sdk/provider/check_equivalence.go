package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// CheckEquivalenceFeature enables CompareChecks, by which the owner of an
// engine decides whether a declared CHECK expression and the CHECK a held
// table keeps under the same name are one constraint. An engine's server
// often stores a CHECK in a form of its own, with literals cast or a list
// rewritten, so only the engine's owner can say that two texts mean one
// constraint; a host that asks compares no CHECK of the engine by its text.
// The provider and each engine that compares checks list it. A host that does
// not know the RPC never calls it, so it is not a two-way feature.
// Contract 25.28.
const CheckEquivalenceFeature = "schema.check-equivalence"

// CheckComparer decides CHECK equivalence offline.
type CheckComparer interface {
	// CompareChecks answers each pair of the request: whether the declared
	// expression and the held CHECK it names are one constraint on the
	// engine. It reads the request alone; it opens nothing and runs nothing.
	// The reply has one table for each requested table and one check for
	// each pair, in the request's order.
	CompareChecks(context.Context, *protocol.CompareChecksRequest) (*protocol.ComparedChecks, error)
}

func checkEquivalenceError(reason string) error {
	return fmt.Errorf("%w: CHECK equivalence: %s", ErrProtocol, reason)
}

// validateCheckEquivalenceEngine checks that an engine that lists
// CheckEquivalenceFeature has the provider's.
func validateCheckEquivalenceEngine(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(engine.GetFeatures(), CheckEquivalenceFeature) && !slices.Contains(description.GetFeatures(), CheckEquivalenceFeature) {
		return fmt.Errorf("%w: engine %s requires the provider's", ErrProtocol, CheckEquivalenceFeature)
	}
	return nil
}

// ValidateCompareChecksRequest refuses a request for an engine the provider
// does not own or that does not list CheckEquivalenceFeature, invalid
// identifier facts, and a table that is not a held table bound to its subject.
// Each pair names a CHECK of its table with an expression, and a declared
// expression; a table has at least one pair and names a constraint once, and
// no two tables share a subject.
func ValidateCompareChecksRequest(description *protocol.Description, request *protocol.CompareChecksRequest) error {
	if request == nil {
		return checkEquivalenceError("the request is missing")
	}
	if !slices.Contains(description.GetFeatures(), CheckEquivalenceFeature) {
		return checkEquivalenceError("the provider does not compare checks")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), CheckEquivalenceFeature) {
		return checkEquivalenceError("the engine does not compare checks")
	}
	facts := &protocol.CompareModelsRequest{Identifiers: request.GetIdentifiers(), Capabilities: request.GetCapabilities()}
	if err := validateComparisonContext(facts, &protocol.ModelComparison{Placement: protocol.ModelComparison_OBJECTS}); err != nil {
		return err
	}
	subjects := make([]*protocol.ComparisonIdentity, 0, len(request.GetTables()))
	for _, table := range request.GetTables() {
		if err := validateCheckComparisonTable(table, request.GetIdentifiers()); err != nil {
			return err
		}
		subjects = append(subjects, table.GetSubject())
	}
	return validateComparisonIdentities(subjects)
}

func validateCheckComparisonTable(table *protocol.CheckComparisonTable, identifiers *protocol.ComparisonIdentifiers) error {
	current := table.GetCurrent()
	if current == nil {
		return checkEquivalenceError("a table requires its held state")
	}
	if err := validateColumnParent(table.GetSubject(), current.GetName(), current.GetSchema(), identifiers); err != nil {
		return err
	}
	if current.GetFeatures() != nil {
		return checkEquivalenceError("a held table carries no captured features")
	}
	if err := validateObservedTable(current); err != nil {
		return err
	}
	if len(table.GetPairs()) == 0 {
		return checkEquivalenceError("a table requires a pair")
	}
	named := make(map[string]bool, len(table.GetPairs()))
	for _, pair := range table.GetPairs() {
		if named[pair.GetConstraint()] {
			return checkEquivalenceError(fmt.Sprintf("table %q names constraint %q twice", current.GetName(), pair.GetConstraint()))
		}
		named[pair.GetConstraint()] = true
		held := slices.IndexFunc(current.GetConstraints(), func(constraint *protocol.ObservedConstraint) bool {
			return constraint.GetName() == pair.GetConstraint()
		})
		if pair.GetConstraint() == "" || held < 0 || current.GetConstraints()[held].GetType() != checkConstraint ||
			strings.TrimSpace(current.GetConstraints()[held].GetCheckClause()) == "" {
			return checkEquivalenceError(fmt.Sprintf("a pair of table %q names %q, which is no CHECK the table holds with an expression", current.GetName(), pair.GetConstraint()))
		}
		if strings.TrimSpace(pair.GetDeclared()) == "" {
			return checkEquivalenceError(fmt.Sprintf("the pair of CHECK %q of table %q has no declared expression", pair.GetConstraint(), current.GetName()))
		}
	}
	return nil
}

// ValidateComparedChecks checks a reply against its request: it is complete,
// and it answers each table and each pair of the request in order, by the
// same subject and the same constraint name.
func ValidateComparedChecks(description *protocol.Description, request *protocol.CompareChecksRequest, result *protocol.ComparedChecks) error {
	if err := ValidateCompareChecksRequest(description, request); err != nil {
		return err
	}
	return ValidateComparedChecksReply(description, request, result)
}

// ValidateComparedChecksReply is [ValidateComparedChecks] for a request that
// [ValidateCompareChecksRequest] accepted with description and that has not
// changed since: it checks the reply and not the request again. The SDK's
// client and server validate each request once and call it for the reply.
func ValidateComparedChecksReply(_ *protocol.Description, request *protocol.CompareChecksRequest, result *protocol.ComparedChecks) error {
	if result == nil || !result.GetComplete() || len(result.GetTables()) != len(request.GetTables()) {
		return checkEquivalenceError("the reply does not answer every table")
	}
	for i, table := range request.GetTables() {
		answered := result.GetTables()[i]
		if CompareIdentities(answered.GetSubject(), table.GetSubject()) != 0 {
			return checkEquivalenceError(fmt.Sprintf("answer %d names another table than the request", i))
		}
		if len(answered.GetChecks()) != len(table.GetPairs()) {
			return checkEquivalenceError(fmt.Sprintf("the answer for table %q does not answer every pair", table.GetCurrent().GetName()))
		}
		for j, pair := range table.GetPairs() {
			if answered.GetChecks()[j].GetConstraint() != pair.GetConstraint() {
				return checkEquivalenceError(fmt.Sprintf("answer %d for table %q names %q where the request names %q", j, table.GetCurrent().GetName(),
					answered.GetChecks()[j].GetConstraint(), pair.GetConstraint()))
			}
		}
	}
	return nil
}
