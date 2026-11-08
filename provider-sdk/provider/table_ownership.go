package provider

import (
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// TableOwnershipFeature lets a planning service own the lifecycle of every
// table of its engine without a table setting model: ModelPlanning.owns_tables.
// The host hands such a service every table it creates, alters, rebuilds or
// drops, as it hands a service with a parent kind, and the service answers
// one parent receipt of kind "table" for each, after the receipts of its
// parent kinds. A service that owns tables may plan no change, parent or
// context kind, reads whole tables and is never outlined, and an engine has
// at most one. A host that does not list the feature sees no service own
// tables this way, so a provider whose only claim on its tables is this one
// needs a host that lists it. It is a two-way feature: the provider lists it,
// and so does the engine whose service owns tables. Contract 25.32.
const TableOwnershipFeature = "planning.table-ownership"

// TableReceiptKind is the kind of the parent receipt a service that owns
// tables answers for each table it plans.
const TableReceiptKind = "table"

// validateTableOwnershipFeature refuses a service that owns tables without
// the engine's feature, an engine feature without the provider's, more than
// one owner on an engine, and an owner that reads outlines.
func validateTableOwnershipFeature(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), TableOwnershipFeature)
	if listed && !slices.Contains(description.GetFeatures(), TableOwnershipFeature) {
		return fmt.Errorf("%w: table ownership requires the provider's feature", ErrProtocol)
	}
	owners := 0
	for _, service := range engine.GetPlanning() {
		if !service.GetOwnsTables() {
			continue
		}
		owners++
		if !listed {
			return fmt.Errorf("%w: planning service %s owns tables without the engine's %s", ErrProtocol, service.GetId(), TableOwnershipFeature)
		}
		if service.GetTables() == protocol.PlanningTables_PLANNING_TABLES_OUTLINED {
			return fmt.Errorf("%w: planning service %s owns tables and reads outlines, which leave out what it plans", ErrProtocol, service.GetId())
		}
	}
	if owners > 1 {
		return fmt.Errorf("%w: engine %s has more than one planning service that owns its tables", ErrProtocol, engine.GetId())
	}
	return nil
}

// negotiateTableOwnership shows no service as owning tables when the host
// does not list TableOwnershipFeature.
func negotiateTableOwnership(engine *protocol.Engine, hostFeatures []string) {
	if slices.Contains(hostFeatures, TableOwnershipFeature) {
		return
	}
	for _, service := range engine.GetPlanning() {
		service.OwnsTables = false
	}
}

// ownsTableLifecycles reports a planning service the host hands every table
// it creates, alters, rebuilds or drops: one with a parent kind, or one that
// owns the engine's tables.
func ownsTableLifecycles(service *protocol.ModelPlanning) bool {
	return len(service.GetParentKinds()) > 0 || service.GetOwnsTables()
}

// parentReceiptKinds returns the kinds of the parent receipts a service
// answers for each table with an action: its parent kinds, and the table
// receipt after them when it owns tables.
func parentReceiptKinds(request *protocol.PlanFeaturesRequest, service *protocol.ModelPlanning) []string {
	kinds := slices.Clone(request.GetParentKinds())
	if service.GetOwnsTables() {
		kinds = append(kinds, TableReceiptKind)
	}
	return kinds
}
