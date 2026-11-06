package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// TableOutlinesFeature enables ModelPlanning.tables: a planning service that
// plans no table-attached model says it reads only the outline of each table
// a request carries, and the host sends each table by its subject, its action
// and the name of each state the plan holds, without its columns, keys,
// indexes and captured features. The host captures every table for every
// planning service it calls, so a migration of 500 new tables sent each of
// the services that assess named objects the whole capture of every table,
// in both directions, though only the service that owns the tables reads
// more than which tables the plan keeps, creates, changes and drops.
// Contract 25.27. The provider and the engine list it, and the host lists it
// in its host features; a host that does not list it sends every table whole,
// and NegotiatedDescription then shows every service as reading whole tables.
const TableOutlinesFeature = "planning.table-outlines"

// validateTableOutlinesFeature refuses a planning service that reads outlines
// without the feature, one with parent kinds, which plans the features an
// outline leaves out, and a value of ModelPlanning.tables this SDK does not
// know.
func validateTableOutlinesFeature(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), TableOutlinesFeature)
	if listed && !slices.Contains(description.GetFeatures(), TableOutlinesFeature) {
		return tableOutlinesError("the engine's feature requires the provider's")
	}
	for _, service := range engine.GetPlanning() {
		switch service.GetTables() {
		case protocol.PlanningTables_PLANNING_TABLES_CAPTURED:
		case protocol.PlanningTables_PLANNING_TABLES_OUTLINED:
			if !listed {
				return tableOutlinesError("planning service " + service.GetId() + " reads outlines without the engine's feature")
			}
			if len(service.GetParentKinds()) != 0 {
				return tableOutlinesError("planning service " + service.GetId() + " plans parent kinds, which an outline leaves out")
			}
		default:
			return tableOutlinesError("planning service " + service.GetId() + " names an unknown table form")
		}
	}
	return nil
}

func tableOutlinesError(reason string) error {
	return fmt.Errorf("%w: table outlines: %s", ErrProtocol, reason)
}

// negotiateTableOutlines shows every planning service of engine as reading
// whole tables when the host does not list TableOutlinesFeature, since such a
// host sends them whole.
func negotiateTableOutlines(engine *protocol.Engine, hostFeatures []string) {
	if slices.Contains(hostFeatures, TableOutlinesFeature) {
		return
	}
	for _, service := range engine.GetPlanning() {
		service.Tables = protocol.PlanningTables_PLANNING_TABLES_CAPTURED
	}
}

// outlinedPlanning reports whether request is for a planning service that
// reads the outlines of its tables.
func outlinedPlanning(description *protocol.Description, request *protocol.PlanFeaturesRequest) bool {
	service := findModelPlanning(description, request.GetEngine(), request.GetPlanningId())
	return service.GetTables() == protocol.PlanningTables_PLANNING_TABLES_OUTLINED
}

// TableOutline returns the outline of table a service that reads outlines
// receives: its subject and action, and each of its states by name alone.
func TableOutline(table *protocol.PlanningTable) *protocol.PlanningTable {
	outline := &protocol.PlanningTable{Subject: table.GetSubject(), Action: table.GetAction()}
	if desired := table.GetDesired(); desired != nil {
		outline.Desired = &protocol.DeclaredTable{Name: desired.GetName()}
	}
	if current := table.GetCurrent(); current != nil {
		outline.Current = &protocol.ObservedTable{Name: current.GetName()}
	}
	return outline
}

// validateOutlinedTables checks the tables of a request for a service that
// reads outlines: tables by unique subjects, each with an action its states
// agree with, and each state named and holding nothing else.
func validateOutlinedTables(request *protocol.PlanFeaturesRequest) error {
	identities := make([]*protocol.ComparisonIdentity, 0, len(request.GetTables()))
	for _, table := range request.GetTables() {
		identities = append(identities, table.GetSubject())
		if table.GetSubject().GetKind() != "table" {
			return fmt.Errorf("%w: planning parent is not a table", ErrProtocol)
		}
		if err := validatePlanningLifecycle(table); err != nil {
			return err
		}
		desired, current := table.GetDesired(), table.GetCurrent()
		if desired != nil && !comparisonReason(desired.GetName()) || current != nil && !comparisonReason(current.GetName()) {
			return fmt.Errorf("%w: missing outlined table name", ErrProtocol)
		}
		if !proto.Equal(table, TableOutline(table)) {
			return tableOutlinesError("an outlined table carries more than its subject, action and the names of its states")
		}
	}
	return validateComparisonIdentities(identities)
}
