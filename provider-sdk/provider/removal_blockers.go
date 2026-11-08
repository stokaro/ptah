package provider

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// RemovalBlockersFeature enables Table.removal_blockers in inspection and
// ObservedTable.removal_blockers in planning: the objects outside the engine's
// model that name a table, or some of its columns, and block their removal,
// such as a Spanner change stream that names the table in its FOR clause. A
// read reports them for each table it describes; the host carries them to the
// planning service unchanged and never compares or exports them, so a plan
// that drops a blocked table or column can be refused before a statement
// runs, by `--dry-run` and when a migration is generated too. A read that
// reports none says that nothing outside the model blocks a removal. It is a
// two-way feature: the provider and each engine whose reads report blockers
// list it, and the engine needs ObservedColumnsFeature, against whose columns
// the SDK checks each blocker. Contract 25.39.
const RemovalBlockersFeature = "schema.removal-blockers"

// removalBlockerKind is the shape of a blocker's kind: a lowercase token.
var removalBlockerKind = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func removalBlockersError(reason string) error {
	return fmt.Errorf("%w: removal blockers: %s", ErrProtocol, reason)
}

// validateRemovalBlockersFeature refuses an engine that lists the feature
// without the provider's, or without complete column observations to check a
// blocker's columns against.
func validateRemovalBlockersFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), RemovalBlockersFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), RemovalBlockersFeature) {
		return removalBlockersError("the engine's feature requires the provider's")
	}
	if !EngineOffers(description, engine.GetId(), ObservedColumnsFeature) {
		return removalBlockersError("engine " + engine.GetId() + " does not report complete column observations")
	}
	return nil
}

// validateInspectedRemovalBlockers refuses blockers in a snapshot whose
// session did not negotiate them, and checks the blockers of each table of one
// that did.
func validateInspectedRemovalBlockers(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	negotiated := EngineOffers(description, engine.GetId(), RemovalBlockersFeature) && slices.Contains(snapshot.GetTargetFeatures(), RemovalBlockersFeature)
	if negotiated && !slices.Contains(snapshot.GetTargetFeatures(), ObservedColumnsFeature) {
		return removalBlockersError("the blockers require negotiated column observations")
	}
	for _, table := range snapshot.GetTables() {
		name := table.GetIdentity().GetName()
		if len(table.GetRemovalBlockers()) != 0 && !negotiated {
			return removalBlockersError("the blockers of table " + name + " were not negotiated")
		}
		columns := make([]string, 0, len(table.GetColumns()))
		for _, column := range table.GetColumns() {
			columns = append(columns, column.GetName())
		}
		if err := validateRemovalBlockerList(name, table.GetRemovalBlockers(), columns); err != nil {
			return err
		}
	}
	return nil
}

// validatePlanningRemovalBlockers refuses blockers on a planning table whose
// request did not negotiate them, and checks the blockers of one that did
// against the held table's columns.
func validatePlanningRemovalBlockers(description *protocol.Description, engine *protocol.Engine, features []string, current *protocol.ObservedTable) error {
	if len(current.GetRemovalBlockers()) == 0 {
		return nil
	}
	if !EngineOffers(description, engine.GetId(), RemovalBlockersFeature) || !slices.Contains(features, RemovalBlockersFeature) {
		return removalBlockersError("the blockers of table " + current.GetName() + " were not negotiated")
	}
	columns := make([]string, 0, len(current.GetColumns()))
	for _, column := range current.GetColumns() {
		columns = append(columns, column.GetName())
	}
	return validateRemovalBlockerList(current.GetName(), current.GetRemovalBlockers(), columns)
}

// validateRemovalBlockerList checks the blockers of one table: each with a
// kind token and a name, no object listed twice, and each column a column of
// the table, named once.
func validateRemovalBlockerList(table string, blockers []*protocol.RemovalBlocker, columns []string) error {
	seen := make(map[[2]string]bool, len(blockers))
	for _, blocker := range blockers {
		if !removalBlockerKind.MatchString(blocker.GetKind()) {
			return removalBlockersError(fmt.Sprintf("a blocker of table %q has no kind token", table))
		}
		if !blockerName(blocker.GetName()) {
			return removalBlockersError(fmt.Sprintf("a %s blocker of table %q has a blank or malformed name", blocker.GetKind(), table))
		}
		key := [2]string{blocker.GetKind(), blocker.GetName()}
		if seen[key] {
			return removalBlockersError(fmt.Sprintf("%s %q is listed twice for table %q", blocker.GetKind(), blocker.GetName(), table))
		}
		seen[key] = true
		named := make(map[string]bool, len(blocker.GetColumns()))
		for _, column := range blocker.GetColumns() {
			if !slices.Contains(columns, column) || named[column] {
				return removalBlockersError(fmt.Sprintf("%s %q names an unknown or repeated column of table %q", blocker.GetKind(), blocker.GetName(), table))
			}
			named[column] = true
		}
	}
	return nil
}

// blockerName reports a name a host can show: valid UTF-8, not blank, and
// without a control character.
func blockerName(name string) bool {
	return utf8.ValidString(name) && strings.TrimSpace(name) != "" && !strings.ContainsFunc(name, unicode.IsControl)
}
