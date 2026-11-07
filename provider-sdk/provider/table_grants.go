package provider

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// TableGrantsFeature enables PlanningTable.current_grants: the host sends a
// planning service the grants each held table holds, so a plan that
// replaces the table can give the new one its grants. Under it every table of
// a planning request whose current state is present, for a service that
// reads whole tables, carries them: complete with every grant the read found
// on the table, a grant on some of its columns included, or not complete and
// empty when the read did not describe grants. A plan that keeps the table
// needs none of them; one that replaces it in a form the engine runs later,
// such as the SQL text of a migration file, cannot read them from the server
// when it runs. A host that does not list the feature sends no grants, as a
// host of contract 25.32 does. It is a two-way feature: the provider lists
// it, and so does each engine whose planning reads them, which must carry
// GRANT common objects. Contract 25.33.
const TableGrantsFeature = "planning.table-grants"

// validateTableGrantsFeature refuses an engine that lists the feature without
// the provider's, or without GRANT among the common objects it carries: the
// grants a request sends are the ones such a read reports.
func validateTableGrantsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), TableGrantsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), TableGrantsFeature) {
		return tableGrantsError("the engine's feature requires the provider's")
	}
	if !engineCommonKinds(engine)[protocol.CommonObjectSupport_GRANT] {
		return tableGrantsError("engine " + engine.GetId() + " carries no GRANT common objects")
	}
	return nil
}

// negotiateTableGrants removes the engine's feature when the negotiation left
// it no GRANT common objects, as a host that does not list
// schema.common-objects leaves it: such a host reads no grant to send.
func negotiateTableGrants(engine *protocol.Engine) {
	if !engineCommonKinds(engine)[protocol.CommonObjectSupport_GRANT] {
		engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == TableGrantsFeature })
	}
}

// validateTableGrants checks the grants of one planning table against the
// negotiation: present exactly when the table's current state is and both
// sides list the feature, empty when not complete, and each a grant on the
// held table that no other entry repeats.
func validateTableGrants(description *protocol.Description, engine *protocol.Engine, features []string, table *protocol.PlanningTable) error {
	grants, current := table.GetCurrentGrants(), table.GetCurrent()
	negotiated := EngineOffers(description, engine.GetId(), TableGrantsFeature) && slices.Contains(features, TableGrantsFeature)
	switch {
	case grants != nil && current == nil:
		return tableGrantsError("grants of a table the request does not hold")
	case grants != nil && !negotiated:
		return tableGrantsError("the grants of table " + current.GetName() + " were not negotiated")
	case grants == nil && current != nil && negotiated:
		return tableGrantsError("held table " + current.GetName() + " needs its grants, complete or not")
	case grants == nil:
		return nil
	case !grants.GetComplete() && len(grants.GetGrants()) != 0:
		return tableGrantsError("table " + current.GetName() + " lists grants the read did not describe")
	}
	seen := make(map[string]bool, len(grants.GetGrants()))
	for _, grant := range grants.GetGrants() {
		if strings.TrimSpace(grant.GetRole()) == "" || strings.TrimSpace(grant.GetPrivilege()) == "" {
			return tableGrantsError("a grant on table " + current.GetName() + " needs a role and a privilege")
		}
		if !grantsHeldTable(grant.GetTarget(), engine.GetId(), current) {
			return tableGrantsError("a grant listed for table " + current.GetName() + " names another object")
		}
		if err := validateColumnList(grant.GetColumns()); err != nil {
			return err
		}
		identity := proto.CloneOf(grant)
		identity.GrantedBy, identity.Implicit, identity.WithGrantOption = "", false, false
		key, err := proto.MarshalOptions{Deterministic: true}.Marshal(identity)
		if err != nil {
			return tableGrantsError("a grant that does not encode: " + err.Error())
		}
		if seen[string(key)] {
			return tableGrantsError("a repeated grant on table " + current.GetName())
		}
		seen[string(key)] = true
	}
	return nil
}

// grantsHeldTable reports whether target names the held table as the read
// named it: kind "table", the table's schema and name, no path and no
// overload, and no engine other than the request's.
func grantsHeldTable(target *protocol.Reference, engine string, current *protocol.ObservedTable) bool {
	return target.GetKind() == "table" && target.GetName() == current.GetName() && target.GetSchema() == current.GetSchema() &&
		len(target.GetPath()) == 0 && len(target.GetOverload()) == 0 && (target.GetEngine() == "" || target.GetEngine() == engine)
}

func tableGrantsError(reason string) error {
	return fmt.Errorf("%w: table grants: %s", ErrProtocol, reason)
}
