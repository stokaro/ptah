package providerhost

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/schemacapture"
)

// planningTableGrants are the grants a planning service receives for a held
// table under provider.TableGrantsFeature: every grant the read found on the
// table, complete, or none and not complete when the read did not describe
// grants. A plan that replaces the table, such as a rebuild written into the
// text of a migration file, gives them back to the new table; the provider
// cannot read them from the server when that text runs.
//
// The catalog keeps one row for each column of a column privilege, as a read
// reports it, so the rows of one grant are joined again into one grant that
// lists its columns, in the order the rows came. A row that repeats one
// already joined adds nothing. Each target names the table as the read named
// it, which is how the observed table beside it names itself.
func planningTableGrants(engine string, current schemacapture.TableObservation) *protocol.PlanningTableGrants {
	result := &protocol.PlanningTableGrants{Complete: current.GrantsDescribed}
	if !current.GrantsDescribed {
		return result
	}
	type grantKey struct {
		role, privilege, grantedBy      string
		withOption, implicit, onColumns bool
	}
	joined := make(map[grantKey]*protocol.ObservedGrant)
	for _, row := range current.Grants {
		key := grantKey{role: row.Role, privilege: row.Privilege, grantedBy: row.GrantedBy, withOption: row.WithOption, implicit: row.Implicit, onColumns: row.Column != ""}
		grant, found := joined[key]
		if !found {
			grant = &protocol.ObservedGrant{Role: row.Role, Privilege: row.Privilege, WithGrantOption: row.WithOption, GrantedBy: row.GrantedBy,
				Implicit: row.Implicit, Target: &protocol.Reference{Engine: engine, Kind: "table", Schema: current.Table.Schema, Name: current.Table.Name}}
			joined[key] = grant
			result.Grants = append(result.Grants, grant)
		}
		if row.Column != "" && !slices.Contains(grant.Columns, row.Column) {
			grant.Columns = append(grant.Columns, row.Column)
		}
	}
	return result
}
