package pgpolicy

import (
	"slices"

	"ptah.run/core/schemaext"
)

// Access reasons the owner records. They name what the change can do, which
// is what an access assessment states: a widening can grant access, and does
// whenever the table enforces row security.
const (
	reasonPermissiveCreated  = "a permissive policy can admit rows to the roles it names"
	reasonRestrictiveCreated = "a restrictive policy can hide rows from the roles it names"
	reasonPermissiveDropped  = "removing a permissive policy can hide the rows it admitted"
	reasonRestrictiveDropped = "removing a restrictive policy can reveal the rows it hid"
	reasonNowRestrictive     = "a permissive policy made restrictive can hide rows it admitted"
	reasonNowPermissive      = "a restrictive policy made permissive can admit rows it hid"
	reasonCommandsAdded      = "the policy applies to commands it did not apply to"
	reasonCommandsRemoved    = "the policy no longer applies to commands it applied to"
	reasonRolesAdded         = "the policy applies to roles it did not apply to"
	reasonRolesRemoved       = "the policy no longer applies to roles it applied to"
	reasonRolesUnresolved    = "a role keyword the server resolves when the policy is created makes its roles unknown here"
	reasonExpressionChanged  = "a USING or WITH CHECK expression changed, and which rows it admits is not established"
	reasonCommentOnly        = "only the policy's comment changes"
	reasonRLSEnabled         = "enabling row security hides every row no policy admits"
	reasonRLSDisabled        = "disabling row security reveals every row its policies hid"
	reasonRLSForced          = "FORCE ROW LEVEL SECURITY subjects the table's owner to its policies"
	reasonRLSUnforced        = "NO FORCE ROW LEVEL SECURITY exempts the table's owner from its policies"
	reasonFlagsUnenforced    = "the table enforces row security neither before nor after the change"
)

// accessRank orders assessments from the weakest claim to the strongest, the
// order a statement carrying several reports the strongest of.
func accessRank(access schemaext.Access) int {
	switch access {
	case schemaext.AccessUnchanged:
		return 0
	case schemaext.AccessNarrows:
		return 1
	case schemaext.AccessUnknown:
		return 2
	case schemaext.AccessWidens:
		return 3
	default:
		return 2
	}
}

// strongest returns the strongest of effects, the first one among equals. An
// empty list is an unchanged assessment with fallback as its reason.
func strongest(fallback string, effects ...schemaext.AccessEffect) schemaext.AccessEffect {
	if len(effects) == 0 {
		return schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: fallback}
	}
	result := effects[0]
	for _, effect := range effects[1:] {
		if accessRank(effect.Access) > accessRank(result.Access) {
			result = effect
		}
	}
	return result
}

// ExpressionFinding is what a comparison established about the USING and WITH
// CHECK expressions of a changed policy. Their text cannot say: the catalog
// respells what an author wrote.
type ExpressionFinding int

const (
	// ExpressionsDiffer records expressions the comparison did not find equal.
	ExpressionsDiffer ExpressionFinding = iota
	// ExpressionsSame records expressions the comparison found equal.
	ExpressionsSame
)

// PolicyAccess assesses a policy change from its operands: what the change can
// do to the rows the roles it names may see or write, when the table enforces
// row security. A nil before creates the policy and a nil after drops it.
//
// The assessment does not depend on whether the table enforces row security,
// because a policy on a table that does not can start applying when row
// security is enabled. A planner that holds the table's switches can narrow it
// with [UnenforcedAccess]. expressions is the comparison's finding about the
// USING and WITH CHECK expressions. A changed expression is unknown, because
// which rows it admits is not established. The declared roles are compared
// through [DesiredPolicy.ComparedRoles], so a keyword a probe resolved counts
// as the role it names.
func PolicyAccess(before *ObservedPolicy, after *DesiredPolicy, expressions ExpressionFinding) schemaext.AccessEffect {
	switch {
	case before == nil && after == nil:
		return schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: reasonExpressionChanged}
	case before == nil:
		return createdAccess(after.EffectiveComposition())
	case after == nil:
		return droppedAccess(before.Composition)
	}
	composition := after.EffectiveComposition()
	if composition != before.Composition {
		if composition == Restrictive {
			return schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: reasonNowRestrictive}
		}
		return schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: reasonNowPermissive}
	}
	var effects []schemaext.AccessEffect
	effects = append(effects, scopeEffects(composition, commandDifference(before.Command, after.EffectiveCommand()), reasonCommandsAdded, reasonCommandsRemoved)...)
	effects = append(effects, roleEffects(composition, before.Roles, after.ComparedRoles())...)
	if expressions != ExpressionsSame {
		effects = append(effects, schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: reasonExpressionChanged})
	}
	return strongest(reasonCommentOnly, effects...)
}

// TableStateAccess assesses a change to a surviving table's switches. FORCE
// matters only while row security is enabled, so a FORCE change on a table that
// enforces it neither before nor after is unchanged.
func TableStateAccess(before *ObservedTableState, after *DesiredTableState) schemaext.AccessEffect {
	if before == nil || after == nil {
		return schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: reasonFlagsUnenforced}
	}
	var effects []schemaext.AccessEffect
	switch {
	case !before.Enabled && after.Enabled:
		effects = append(effects, schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: reasonRLSEnabled})
	case before.Enabled && !after.Enabled:
		effects = append(effects, schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: reasonRLSDisabled})
	}
	switch {
	case !before.Forced && after.Forced && after.Enabled:
		effects = append(effects, schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: reasonRLSForced})
	case before.Forced && !after.Forced && before.Enabled:
		effects = append(effects, schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: reasonRLSUnforced})
	}
	return strongest(reasonFlagsUnenforced, effects...)
}

// UnenforcedAccess is the assessment of a policy change on a table that
// enforces row security neither before nor after the plan: the change admits
// and hides nothing, because no policy of the table applies.
func UnenforcedAccess() schemaext.AccessEffect {
	return schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: reasonFlagsUnenforced}
}

func createdAccess(composition Composition) schemaext.AccessEffect {
	if composition == Restrictive {
		return schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: reasonRestrictiveCreated}
	}
	return schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: reasonPermissiveCreated}
}

func droppedAccess(composition Composition) schemaext.AccessEffect {
	if composition == Restrictive {
		return schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: reasonRestrictiveDropped}
	}
	return schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: reasonPermissiveDropped}
}

// reach records whether a change makes a policy reach something it did not,
// and the other way round.
type reach struct {
	added, removed bool
}

// scopeEffects reports a policy reaching more or fewer commands or roles. For
// a permissive policy, reaching more can grant access; for a restrictive one,
// reaching more can only withhold it.
func scopeEffects(composition Composition, scope reach, addedReason, removedReason string) []schemaext.AccessEffect {
	grows, shrinks := schemaext.AccessWidens, schemaext.AccessNarrows
	if composition == Restrictive {
		grows, shrinks = schemaext.AccessNarrows, schemaext.AccessWidens
	}
	var effects []schemaext.AccessEffect
	if scope.added {
		effects = append(effects, schemaext.AccessEffect{Access: grows, Reason: addedReason})
	}
	if scope.removed {
		effects = append(effects, schemaext.AccessEffect{Access: shrinks, Reason: removedReason})
	}
	return effects
}

// commandDifference reports whether after reaches a command before did not,
// and the other way round. ALL reaches every command.
func commandDifference(before, after Command) reach {
	beforeSet, afterSet := commandSet(before), commandSet(after)
	var result reach
	for _, command := range afterSet {
		result.added = result.added || !slices.Contains(beforeSet, command)
	}
	for _, command := range beforeSet {
		result.removed = result.removed || !slices.Contains(afterSet, command)
	}
	return result
}

func commandSet(command Command) []Command {
	if command == CommandAll {
		return []Command{CommandSelect, CommandInsert, CommandUpdate, CommandDelete}
	}
	return []Command{command}
}

// roleEffects compares the roles a policy reaches. PUBLIC reaches every role,
// so a list with it reaches at least what any other list does. A keyword the
// server resolves when the policy is created cannot be compared offline.
func roleEffects(composition Composition, before, after []RoleSelector) []schemaext.AccessEffect {
	if slices.ContainsFunc(after, resolvedAtCreation) || slices.ContainsFunc(before, resolvedAtCreation) {
		return []schemaext.AccessEffect{{Access: schemaext.AccessUnknown, Reason: reasonRolesUnresolved}}
	}
	public := RoleSelector{Keyword: Public}
	beforePublic, afterPublic := slices.Contains(before, public), slices.Contains(after, public)
	var scope reach
	switch {
	case beforePublic && afterPublic:
	case afterPublic:
		scope.added = true
	case beforePublic:
		scope.removed = true
	default:
		for _, role := range after {
			scope.added = scope.added || !slices.Contains(before, role)
		}
		for _, role := range before {
			scope.removed = scope.removed || !slices.Contains(after, role)
		}
	}
	return scopeEffects(composition, scope, reasonRolesAdded, reasonRolesRemoved)
}

func resolvedAtCreation(role RoleSelector) bool {
	return role.Keyword == CurrentRole || role.Keyword == CurrentUser || role.Keyword == SessionUser
}

// EffectiveCommand is the command the declaration requests: ALL when it names
// none.
func (v *DesiredPolicy) EffectiveCommand() Command {
	if v.Command == "" {
		return CommandAll
	}
	return v.Command
}

// ComparedRoles is the role list a comparison holds against an observation:
// the server's resolution where a probe attached one, and the
// [DesiredPolicy.EffectiveRoles] otherwise. A keyword the server resolves when
// the policy is created stays a keyword without a probe, so it never equals a
// role name. The result is a new slice.
func (v *DesiredPolicy) ComparedRoles() []RoleSelector {
	if v.Normalized != nil {
		return slices.Clone(v.Normalized.Roles)
	}
	return v.EffectiveRoles()
}

// EffectiveRoles is the role list the declaration requests: PUBLIC when it
// names none. The result is a new slice.
func (v *DesiredPolicy) EffectiveRoles() []RoleSelector {
	if v.Roles == nil {
		return []RoleSelector{{Keyword: Public}}
	}
	return slices.Clone(v.Roles)
}

// EffectiveComposition is the composition the declaration requests:
// permissive when it names none.
func (v *DesiredPolicy) EffectiveComposition() Composition {
	if v.Composition == "" {
		return Permissive
	}
	return v.Composition
}
