package provider

import (
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// RoleScopeReasonsFeature enables ObservedCommonObjects.roles_out_of_scope_reason:
// the owner's own words for why a read leaves the roles it names out of scope
// out of the description. A host prints them in the note that counts those
// roles. It is a two-way feature. A provider lists it, and so does each engine
// that carries the ROLE kind and states reasons. Contract 25.26.
const RoleScopeReasonsFeature = "schema.role-scope-reasons"

// maxRoleScopeReasonBytes bounds a reason, which a host prints inside one
// sentence.
const maxRoleScopeReasonBytes = 300

// validateRoleScopeReasonsFeature requires an engine that lists the feature
// to have the provider's feature and to carry roles on the common-object wire.
func validateRoleScopeReasonsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), RoleScopeReasonsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), RoleScopeReasonsFeature) ||
		!slices.Contains(engine.GetCommonObjects().GetKinds(), protocol.CommonObjectSupport_ROLE) {
		return commonObjectError("role scope reasons need the provider's " + RoleScopeReasonsFeature + " and the engine's ROLE kind")
	}
	return nil
}

// validateRolesOutOfScopeReason checks the reason a read gives for the roles
// it leaves out of scope: present only when the engine lists the feature and
// the read names such a role, and then one trimmed line of bounded length
// that does not end with a period, since a host continues the sentence after
// it.
func validateRolesOutOfScopeReason(engine *protocol.Engine, observed *protocol.ObservedCommonObjects) error {
	reason := observed.GetRolesOutOfScopeReason()
	if reason == "" {
		return nil
	}
	if !slices.Contains(engine.GetFeatures(), RoleScopeReasonsFeature) {
		return commonObjectError("a reason for roles out of scope needs " + RoleScopeReasonsFeature)
	}
	if len(observed.GetRolesOutOfScope()) == 0 {
		return commonObjectError("a reason for roles out of scope needs a role out of scope")
	}
	if !comparisonReason(reason) || len(reason) > maxRoleScopeReasonBytes || strings.HasSuffix(reason, ".") {
		return commonObjectError("a reason for roles out of scope is one trimmed line of at most 300 bytes that does not end with a period")
	}
	return nil
}
