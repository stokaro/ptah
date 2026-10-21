package providerhost

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// resolvedChange carries owner-computed facts with a self-contained payload.
// All interface methods are local; decoding revalidates the payload with its owner.
type resolvedChange struct{ model *resolvedModel }

func (v *resolvedChange) Kind() schemaext.Kind { return v.model.Kind() }
func (v *resolvedChange) CloneChange() schemaext.ChangeValue {
	return &resolvedChange{model: v.model.cloneModel()}
}
func (v *resolvedChange) ReplacesOwner() bool {
	return v.model.normalized.GetChangeSemantics().GetReplacesOwner()
}
func (v *resolvedChange) RemovesState() bool {
	return v.model.normalized.GetChangeSemantics().GetRemovesState()
}
func (v *resolvedChange) Effect() schemaext.Effect {
	facts := v.model.normalized.GetChangeSemantics()
	impacts := map[protocol.ChangeSemantics_Impact]schemaext.Impact{protocol.ChangeSemantics_ADDITIVE: schemaext.Additive, protocol.ChangeSemantics_BEHAVIORAL: schemaext.Behavioral, protocol.ChangeSemantics_DESTRUCTIVE: schemaext.Destructive}
	return schemaext.Effect{Impact: impacts[facts.GetImpact()], Reason: facts.GetReason()}
}
func (v *resolvedChange) AccessEffect() schemaext.AccessEffect {
	facts := v.model.normalized.GetChangeSemantics()
	assessments := map[protocol.ChangeSemantics_Access]schemaext.Access{protocol.ChangeSemantics_ACCESS_UNKNOWN: schemaext.AccessUnknown,
		protocol.ChangeSemantics_UNCHANGED: schemaext.AccessUnchanged,
		protocol.ChangeSemantics_WIDENS:    schemaext.AccessWidens,
		protocol.ChangeSemantics_NARROWS:   schemaext.AccessNarrows}
	return schemaext.AccessEffect{Access: assessments[facts.GetAccess()], Reason: facts.GetAccessReason()}
}

func (v *resolvedModel) Requires() []objectidentity.ID {
	result := make([]objectidentity.ID, len(v.normalized.GetRequiredObjects()))
	for i, subject := range v.normalized.GetRequiredObjects() {
		result[i] = boundIdentity(subject)
	}
	slices.SortFunc(result, schemaext.CompareRefs)
	return result
}

func (m *modelContract) payload(value *resolvedModel) schemaext.Payload {
	if m.key.representation == schemaext.Change {
		return &resolvedChange{model: value}
	}
	return value
}

type changeFacts struct {
	ReplacesOwner bool   `json:"replaces_owner"`
	RemovesState  bool   `json:"removes_state"`
	Impact        string `json:"impact"`
	Reason        string `json:"reason"`
	Access        string `json:"access"`
	AccessReason  string `json:"access_reason"`
}

func normalizedChangeFacts(facts *protocol.ChangeSemantics) *changeFacts {
	if facts == nil {
		return nil
	}
	return &changeFacts{ReplacesOwner: facts.GetReplacesOwner(), RemovesState: facts.GetRemovesState(), Impact: facts.GetImpact().String(), Reason: facts.GetReason(), Access: facts.GetAccess().String(), AccessReason: facts.GetAccessReason()}
}

// Dependency equality uses bound names. Source spelling and quote/default
// markers remain available through Requires, but cannot change a fingerprint.
type dependencyIdentity struct {
	Kind      string   `json:"kind"`
	Database  string   `json:"database"`
	Catalog   string   `json:"catalog"`
	Schema    string   `json:"schema"`
	Parent    string   `json:"parent"`
	Name      string   `json:"name"`
	Path      []string `json:"path"`
	Signature []string `json:"signature"`
}

func (v *resolvedModel) dependencyIdentities() []dependencyIdentity {
	required := v.Requires()
	result := make([]dependencyIdentity, len(required))
	for i, subject := range required {
		result[i] = dependencyIdentity{Kind: string(subject.Kind), Database: subject.Database.Normalized, Catalog: subject.Catalog.Normalized,
			Schema: subject.Schema.Normalized, Parent: subject.Parent.Normalized, Name: subject.Name.Normalized,
			Path: normalizedParts(subject.Namespace), Signature: normalizedParts(subject.Signature)}
	}
	return result
}

func normalizedParts(parts objectidentity.Parts) []string {
	values := parts.Values()
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = value.Normalized
	}
	return result
}
