package conformance

import (
	"fmt"
	"slices"
	"strings"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// ModelIdentity is the identity under which a document captures a claim of
// one of a provider's models: the owner, the engine that declares the model,
// its kind and representation, its schema version and the digest of its
// definition. A file a host exported names a model this way, so an identity
// one release described has to read under the next.
type ModelIdentity struct {
	Owner            string `json:"owner"`
	Engine           string `json:"engine"`
	Kind             string `json:"kind"`
	Representation   string `json:"representation"`
	SchemaVersion    uint32 `json:"schema_version"`
	DefinitionSHA256 string `json:"definition_sha256"`
}

func (m ModelIdentity) String() string {
	return fmt.Sprintf("%s %s %s of %s, schema version %d, sha256:%s", m.Engine, m.Representation, m.Kind, m.Owner, m.SchemaVersion, m.DefinitionSHA256)
}

// ModelIdentities returns the identities of the desired and observed models
// a description declares, sorted by engine, kind and representation: the
// models a captured claim names. An operation or a change binds to the
// release that planned it, so a later release owes it no reading. An author
// records the identities for a release, as JSON for example, and passes them
// to the suite as Config.Models when checking the next release.
func ModelIdentities(description *protocol.Description) []ModelIdentity {
	var identities []ModelIdentity
	for _, engine := range description.GetEngines() {
		for _, model := range engine.GetValueModels() {
			if model.GetRepresentation() != protocol.ValueInput_DESIRED && model.GetRepresentation() != protocol.ValueInput_OBSERVED {
				continue
			}
			identities = append(identities, ModelIdentity{
				Owner: description.GetProviderId(), Engine: engine.GetId(), Kind: model.GetKind(), Representation: model.GetRepresentation().String(),
				SchemaVersion: model.GetSchemaVersion(), DefinitionSHA256: model.GetDefinitionSha256(),
			})
		}
	}
	slices.SortFunc(identities, func(a, b ModelIdentity) int {
		return strings.Compare(a.Engine+"\x00"+a.Kind+"\x00"+a.Representation, b.Engine+"\x00"+b.Kind+"\x00"+b.Representation)
	})
	return identities
}

// models checks that every recorded identity still reads: it is the
// identity of a model the provider describes, or a predecessor a model of
// the same engine names. A predecessor reaches only a host that lists
// models.predecessors, and one of a model's own earlier identities only a
// host that also lists models.own-predecessors, so the full description is
// the one to read.
func (r *run) models() {
	if len(r.config.Models) == 0 {
		r.add(checkModels, NotApplicable, "the configuration records no model identity")
		return
	}
	current := ModelIdentities(r.full)
	var lost []string
	for _, recorded := range r.config.Models {
		if slices.Contains(current, recorded) || r.predecessor(recorded) {
			continue
		}
		lost = append(lost, recorded.String()+" no longer reads: the provider neither describes it nor names it as a predecessor")
	}
	r.result(checkModels, lost)
}

func (r *run) predecessor(recorded ModelIdentity) bool {
	for _, engine := range r.full.GetEngines() {
		if engine.GetId() != recorded.Engine {
			continue
		}
		for _, former := range engine.GetModelPredecessors() {
			if former.GetFormerOwner() == recorded.Owner && former.GetFormerKind() == recorded.Kind &&
				former.GetSchemaVersion() == recorded.SchemaVersion && former.GetDefinitionSha256() == recorded.DefinitionSHA256 &&
				former.GetRepresentation().String() == recorded.Representation {
				return true
			}
		}
	}
	return false
}
