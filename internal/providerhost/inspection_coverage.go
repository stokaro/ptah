package providerhost

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/schemaext"
)

func (r reader) observedCoverage(source *protocol.ModelCoverage) (schemaext.Coverage, error) {
	h := r.session.target.host
	var kinds []schemaext.KindCoverage
	for _, record := range source.GetKinds() {
		kind := schemaext.Kind(record.GetModel().GetKind())
		contract := h.models[modelKey{kind: kind, representation: schemaext.Observed}]
		if contract == nil {
			return schemaext.Coverage{}, fmt.Errorf("inspected coverage model is not registered")
		}
		knowledge, err := observedKnowledge(record.GetKnowledge())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		// A session establishes no global absence across other databases or engines.
		// Shared kind coverage has no namespace scope. Explicit subjects retain facts
		// until that representation can express the exact inspected namespace.
		if knowledge.State == schemaext.Complete {
			knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "inspection covers one session, not the global model namespace"}
		}
		kinds = append(kinds, schemaext.KindCoverage{Model: schemaext.CodecIdentity{Owner: h.description.GetProviderId(), Kind: kind,
			Representation: schemaext.Observed, Version: 1, Definition: contract.signature}, Knowledge: knowledge})
	}
	var subjects []schemaext.SubjectCoverage
	for _, record := range source.GetSubjects() {
		subject, err := r.observedIdentity(record.GetSubject(), record.GetNormalizedSubject())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		knowledge, err := observedKnowledge(record.GetKnowledge())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: schemaext.Kind(record.GetKind()), Subject: subject,
			Targets: []string{r.session.remote.GetEngine()}, Knowledge: knowledge})
	}
	return schemaext.NewCoverage(schemaext.Observed, kinds, subjects)
}

func observedKnowledge(value *protocol.ModelKnowledge) (schemaext.Knowledge, error) {
	states := map[protocol.ModelKnowledge_State]schemaext.KnowledgeState{
		protocol.ModelKnowledge_COMPLETE: schemaext.Complete, protocol.ModelKnowledge_ABSENT: schemaext.Absent,
		protocol.ModelKnowledge_UNINSPECTED: schemaext.Uninspected, protocol.ModelKnowledge_UNREPRESENTABLE: schemaext.Unrepresentable,
	}
	state, ok := states[value.GetState()]
	if !ok {
		return schemaext.Knowledge{}, fmt.Errorf("invalid observed knowledge")
	}
	return schemaext.Knowledge{State: state, Reason: value.GetReason()}, nil
}
