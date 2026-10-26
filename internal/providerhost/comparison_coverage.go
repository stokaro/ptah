package providerhost

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func (t *target) comparisonCoverage(representation schemaext.Representation, coverage schemaext.Coverage) (*protocol.ComparisonCoverage, error) {
	result := &protocol.ComparisonCoverage{}
	for _, record := range coverage.KindRecords() {
		contract := t.coverageModel(representation, record.Model)
		if contract == nil || contract.engines[t.engine.GetId()] == nil {
			return nil, fmt.Errorf("%w: comparison coverage model changed", schemaext.ErrIncompatibleCodec)
		}
		kind := &protocol.ComparisonKindCoverage{Model: proto.CloneOf(contract.engines[t.engine.GetId()])}
		for _, captured := range record.Knowledge.Sources() {
			source := &protocol.ComparisonKnowledgeSource{Outside: comparisonKnowledge(captured.Outside)}
			for _, region := range captured.Regions {
				source.Regions = append(source.Regions, &protocol.ComparisonRegion{Scope: comparisonNamespace(region.Scope), Knowledge: comparisonKnowledge(region.Knowledge)})
			}
			kind.Sources = append(kind.Sources, source)
		}
		result.Kinds = append(result.Kinds, kind)
	}
	for _, record := range coverage.SubjectRecords() {
		result.Subjects = append(result.Subjects,
			&protocol.ComparisonSubjectCoverage{Kind: string(record.Kind),
				Subject:   comparisonIdentity(record.Subject),
				Knowledge: comparisonKnowledge(record.Knowledge),
				Targets:   slices.Clone(record.Targets)})
	}
	return result, nil
}

// coverageModel validates model ownership independently of engine assignment.
// Planning routes valid knowledge for another engine away from this target,
// while comparison still requires the selected engine's assignment.
func (t *target) coverageModel(representation schemaext.Representation, model schemaext.CodecIdentity) *modelContract {
	contract := t.host.models[modelKey{kind: model.Kind, representation: representation}]
	if contract == nil || model.Owner != t.host.description.GetProviderId() || model.Representation != representation ||
		model.Version != 1 || model.Definition != contract.signature {
		return nil
	}
	return contract
}

func (t *target) boundCoverage(representation schemaext.Representation, coverage *protocol.ComparisonCoverage) (schemaext.Coverage, error) {
	kinds := make([]schemaext.KindCoverage, 0, len(coverage.GetKinds()))
	for _, record := range coverage.GetKinds() {
		kind := schemaext.Kind(record.GetModel().GetKind())
		contract := t.host.models[modelKey{kind: kind, representation: representation}]
		if contract == nil {
			return schemaext.Coverage{}, fmt.Errorf("%w: comparison returned an unregistered coverage model", schemaext.ErrIncompatibleCodec)
		}
		domain, err := boundKnowledgeDomain(record.GetSources())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		kinds = append(kinds, schemaext.KindCoverage{Model: schemaext.CodecIdentity{Owner: t.host.description.GetProviderId(), Kind: kind, Representation: representation, Version: 1, Definition: contract.signature}, Knowledge: domain})
	}
	subjects := make([]schemaext.SubjectCoverage, 0, len(coverage.GetSubjects()))
	for _, record := range coverage.GetSubjects() {
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: schemaext.Kind(record.GetKind()), Subject: boundIdentity(record.GetSubject()), Knowledge: boundKnowledge(record.GetKnowledge()), Targets: slices.Clone(record.GetTargets())})
	}
	return schemaext.NewCoverage(representation, kinds, subjects)
}

func boundKnowledgeDomain(sources []*protocol.ComparisonKnowledgeSource) (schemaext.KnowledgeDomain, error) {
	var domain schemaext.KnowledgeDomain
	for i, source := range sources {
		regions := make([]schemaext.NamespaceKnowledge, 0, len(source.GetRegions()))
		for _, region := range source.GetRegions() {
			regions = append(regions, schemaext.NamespaceKnowledge{Scope: boundNamespace(region.GetScope()), Knowledge: boundKnowledge(region.GetKnowledge())})
		}
		captured, err := schemaext.NewScopedKnowledge(boundKnowledge(source.GetOutside()), regions...)
		if err != nil {
			return schemaext.KnowledgeDomain{}, err
		}
		if i == 0 {
			domain = captured
			continue
		}
		domain, err = domain.Intersect(captured)
		if err != nil {
			return schemaext.KnowledgeDomain{}, err
		}
	}
	return domain, nil
}

func comparisonKnowledge(knowledge schemaext.Knowledge) *protocol.ModelKnowledge {
	states := map[schemaext.KnowledgeState]protocol.ModelKnowledge_State{schemaext.Uninspected: protocol.ModelKnowledge_UNINSPECTED,
		schemaext.Complete:        protocol.ModelKnowledge_COMPLETE,
		schemaext.Absent:          protocol.ModelKnowledge_ABSENT,
		schemaext.Defaulted:       protocol.ModelKnowledge_DEFAULTED,
		schemaext.Unrepresentable: protocol.ModelKnowledge_UNREPRESENTABLE}
	return &protocol.ModelKnowledge{State: states[knowledge.State], Reason: knowledge.Reason}
}

func boundKnowledge(knowledge *protocol.ModelKnowledge) schemaext.Knowledge {
	states := map[protocol.ModelKnowledge_State]schemaext.KnowledgeState{protocol.ModelKnowledge_UNINSPECTED: schemaext.Uninspected,
		protocol.ModelKnowledge_COMPLETE:        schemaext.Complete,
		protocol.ModelKnowledge_ABSENT:          schemaext.Absent,
		protocol.ModelKnowledge_DEFAULTED:       schemaext.Defaulted,
		protocol.ModelKnowledge_UNREPRESENTABLE: schemaext.Unrepresentable}
	return schemaext.Knowledge{State: states[knowledge.GetState()], Reason: knowledge.GetReason()}
}

func comparisonNamespace(scope schemaext.NamespaceScope) *protocol.ComparisonNamespace {
	result := &protocol.ComparisonNamespace{Targets: slices.Clone(scope.Targets), PathPrefix: comparisonParts(scope.PathPrefix), PathExact: scope.PathExact}
	for _, kind := range scope.SubjectKinds {
		result.SubjectKinds = append(result.SubjectKinds, string(kind))
	}
	for _, binding := range []struct {
		source      *objectidentity.Part
		destination **protocol.ComparisonPart
	}{
		{scope.Database, &result.Database}, {scope.Catalog, &result.Catalog}, {scope.Schema, &result.Schema}, {scope.Parent, &result.Parent},
	} {
		if binding.source != nil {
			*binding.destination = comparisonPart(*binding.source)
		}
	}

	return result
}

func boundNamespace(scope *protocol.ComparisonNamespace) schemaext.NamespaceScope {
	result := schemaext.NamespaceScope{Targets: slices.Clone(scope.GetTargets()), PathPrefix: boundParts(scope.GetPathPrefix()), PathExact: scope.GetPathExact()}
	for _, kind := range scope.GetSubjectKinds() {
		result.SubjectKinds = append(result.SubjectKinds, objectidentity.Kind(kind))
	}
	for _, binding := range []struct {
		source      *protocol.ComparisonPart
		destination **objectidentity.Part
	}{
		{scope.GetDatabase(), &result.Database}, {scope.GetCatalog(), &result.Catalog}, {scope.GetSchema(), &result.Schema}, {scope.GetParent(), &result.Parent},
	} {
		if binding.source != nil {
			part := boundPart(binding.source)
			*binding.destination = &part
		}
	}

	return result
}
