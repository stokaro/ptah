package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func (p *comparer) CompareModels(ctx context.Context, request *protocol.CompareModelsRequest) (*protocol.ComparedModels, error) {
	if err := validateRequestedSubjects(request); err != nil {
		return nil, err
	}
	result := &protocol.ComparedModels{Complete: true, Desired: proto.CloneOf(request.GetDesired())}
	for _, desired := range request.GetDesired().GetRecords() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if desired.GetValue() == nil {
			continue
		}
		after, err := p.seconds(ctx, request, request.GetDesired(), desired)
		if err != nil {
			return nil, err
		}
		current := findRecord(request.GetCurrent(), desired)
		if current == nil || current.GetValue() == nil || !known(request.GetCurrent().GetCoverage(), desired) {
			result.Undecided = append(result.Undecided, &protocol.ComparisonUndecided{Kind: desired.GetKind(), Subject: proto.CloneOf(desired.GetSubject()), Reason: "Current retention is unknown"})
			continue
		}
		before, err := p.seconds(ctx, request, request.GetCurrent(), current)
		if err != nil {
			return nil, err
		}
		if parentTransition(request, desired.GetSubject()) {
			continue
		}
		forced := slices.ContainsFunc(request.GetRequests(), func(action *protocol.RequestedChange) bool {
			return provider.CompareIdentities(action.GetSubject(), desired.GetSubject()) == 0 && action.GetAction() == "rotate"
		})
		if before == after && !forced {
			continue
		}
		change, err := p.change(request, desired, before, after)
		if err != nil {
			return nil, err
		}
		result.Changes = append(result.Changes, change)
	}
	for _, current := range request.GetCurrent().GetRecords() {
		if current.GetValue() == nil || findRecord(request.GetDesired(), current) != nil {
			continue
		}
		result.Undecided = append(result.Undecided, &protocol.ComparisonUndecided{Kind: current.GetKind(), Subject: proto.CloneOf(current.GetSubject()), Reason: "This fixture does not implement removal"})
	}
	return result, nil
}

func findRecord(state *protocol.ComparisonState, subject *protocol.ComparisonRecord) *protocol.ComparisonRecord {
	for _, record := range state.GetRecords() {
		if record.GetKind() == subject.GetKind() && provider.CompareIdentities(record.GetSubject(), subject.GetSubject()) == 0 {
			return record
		}
	}
	return nil
}

func parentTransition(request *protocol.CompareModelsRequest, subject *protocol.ComparisonIdentity) bool {
	for _, parent := range request.GetParents() {
		if provider.CompareIdentities(parent.GetSubject(), subject) == 0 {
			return !parent.GetDesired() || !parent.GetCurrent()
		}
	}
	return false
}

func (p *comparer) change(request *protocol.CompareModelsRequest, desired *protocol.ComparisonRecord, before, after uint64) (*protocol.ComparedChange, error) {
	payload, err := json.Marshal(retentionChange{Before: &before, After: &after})
	if err != nil {
		return nil, err
	}
	var model *protocol.ValueModel
	for _, candidate := range p.description.GetEngines()[0].GetValueModels() {
		if candidate.GetKind() == changeKind {
			model = candidate
		}
	}
	if model == nil {
		return nil, fmt.Errorf("change model is unavailable")
	}
	value := proto.CloneOf(desired.GetValue().GetValue())
	value.Kind, value.Payload, value.DefinitionSha256 = changeKind, payload, model.GetDefinitionSha256()
	normalized, err := normalizeChange(&protocol.ValueInput{Id: desired.GetValue().GetId(), Representation: protocol.ValueInput_CHANGE, Value: value})
	if err != nil {
		return nil, err
	}
	return &protocol.ComparedChange{Kind: desired.GetKind(), Subject: proto.CloneOf(desired.GetSubject()), Value: normalized}, nil
}

func known(coverage *protocol.ComparisonCoverage, subject *protocol.ComparisonRecord) bool {
	for _, record := range coverage.GetSubjects() {
		if record.GetKind() == subject.GetKind() && provider.CompareIdentities(record.GetSubject(), subject.GetSubject()) == 0 {
			return record.GetKnowledge().GetState() == protocol.ModelKnowledge_COMPLETE
		}
	}
	for _, record := range coverage.GetKinds() {
		if record.GetModel().GetKind() != subject.GetKind() {
			continue
		}
		if len(record.GetSources()) == 0 {
			return false
		}
		for _, source := range record.GetSources() {
			// This fixture's comparison only establishes global or subject knowledge.
			// Refusing scoped knowledge avoids inventing namespace matching semantics.
			if len(source.GetRegions()) > 0 || source.GetOutside().GetState() != protocol.ModelKnowledge_COMPLETE {
				return false
			}
		}
		return true
	}
	return false
}

func validateRequestedSubjects(request *protocol.CompareModelsRequest) error {
	for _, action := range request.GetRequests() {
		found := slices.ContainsFunc(request.GetDesired().GetRecords(), func(record *protocol.ComparisonRecord) bool {
			return record.GetValue() != nil && provider.CompareIdentities(record.GetSubject(), action.GetSubject()) == 0
		})
		if !found {
			return fmt.Errorf("explicit action requires a declared retention object")
		}
	}
	return nil
}
