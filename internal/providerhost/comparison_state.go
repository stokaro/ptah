package providerhost

import (
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// A semantic batch cannot mix captured engine or feature profiles. The builder
// retains whether an empty profile was established by a concrete value.
type comparisonCapture struct {
	target   *target
	request  *protocol.CompareModelsRequest
	captured bool
}

func (c *comparisonCapture) value(representation schemaext.Representation, payload schemaext.Payload, id string) (*protocol.NormalizedValue, error) {
	contract := c.target.host.models[modelKey{kind: payload.Kind(), representation: representation}]
	if contract == nil || contract.engines[c.request.GetEngine()] == nil {
		return nil, fmt.Errorf("%w: comparison model is unavailable", schemaext.ErrInvalidValue)
	}
	value, err := contract.value(payload)
	if err != nil {
		return nil, err
	}
	features := featureSet(value.wire.Features)
	if !c.captured {
		c.request.TargetFeatures = features
		c.captured = true
	}
	if value.wire.Engine != c.request.GetEngine() || !slices.Equal(features, c.request.GetTargetFeatures()) || value.normalized == nil {
		return nil, fmt.Errorf("%w: comparison requires one captured target profile", schemaext.ErrInvalidValue)
	}
	result := proto.CloneOf(value.normalized)
	result.Id = id
	return result, nil
}

func (c *comparisonCapture) objects(representation schemaext.Representation, state schemaext.ObjectState) (*protocol.ComparisonState, error) {
	coverage, err := c.target.comparisonCoverage(representation, state.Coverage)
	if err != nil {
		return nil, err
	}
	result := &protocol.ComparisonState{Representation: protocolRepresentation(representation), Coverage: coverage}
	objects, err := state.Objects.All()
	if err != nil {
		return nil, err
	}
	for i, object := range objects {
		value, err := c.value(representation, object.Value, strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		result.Records = append(result.Records, &protocol.ComparisonRecord{Kind: string(object.Value.Kind()), Subject: comparisonIdentity(object.Ref), Targets: slices.Clone(object.Targets), Value: value})
	}
	return result, nil
}

func (c *comparisonCapture) facets(representation schemaext.Representation, state schemaext.FacetState) (*protocol.ComparisonState, error) {
	coverage, err := c.target.comparisonCoverage(representation, state.Coverage)
	if err != nil {
		return nil, err
	}
	result := &protocol.ComparisonState{Representation: protocolRepresentation(representation), Coverage: coverage}
	for _, record := range state.Records {
		for _, kind := range record.Values.DeclaredKinds() {
			captured := &protocol.ComparisonRecord{Kind: string(kind), Subject: comparisonIdentity(record.Subject), Targets: record.Values.TargetScope(kind)}
			value, found, err := record.Values.Get(kind)
			if err != nil {
				return nil, err
			}
			if found {
				captured.Value, err = c.value(representation, value, strconv.Itoa(len(result.Records)))
				if err != nil {
					return nil, err
				}
			}
			result.Records = append(result.Records, captured)
		}
	}
	return result, nil
}

func (s modelComparer) resolved(request *protocol.CompareModelsRequest, representation schemaext.Representation, value *protocol.NormalizedValue) (*resolvedModel, error) {
	contract := s.target.host.models[modelKey{kind: schemaext.Kind(value.GetValue().GetKind()), representation: representation}]
	if contract == nil || contract.engines[request.GetEngine()] == nil {
		return nil, fmt.Errorf("%w: returned comparison model is unavailable", schemaext.ErrInvalidValue)
	}
	return contract.resolved(modelWire{Format: 1, Engine: request.GetEngine(), Features: slices.Clone(request.GetTargetFeatures())}, value)
}

func (s modelComparer) boundObjects(request *protocol.CompareModelsRequest, state *protocol.ComparisonState) (schemaext.ObjectState, error) {
	coverage, err := s.target.boundCoverage(schemaext.Desired, state.GetCoverage())
	if err != nil {
		return schemaext.ObjectState{}, err
	}
	objects := make([]schemaext.Object, 0, len(state.GetRecords()))
	for _, record := range state.GetRecords() {
		value, err := s.resolved(request, schemaext.Desired, record.GetValue())
		if err != nil {
			return schemaext.ObjectState{}, err
		}
		objects = append(objects, schemaext.Object{Ref: boundIdentity(record.GetSubject()), Value: value, Targets: slices.Clone(record.GetTargets())})
	}
	captured, err := schemaext.NewObjects(objects...)
	if err != nil {
		return schemaext.ObjectState{}, err
	}
	return schemaext.ObjectState{Objects: captured, Coverage: coverage}, nil
}

func (s modelComparer) boundChanges(request *protocol.CompareModelsRequest, changes []*protocol.ComparedChange) ([]schemaext.ChangeRecord, error) {
	result := make([]schemaext.ChangeRecord, 0, len(changes))
	for _, change := range changes {
		model, err := s.resolved(request, schemaext.Change, change.GetValue())
		if err != nil {
			return nil, err
		}
		result = append(result, schemaext.ChangeRecord{Subject: boundIdentity(change.GetSubject()), Value: &resolvedChange{model: model}})
	}
	return result, nil
}

func (s modelComparer) boundFacets(request *protocol.CompareModelsRequest, state *protocol.ComparisonState, original schemaext.FacetState) (schemaext.FacetState, error) {
	coverage, err := s.target.boundCoverage(schemaext.Desired, state.GetCoverage())
	if err != nil {
		return schemaext.FacetState{}, err
	}
	records := make(map[objectidentity.Key]schemaext.FacetRecord)
	// Exclusions have no values to insert. Keep the immutable original exclusion
	// records after SDK validation proves they survived the owner response.
	for _, record := range original.Records {
		for _, kind := range record.Values.Kinds() {
			record.Values = record.Values.Without(kind)
		}
		if len(record.Values.DeclaredKinds()) > 0 {
			records[record.Subject.Key()] = record
		}
	}
	for _, captured := range state.GetRecords() {
		if captured.GetValue() == nil {
			continue
		}
		subject := boundIdentity(captured.GetSubject())
		record := records[subject.Key()]
		record.Subject = subject
		value, err := s.resolved(request, schemaext.Desired, captured.GetValue())
		if err != nil {
			return schemaext.FacetState{}, err
		}
		record.Values, err = record.Values.With(value)
		if err != nil {
			return schemaext.FacetState{}, err
		}
		record.Values, err = record.Values.WithTargetScope(value.Kind(), captured.GetTargets()...)
		if err != nil {
			return schemaext.FacetState{}, err
		}
		records[subject.Key()] = record
	}
	result := schemaext.FacetState{Coverage: coverage, Records: make([]schemaext.FacetRecord, 0, len(records))}
	for _, record := range records {
		result.Records = append(result.Records, record)
	}
	slices.SortFunc(result.Records, func(a, b schemaext.FacetRecord) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	return result, nil
}
