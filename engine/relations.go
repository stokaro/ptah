package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// RelationDiscovery assigns contextual dependency discovery to one model owner
// for a target and source representation. It does not enroll source coverage.
type RelationDiscovery struct {
	Target         string
	Representation schemaext.Representation
	Kinds          []schemaext.Kind
	Service        schemaext.RelationService
}

type relationKey struct {
	target         string
	representation schemaext.Representation
	kind           schemaext.Kind
}

func (r *Runtime) registerRelations(owner string, declaration RelationDiscovery) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || len(declaration.Kinds) == 0 || declaration.Service == nil || nilService(declaration.Service) ||
		(declaration.Representation != schemaext.Desired && declaration.Representation != schemaext.Observed) {
		return fmt.Errorf("%w: incomplete relation discovery registration", ErrInvalidRegistration)
	}
	index := len(r.relationServices)
	for _, kind := range declaration.Kinds {
		if !r.ownsCodec(owner, kind, declaration.Representation) {
			return fmt.Errorf("%w: relation discovery does not own %q/%q", ErrInvalidRegistration, declaration.Representation, kind)
		}
		key := relationKey{target.name, declaration.Representation, kind}
		if _, exists := r.relations[key]; exists {
			return fmt.Errorf("%w: duplicate relation discovery for %q/%q", ErrInvalidRegistration, target.name, kind)
		}
		r.relations[key] = index
	}
	declaration.Kinds = slices.Clone(declaration.Kinds)
	slices.Sort(declaration.Kinds)
	r.relationServices = append(r.relationServices, declaration)
	return nil
}

// CaptureRelations validates the complete source before calling selected owners.
// Each owner receives one batch for its assigned models, including an empty
// value list. Registration grows the required vocabulary, never the source's
// authority over it. Missing services, malformed replies, or cancellation return
// no snapshot. The returned capture retains complete definitions and coverage.
func (r *Runtime) CaptureRelations(ctx context.Context, request schemaext.RelationRequest) (schemaext.RelationSnapshot, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return schemaext.RelationSnapshot{}, err
	}
	target, found := r.lookup(request.Target)
	if !found {
		return schemaext.RelationSnapshot{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request.Target = target.name
	request.Kinds = r.relationKinds(request)
	request, err := r.codecs.SnapshotRelationRequest(ctx, request)
	if err != nil {
		return schemaext.RelationSnapshot{}, err
	}
	for _, kind := range request.Kinds {
		if _, found := r.relations[relationKey{target.name, request.Representation, kind}]; !found {
			return schemaext.RelationSnapshot{}, fmt.Errorf("%w: no relation discovery for %q/%q/%q", ptaherr.ErrUnsupportedFeature, target.name, request.Representation, kind)
		}
	}
	batches := make([][]int, len(r.relationServices))
	for i, value := range request.Values {
		service := r.relations[relationKey{target.name, request.Representation, value.Subject.Kind}]
		batches[service] = append(batches[service], i)
	}
	result := schemaext.RelationResult{Complete: true, Values: make([]schemaext.ValueRelations, len(request.Values))}
	for i, service := range r.relationServices {
		if service.Target != target.name || service.Representation != request.Representation {
			continue
		}
		reply, err := r.describeRelations(ctx, request, service, batches[i])
		if err != nil {
			return schemaext.RelationSnapshot{}, err
		}
		for j, position := range batches[i] {
			result.Values[position] = reply[j]
		}
	}
	return r.codecs.AcceptRelations(ctx, request, result)
}

func (r *Runtime) relationKinds(request schemaext.RelationRequest) []schemaext.Kind {
	var kinds []schemaext.Kind
	for _, service := range r.relationServices {
		if service.Target == request.Target && service.Representation == request.Representation {
			kinds = append(kinds, service.Kinds...)
		}
	}
	for _, record := range request.Coverage.KindRecords() {
		kinds = append(kinds, record.Model.Kind)
	}
	for _, value := range request.Values {
		kinds = append(kinds, value.Subject.Kind)
	}
	slices.Sort(kinds)
	return slices.Compact(kinds)
}

func (r *Runtime) describeRelations(ctx context.Context, request schemaext.RelationRequest, service RelationDiscovery, indices []int) ([]schemaext.ValueRelations, error) {
	batch := request
	batch.Kinds = slices.Clone(service.Kinds)
	batch.Coverage = request.Coverage.SelectKinds(service.Kinds)
	batch.Values = make([]schemaext.RelationValue, len(indices))
	for i, position := range indices {
		batch.Values[i] = request.Values[position]
	}
	input, err := r.codecs.SnapshotRelationRequest(ctx, batch)
	if err != nil {
		return nil, err
	}
	reply, err := service.Service.DescribeRelations(ctx, input)
	if err != nil {
		return nil, err
	}
	accepted, err := r.codecs.AcceptRelations(ctx, batch, reply)
	if err != nil {
		return nil, err
	}
	return accepted.Records(), nil
}
