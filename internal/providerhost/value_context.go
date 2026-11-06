package providerhost

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

func databaseContext(value *protocol.DatabaseContext) schemaext.DatabaseContext {
	return schemaext.DatabaseContext{Database: value.GetDatabase(), SourceDatabase: value.GetSourceDatabase()}
}

func wireDatabaseContext(value schemaext.DatabaseContext) *protocol.DatabaseContext {
	if value.IsZero() {
		return nil
	}
	return &protocol.DatabaseContext{Database: value.Database, SourceDatabase: value.SourceDatabase}
}

func (t *target) ResolveDatabaseContext(ctx context.Context, request schemaext.DatabaseContextRequest) (schemaext.DatabaseContext, error) {
	if request.Target != t.engine.GetId() {
		return schemaext.DatabaseContext{}, fmt.Errorf("%w: database context target changed", schemaext.ErrInvalidValue)
	}
	value, err := t.host.client.ResolveDatabaseContext(ctx, &protocol.DatabaseContextRequest{Engine: request.Target, Address: request.Address})
	if err != nil {
		return schemaext.DatabaseContext{}, err
	}
	return databaseContext(value), nil
}

func (t *target) registerContextBindings(result *engine.Provider) {
	if !t.offers(provider.ValueContextFeature) {
		return
	}
	var kinds []schemaext.Kind
	for _, model := range t.engine.GetValueModels() {
		if model.GetRepresentation() == protocol.ValueInput_DESIRED || model.GetRepresentation() == protocol.ValueInput_OBSERVED {
			kinds = append(kinds, schemaext.Kind(model.GetKind()))
		}
	}
	slices.Sort(kinds)
	kinds = slices.Compact(kinds)
	if len(kinds) > 0 {
		result.ContextBindings = append(result.ContextBindings, engine.ContextBinding{Target: t.engine.GetId(), Kinds: kinds, Service: t})
	}
}

func (t *target) BindValueContexts(ctx context.Context, request schemaext.ValueContextRequest) ([]schemaext.Value, error) {
	if request.Target != t.engine.GetId() {
		return nil, fmt.Errorf("%w: value context target changed", schemaext.ErrInvalidValue)
	}
	if err := request.Context.Validate(); err != nil {
		return nil, err
	}
	bound := wireDatabaseContext(request.Context)
	from := wireDatabaseContext(request.From)
	remote := &protocol.NormalizeValuesRequest{Engine: request.Target}
	contracts := make([]*modelContract, len(request.Values))
	sources := make([]*resolvedModel, len(request.Values))
	owners := make([]*protocol.Reference, len(request.Values))
	normalizedOwners := make([]*protocol.Reference, len(request.Values))
	for i, value := range request.Values {
		contract := t.host.models[modelKey{kind: value.Kind(), representation: request.Representation}]
		if contract == nil || contract.engines[request.Target] == nil {
			return nil, fmt.Errorf("%w: context binding model is unavailable", schemaext.ErrInvalidValue)
		}
		captured, err := contract.value(value)
		if err != nil {
			return nil, err
		}
		features := featureSet(captured.wire.Features)
		if i == 0 {
			remote.TargetFeatures = features
		}
		if captured.wire.Engine != request.Target || !slices.Equal(features, remote.TargetFeatures) {
			return nil, fmt.Errorf("%w: context binding requires one captured target profile", schemaext.ErrInvalidValue)
		}
		input, err := boundInput(captured, bound, from)
		if err != nil {
			return nil, err
		}
		owners[i], normalizedOwners[i] = input.GetOwner(), movedOwner(captured.normalized.GetNormalizedOwner(), from, captured.normalized.GetValue().GetContext())
		remote.Values = append(remote.Values, &protocol.ValueInput{Id: strconv.Itoa(i), Representation: protocolRepresentation(request.Representation), Value: input})
		contracts[i], sources[i] = contract, captured
	}
	if len(remote.Values) == 0 {
		return nil, nil
	}
	normalized, err := t.host.client.NormalizeValues(ctx, remote)
	if err != nil {
		return nil, err
	}
	result := make([]schemaext.Value, len(normalized.GetValues()))
	for i, value := range normalized.GetValues() {
		// Binding may complete dependency facts, but neither owner spelling is
		// a database context carrier. Preserve both identities exactly, as a
		// move leaves them.
		if !proto.Equal(value.GetValue().GetOwner(), owners[i]) || !proto.Equal(value.GetNormalizedOwner(), normalizedOwners[i]) || value.GetRequiresDatabaseContext() {
			return nil, fmt.Errorf("%w: context binding changed an owner or left dependencies unresolved", provider.ErrProtocol)
		}
		resolved, err := contracts[i].resolved(sources[i].wire, value)
		if err != nil {
			return nil, err
		}
		result[i] = resolved
	}
	return result, nil
}

// boundInput is the captured value as a binding to bound sends it: a value
// bound to no context or to bound already takes bound, and one bound to from
// moves, its owner as [movedOwner] names it. A value bound to any other
// context is refused.
func boundInput(captured *resolvedModel, bound, from *protocol.DatabaseContext) (*protocol.Envelope, error) {
	input := proto.CloneOf(captured.normalized.GetValue())
	if input.GetContext() != nil && !proto.Equal(input.GetContext(), bound) && (from == nil || !proto.Equal(input.GetContext(), from)) {
		return nil, fmt.Errorf("%w: model database context differs from the selected target", schemaext.ErrIncompatibleCodec)
	}
	input.Owner = movedOwner(input.GetOwner(), from, input.GetContext())
	input.Context = proto.CloneOf(bound)
	return input, nil
}

// movedOwner is owner as a value bound to captured names it once the value
// moves out of from: when captured is from, an owner that names from's
// database names its object relative to the database instead, as a session
// reads an object of its own database. Any other owner is kept, and so is
// every owner of a value that does not move. The relative name means the
// object at the same place in the database the value moves to.
func movedOwner(owner *protocol.Reference, from, captured *protocol.DatabaseContext) *protocol.Reference {
	if owner == nil || from == nil || !proto.Equal(captured, from) || owner.GetDatabase() != from.GetDatabase() {
		return owner
	}
	moved := proto.CloneOf(owner)
	moved.Database = ""
	return moved
}
