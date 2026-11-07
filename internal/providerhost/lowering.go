package providerhost

import (
	"context"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/uniquelowering"
)

// uniqueIndexLowering reads a declared UNIQUE constraint, and a column's
// UNIQUE, as the global unique index of an engine whose session states that
// it has no UNIQUE constraint (capability.UniqueConstraints false) and that
// reports the indexes its tables hold. YDB is such an engine: its reader
// reports the unique index a plan created for the constraint, so without the
// lowering a schema applied once would compare as a missing constraint and a
// stray index forever, and the provider would receive a constraint it has no
// way to create. The indexes take the names [uniquelowering.IndexName] gives
// them, which the provider gives the index it creates for a constraint.
//
// A session that does not state the key leaves the declaration as it is: the
// absence of an answer is not an engine without UNIQUE constraints.
type uniqueIndexLowering struct{}

// providerLowering is a provider engine's lowering before comparison: on an
// engine that does not carry namespaces, the refusal of a declared schema
// nothing would hand to the provider (see target.refuseUncarriedSchemas); its
// enum columns in the form the engine states (see target.enumColumns); then,
// for an engine that reports its indexes, its UNIQUE constraints as the
// unique indexes of uniqueIndexLowering.
type providerLowering struct {
	target *target
	// uniqueIndexes applies uniqueIndexLowering after the enum columns.
	uniqueIndexes bool
}

func (l providerLowering) LowerDesired(ctx context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := l.target.refuseUncarriedSchemas(request.Desired, request.Current, request.Semantics); err != nil {
		return nil, err
	}
	desired, err := l.target.enumColumns(request.Desired)
	if err != nil {
		return nil, err
	}
	if !l.uniqueIndexes {
		return desired, nil
	}
	request.Desired = desired
	return uniqueIndexLowering{}.LowerDesired(ctx, request)
}

func (uniqueIndexLowering) LowerDesired(ctx context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !request.Capabilities.Established(capability.UniqueConstraints) || request.Capabilities.Has(capability.UniqueConstraints) {
		return request.Desired, nil
	}
	return uniquelowering.ConstraintsAsIndexes(request.Desired), nil
}
