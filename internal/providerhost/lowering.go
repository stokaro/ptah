package providerhost

import (
	"context"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/ydblowering"
)

// uniqueIndexLowering reads a declared UNIQUE constraint, and a column's
// UNIQUE, as the global unique index of an engine whose session states that
// it has no UNIQUE constraint (capability.UniqueConstraints false) and that
// reports the indexes its tables hold. YDB is such an engine: its reader
// reports the unique index a plan created for the constraint, so without the
// lowering a schema applied once would compare as a missing constraint and a
// stray index forever, and the provider would receive a constraint it has no
// way to create. The indexes take the names the host's YDB renderer gives
// them, so a table that renderer created compares as declared too.
//
// A session that does not state the key leaves the declaration as it is: the
// absence of an answer is not an engine without UNIQUE constraints.
type uniqueIndexLowering struct{}

func (uniqueIndexLowering) LowerDesired(ctx context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !request.Capabilities.Established(capability.UniqueConstraints) || request.Capabilities.Has(capability.UniqueConstraints) {
		return request.Desired, nil
	}
	return ydblowering.UniqueConstraintsAsIndexes(request.Desired), nil
}
