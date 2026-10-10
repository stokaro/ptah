// Package ydblowering rewrites a desired schema into the shape a YDB read
// reports it in, before a comparison reads it: a UNIQUE constraint as the
// global unique index YDB holds, a privilege as the permission name YDB
// reports, and a changefeed or a topic with the consumers the database's
// transfers read it through. [Service] is the YDB target's lowering service,
// which the comparison reaches through the runtime; the conversion that
// lowers a desired schema for rendering calls the same functions.
package ydblowering

import (
	"context"

	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
)

// Service is the YDB target's desired lowering.
type Service struct{}

// LowerDesired lowers request.Desired for YDB, as the reader reports a plan's
// result: UNIQUE constraints as indexes, privileges as permission names, and
// the consumers transfers read through. It modifies neither side.
func (Service) LowerDesired(ctx context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	desired := UniqueConstraintsAsIndexesFor(request.Desired, request.Target, request.Capabilities)
	desired = YDBPermissionNamesFor(desired, request.Target)
	return AdoptTransferConsumers(desired, request.Current, request.Semantics)
}
