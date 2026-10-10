package schemapreparation

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// LoweringRequest is one target's whole desired schema before a comparison
// reads it, with what the database holds.
type LoweringRequest struct {
	// Target is the selected target's canonical name.
	Target string
	// Desired is the desired schema, already scoped to the target. A service
	// must not modify it.
	Desired *schemamodel.Database
	// Current is the database's read. A service must not modify it.
	Current *catalog.Database
	// Capabilities are the target's, as the comparison resolved them.
	Capabilities capability.Capabilities
	// Semantics are the identifier rules the comparison matches names by.
	Semantics identifier.Semantics
}

// Lowering rewrites a target's whole desired schema into the shape its reader
// reports the declarations in, such as YDB's UNIQUE constraints as the unique
// indexes the server holds them as, so a comparison does not read the
// difference in spelling as a change. A target registers one only where its
// reader reports a declaration in another shape; without one, a comparison
// reads the desired schema as declared.
type Lowering interface {
	// LowerDesired returns the lowered desired schema, which may be
	// request.Desired itself when nothing changes. It must not modify the
	// request.
	LowerDesired(ctx context.Context, request LoweringRequest) (*schemamodel.Database, error)
}
