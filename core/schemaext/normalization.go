package schemaext

import (
	"context"
	"database/sql"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// ProbeSession runs an owner's normalization probes on a connected target.
// Every call runs body inside one transaction and rolls that transaction back
// whatever body does, so nothing a probe creates survives it. ran is false,
// with a nil error, when no isolated transaction was available; body did not
// run, and the caller must treat every probe as unanswered.
type ProbeSession interface {
	WithRolledBackTransaction(ctx context.Context, label string, body func(context.Context, *sql.Tx) error) (ran bool, err error)
}

// NormalizationRequest asks the owners of declared objects to attach a
// connected target's own spelling of what they declare, before a comparison
// runs. Some engines store a rewritten definition, so a declaration and the
// catalog differ for an object that has not changed; the target's rewrite of
// the declaration is the fact a comparison needs. Current carries the
// observations the same read produced, so an owner can limit its probes to
// objects the target holds. Inputs are snapshots.
type NormalizationRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Desired      ObjectState
	Current      ObjectState
	Session      ProbeSession
}

// NormalizationResult returns every declared object under its original
// identity and kind, with the source coverage unchanged. An owner attaches
// only the target facts its model defines for this purpose; it never adds,
// removes or renames a declaration. A probe the target refused leaves its
// object without the fact, which a comparer must treat as unanswered.
type NormalizationResult struct {
	Complete bool
	Desired  ObjectState
}

// NormalizationService probes a connected target for one batch of declared
// objects. Its probes may create objects only inside the session's rolled-back
// transactions; it performs no other database work and must not mutate the
// supplied snapshots. Errors and cancellation discard the whole result.
type NormalizationService interface {
	NormalizeObjects(context.Context, NormalizationRequest) (NormalizationResult, error)
}
