package schemaext

import (
	"context"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// FacetNormalizationRequest asks an owner to attach target facts to declared
// settings before live comparison. Owners records whether each common object
// exists on either side, including objects with no attached settings. Inputs
// are snapshots. Session is the only permitted route to database probes.
type FacetNormalizationRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Desired      FacetState
	Current      FacetState
	Owners       []ParentState
	Session      ProbeSession
}

// FacetNormalizationResult retains every declared setting under its original
// subject, kind, target binding and source coverage. An owner may attach facts
// defined by its desired model. An unanswered probe leaves no target fact;
// comparison must retain that uncertainty. Complete is required on success.
type FacetNormalizationResult struct {
	Complete bool
	Desired  FacetState
}

// FacetNormalizationService normalizes one owner's attached settings. Probes
// run only through Session's rolled-back transactions. A nil session permits
// no probes. Implementations leave inputs unchanged and discard the entire
// result on error or cancellation.
type FacetNormalizationService interface {
	NormalizeFacets(context.Context, FacetNormalizationRequest) (FacetNormalizationResult, error)
}
