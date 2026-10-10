package schemaext

import (
	"context"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// FeatureState captures named objects and settings attached to common objects.
// Coverage records the source's authority for both surfaces. Adding a provider
// to the runtime never enrolls its models in an already captured source.
type FeatureState struct {
	Objects  Objects
	Facets   []FacetRecord
	Coverage Coverage
}

// ComparisonRequest is the common feature comparison boundary. Providers keep
// distinct object and facet semantics while the caller supplies one captured
// state and receives one complete accounting of changes and limitations.
type ComparisonRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Desired      FeatureState
	Current      FeatureState
	Owners       []ParentState
	// Requests are changes the caller asks for that a comparison cannot find,
	// one per subject and action. Each goes to the owner of its subject's
	// named-object kind; see [ChangeRequest].
	Requests []ChangeRequest
}

// ComparisonResult joins the selected object and facet owners' replies. No
// changes are usable without Complete. An operational error exposes no result.
type ComparisonResult struct {
	Complete     bool
	Desired      FeatureState
	Changes      []ChangeRecord
	FacetChanges []FacetChange
	Undecided    []UndecidedChange
}

// ComparisonService compares both feature surfaces as one contextual operation.
// It retains each model's semantics and source coverage without translating
// attached settings into synthetic named objects.
type ComparisonService interface {
	CompareFeatures(context.Context, ComparisonRequest) (ComparisonResult, error)
}
