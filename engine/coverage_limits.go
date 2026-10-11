package engine

import (
	"fmt"

	"ptah.run/core/schemaext"
)

type limitKey struct {
	header string
	model  schemaext.Kind
}

// registerCoverageLimits validates every provider's limit mappings once the
// vocabulary is known. A mapping names a kind a document may name, and a
// named-object model of the provider that registers it: a facet model, or a
// model another provider owns, is refused.
func (r *Runtime) registerCoverageLimits(providers []Provider) error {
	seen := make(map[limitKey]string)
	for _, provider := range providers {
		for _, mapping := range provider.CoverageLimits {
			if err := mapping.Validate(); err != nil {
				return fmt.Errorf("%w: provider %q: %w", ErrInvalidRegistration, provider.ID, err)
			}
			if _, err := r.vocabulary.ParseKind(string(mapping.Header)); err != nil {
				return fmt.Errorf("%w: provider %q maps a coverage kind this runtime does not read: %w", ErrInvalidRegistration, provider.ID, err)
			}
			if !r.ownsCodec(provider.ID, mapping.Model, schemaext.Desired) || !r.ownsCodec(provider.ID, mapping.Model, schemaext.Observed) ||
				!r.comparesObjects(mapping.Model) {
				return fmt.Errorf("%w: provider %q maps coverage kind %q onto %q, which is not a named-object model it owns",
					ErrInvalidRegistration, provider.ID, mapping.Header, mapping.Model)
			}
			key := limitKey{header: string(mapping.Header), model: mapping.Model}
			if owner, found := seen[key]; found {
				return fmt.Errorf("%w: coverage kind %q is mapped onto %q by %q and %q",
					ErrInvalidRegistration, mapping.Header, mapping.Model, owner, provider.ID)
			}
			seen[key] = provider.ID
			r.limits = append(r.limits, mapping)
		}
	}
	return nil
}

// comparesObjects reports whether a named-object comparison of kind is
// registered for any target.
func (r *Runtime) comparesObjects(kind schemaext.Kind) bool {
	for key := range r.comparisons {
		if key.kind == kind {
			return true
		}
	}
	return false
}

// foldCoverageLimits folds each side's not-described records into the
// coverage of the models they limit, before any owner compares. A side's
// records are asked about the subjects the other side holds: a removal is the
// current side's object the desired side may not describe, and an addition
// the desired side's object the read may not have looked for.
func (r *Runtime) foldCoverageLimits(request schemaext.ComparisonRequest) (schemaext.ComparisonRequest, error) {
	desiredSubjects, currentSubjects := request.Current.Objects.Refs(), request.Desired.Objects.Refs()
	for _, mapping := range r.limits {
		var err error
		if request.Desired.Coverage, err = request.Desired.Coverage.WithLimits(request.DesiredLimits, mapping, desiredSubjects); err != nil {
			return schemaext.ComparisonRequest{}, err
		}
		if request.Current.Coverage, err = request.Current.Coverage.WithLimits(request.CurrentLimits, mapping, currentSubjects); err != nil {
			return schemaext.ComparisonRequest{}, err
		}
	}
	return request, nil
}
