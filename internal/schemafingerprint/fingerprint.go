// Package schemafingerprint binds common schema data, named feature objects,
// attached facets, and source knowledge to the selected codec definitions.
package schemafingerprint

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/opencontainers/go-digest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Observed fingerprints a catalog without modifying the caller's snapshot.
func Observed(ctx context.Context, runtime schemaext.ModelRuntime, database *catalog.Database) (string, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return "", err
	}
	if database == nil {
		return "", fmt.Errorf("schema fingerprint requires schema")
	}
	common := cloneObservedEnvelopes(database)
	common.FeatureObjects, common.FeatureCoverage = schemaext.Objects{}, schemaext.Coverage{}
	common.PlanningContextObjects, common.PlanningContextCoverage = schemaext.Objects{}, schemaext.Coverage{}
	return fingerprint(ctx, runtime.Codecs(), schemaext.Observed, &common, features{
		objects: database.FeatureObjects, coverage: database.FeatureCoverage,
		contextObjects: database.PlanningContextObjects, contextCoverage: database.PlanningContextCoverage,
	}, common.FacetSlots())
}

// Desired fingerprints a declaration without modifying the caller's snapshot.
func Desired(ctx context.Context, runtime schemaext.ModelRuntime, database *schemamodel.Database) (string, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return "", err
	}
	if database == nil {
		return "", fmt.Errorf("desired schema fingerprint requires schema")
	}
	common := cloneDesiredEnvelopes(database)
	common.FeatureObjects, common.FeatureCoverage = schemaext.Objects{}, schemaext.Coverage{}
	return fingerprint(ctx, runtime.Codecs(), schemaext.Desired, &common, features{
		objects: database.FeatureObjects, coverage: database.FeatureCoverage,
	}, common.FacetSlots())
}

// features is the feature data of a schema, which only the codec registry
// encodes: the named objects and what the source knows of their kinds, and
// for a filtered read the planning context it carries beside them.
type features struct {
	objects, contextObjects   schemaext.Objects
	coverage, contextCoverage schemaext.Coverage
}

// The document is a hash preimage, not a transport format. Each facet slot is
// paired with the common envelopes' deterministic traversal order. Keeping empty
// slots prevents an attached value from moving to another common object without
// changing the digest. Common slices retain their order, as in the source model.
type document struct {
	Format         uint32                     `json:"format"`
	Representation schemaext.Representation   `json:"representation"`
	Common         any                        `json:"common"`
	Objects        []objectDigest             `json:"objects"`
	Coverage       schemaext.CoverageDocument `json:"coverage"`
	Facets         []string                   `json:"facets"`
	// Context and ContextCoverage are the planning context a filtered read
	// carries outside its scope. A planner reads it, so a plan made over one
	// context does not match a schema whose context differs. Both are left
	// out when the read carries none.
	Context         []objectDigest              `json:"context,omitempty"`
	ContextCoverage *schemaext.CoverageDocument `json:"context_coverage,omitempty"`
}

func fingerprint(ctx context.Context, registry schemaext.Registry, representation schemaext.Representation,
	common any, data features, slots []*schemaext.Facets,
) (string, error) {
	encoded, err := objectDigests(ctx, registry, representation, data.objects)
	if err != nil {
		return "", err
	}
	knowledge, err := registry.EncodeCoverage(ctx, representation, data.coverage)
	if err != nil {
		return "", err
	}
	contextDigests, err := objectDigests(ctx, registry, representation, data.contextObjects)
	if err != nil {
		return "", err
	}
	var contextKnowledge *schemaext.CoverageDocument
	if !data.contextCoverage.IsZero() {
		document, err := registry.EncodeCoverage(ctx, representation, data.contextCoverage)
		if err != nil {
			return "", err
		}
		contextKnowledge = &document
	}
	facets := make([]string, len(slots))
	for i, slot := range slots {
		values, err := slot.Values()
		if err != nil {
			return "", err
		}
		payloads := make([]schemaext.Payload, len(values))
		for j, value := range values {
			payloads[j] = value
		}
		facets[i], err = registry.Fingerprint(ctx, representation, payloads)
		if err != nil {
			return "", err
		}
		*slot = schemaext.Facets{}
	}
	payload, err := json.Marshal(document{Format: 1, Representation: representation, Common: common, Objects: encoded, Coverage: knowledge,
		Facets: facets, Context: contextDigests, ContextCoverage: contextKnowledge})
	if err != nil {
		return "", fmt.Errorf("marshal schema fingerprint input: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return digest.FromBytes(payload).String(), nil
}

// objectDigests fingerprints each object through the registry, in the order
// [schemaext.Objects.All] returns them. No objects is an empty slice, which
// the document writes as an empty list.
func objectDigests(ctx context.Context, registry schemaext.Registry, representation schemaext.Representation, objects schemaext.Objects) ([]objectDigest, error) {
	values, err := objects.All()
	if err != nil {
		return nil, err
	}
	encoded := make([]objectDigest, len(values))
	for i, object := range values {
		value, err := registry.Fingerprint(ctx, representation, []schemaext.Payload{object.Value})
		if err != nil {
			return nil, err
		}
		encoded[i] = objectDigest{Subject: object.Ref, Value: value}
	}
	return encoded, nil
}

type objectDigest struct {
	Subject objectidentity.ID `json:"subject"`
	Value   string            `json:"value"`
}
