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
	return fingerprint(ctx, runtime.Codecs(), schemaext.Observed, &common, database.FeatureObjects, database.FeatureCoverage, common.FacetSlots())
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
	return fingerprint(ctx, runtime.Codecs(), schemaext.Desired, &common, database.FeatureObjects, database.FeatureCoverage, common.FacetSlots())
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
}

func fingerprint(ctx context.Context, registry schemaext.Registry, representation schemaext.Representation,
	common any, objects schemaext.Objects, coverage schemaext.Coverage, slots []*schemaext.Facets,
) (string, error) {
	values, err := objects.All()
	if err != nil {
		return "", err
	}
	encoded := make([]objectDigest, len(values))
	for i, object := range values {
		value, err := registry.Fingerprint(ctx, representation, []schemaext.Payload{object.Value})
		if err != nil {
			return "", err
		}
		encoded[i] = objectDigest{Subject: object.Ref, Value: value}
	}
	knowledge, err := registry.EncodeCoverage(ctx, representation, coverage)
	if err != nil {
		return "", err
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
	payload, err := json.Marshal(document{Format: 1, Representation: representation, Common: common, Objects: encoded, Coverage: knowledge, Facets: facets})
	if err != nil {
		return "", fmt.Errorf("marshal schema fingerprint input: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return digest.FromBytes(payload).String(), nil
}

type objectDigest struct {
	Subject objectidentity.ID `json:"subject"`
	Value   string            `json:"value"`
}
