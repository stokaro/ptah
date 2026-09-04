package ociartifact

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/oci"
)

// layoutRepository is an image layout presented as something attachment code
// can verify against.
//
// [referrerRepository] is three interfaces, and an *oci.Store answers two of
// them already: it is an oras.ReadOnlyTarget and a registry.TagLister.
// Measured 2026-09-04 against oras-go v2, it is NOT a registry.ReferrerLister
// -- a directory has no /referrers endpoint -- but it does maintain the reverse
// graph behind content.PredecessorFinder. So the missing third is an adapter
// rather than a reimplementation, which is why attaching to a layout is a
// smaller change than stokaro/ptah#2839 expected.
type layoutRepository struct {
	*oci.Store
}

// Referrers lists the manifests that name desc as their subject.
//
// The filter is the whole of this function, and it is not optional.
// `Predecessors` answers every manifest that POINTS at the subject, which is a
// superset of its referrers: measured on one subject with two predecessors, it
// returned both a manifest carrying `subject` and a manifest merely carrying
// the subject as a layer. A registry's /referrers cannot return the second by
// construction; a local graph can, so this reads each candidate and keeps only
// the ones whose own `subject` field names the descriptor asked about.
//
// artifactType filters the result the way the registry API does, and an empty
// one returns every referrer.
func (r layoutRepository) Referrers(
	ctx context.Context,
	desc ocispec.Descriptor,
	artifactType string,
	fn func(referrers []ocispec.Descriptor) error,
) error {
	predecessors, err := r.Predecessors(ctx, desc)
	if err != nil {
		return fmt.Errorf("list the layout's predecessors of %s: %w", desc.Digest, err)
	}
	referrers := make([]ocispec.Descriptor, 0, len(predecessors))
	for _, candidate := range predecessors {
		manifest, err := r.readManifest(ctx, candidate)
		if err != nil {
			return err
		}
		if manifest.Subject == nil || manifest.Subject.Digest != desc.Digest {
			continue
		}
		referrer := candidate
		// From the manifest, not from the predecessor descriptor. Measured:
		// a store reopened on the directory answers Predecessors with the
		// artifact type EMPTY, so a listing that trusted the descriptor
		// filtered every referrer away on the first call after a restart --
		// and a registry's /referrers never does that, because it builds the
		// descriptor from the manifest it holds.
		referrer.ArtifactType = manifest.artifactType()
		if len(manifest.Annotations) > 0 {
			referrer.Annotations = manifest.Annotations
		}
		if artifactType != "" && referrer.ArtifactType != artifactType {
			continue
		}
		referrers = append(referrers, referrer)
	}
	if len(referrers) == 0 {
		return nil
	}
	return fn(referrers)
}

// referrerManifest is the part of a manifest a referrer listing is built from.
type referrerManifest struct {
	Subject      *ocispec.Descriptor `json:"subject"`
	ArtifactType string              `json:"artifactType"`
	Config       ocispec.Descriptor  `json:"config"`
	Annotations  map[string]string   `json:"annotations"`
}

// artifactType is what the OCI specification says a referrer's artifact type
// is: the manifest's own, and the config media type where it declares none.
func (m referrerManifest) artifactType() string {
	if m.ArtifactType != "" {
		return m.ArtifactType
	}
	return m.Config.MediaType
}

// readManifest reads the fields a referrer listing needs.
//
// A predecessor that cannot be read as a manifest is not an error: an image
// layout holds blobs of every kind, and one that is not JSON is simply not a
// referrer.
func (r layoutRepository) readManifest(
	ctx context.Context, desc ocispec.Descriptor,
) (referrerManifest, error) {
	reader, err := r.Fetch(ctx, desc)
	if err != nil {
		return referrerManifest{}, fmt.Errorf("read the layout manifest %s: %w", desc.Digest, err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		return referrerManifest{}, fmt.Errorf("read the layout manifest %s: %w", desc.Digest, err)
	}
	var decoded referrerManifest
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return referrerManifest{}, nil //nolint:nilerr // not a manifest, so not a referrer
	}
	return decoded, nil
}

// attachEndpoint is where an attachment is written and how its discoverability
// is checked afterwards.
//
// It exists for the reason copyEndpoint does: the reference kind is recognized
// once, here, rather than by every verb that has to know whether it is talking
// to a registry or to a directory.
type attachEndpoint struct {
	target     oras.Target
	repository referrerRepository
	reference  Reference
	selector   string
	display    string
	probe      indexProbe
}

// layoutIndexProbe answers what "does the target serve the referrers index"
// means for a directory.
//
// It answers yes, and the reason is what the refusal is for rather than what
// it is named after. ErrReferrerIndexRequired exists to stop a publish whose
// attachment nothing could discover; in a layout, discovery is the store's own
// graph and it works -- measured, a subject's referrer is listed back with its
// artifact type intact. Refusing here would refuse a publish that IS
// discoverable, for the sake of an HTTP endpoint that is not how discovery
// happens in a directory.
//
// The refusal keeps its meaning where it has one: a registry that does not
// serve the index still fails required-api, before anything is written.
func layoutIndexProbe(_ context.Context) (bool, string, error) {
	return true, "the image layout keeps its own referrers graph", nil
}

// attachEndpointFor resolves where an attachment goes, for a registry
// reference or an oci-layout:// directory alike.
func (c *Client) attachEndpointFor(raw string) (attachEndpoint, error) {
	if IsLayoutRef(raw) {
		store, tag, err := openLayoutStore(raw)
		if err != nil {
			return attachEndpoint{}, err
		}
		return attachEndpoint{
			target:     store,
			repository: layoutRepository{Store: store},
			reference:  layoutReference(raw, tag),
			selector:   tag,
			display:    strings.TrimSpace(raw),
			probe:      layoutIndexProbe,
		}, nil
	}
	ref, err := ParseRef(raw)
	if err != nil {
		return attachEndpoint{}, err
	}
	repository, err := c.repository(ref)
	if err != nil {
		return attachEndpoint{}, err
	}
	target, err := c.attachmentTarget(ref)
	if err != nil {
		return attachEndpoint{}, err
	}
	return attachEndpoint{
		target:     target,
		repository: repository,
		reference:  ref,
		selector:   ref.Selector(),
		display:    ref.String(),
		probe:      c.indexProbe(raw),
	}, nil
}
