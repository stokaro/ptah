package embedgen

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/internal/embeddigest"
)

// Identity is the content address of a generation: the digest of every part of
// the transformation that decides what a vector MEANS.
//
// Two generations with one identity produce comparable vectors. Two with
// different identities do not, and nothing may mix them -- which is the whole
// reason the value exists rather than a version number somebody increments.
type Identity struct {
	// Digest is the hex SHA-256 of the canonical encoding.
	Digest string
	// Reproducibility says whether this identity can be reproduced from the
	// specification alone.
	Reproducibility Reproducibility
	// ReproducibilityReason names what is missing when it is partial, and is
	// empty when it is full.
	ReproducibilityReason string
}

// Reproducibility is whether a generation can be rebuilt from its
// specification.
type Reproducibility string

const (
	// ReproducibilityFull means every load-bearing input is pinned, including
	// an immutable model revision.
	ReproducibilityFull Reproducibility = "full"
	// ReproducibilityPartial means at least one input is not pinned. The
	// identity is still stable and still separates generations; what it cannot
	// promise is that asking the same provider again produces the same vectors.
	ReproducibilityPartial Reproducibility = "partial"
)

// Short is the identity's leading twelve hex characters, for a name a person
// reads and a column suffix a database accepts.
func (i Identity) Short() string {
	return embeddigest.Short(i.Digest)
}

// Identity computes the generation identity for this specification.
//
// The encoding is length-prefixed, component by component, so no separator a
// value could contain decides a boundary: a field named `a:b` and two fields
// named `a` and `b` are different specifications and must have different
// digests. That is the same rule identities elsewhere in this tree follow, for
// the same reason.
//
// Reproducibility is reported, never fabricated. A provider that exposes no
// immutable revision gets `partial` and a reason, because an identity that
// claimed `full` over a mutable alias would promise a rebuild it cannot deliver
// (stokaro/ptah#2068).
func (s Spec) Identity() Identity {
	identity := Identity{
		Digest:          embeddigest.Of(s.identityComponents()...),
		Reproducibility: ReproducibilityFull,
	}
	if strings.TrimSpace(s.Model.Revision) == "" {
		identity.Reproducibility = ReproducibilityPartial
		identity.ReproducibilityReason = fmt.Sprintf(
			"provider %q exposes no immutable revision for model %q, so asking it again may answer with different vectors",
			s.Model.Provider, s.Model.Identifier)
	}
	return identity
}

// identityComponents is the ordered list of values the digest is taken over.
//
// Each label is the field's own name in snake case, prefixed by its group.
// That is not decoration: TestIdentity_EveryFieldIsClassified derives the label
// it expects from the Go field, so a shorthand here would read as a field that
// is not in the digest at all.
//
// It is written out rather than reflected so that adding a field is a decision:
// TestIdentity_EveryFieldIsClassified enumerates the struct and requires each
// field to appear here or in the excluded list, so a field that joins Spec
// without joining one of the two lists fails.
func (s Spec) identityComponents() []string {
	components := []string{
		"spec", strconv.Itoa(SpecVersion),
		"contract", strconv.Itoa(ContractVersion),

		"source.schema", s.Source.Schema,
		"source.table", s.Source.Table,
		"source.filter", s.Source.Filter,
		"source.version_strategy", string(s.Source.VersionStrategy),
		"source.version_field", s.Source.VersionField,

		"pre.separator", s.Preprocessing.Separator,
		"pre.prefix", s.Preprocessing.Prefix,
		"pre.null_policy", string(s.Preprocessing.NullPolicy),
		"pre.empty_policy", string(s.Preprocessing.EmptyPolicy),
		"pre.unicode_normalization", string(s.Preprocessing.UnicodeNormalization),
		"pre.collapse_whitespace", strconv.FormatBool(s.Preprocessing.CollapseWhitespace),
		"pre.max_input_bytes", strconv.Itoa(s.Preprocessing.MaxInputBytes),
		"pre.truncate", string(s.Preprocessing.Truncate),

		"model.provider", s.Model.Provider,
		"model.endpoint_class", string(s.Model.EndpointClass),
		"model.identifier", s.Model.Identifier,
		"model.revision", s.Model.Revision,
		"model.requested_dimension", strconv.Itoa(s.Model.RequestedDimension),
		"model.reported_dimension", strconv.Itoa(s.Model.ReportedDimension),
		"model.normalization", string(s.Model.Normalization),
		"model.pooling", s.Model.Pooling,

		"target.schema", s.Target.Schema,
		"target.table", s.Target.Table,
		"target.column", s.Target.Column,
		"target.representation", s.Target.Representation,
		"target.metric", string(s.Target.Metric),

		"target.index_method", s.Target.IndexMethod,
	}
	// The two ordered field lists are encoded in order, because order decides
	// the input text: title-then-body is not body-then-title, and a key's
	// component order decides how a target row is addressed.
	//
	// The first list carries its length and the second does not, and that is
	// not an oversight either way. Without the first count, a key field spelled
	// `source.input_fields` is indistinguishable from the label that starts the
	// next list, and two specifications addressing their rows by different
	// columns collide -- TestIdentity_OneListCannotSwallowTheNextOnesLabel is
	// that pair. With it, everything after the second label is the second list
	// and a count there would be a rule no fixture could ever separate from
	// the one above it.
	components = append(components, "source.key_fields", strconv.Itoa(len(s.Source.KeyFields)))
	components = append(components, s.Source.KeyFields...)
	components = append(components, "source.input_fields")
	components = append(components, s.Source.InputFields...)
	return components
}

// excludedFromIdentity names every Spec field deliberately outside the digest,
// with the reason. TestIdentity_EveryFieldIsClassified reads it.
//
// Each entry is a promise that changing that field leaves every existing vector
// valid and comparable.
var excludedFromIdentity = map[string]string{
	"Name":        "a display name; renaming a generation does not make its vectors incomparable",
	"Description": "prose for a human, for the same reason",
	"Target.IndexOptions": "recall tuning trades build cost against recall over the SAME vectors, and ADR 0010 " +
		"measured a 26.5%-100% recall span on one unchanged index from a session setting alone, which makes it a " +
		"property of the query rather than of the generation",
}

// SameGeneration reports whether two specifications describe one generation.
func SameGeneration(left, right Spec) bool {
	return left.Identity().Digest == right.Identity().Digest
}

// sortedKeys is used by the classification test and by diagnostics that list
// the exclusions in a stable order.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
