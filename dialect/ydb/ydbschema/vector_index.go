package ydbschema

import (
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

// VectorIndexKind identifies the settings of a YDB vector index, `GLOBAL USING
// vector_kmeans_tree`: the values of its WITH (...) clause. The facet is
// attached to the common index; the key and covered columns stay there.
//
// No statement changes a setting of a built index (`ALTER INDEX ... SET
// (levels = 2)` answers `Unknown table setting: levels`), so a different
// setting is a different index, which a plan builds again under its name.
const VectorIndexKind schemaext.Kind = "ptah.run/ydb/vector-index"

// The values YDB takes for each named setting, measured on 25.1.4.7 and
// 26.2.1.14: `distance=inner_product` answers `Invalid distance:
// inner_product`, `similarity=euclidean` answers `Invalid similarity:
// euclidean`, and `vector_type=double` answers `Invalid vector_type: double`.
// The server reads a value in any case; the model holds it in lower case, as
// a read reports it.
var (
	vectorDistances    = []string{"cosine", "euclidean", "manhattan"}
	vectorSimilarities = []string{"inner_product", "cosine"}
	vectorElementTypes = []string{"float", "uint8", "int8", "bit"}
)

// VectorDistances returns the distances a vector index orders by, in lower
// case. Each call returns an independent slice.
func VectorDistances() []string { return slices.Clone(vectorDistances) }

// VectorSimilarities returns the similarities a vector index orders by, in
// lower case. Each call returns an independent slice.
func VectorSimilarities() []string { return slices.Clone(vectorSimilarities) }

// VectorElementTypes returns the element types a vector index stores, in
// lower case. Each call returns an independent slice.
func VectorElementTypes() []string { return slices.Clone(vectorElementTypes) }

// VectorSettings is the settings of a vector index, each field named for the
// setting it carries as YQL spells it. An empty field is a setting the value
// does not state.
type VectorSettings struct {
	// Distance is the distance the index orders by: cosine, euclidean or
	// manhattan.
	Distance string `json:"distance,omitempty"`
	// Similarity is the similarity the index orders by: inner_product or
	// cosine.
	Similarity string `json:"similarity,omitempty"`
	// VectorType is the type of a stored vector's elements: float, uint8,
	// int8 or bit.
	VectorType string `json:"vector_type,omitempty"`
	// Dimension is vector_dimension, the number of elements in a vector.
	Dimension uint64 `json:"vector_dimension,omitempty"`
	// Levels is the depth of the k-means tree.
	Levels uint64 `json:"levels,omitempty"`
	// Clusters is the number of clusters each level splits into.
	Clusters uint64 `json:"clusters,omitempty"`
}

// DesiredVectorIndex is the settings a declaration states for a vector index.
// A source attaches one to every index it declares as a vector index, and to
// an index of another kind that states a vector setting, so a declaration
// that states none of them, or states them on the wrong kind of index, is
// still seen and refused. A setting may be missing: YDB 25.3 and later refuse
// an index that leaves one out (`levels should be set`), and that refusal
// belongs to the stage that builds the index, not to the model.
type DesiredVectorIndex VectorSettings

// ObservedVectorIndex is the settings a read found a vector index built with.
// It names exactly one metric, an element type and a dimension. Levels and
// clusters are what the server reported, zero where it reported none.
type ObservedVectorIndex VectorSettings

// Kind returns the owned vector index identity.
func (*DesiredVectorIndex) Kind() schemaext.Kind { return VectorIndexKind }

// Kind returns the owned vector index identity.
func (*ObservedVectorIndex) Kind() schemaext.Kind { return VectorIndexKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredVectorIndex) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredVectorIndex)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedVectorIndex) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedVectorIndex)(nil)
	}
	return new(*v)
}

// Equal compares declarations setting by setting, without resolving them.
func (v *DesiredVectorIndex) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredVectorIndex)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares observations setting by setting.
func (v *ObservedVectorIndex) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedVectorIndex)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Settings returns the declared settings. A nil receiver states none.
func (v *DesiredVectorIndex) Settings() VectorSettings {
	if v == nil {
		return VectorSettings{}
	}
	return VectorSettings(*v)
}

// Settings returns the observed settings. A nil receiver holds none.
func (v *ObservedVectorIndex) Settings() VectorSettings {
	if v == nil {
		return VectorSettings{}
	}
	return VectorSettings(*v)
}

// Desired captures the observed settings as a declaration that keeps them
// exactly. A nil receiver remains nil.
func (v *ObservedVectorIndex) Desired() *DesiredVectorIndex {
	if v == nil {
		return nil
	}
	return new(DesiredVectorIndex(*v))
}

// ValidateDesiredVectorIndex refuses a declaration no source writes: a
// metric or an element type that is not one YDB takes, in lower case. Every
// setting may be missing, and both metrics may be named; whether the
// declaration builds an index is decided where it is built. Nil is invalid.
// Errors are schemaext.InvalidModelError values wrapping
// schemaext.ErrInvalidValue.
func ValidateDesiredVectorIndex(v *DesiredVectorIndex) error {
	if v == nil {
		return vectorValidation(schemaext.Desired, fmt.Errorf("%w: nil YDB vector index declaration", schemaext.ErrInvalidValue))
	}
	return vectorValidation(schemaext.Desired, validateVectorNames(VectorSettings(*v)))
}

// ValidateObservedVectorIndex refuses an observation YDB cannot report: one
// that names no metric or both, a metric or an element type YDB does not
// take, no element type, or a zero dimension. Nil is invalid.
func ValidateObservedVectorIndex(v *ObservedVectorIndex) error {
	if v == nil {
		return vectorValidation(schemaext.Observed, fmt.Errorf("%w: nil YDB vector index observation", schemaext.ErrInvalidValue))
	}
	settings := VectorSettings(*v)
	if err := validateVectorNames(settings); err != nil {
		return vectorValidation(schemaext.Observed, err)
	}
	switch {
	case (settings.Distance == "") == (settings.Similarity == ""):
		return vectorValidation(schemaext.Observed, fmt.Errorf("%w: an observed vector index names exactly one of distance and similarity", schemaext.ErrInvalidValue))
	case settings.VectorType == "":
		return vectorValidation(schemaext.Observed, fmt.Errorf("%w: an observed vector index names its vector_type", schemaext.ErrInvalidValue))
	case settings.Dimension == 0:
		return vectorValidation(schemaext.Observed, fmt.Errorf("%w: an observed vector index has a vector_dimension of at least 1", schemaext.ErrInvalidValue))
	}
	return nil
}

func validateVectorNames(settings VectorSettings) error {
	for _, setting := range []struct {
		name, value string
		allowed     []string
	}{
		{"distance", settings.Distance, vectorDistances},
		{"similarity", settings.Similarity, vectorSimilarities},
		{"vector_type", settings.VectorType, vectorElementTypes},
	} {
		if setting.value != "" && !slices.Contains(setting.allowed, setting.value) {
			return fmt.Errorf("%w: vector index %s %q is not one YDB takes, in lower case", schemaext.ErrInvalidValue, setting.name, setting.value)
		}
	}
	return nil
}

func vectorValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: VectorIndexKind, Representation: representation, Message: err.Error()}
}

// HasVectorIndex reports whether facets hold vector index settings, declared
// or observed.
func HasVectorIndex(facets schemaext.Facets) bool {
	return slices.Contains(facets.Kinds(), VectorIndexKind)
}
