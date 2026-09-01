package embedgen

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/embeddigest"
)

// TargetObjects is the schema a generation needs in order to exist: the column
// its vectors live in, and the index its queries use.
//
// It is a description rather than DDL. Ptah already renders, compares and plans
// schema objects for every target it supports, so a generation says what it
// needs in that vocabulary and the existing pipeline decides how to bring it
// about. Writing a second DDL path here would be a second answer to what a
// vector column is (stokaro/ptah#2068).
type TargetObjects struct {
	// Column is the vector column this generation writes.
	Column schemamodel.Field
	// Index is the vector index over it, and is empty when the specification
	// names no index method.
	Index schemamodel.Index
	// HasIndex reports whether Index is set, because a zero Index is a
	// legitimate answer -- a generation may be backfilled before its index is
	// built, which is what Phase G is for.
	HasIndex bool
	// RequiredExtensions names what the target needs installed before any of
	// this can be created.
	RequiredExtensions []string
}

// TargetObjects derives the schema objects this generation needs.
//
// The vector column is NEW, never a replacement: Decision 6 says an existing
// generation is not overwritten in place, and this is where that becomes
// structural. Two generations over one table produce two columns, and the
// cutover that follows is a pointer move rather than a data migration.
func (s Spec) TargetObjects() (TargetObjects, error) {
	if err := s.validateTarget(); err != nil {
		return TargetObjects{}, err
	}

	objects := TargetObjects{
		Column: schemamodel.Field{
			StructName: s.Target.Table,
			Name:       s.Target.Column,
			Type:       fmt.Sprintf("%s(%d)", s.Target.Representation, s.Model.ReportedDimension),
			// Nullable because a generation is populated over time and a row
			// without its vector yet is the normal state during a backfill, not
			// an error. Verification is what reports coverage; NOT NULL would
			// make the column uncreatable until the backfill finished.
			Nullable: true,
			Comment: fmt.Sprintf("embedding generation %s (%s, %s)",
				s.Identity().Short(), s.Model.Identifier, s.Target.Metric),
		},
		RequiredExtensions: []string{"vector"},
	}
	if strings.TrimSpace(s.Target.IndexMethod) == "" {
		return objects, nil
	}

	operatorClass, err := s.OperatorClass()
	if err != nil {
		return TargetObjects{}, err
	}
	objects.Index = schemamodel.Index{
		StructName: s.Target.Table,
		Name:       s.indexName(),
		Fields:     []string{s.Target.Column},
		Type:       s.Target.IndexMethod,
		Operator:   operatorClass,
	}
	objects.HasIndex = true
	return objects, nil
}

// validateTarget refuses a target that cannot describe a vector column.
func (s Spec) validateTarget() error {
	switch {
	case strings.TrimSpace(s.Target.Table) == "":
		return fmt.Errorf("target objects: the specification names no target table")
	case strings.TrimSpace(s.Target.Column) == "":
		return fmt.Errorf("target objects: the specification names no target column")
	case strings.TrimSpace(s.Target.Representation) == "":
		return fmt.Errorf("target objects: the specification names no target representation")
	case s.Model.ReportedDimension <= 0:
		// The dimension comes from the PROVIDER, so a specification that has
		// not asked one yet cannot describe its column. Guessing it from the
		// requested dimension would build a column the first response does not
		// fit.
		return fmt.Errorf(
			"target objects: the model reports no output dimension yet; run the provider test first")
	default:
		return nil
	}
}

// indexName is the generation's index name.
//
// The identity's short form is in the name because two generations over one
// table each need their own index, and a name derived from the column alone
// would collide the moment a second generation appeared -- which is the whole
// shape this design is for.
func (s Spec) indexName() string {
	return IndexName(s.Target.Table, s.Target.Column, s.Identity().Digest)
}

// IndexName is the name a generation's index has, from the three facts that
// decide it.
//
// It takes the facts rather than a specification because retirement has no
// specification for the generation it is destroying -- it has a registry row,
// which records the table, the column and the identity and nothing else. The
// retirement used to build the name from the CURRENT specification with the
// column swapped in, and Target.Column is an identity field, so the digest in
// the name belonged to a hybrid that was no generation at all. The
// `DROP INDEX IF EXISTS` then matched nothing, the index survived, and the verb
// reported the generation gone at exit 0 (stokaro/ptah#2642).
//
// One function, so a name that is created and a name that is dropped cannot
// come from two derivations. That is the failure this replaces rather than a
// risk of repeating it.
func IndexName(table, column, identity string) string {
	return fmt.Sprintf("%s_%s_%s_idx", table, column, embeddigest.Short(identity))
}

// OperatorClass is the pgvector operator class for this representation and
// metric.
//
// The pair is looked up rather than composed, because the combinations that
// exist are a fact about pgvector and not a naming pattern: a composed name
// that happened to be wrong would fail at CREATE INDEX with the target's own
// error, after the column and the backfill.
//
// Which classes exist is only half the question, and this half is answered
// offline. The other half is which ACCESS METHOD accepts one:
// `sparsevec_cosine_ops` is a real class that `hnsw` takes and `ivfflat` does
// not, so the pair below is satisfied and the index still fails
// (stokaro/ptah#2648 finding 1). That question needs the target's catalog,
// which is why this is exported -- the planner asks about the name the index
// would actually be built with rather than one it composed for itself.
func (s Spec) OperatorClass() (string, error) {
	classes := map[string]map[DistanceMetric]string{
		"vector": {
			MetricCosine:       "vector_cosine_ops",
			MetricL2:           "vector_l2_ops",
			MetricInnerProduct: "vector_ip_ops",
		},
		"halfvec": {
			MetricCosine:       "halfvec_cosine_ops",
			MetricL2:           "halfvec_l2_ops",
			MetricInnerProduct: "halfvec_ip_ops",
		},
		"sparsevec": {
			MetricCosine:       "sparsevec_cosine_ops",
			MetricL2:           "sparsevec_l2_ops",
			MetricInnerProduct: "sparsevec_ip_ops",
		},
	}
	byMetric, known := classes[strings.ToLower(strings.TrimSpace(s.Target.Representation))]
	if !known {
		return "", fmt.Errorf(
			"target objects: no operator class is known for representation %q", s.Target.Representation)
	}
	class, known := byMetric[s.Target.Metric]
	if !known {
		return "", fmt.Errorf(
			"target objects: no operator class is known for representation %q under metric %q",
			s.Target.Representation, s.Target.Metric)
	}
	return class, nil
}
