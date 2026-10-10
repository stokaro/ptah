package ydbindex

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbpartition"
	"ptah.run/internal/ydbtype"
)

// The attributes that declare a vector index's settings, spelled as YDB's
// WITH (...) clause spells them. The annotation parser, the YAML reader, the
// annotation registry and the Go writer all read these names.
const (
	AttributeDistance        = "distance"
	AttributeSimilarity      = "similarity"
	AttributeVectorType      = "vector_type"
	AttributeVectorDimension = "vector_dimension"
	AttributeLevels          = "levels"
	AttributeClusters        = "clusters"
)

// VectorAttributes lists the vector attributes in the order a declaration's
// errors are reported and an index's settings are written.
func VectorAttributes() []string {
	return []string{
		AttributeDistance, AttributeSimilarity, AttributeVectorType,
		AttributeVectorDimension, AttributeLevels, AttributeClusters,
	}
}

// The values YDB takes for each named setting; see
// [ydbschema.VectorDistances].
var (
	distances    = ydbschema.VectorDistances()
	similarities = ydbschema.VectorSimilarities()
	vectorTypes  = ydbschema.VectorElementTypes()
)

// BitVectorType is the element type that stores a vector as bits, which only
// some lines build an index over.
const BitVectorType = "bit"

// The limits YDB 25.3 and later hold an index to, measured on 25.3.1.25 to
// 26.2.1.14: `Invalid levels: 17 should be between 1 and 16`, `Invalid
// clusters: 2049 should be between 2 and 2048`, and `Invalid
// clusters^levels: 1025^3 should be less than 1073741824` while 1024^3,
// which equals it, is accepted. 25.1.4.7 and 25.2.1.24 check none of them,
// and take levels = 100 or clusters = 1; Ptah holds every line to the limits,
// for the reason [ydbtype.MaxVectorDimension] gives.
const (
	maxLevels       = 16
	minClusters     = 2
	maxClusters     = 2048
	maxClusterCount = 1 << 30
)

// ParseVectorDeclaration reads the vector attributes of one index declaration
// out of values, keyed by attribute name, and ignores every other key. It
// returns nil where none is present.
//
// Each value is checked for the form YDB takes, so a typo is refused where it
// was written: a metric and an element type YDB names, in either case and
// kept in lower case, and a whole number of at least 1 for the counts. Whether
// the declaration is complete, and its counts within YDB's limits, is
// [ResolveVector]'s question, which every renderer and planner asks.
func ParseVectorDeclaration(values map[string]string) (*ydbschema.DesiredVectorIndex, error) {
	var spec ydbschema.DesiredVectorIndex
	present := false
	for _, attribute := range VectorAttributes() {
		value, ok := values[attribute]
		if !ok {
			continue
		}
		present = true
		if err := setVectorAttribute(&spec, attribute, strings.TrimSpace(value)); err != nil {
			return nil, err
		}
	}
	if !present {
		return nil, nil
	}
	return &spec, nil
}

// DeclareVector reads the vector index settings a source declares for one
// index: the vector attributes in values, as [ParseVectorDeclaration] reads
// them, for an index of method indexType with operator class operator.
//
// It returns nil for an index that is not a vector index and states no vector
// setting. A vector index that states none returns an empty declaration, so
// the stage that builds it refuses it rather than missing it, and so does an
// index of another kind that states one.
//
// A pgvector operator class names the metric on YDB too (see
// [ResolveVector]), so a declaration naming its metric that way states the
// setting: the class is folded into the declaration here, which is what
// lets a comparison that sees only the declaration read it. A class with no
// YDB counterpart, or one naming another metric than the settings, is
// refused, because no YDB index can be built from it.
func DeclareVector(values map[string]string, indexType, operator string) (*ydbschema.DesiredVectorIndex, error) {
	declared, err := ParseVectorDeclaration(values)
	if err != nil {
		return nil, err
	}
	if declared == nil {
		if kind, err := KindOf(indexType); err != nil || kind != Vector {
			return nil, nil
		}
		declared = &ydbschema.DesiredVectorIndex{}
	}
	settings := ydbschema.VectorSettings(*declared)
	if err := resolveOperator(&settings, operator); err != nil {
		return nil, err
	}
	return new(ydbschema.DesiredVectorIndex(settings)), nil
}

// setVectorAttribute reads one attribute's value into spec.
func setVectorAttribute(spec *ydbschema.DesiredVectorIndex, attribute, value string) error {
	switch attribute {
	case AttributeDistance:
		return parseName(attribute, value, distances, &spec.Distance)
	case AttributeSimilarity:
		return parseName(attribute, value, similarities, &spec.Similarity)
	case AttributeVectorType:
		return parseName(attribute, value, vectorTypes, &spec.VectorType)
	case AttributeVectorDimension:
		return parseCount(attribute, value, &spec.Dimension)
	case AttributeLevels:
		return parseCount(attribute, value, &spec.Levels)
	default:
		return parseCount(attribute, value, &spec.Clusters)
	}
}

// parseName reads one of names, in either case, into target.
func parseName(attribute, value string, names []string, target *string) error {
	lower := strings.ToLower(value)
	if !slices.Contains(names, lower) {
		return &ydbpartition.DeclarationError{Attribute: attribute, Value: value,
			Reason: "write one of " + strings.Join(names, ", ")}
	}
	*target = lower
	return nil
}

// parseCount reads a count of at least 1 into target, the counting shape YDB
// requires at a vector index's vector_dimension, levels and clusters alike.
func parseCount(attribute, value string, target *uint64) error {
	count, err := strconv.ParseUint(value, 10, 64)
	if err != nil || count == 0 {
		return &ydbpartition.DeclarationError{Attribute: attribute, Value: value,
			Reason: "write a whole number of at least 1; YDB refuses a count of zero"}
	}
	*target = count
	return nil
}

// pgvectorOperators are pgvector's operator classes for its float vector,
// with the metric each names on YDB. A declaration written for PostgreSQL
// names its metric through one of these, and YDB names the same metric with a
// setting: `<=>` is cosine distance, `<->` Euclidean distance, `<+>` taxicab
// distance and `<#>` the negative inner product, which orders as the inner
// product similarity does.
var pgvectorOperators = map[string]ydbschema.VectorSettings{
	"vector_cosine_ops": {Distance: "cosine"},
	"vector_l2_ops":     {Distance: "euclidean"},
	"vector_l1_ops":     {Distance: "manhattan"},
	"vector_ip_ops":     {Similarity: "inner_product"},
}

// pgvectorParameters are the build parameters of pgvector's indexes, which a
// declaration written for PostgreSQL carries as storage parameters, with the
// method each belongs to.
var pgvectorParameters = map[string]string{
	"m":               "hnsw",
	"ef_construction": "hnsw",
	"lists":           "ivfflat",
}

// ResolveVector reads a vector index's declaration as the settings it is
// built with, and says why YDB would refuse it.
//
// operator is the operator class the declaration names, which a declaration
// written for pgvector uses for its metric: vector_cosine_ops reads as
// `distance=cosine`, vector_l2_ops as `distance=euclidean`, vector_l1_ops as
// `distance=manhattan` and vector_ip_ops as `similarity=inner_product`. A class
// that names a metric the settings name too has to agree with them. The
// renderer and the comparison both resolve through here, so a declaration
// naming its metric either way is one index, read back with the setting.
//
// The settings are then held to what YDB requires: one metric, an element
// type, and a dimension, a depth and a width within the limits 25.3 and later
// enforce. Every setting is required, because 25.3 and later refuse an index
// that leaves one out (`levels should be set`), and 25.1 keeps no default it
// reports back.
func ResolveVector(spec *ydbschema.VectorSettings, operator string) (ydbschema.VectorSettings, error) {
	if spec == nil {
		return ydbschema.VectorSettings{}, fmt.Errorf("a %s index declares none of its settings; declare its distance or "+
			"similarity, vector_type, vector_dimension, levels and clusters", VectorMethod)
	}
	resolved := *spec
	resolved.Distance = strings.ToLower(resolved.Distance)
	resolved.Similarity = strings.ToLower(resolved.Similarity)
	resolved.VectorType = strings.ToLower(resolved.VectorType)
	if err := resolveOperator(&resolved, operator); err != nil {
		return ydbschema.VectorSettings{}, err
	}
	if reason := vectorRefusal(resolved); reason != "" {
		return ydbschema.VectorSettings{}, fmt.Errorf("%s", reason)
	}
	return resolved, nil
}

// CheckVectorOperator refuses a pgvector operator class a vector index's
// settings cannot be built with: one with no YDB counterpart, and one naming
// another metric than the settings. A class naming the metric the settings
// name, and no class, are accepted. It states no metric: a source folds the
// class into the declaration it reads (see [DeclareVector]), and the stages
// that build or compare the index read the declaration alone, so they agree
// about what it builds.
func CheckVectorOperator(spec ydbschema.VectorSettings, operator string) error {
	probe := spec
	probe.Distance, probe.Similarity = strings.ToLower(probe.Distance), strings.ToLower(probe.Similarity)
	return resolveOperator(&probe, operator)
}

// resolveOperator folds a pgvector operator class into the metric it names.
func resolveOperator(spec *ydbschema.VectorSettings, operator string) error {
	operator = strings.ToLower(strings.TrimSpace(operator))
	if operator == "" {
		return nil
	}
	metric, known := pgvectorOperators[operator]
	if !known {
		return fmt.Errorf("operator class %q has no YDB counterpart: a YDB vector index names its metric with "+
			"distance (cosine, euclidean, manhattan) or similarity (inner_product, cosine), and its vectors are "+
			"float, uint8, int8 or bit", operator)
	}
	switch {
	case spec.Distance == "" && spec.Similarity == "":
		spec.Distance, spec.Similarity = metric.Distance, metric.Similarity
	case spec.Distance != metric.Distance || spec.Similarity != metric.Similarity:
		return fmt.Errorf("operator class %q names %s, and the index's settings name %s", operator,
			metricName(metric), metricName(*spec))
	}
	return nil
}

// metricName writes the metric a spec names as its setting.
func metricName(spec ydbschema.VectorSettings) string {
	if spec.Similarity != "" {
		return AttributeSimilarity + "=" + spec.Similarity
	}
	return AttributeDistance + "=" + spec.Distance
}

// vectorRefusal says why YDB refuses a vector index with spec, quoting the
// server, and answers "" for one it builds.
func vectorRefusal(spec ydbschema.VectorSettings) string {
	switch {
	case spec.Distance != "" && spec.Similarity != "":
		return "a vector index names one metric, distance or similarity, and this one names both " +
			"(`only one of distance or similarity should be set, not both`)"
	case spec.Distance == "" && spec.Similarity == "":
		return "a vector index names its metric with distance or similarity (`either distance or similarity " +
			"should be set`)"
	case spec.Distance != "" && !slices.Contains(distances, spec.Distance):
		return fmt.Sprintf("distance %q is not one YDB takes (`Invalid distance`)", spec.Distance)
	case spec.Similarity != "" && !slices.Contains(similarities, spec.Similarity):
		return fmt.Sprintf("similarity %q is not one YDB takes (`Invalid similarity`)", spec.Similarity)
	case spec.VectorType == "":
		return "a vector index names its element type with vector_type (`vector_type should be set`)"
	case !slices.Contains(vectorTypes, spec.VectorType):
		return fmt.Sprintf("vector_type %q is not one YDB takes (`Invalid vector_type`)", spec.VectorType)
	case spec.Dimension < 1 || spec.Dimension > ydbtype.MaxVectorDimension:
		return fmt.Sprintf("a vector index's vector_dimension is between 1 and %d (`Invalid vector_dimension: %d "+
			"should be between 1 and %d`)", ydbtype.MaxVectorDimension, spec.Dimension, ydbtype.MaxVectorDimension)
	case spec.Levels < 1 || spec.Levels > maxLevels:
		return fmt.Sprintf("a vector index's levels are between 1 and %d (`Invalid levels: %d should be between "+
			"1 and %d`)", maxLevels, spec.Levels, maxLevels)
	case spec.Clusters < minClusters || spec.Clusters > maxClusters:
		return fmt.Sprintf("a vector index's clusters are between %d and %d (`Invalid clusters: %d should be "+
			"between %d and %d`)", minClusters, maxClusters, spec.Clusters, minClusters, maxClusters)
	case clusterCount(spec.Clusters, spec.Levels) > maxClusterCount:
		return fmt.Sprintf("a vector index's clusters to the power of its levels is at most %d (`Invalid "+
			"clusters^levels: %d^%d should be less than %d`)", maxClusterCount, spec.Clusters, spec.Levels, maxClusterCount)
	}
	return ""
}

// clusterCount is clusters to the power of levels, stopping once it passes
// the limit so a large product does not overflow.
func clusterCount(clusters, levels uint64) uint64 {
	count := uint64(1)
	for range levels {
		count *= clusters
		if count > maxClusterCount {
			return count
		}
	}
	return count
}

// VectorClause writes a resolved spec as the WITH (...) clause of a vector
// index, every setting named, in a fixed order.
func VectorClause(spec ydbschema.VectorSettings) string {
	settings := make([]string, 0, len(VectorAttributes())-1)
	settings = append(settings, metricName(spec))
	settings = append(settings,
		AttributeVectorType+"="+spec.VectorType,
		AttributeVectorDimension+"="+strconv.FormatUint(spec.Dimension, 10),
		AttributeLevels+"="+strconv.FormatUint(spec.Levels, 10),
		AttributeClusters+"="+strconv.FormatUint(spec.Clusters, 10),
	)
	return "WITH (" + strings.Join(settings, ", ") + ")"
}

// StorageParameterRefusal says why a vector index cannot carry the storage
// parameter name, a PostgreSQL WITH (...) entry, and names what YDB takes
// instead. pgvector's build parameters are refused by the method they belong
// to.
func StorageParameterRefusal(name string) string {
	if method, pgvector := pgvectorParameters[strings.ToLower(name)]; pgvector {
		return fmt.Sprintf("storage parameter %q belongs to pgvector's %s index; a %s index is shaped by its "+
			"levels and clusters", name, method, VectorMethod)
	}
	return fmt.Sprintf("storage parameter %q has no YDB counterpart: a %s index takes its settings as the "+
		"distance or similarity, vector_type, vector_dimension, levels and clusters attributes", name, VectorMethod)
}
