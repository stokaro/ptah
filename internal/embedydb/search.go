package embedydb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedeval"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedverify"
)

// searchTopSize is the pragma that decides how many clusters of a
// vector_kmeans_tree a search descends into, and so what it can find.
const searchTopSize = "ydb.KMeansTreeSearchTopSize"

// Searcher runs an evaluation corpus against a generation on YDB.
//
// It asks twice per case: through the generation's index, as an application
// would with `VIEW`, and without it, which is an exhaustive scan. The second
// separates a bad corpus from a bad index. A generation with no index, or one
// whose index is not there, answers both with the exhaustive scan, as a query
// without `VIEW` does.
type Searcher struct {
	backend *Backend
	spec    embedgen.Spec
	order   string

	once    sync.Once
	indexed string
	err     error
}

// NewSearcher returns a searcher for a generation.
func (b *Backend) NewSearcher(spec embedgen.Spec) (embedbackend.Searcher, error) {
	if _, err := TargetObjects(spec); err != nil {
		return nil, err
	}
	if len(spec.Source.KeyFields) == 0 {
		return nil, fmt.Errorf("the specification names no key fields, so a result cannot be named")
	}
	order, err := distanceOrder(spec.Target.Metric, quote(spec.Target.Column))
	if err != nil {
		return nil, err
	}
	return &Searcher{backend: b, spec: spec, order: order}, nil
}

// distanceOrder is the ORDER BY a nearest-neighbour search sorts by: the Knn
// distance ascending, or the inner-product similarity descending. It is chosen
// from a closed set, because a wrong function answers every query with
// plausible rows in the wrong order.
func distanceOrder(metric embedgen.DistanceMetric, column string) (string, error) {
	switch metric {
	case embedgen.MetricCosine:
		return "Knn::CosineDistance(" + column + ", $p1)", nil
	case embedgen.MetricL2:
		return "Knn::EuclideanDistance(" + column + ", $p1)", nil
	case embedgen.MetricInnerProduct:
		return "Knn::InnerProductSimilarity(" + column + ", $p1) DESC", nil
	default:
		return "", fmt.Errorf("%w: no YDB distance function for metric %q", ErrUnsupported, metric)
	}
}

// QueryParameters reports the setting that decides what an indexed search
// returns. Ptah does not set it, so a search runs under the server's default.
func (s *Searcher) QueryParameters(context.Context) (string, error) {
	return searchTopSize + "=(server default)", nil
}

// index finds the generation's index once, empty when there is none to search
// through.
func (s *Searcher) index(ctx context.Context) (string, error) {
	s.once.Do(func() {
		objects, err := TargetObjects(s.spec)
		if err != nil || !objects.HasIndex {
			s.err = err
			return
		}
		described, _, err := s.backend.describe(ctx, s.spec.Target.Schema, s.spec.Target.Table)
		if err != nil {
			s.err = err
			return
		}
		if index, found := described.Index(objects.Index.Name); found && index.Ready {
			s.indexed = objects.Index.Name
		}
	})
	return s.indexed, s.err
}

// Search runs one case through the index and without it.
func (s *Searcher) Search(
	ctx context.Context, testCase embedeval.Case, vector []float32, depth int,
) (embedeval.Result, error) {
	result := embedeval.Result{CaseID: testCase.ID}
	index, err := s.index(ctx)
	if err != nil {
		return result, err
	}
	indexed, err := s.nearest(ctx, vector, depth, index)
	if err != nil {
		result.Err = err.Error()
		return result, nil
	}
	exact, err := s.nearest(ctx, vector, depth, "")
	if err != nil {
		result.Err = err.Error()
		return result, nil
	}
	result.Keys, result.ExactKeys, result.ExactRun = indexed, exact, true
	return result, nil
}

// nearest returns the keys closest to a vector, through index when one is
// named and by exhaustive scan when none is.
func (s *Searcher) nearest(ctx context.Context, vector []float32, depth int, index string) ([]string, error) {
	if depth <= 0 {
		return nil, fmt.Errorf("a search depth of %d asks for nothing", depth)
	}
	encoded, err := EncodeVector(vector)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(s.spec.Source.KeyFields))
	for position, field := range s.spec.Source.KeyFields {
		keys[position] = asText("", field)
	}
	view := ""
	if index != "" {
		view = " VIEW " + quote(index)
	}
	column := quote(s.spec.Target.Column)
	// #nosec G201 -- YQL takes no parameter for a path, a column or an index.
	// The names come from the specification through quote; the function is
	// chosen from a closed set; the vector and the generation are parameters
	// and the depth is an integer.
	query := fmt.Sprintf("SELECT %s FROM %s%s WHERE %s IS NOT NULL AND %s = $p2 ORDER BY %s LIMIT %d",
		strings.Join(keys, ", "), quoteTable(s.spec.Target.Schema, s.spec.Target.Table), view,
		column, quote(s.spec.Target.Column+generationSuffix), s.order, depth)
	rows, err := s.backend.db.QueryContext(ctx, query, encoded, s.spec.Identity().Digest)
	if err != nil {
		return nil, fmt.Errorf("search %s: %w", s.spec.Target.Table, err)
	}
	defer func() { _ = rows.Close() }()
	var found []string
	for rows.Next() {
		values := make([]sql.NullString, len(keys))
		destinations := make([]any, len(values))
		for position := range values {
			destinations[position] = &values[position]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("read a search result: %w", err)
		}
		components := make([]string, len(values))
		for position, value := range values {
			components[position] = value.String
		}
		// The separator a corpus file writes, so a result matches what a
		// person wrote there.
		found = append(found, strings.Join(components, embedverify.KeyFieldSeparator))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the search results: %w", err)
	}
	return found, nil
}
