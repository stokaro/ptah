package embedpg

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/internal/embedeval"
	"go.5x5.cz/ptah/internal/embedgen"
)

// Searcher runs an evaluation corpus against a live generation.
//
// It asks the database twice per case: once the way an application would, and
// once with the index switched off. The second is what separates a bad corpus
// from a bad index -- if an exhaustive scan finds the right documents and the
// index does not, the vectors are fine and the recall setting is not
// (stokaro/ptah#2068).
type Searcher struct {
	db   *sql.DB
	spec embedgen.Spec
}

// NewSearcher returns a searcher for a generation.
func NewSearcher(db *sql.DB, spec embedgen.Spec) (*Searcher, error) {
	if _, err := spec.TargetObjects(); err != nil {
		return nil, err
	}
	if len(spec.Source.KeyFields) == 0 {
		return nil, fmt.Errorf("the specification names no key fields, so a result cannot be named")
	}
	return &Searcher{db: db, spec: spec}, nil
}

// QueryParameters reports the session settings that decide what a search
// returns.
//
// ADR 0010 measured a 26.5%-100% recall span on one unchanged index from
// `ivfflat.probes` alone, so a retrieval number without these is not a number
// anybody can compare. They are read from the session rather than assumed,
// because a default that moved between PostgreSQL releases would silently make
// two runs incomparable.
func (s *Searcher) QueryParameters(ctx context.Context) (string, error) {
	settings := []string{"hnsw.ef_search", "ivfflat.probes"}
	rendered := make([]string, 0, len(settings))
	for _, setting := range settings {
		value, err := s.showSetting(ctx, setting)
		if err != nil {
			return "", err
		}
		rendered = append(rendered, setting+"="+value)
	}
	return strings.Join(rendered, ","), nil
}

// showSetting reads one session setting, or reports it as absent.
//
// A setting the server does not have is recorded as absent rather than skipped:
// two runs on servers with different pgvector versions are not comparable, and
// leaving the missing one out would make their parameter strings equal.
func (s *Searcher) showSetting(ctx context.Context, name string) (string, error) {
	// current_setting's missing_ok form answers NULL rather than an error for a
	// setting the server does not have, which is the case this is here for --
	// an ivfflat index and an hnsw one each leave the other's knob unset.
	var value sql.NullString
	err := s.db.QueryRowContext(ctx, "SELECT current_setting($1, true)", name).Scan(&value)
	if err != nil {
		return "", fmt.Errorf("read the %s setting: %w", name, err)
	}
	if !value.Valid || value.String == "" {
		return "(absent)", nil
	}
	return value.String, nil
}

// Search runs one case against the generation, through the index and without
// it.
func (s *Searcher) Search(
	ctx context.Context, testCase embedeval.Case, vector []float32, depth int,
) (embedeval.Result, error) {
	result := embedeval.Result{CaseID: testCase.ID}
	indexed, err := s.nearest(ctx, vector, depth, useIndex)
	if err != nil {
		result.Err = err.Error()
		return result, nil
	}
	exact, err := s.nearest(ctx, vector, depth, withoutIndex)
	if err != nil {
		result.Err = err.Error()
		return result, nil
	}
	result.Keys = indexed
	result.ExactKeys = exact
	result.ExactRun = true
	return result, nil
}

// scanMode says whether a search may use the index.
type scanMode int

const (
	// useIndex is what an application's query does.
	useIndex scanMode = iota
	// withoutIndex forces the exhaustive scan the index is measured against.
	withoutIndex
)

// nearest returns the keys closest to a vector.
func (s *Searcher) nearest(
	ctx context.Context, vector []float32, depth int, mode scanMode,
) ([]string, error) {
	generation := s.spec.Identity().Digest
	if depth <= 0 {
		return nil, fmt.Errorf("a search depth of %d asks for nothing", depth)
	}
	// One connection for the whole search, because the settings that force an
	// exhaustive scan are per-session and a pooled query would get a different
	// backend -- and silently use the index it was asked not to.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open a connection for the search: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if err := applyScanMode(ctx, conn, mode); err != nil {
		return nil, err
	}
	query, err := s.nearestQuery()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(ctx, query, vectorLiteral(vector), depth, generation)
	if err != nil {
		return nil, fmt.Errorf("search %s: %w", s.spec.Target.Table, err)
	}
	defer rows.Close()

	return scanKeys(rows, len(s.spec.Source.KeyFields))
}

// applyScanMode switches the index off where the caller asked for an exhaustive
// scan.
//
// `enable_indexscan` alone is not enough: the planner will reach for a bitmap
// scan instead and the answer comes back through the index anyway, which is the
// comparison silently measuring itself.
func applyScanMode(ctx context.Context, conn *sql.Conn, mode scanMode) error {
	if mode == useIndex {
		return nil
	}
	for _, statement := range []string{
		"SET LOCAL enable_indexscan = off",
		"SET LOCAL enable_bitmapscan = off",
		"SET LOCAL enable_indexonlyscan = off",
	} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("force an exhaustive scan: %w", err)
		}
	}
	return nil
}

// nearestQuery renders the nearest-neighbour search.
func (s *Searcher) nearestQuery() (string, error) {
	operator, err := distanceOperator(s.spec.Target.Metric)
	if err != nil {
		return "", err
	}
	keys := quoteAll(s.spec.Source.KeyFields)
	column := quoteIdentifier(s.spec.Target.Column)
	// #nosec G201 -- PostgreSQL takes no bind parameter for a relation, a
	// column or an operator. The identifiers come from the specification and go
	// through quoteIdentifier; the operator is chosen from a closed set by
	// distanceOperator; the vector and the depth are placeholders.
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s IS NOT NULL AND %s = $3 ORDER BY %s %s $1::vector LIMIT $2",
		strings.Join(castToText(keys), ", "), s.qualifiedTable(), column,
		quoteIdentifier(s.spec.Target.Column+GenerationSuffix),
		column, operator), nil
}

// distanceOperator is the pgvector operator for a metric.
//
// Chosen from a closed set rather than composed, and refused for a metric this
// build does not know: a wrong operator answers every query with the wrong
// distance, which produces plausible results in the wrong order.
func distanceOperator(metric embedgen.DistanceMetric) (string, error) {
	switch metric {
	case embedgen.MetricCosine:
		return "<=>", nil
	case embedgen.MetricL2:
		return "<->", nil
	case embedgen.MetricInnerProduct:
		return "<#>", nil
	default:
		return "", fmt.Errorf("no pgvector distance operator for metric %q", metric)
	}
}

// qualifiedTable renders the target table with its schema when it has one.
func (s *Searcher) qualifiedTable() string {
	if schema := strings.TrimSpace(s.spec.Target.Schema); schema != "" {
		return quoteIdentifier(schema) + "." + quoteIdentifier(s.spec.Target.Table)
	}
	return quoteIdentifier(s.spec.Target.Table)
}

// scanKeys reads the result keys, joined the way verification names them.
func scanKeys(rows *sql.Rows, keyCount int) ([]string, error) {
	var keys []string
	for rows.Next() {
		values := make([]sql.NullString, keyCount)
		targets := make([]any, len(values))
		for index := range values {
			targets[index] = &values[index]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("read a search result: %w", err)
		}
		components := make([]string, len(values))
		for index, value := range values {
			components[index] = value.String
		}
		keys = append(keys, strings.Join(components, "\x1f"))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the search results: %w", err)
	}
	return keys, nil
}
