package ydb

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/internal/ydbstream"
)

type streamingQuerySource interface {
	DescribeStreamingQuery(context.Context, string) (ast.StreamingQuerySpec, error)
}

// DescribeStreamingQuery reads persistent metadata from the system view.
// Execution status and checkpoints are deliberately excluded: RUN is the
// declared state even while the query starts, stops, or retries an error.
func (s *grpcSource) DescribeStreamingQuery(ctx context.Context, path string) (ast.StreamingQuerySpec, error) {
	result, err := s.query(ctx, "SELECT Path, Text, Run, ResourcePool FROM "+systemView(s.database, "streaming_queries")+" WHERE Path = "+strconv.Quote(path))
	if err != nil {
		return ast.StreamingQuerySpec{}, err
	}
	if len(result.GetRows()) != 1 {
		return ast.StreamingQuerySpec{}, fmt.Errorf("streaming query %s: system view returned %d rows; retry after concurrent changes finish", path, len(result.GetRows()))
	}
	row := result.GetRows()[0]
	for _, name := range []string{"Path", "Text", "Run", "ResourcePool"} {
		if isNull(cell(row, result, name)) {
			return ast.StreamingQuerySpec{}, fmt.Errorf("streaming query %s: missing %s in its system-view description", path, name)
		}
	}
	if textOf(row, result, "Path") != path {
		return ast.StreamingQuerySpec{}, fmt.Errorf("streaming query %s: system view returned a different path", path)
	}
	spec := ast.StreamingQuerySpec{Text: textOf(row, result, "Text"), Run: new(boolOf(row, result, "Run")), ResourcePool: textOf(row, result, "ResourcePool")}
	if err := ydbstream.Validate(spec); err != nil {
		return ast.StreamingQuerySpec{}, err
	}
	return spec, nil
}

func (r *Reader) streamingQuery(ctx context.Context, source Source, schema, name string, db *catalog.Database) error {
	reader, ok := source.(streamingQuerySource)
	if !ok {
		return fmt.Errorf("streaming query %s: metadata source cannot describe streaming queries", r.absolute(schema, name))
	}
	spec, err := reader.DescribeStreamingQuery(ctx, r.absolute(schema, name))
	if errors.Is(err, ErrPrincipalsRefused) {
		db.NotDescribed = db.NotDescribed.With(coverage.Refused(coverage.StreamingQuery))
		return nil
	}
	if err != nil {
		return err
	}
	db.StreamingQueries = append(db.StreamingQueries, catalog.StreamingQuery{Name: name, Schema: schema, Spec: spec.Clone()})
	return nil
}
