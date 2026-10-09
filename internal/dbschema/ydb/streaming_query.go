package ydb

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
)

type streamingQuerySource interface {
	DescribeStreamingQuery(context.Context, string) (ydbstreaming.Spec, error)
}

// DescribeStreamingQuery reads persistent metadata from the system view.
// Execution status and checkpoints are deliberately excluded: RUN is the
// declared state even while the query starts, stops, or retries an error.
func (s *grpcSource) DescribeStreamingQuery(ctx context.Context, path string) (ydbstreaming.Spec, error) {
	result, err := s.query(ctx, "SELECT Path, Text, Run, ResourcePool FROM "+systemView(s.database, "streaming_queries")+" WHERE Path = "+strconv.Quote(path))
	if err != nil {
		return ydbstreaming.Spec{}, err
	}
	if len(result.GetRows()) != 1 {
		return ydbstreaming.Spec{}, fmt.Errorf("streaming query %s: system view returned %d rows; retry after concurrent changes finish", path, len(result.GetRows()))
	}
	row := result.GetRows()[0]
	for _, name := range []string{"Path", "Text", "Run", "ResourcePool"} {
		if isNull(cell(row, result, name)) {
			return ydbstreaming.Spec{}, fmt.Errorf("streaming query %s: missing %s in its system-view description", path, name)
		}
	}
	if textOf(row, result, "Path") != path {
		return ydbstreaming.Spec{}, fmt.Errorf("streaming query %s: system view returned a different path", path)
	}
	spec := ydbstreaming.Spec{Text: textOf(row, result, "Text"), Run: new(boolOf(row, result, "Run")), ResourcePool: textOf(row, result, "ResourcePool")}
	if err := ydbstreaming.Validate(spec); err != nil {
		return ydbstreaming.Spec{}, err
	}
	return spec, nil
}

func (r *Reader) streamingQuery(ctx context.Context, source Source, schema, name string, db *catalog.Database) error {
	if !r.inScope(schema) {
		return nil
	}
	if !r.caps.Has(capability.StreamingQueries) {
		return unreadStreamingQuery(db, schema, name, "target capability streaming_queries is unavailable")
	}
	reader, ok := source.(streamingQuerySource)
	if !ok {
		return fmt.Errorf("streaming query %s: metadata source cannot describe streaming queries", r.absolute(schema, name))
	}
	spec, err := reader.DescribeStreamingQuery(ctx, r.absolute(schema, name))
	if errors.Is(err, ErrPrincipalsRefused) {
		return unreadStreamingQuery(db, schema, name, "streaming query metadata access was refused")
	}
	if err != nil {
		return err
	}
	db.FeatureObjects, err = db.FeatureObjects.With(ydbstreaming.ObservedObject(schema, name, spec))
	return err
}

func unreadStreamingQuery(db *catalog.Database, schema, name, reason string) error {
	records := append(db.FeatureCoverage.SubjectRecords(), schemaext.SubjectCoverage{
		Kind: ydbstreaming.Kind, Subject: ydbstreaming.Ref(schema, name), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: reason},
	})
	known, err := schemaext.NewCoverage(schemaext.Observed, db.FeatureCoverage.KindRecords(), records)
	if err != nil {
		return err
	}
	db.FeatureCoverage = known
	return nil
}
