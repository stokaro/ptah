package pgtableprobe_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/dialect/postgres/pgtableprobe"
	"ptah.run/internal/dbschema/dbtest"
)

func exclusionProbeRequest(session schemaext.ProbeSession) schemaext.NormalizationRequest {
	ref := pgtable.ExclusionRef(`"App.One"."Rooms"`, `"No.Overlap"`, identifier.ForDialect("postgres"))
	desired := &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "(room + 1) WITH =", Comment: "Room schedule"}}
	observed := &pgtable.ObservedExclusion{UsingMethod: "btree", Elements: "room WITH ="}
	return schemaext.NormalizationRequest{Target: "postgres", Session: session, Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: desired}))}, Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: observed}))}}
}

func TestExclusionNormalization_ServerSpelling(t *testing.T) {
	c := qt.New(t)
	var statements []string
	db := dbtest.OpenWithExec(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{Columns: []string{"definition"}, Rows: [][]driver.Value{{"EXCLUDE USING btree (((room + 1)) WITH =)"}}}, nil
	}, func(statement string, _ []driver.NamedValue) (driver.Result, error) {
		statements = append(statements, statement)
		return driver.RowsAffected(0), nil
	})
	request := exclusionProbeRequest(probeSession{db: db.SQL})
	result, err := (pgtableprobe.ExclusionService{}).NormalizeObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	objects, err := result.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	value := objects[0].Value.(*pgtable.DesiredExclusion)
	c.Assert(value.Elements, qt.Equals, "(room + 1) WITH =")
	c.Assert(value.Normalized.Elements, qt.Equals, "((room + 1)) WITH =")
	c.Assert(value.Normalized.SourceKey, qt.Equals, pgtable.ExclusionComparisonKey(value.Exclusion))
	c.Assert(value.Comment, qt.Equals, "Room schedule")
	c.Assert(statements, qt.DeepEquals, []string{`CREATE TEMPORARY TABLE "pg_temp"."Rooms" (LIKE "App.One"."Rooms", EXCLUDE USING btree ((room + 1) WITH =))`})
	input, err := request.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(input[0].Value.(*pgtable.DesiredExclusion).Normalized, qt.IsNil)
}

func TestExclusionNormalization_WithoutSessionClearsFacts(t *testing.T) {
	c := qt.New(t)
	request := exclusionProbeRequest(nil)
	objects, err := request.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	value := objects[0].Value.(*pgtable.DesiredExclusion)
	value.Normalized = &pgtable.NormalizedExclusion{SourceKey: pgtable.ExclusionComparisonKey(value.Exclusion), Elements: "((room + 1)) WITH ="}
	request.Desired.Objects = must.Must(schemaext.NewObjects(objects...))
	result, err := (pgtableprobe.ExclusionService{}).NormalizeObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	normalized, err := result.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(normalized[0].Value.(*pgtable.DesiredExclusion).Normalized, qt.IsNil)
	c.Assert(value.Normalized, qt.IsNotNil)
}

func TestExclusionNormalization_ReadFailure(t *testing.T) {
	c := qt.New(t)
	sentinel := errors.New("read fails")
	db := dbtest.OpenWithExec(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) { return dbtest.QueryResult{}, sentinel }, func(string, []driver.NamedValue) (driver.Result, error) { return driver.RowsAffected(0), nil })
	result, err := (pgtableprobe.ExclusionService{}).NormalizeObjects(t.Context(), exclusionProbeRequest(probeSession{db: db.SQL}))
	c.Assert(err, qt.ErrorIs, sentinel)
	c.Assert(result, qt.DeepEquals, schemaext.NormalizationResult{})
}

func TestExclusionNormalization_Cancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (pgtableprobe.ExclusionService{}).NormalizeObjects(ctx, exclusionProbeRequest(nil))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.NormalizationResult{})
}

// An unrepresented catalog response invalidates the complete batch, including
// normalization collected before that response.
func TestExclusionNormalization_UnreadDefinitionDiscardsBatch(t *testing.T) {
	c := qt.New(t)
	definitions := []string{"EXCLUDE USING btree (r WITH =)", "EXCLUDE USING btree (r WITH =) INCLUDE (s)"}
	calls := 0
	db := dbtest.OpenWithExec(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		definition := definitions[calls]
		calls++
		return dbtest.QueryResult{Columns: []string{"definition"}, Rows: [][]driver.Value{{definition}}}, nil
	}, func(string, []driver.NamedValue) (driver.Result, error) { return driver.RowsAffected(0), nil })
	request := schemaext.NormalizationRequest{Target: "postgres", Session: probeSession{db: db.SQL}}
	var err error
	for _, name := range []string{"first", "second"} {
		ref := pgtable.ExclusionRef("rooms", name, identifier.ForDialect("postgres"))
		request.Desired.Objects, err = request.Desired.Objects.With(schemaext.Object{Ref: ref,
			Value: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "r WITH ="}}})
		c.Assert(err, qt.IsNil)
		request.Current.Objects, err = request.Current.Objects.With(schemaext.Object{Ref: ref,
			Value: &pgtable.ObservedExclusion{UsingMethod: "btree", Elements: "r WITH ="}})
		c.Assert(err, qt.IsNil)
	}
	result, err := (pgtableprobe.ExclusionService{}).NormalizeObjects(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `(?s).*normalize EXCLUDE .*second: .*clause INCLUDE`)
	c.Assert(result, qt.DeepEquals, schemaext.NormalizationResult{})
	c.Assert(calls, qt.Equals, 2)
	objects, err := request.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects[0].Value.(*pgtable.DesiredExclusion).Normalized, qt.IsNil)
}

type countingExclusionSession struct{ calls *int }

func (s countingExclusionSession) WithRolledBackTransaction(context.Context, string, func(context.Context, *sql.Tx) error) (bool, error) {
	*s.calls++
	return false, nil
}

func TestExclusionNormalization_EmptyBatchDoesNotProbe(t *testing.T) {
	c := qt.New(t)
	calls := 0
	request := schemaext.NormalizationRequest{Target: "postgres", Session: countingExclusionSession{calls: &calls}}
	result, err := (pgtableprobe.ExclusionService{}).NormalizeObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
	c.Assert(calls, qt.Equals, 0)
}

func TestExclusionNormalization_RefusesAnotherTarget(t *testing.T) {
	c := qt.New(t)
	request := exclusionProbeRequest(nil)
	request.Target = "sqlite"
	result, err := (pgtableprobe.ExclusionService{}).NormalizeObjects(t.Context(), request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(err, qt.ErrorMatches, `.*EXCLUDE normalization on "sqlite"`)
	c.Assert(result, qt.DeepEquals, schemaext.NormalizationResult{})
}
