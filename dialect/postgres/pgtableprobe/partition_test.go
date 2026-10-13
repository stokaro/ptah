package pgtableprobe_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/dialect/postgres/pgtableprobe"
	"ptah.run/internal/dbschema/dbtest"
)

type probeSession struct {
	db          *sql.DB
	unavailable bool
}

func (s probeSession) WithRolledBackTransaction(ctx context.Context, _ string, body func(context.Context, *sql.Tx) error) (bool, error) {
	if s.unavailable {
		return false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	return true, body(ctx, tx)
}
func probeRequest(session schemaext.ProbeSession) schemaext.FacetNormalizationRequest {
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Table("App.words")
	return schemaext.FacetNormalizationRequest{Target: "postgres", Session: session,
		Owners:  []schemaext.ParentState{{Subject: subject, Desired: true, Current: true}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: must.Must(schemaext.NewFacets(&pgtable.DesiredPartition{Type: "LIST", Parts: []pgtable.PartitionPart{{Expr: "lower(word)"}}}))}}}}
}
func TestNormalizeFacets_CapturesServerKeyWithoutChangingDeclaration(t *testing.T) {
	c := qt.New(t)
	var statements []string
	db := dbtest.OpenWithExec(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		statements = append(statements, query)
		return dbtest.QueryResult{Columns: []string{"key"}, Rows: [][]driver.Value{{"LIST (lower((word)::text))"}}}, nil
	}, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		statements = append(statements, query)
		return driver.RowsAffected(0), nil
	})
	request := probeRequest(probeSession{db: db.SQL})
	result, err := (pgtableprobe.Service{}).NormalizeFacets(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	value, _, err := schemaext.FacetAs[*pgtable.DesiredPartition](result.Desired.Records[0].Values, pgtable.PartitionKind)
	c.Assert(err, qt.IsNil)
	c.Assert(value.Parts[0].Expr, qt.Equals, "lower(word)")
	c.Assert(value.Normalized.Key, qt.Equals, pgtable.PartitionComparisonKey(&pgtable.Partition{Type: "LIST", Parts: []pgtable.PartitionPart{{Expr: "lower((word)::text)"}}}))
	c.Assert(pgtable.ValidateDesiredPartition(value), qt.IsNil)
	original, _, err := schemaext.FacetAs[*pgtable.DesiredPartition](request.Desired.Records[0].Values, pgtable.PartitionKind)
	c.Assert(err, qt.IsNil)
	c.Assert(original.Normalized, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{`CREATE TABLE pg_temp.ptah_partition_probe (LIKE "App"."words") PARTITION BY LIST ((lower(word)))`, `SELECT pg_get_partkeydef('pg_temp.ptah_partition_probe'::regclass)`})
	value.Parts[0].Expr = "changed"
	c.Assert(original.Parts[0].Expr, qt.Equals, "lower(word)")
}
func TestNormalizeFacets_UnavailableProbeIsUnanswered(t *testing.T) {
	for _, session := range []schemaext.ProbeSession{nil, probeSession{unavailable: true}} {
		c := qt.New(t)
		result, err := (pgtableprobe.Service{}).NormalizeFacets(t.Context(), probeRequest(session))
		c.Assert(err, qt.IsNil)
		value, _, err := schemaext.FacetAs[*pgtable.DesiredPartition](result.Desired.Records[0].Values, pgtable.PartitionKind)
		c.Assert(err, qt.IsNil)
		c.Assert(value.Normalized, qt.IsNil)
	}
}
func TestNormalizeFacets_RefusedCreateIsUnanswered(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(t, nil, func(string, []driver.NamedValue) (driver.Result, error) { return nil, errors.New("server refuses key") })
	result, err := (pgtableprobe.Service{}).NormalizeFacets(t.Context(), probeRequest(probeSession{db: db.SQL}))
	c.Assert(err, qt.IsNil)
	value, _, err := schemaext.FacetAs[*pgtable.DesiredPartition](result.Desired.Records[0].Values, pgtable.PartitionKind)
	c.Assert(err, qt.IsNil)
	c.Assert(value.Normalized, qt.IsNil)
}
func TestNormalizeFacets_RejectsStatementEscapeBeforeProbing(t *testing.T) {
	c := qt.New(t)
	request := probeRequest(probeSession{})
	request.Desired.Records[0].Values = must.Must(schemaext.NewFacets(&pgtable.DesiredPartition{Type: "LIST", Parts: []pgtable.PartitionPart{{Expr: "word)); COMMIT; SELECT 1; --"}}}))
	result, err := (pgtableprobe.Service{}).NormalizeFacets(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, schemaext.FacetNormalizationResult{})
}
func TestNormalizeFacets_QueryFailureDiscardsResult(t *testing.T) {
	c := qt.New(t)
	sentinel := errors.New("catalog unavailable")
	db := dbtest.OpenWithExec(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) { return dbtest.QueryResult{}, sentinel }, func(string, []driver.NamedValue) (driver.Result, error) { return driver.RowsAffected(0), nil })
	result, err := (pgtableprobe.Service{}).NormalizeFacets(t.Context(), probeRequest(probeSession{db: db.SQL}))
	c.Assert(err, qt.ErrorIs, sentinel)
	c.Assert(result, qt.DeepEquals, schemaext.FacetNormalizationResult{})
}
func TestNormalizeFacets_Cancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (pgtableprobe.Service{}).NormalizeFacets(ctx, probeRequest(nil))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.FacetNormalizationResult{})
}
