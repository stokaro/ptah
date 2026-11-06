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
	"ptah.run/core/ptaherr"
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

const partitionBoundSource = "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"

func boundProbeRequest(session schemaext.ProbeSession, held bool) schemaext.FacetNormalizationRequest {
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "e1")
	return schemaext.FacetNormalizationRequest{Target: "postgres", Session: session,
		Owners: []schemaext.ParentState{{Subject: subject, Desired: true, Current: held}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: subject, Values: must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{
			PartitionMember: pgtable.PartitionMember{ParentSchema: "App", Parent: "events", Bound: partitionBoundSource},
		}))}}}}
}

func normalizedBound(c *qt.C, result schemaext.FacetNormalizationResult) *pgtable.NormalizedPartitionBound {
	c.Helper()
	value, found, err := schemaext.FacetAs[*pgtable.DesiredPartitionMember](result.Desired.Records[0].Values, pgtable.PartitionMemberKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Bound, qt.Equals, partitionBoundSource)
	return value.Normalized
}

// TestNormalizeBound_CapturesTheServersSpelling reads the bound of a temporary copy
// built with the parent's own key, and leaves the declaration as written.
func TestNormalizeBound_CapturesTheServersSpelling(t *testing.T) {
	c := qt.New(t)
	server := "FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')"
	var statements []string
	answers := map[string]string{
		`SELECT pg_get_partkeydef($1::regclass)`: "RANGE (happened_at)",
		`SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = 'pg_temp.ptah_partition_bound_probe'::regclass`: server,
	}
	db := dbtest.OpenWithExec(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		statements = append(statements, query)
		return dbtest.QueryResult{Columns: []string{"answer"}, Rows: [][]driver.Value{{answers[query]}}}, nil
	}, func(query string, _ []driver.NamedValue) (driver.Result, error) {
		statements = append(statements, query)
		return driver.RowsAffected(0), nil
	})

	result, err := (pgtableprobe.PartitionMemberService{}).NormalizeFacets(t.Context(), boundProbeRequest(probeSession{db: db.SQL}, true))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(normalizedBound(c, result), qt.DeepEquals, &pgtable.NormalizedPartitionBound{
		SourceKey: pgtable.PartitionBoundKey(partitionBoundSource), Key: pgtable.PartitionBoundKey(server)})
	c.Assert(statements, qt.DeepEquals, []string{
		`SELECT pg_get_partkeydef($1::regclass)`,
		`CREATE TABLE pg_temp.ptah_partition_bound_parent (LIKE "App"."events") PARTITION BY RANGE (happened_at)`,
		`CREATE TABLE pg_temp.ptah_partition_bound_probe PARTITION OF pg_temp.ptah_partition_bound_parent ` + partitionBoundSource,
		`SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = 'pg_temp.ptah_partition_bound_probe'::regclass`,
	})
}

// TestNormalizeBound_LeavesNoFact covers every way the probe has no answer. A table
// the database does not hold yet has no bound to compare with, and a refused bound is
// the apply's to report.
func TestNormalizeBound_LeavesNoFact(t *testing.T) {
	refused := func(string, []driver.NamedValue) (driver.Result, error) {
		return nil, errors.New("server refuses bound")
	}
	key := func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{Columns: []string{"key"}, Rows: [][]driver.Value{{"RANGE (a)"}}}, nil
	}
	missing := func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, errors.New("no such relation")
	}
	for _, test := range []struct {
		name    string
		session func(t *testing.T) schemaext.ProbeSession
		held    bool
	}{
		{name: "no session", session: func(*testing.T) schemaext.ProbeSession { return nil }, held: true},
		{name: "no transaction", session: func(*testing.T) schemaext.ProbeSession { return probeSession{unavailable: true} }, held: true},
		{name: "a partition not held yet", session: func(*testing.T) schemaext.ProbeSession { return probeSession{unavailable: true} }},
		{name: "a parent the server cannot find", session: func(t *testing.T) schemaext.ProbeSession {
			return probeSession{db: dbtest.OpenWithExec(t, missing, refused).SQL}
		}, held: true},
		{name: "a bound the server refuses", session: func(t *testing.T) schemaext.ProbeSession {
			return probeSession{db: dbtest.OpenWithExec(t, key, refused).SQL}
		}, held: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := (pgtableprobe.PartitionMemberService{}).NormalizeFacets(t.Context(), boundProbeRequest(test.session(t), test.held))

			c.Assert(err, qt.IsNil)
			c.Assert(normalizedBound(c, result), qt.IsNil)
		})
	}
}

func TestNormalizeBound_RefusesAStatementInTheBound(t *testing.T) {
	c := qt.New(t)
	request := boundProbeRequest(probeSession{}, true)
	request.Desired.Records[0].Values = must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{
		PartitionMember: pgtable.PartitionMember{Parent: "events", Bound: "DEFAULT; COMMIT"},
	}))

	result, err := (pgtableprobe.PartitionMemberService{}).NormalizeFacets(t.Context(), request)

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, schemaext.FacetNormalizationResult{})
}

func TestNormalizeBound_RefusesATargetWithoutPartitions(t *testing.T) {
	c := qt.New(t)
	request := boundProbeRequest(probeSession{}, true)
	request.Target = "mysql"

	_, err := (pgtableprobe.PartitionMemberService{}).NormalizeFacets(t.Context(), request)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
}
