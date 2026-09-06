package dbexprprobe_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbexprprobe"
)

// sqliteConn opens a file-backed SQLite connection: a live server whose
// dialect no resolver here probes, which is exactly what the gate tests need.
func sqliteConn(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	dbPath := filepath.Join(c.TB.(*testing.T).TempDir(), "gate.db")
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// TestResolversRefuseANilConnection pins that every resolver that needs a
// transaction names the missing connection instead of dereferencing it. The
// probes are non-empty on purpose: an empty list returns nil before the guard,
// and the test would pass without the guard existing.
func TestResolversRefuseANilConnection(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	_, err := dbexprprobe.ResolveCheckExpressions(ctx, nil, []dbexprprobe.CheckExpressionProbe{{Key: "k"}})
	c.Assert(err, qt.ErrorMatches, `resolve check expressions: database connection is nil`)

	_, err = dbexprprobe.ResolveDomainExpressions(ctx, nil, []dbexprprobe.DomainExpressionProbe{{Key: "k"}})
	c.Assert(err, qt.ErrorMatches, `resolve domain expressions: database connection is nil`)

	_, err = dbexprprobe.ResolvePolicyExpressions(ctx, nil, []dbexprprobe.PolicyExpressionProbe{{Key: "k"}})
	c.Assert(err, qt.ErrorMatches, `resolve policy expressions: database connection is nil`)

	_, err = dbexprprobe.ResolveIndexExpressions(ctx, nil, []dbexprprobe.IndexExpressionProbe{{Key: "k"}})
	c.Assert(err, qt.ErrorMatches, `resolve index expressions: database connection is nil`)

	_, err = dbexprprobe.ResolveContinuousAggregateBodies(ctx, nil, []dbexprprobe.ContinuousAggregateProbe{{Key: "k"}})
	c.Assert(err, qt.ErrorMatches, `resolve continuous aggregate bodies: database connection is nil`)
}

// TestResolversAnswerNilForAnEmptyProbeList pins the no-work fast path: no
// probes means no session, no transaction, and a nil map even on a nil
// connection's sibling -- a live one.
func TestResolversAnswerNilForAnEmptyProbeList(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := sqliteConn(c)

	checks, err := dbexprprobe.ResolveCheckExpressions(ctx, conn, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.IsNil)

	domains, err := dbexprprobe.ResolveDomainExpressions(ctx, conn, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(domains, qt.IsNil)

	aggregates, err := dbexprprobe.ResolveContinuousAggregateBodies(ctx, conn, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(aggregates, qt.IsNil)
}

// TestResolversAnswerNilForADialectThatStoresWhatItWasGiven pins the dialect
// gate on a live connection: SQLite stores the text it was given, so every
// resolver returns nil rather than probing -- and rather than erroring, which
// would break every comparison against an engine that needs no normalization.
func TestResolversAnswerNilForADialectThatStoresWhatItWasGiven(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := sqliteConn(c)

	checks, err := dbexprprobe.ResolveCheckExpressions(ctx, conn,
		[]dbexprprobe.CheckExpressionProbe{{Key: "t.ck", Expression: "price >= 0",
			Columns: []dbexprprobe.CheckProbeColumn{{Name: "price", Type: "numeric"}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.IsNil)

	domains, err := dbexprprobe.ResolveDomainExpressions(ctx, conn,
		[]dbexprprobe.DomainExpressionProbe{{Key: "d", BaseType: "text", Check: "VALUE <> ''"}})
	c.Assert(err, qt.IsNil)
	c.Assert(domains, qt.IsNil)

	policies, err := dbexprprobe.ResolvePolicyExpressions(ctx, conn,
		[]dbexprprobe.PolicyExpressionProbe{{Key: "t.pol", Using: "owner = 'x'",
			Columns: []dbexprprobe.CheckProbeColumn{{Name: "owner", Type: "text"}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(policies, qt.IsNil)

	indexes, err := dbexprprobe.ResolveIndexExpressions(ctx, conn,
		[]dbexprprobe.IndexExpressionProbe{{Key: "t_idx", Expression: "lower(code)",
			Columns: []dbexprprobe.CheckProbeColumn{{Name: "code", Type: "text"}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(indexes, qt.IsNil)

	aggregates, err := dbexprprobe.ResolveContinuousAggregateBodies(ctx, conn,
		[]dbexprprobe.ContinuousAggregateProbe{{Key: "agg", Body: "SELECT 1"}})
	c.Assert(err, qt.IsNil)
	c.Assert(aggregates, qt.IsNil)

	desired, err := dbexprprobe.ResolveGeneratedExpressions(ctx, conn,
		[]dbexprprobe.GeneratedExpressionProbe{{Table: "t", ProbeTable: "p",
			Create: "CREATE TABLE p (id INTEGER)", Generated: []string{"g"}}})
	c.Assert(err, qt.IsNil)
	c.Assert(desired, qt.IsNil)
}

// TestGeneratedExpressionProbeTable pins the two facts the name exists for:
// the index keeps two tables of one run apart, and the process id keeps two
// runs on one shared dev database apart.
func TestGeneratedExpressionProbeTable(t *testing.T) {
	c := qt.New(t)

	first := dbexprprobe.GeneratedExpressionProbeTable(0)
	second := dbexprprobe.GeneratedExpressionProbeTable(1)

	c.Assert(first, qt.Not(qt.Equals), second)
	c.Assert(strings.HasPrefix(first, "ptah_genexpr_probe_"), qt.IsTrue)
	c.Assert(first, qt.Contains, fmt.Sprintf("_%d_", os.Getpid()))
}
