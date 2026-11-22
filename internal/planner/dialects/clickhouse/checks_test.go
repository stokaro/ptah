package clickhouse_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/rundiag"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// checkTable is a declared table t over an empty sorting key, with the fields
// and constraints a test gives it.
func checkTable(fields []schemamodel.Field, constraints ...schemamodel.Constraint) *schemamodel.Database {
	storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()"}
	return &schemamodel.Database{
		Tables:      []schemamodel.Table{{Name: "t", StructName: "T", Facets: must.Must(schemaext.NewFacets(storage.Desired()))}},
		Fields:      fields,
		Constraints: constraints,
	}
}

// observedCheckTable is table t as a read reports it, with its Int32 columns,
// a String column s where columns name it, and the CHECK constraints the
// server prints.
func observedCheckTable(columns []string, checks ...catalog.Constraint) *catalog.Database {
	storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()"}
	table := catalog.Table{Name: "t", Facets: must.Must(schemaext.NewFacets(storage))}
	for _, column := range columns {
		columnType := map[bool]string{true: "String", false: "Int32"}[column == "s"]
		table.Columns = append(table.Columns, catalog.Column{Name: column, DataType: columnType, ColumnType: columnType, IsNullable: "NO"})
	}
	for i := range checks {
		checks[i].TableName, checks[i].Type = "t", "CHECK"
	}
	return &catalog.Database{FeatureCoverage: observedStorageCoverage(), Tables: []catalog.Table{table}, Constraints: checks}
}

func int32Field(name, check string) schemamodel.Field {
	return schemamodel.Field{StructName: "T", Name: name, Type: "Int32", Check: check}
}

func tableCheck(name, expression string) schemamodel.Constraint {
	return schemamodel.Constraint{StructName: "T", Table: "t", Type: "CHECK", Name: name, CheckExpression: expression}
}

func reportedCheck(name, clause string) catalog.Constraint {
	return catalog.Constraint{Name: name, CheckClause: new(clause)}
}

func checkStatements(c *qt.C, desired *schemamodel.Database, current *catalog.Database) []string {
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	return statements
}

// ClickHouse refuses a DROP COLUMN of a column a CHECK reads and an ADD
// CONSTRAINT over a column the table lacks, so the CHECK drops go first and
// the additions last. An unnamed CHECK takes the name ClickHouse needs, and an
// added column's CHECK is a constraint of its own, since ADD COLUMN has none.
func TestGenerateMigrationAST_PlansCheckChangesAroundTheColumnChanges(t *testing.T) {
	c := qt.New(t)
	desired := checkTable(
		[]schemamodel.Field{int32Field("id", ""), int32Field("v", ""), int32Field("w", "w > 0")},
		tableCheck("", "v > 0"), tableCheck("t_named", "v < 100"),
	)
	current := observedCheckTable([]string{"id", "v", "u"}, reportedCheck("t_u_check", "u > 0"), reportedCheck("t_named", "v < 50"))

	got := checkStatements(c, desired, current)

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE t DROP CONSTRAINT t_named",
		"ALTER TABLE t DROP CONSTRAINT t_u_check",
		"ALTER TABLE t ADD COLUMN w Int32",
		"ALTER TABLE t DROP COLUMN u",
		heldRows + "SELECT throwIf(count() > 0, 't_w_check is broken by rows t holds') FROM t WHERE NOT ifNull((w > 0), 0)",
		"ALTER TABLE t ADD CONSTRAINT t_w_check CHECK (w > 0)",
		"SELECT throwIf(count() > 0, 't_w_check is broken by rows t holds') FROM t WHERE NOT ifNull((w > 0), 0)",
		heldRows + "SELECT throwIf(count() > 0, 't_check is broken by rows t holds') FROM t WHERE NOT ifNull((v > 0), 0)",
		"ALTER TABLE t ADD CONSTRAINT t_check CHECK (v > 0)",
		"SELECT throwIf(count() > 0, 't_check is broken by rows t holds') FROM t WHERE NOT ifNull((v > 0), 0)",
		heldRows + "SELECT throwIf(count() > 0, 't_named is broken by rows t holds') FROM t WHERE NOT ifNull((v < 100), 0)",
		"ALTER TABLE t ADD CONSTRAINT t_named CHECK (v < 100)",
		"SELECT throwIf(count() > 0, 't_named is broken by rows t holds') FROM t WHERE NOT ifNull((v < 100), 0)",
	})
}

// heldRows is the note before the scan that precedes each ADD CONSTRAINT of
// table t.
const heldRows = "-- CLICKHOUSE: ADD CONSTRAINT checks only the rows written after it; the SELECTs before and after it each scan t for held rows the CHECK refuses\n"

// A table whose only change is a CHECK has no column change for the request
// to capture it by, and its steps still plan.
func TestGenerateMigrationAST_PlansACheckOnlyChange(t *testing.T) {
	c := qt.New(t)
	desired := checkTable([]schemamodel.Field{int32Field("id", ""), int32Field("v", "")}, tableCheck("", "v > 1"))
	current := observedCheckTable([]string{"id", "v"}, reportedCheck("t_check", "v > 0"))

	got := checkStatements(c, desired, current)

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE t DROP CONSTRAINT t_check",
		heldRows + "SELECT throwIf(count() > 0, 't_check is broken by rows t holds') FROM t WHERE NOT ifNull((v > 1), 0)",
		"ALTER TABLE t ADD CONSTRAINT t_check CHECK (v > 1)",
		"SELECT throwIf(count() > 0, 't_check is broken by rows t holds') FROM t WHERE NOT ifNull((v > 1), 0)",
	})
}

// The same holds for a table whose only change is its comment.
func TestGenerateMigrationAST_PlansATableCommentOnlyChange(t *testing.T) {
	c := qt.New(t)
	desired := checkTable([]schemamodel.Field{int32Field("id", "")})
	desired.Tables[0].Comment = "hello"
	current := observedCheckTable([]string{"id"})

	got := checkStatements(c, desired, current)

	c.Assert(got, qt.DeepEquals, []string{"ALTER TABLE t MODIFY COMMENT 'hello'"})
}

// What each line prints for `SELECT (<condition>)`, measured on 26.10.3.7 and
// 24.10.4.191 over each declared spelling and over the text its
// create_table_query holds for it.
var (
	printed2610 = map[string]string{
		"CAST(v AS Int64) > 0": "(CAST(v, 'Int64') > 0)", "v::Int64 > 0": "(CAST(v, 'Int64') > 0)",
		"(CAST(v, 'Int64') > 0)": "(CAST(v, 'Int64') > 0)",
		"s ILIKE 'a%' OR s = ''": "((s ILIKE 'a%') OR (s = ''))", "((s ILIKE 'a%') OR (s = ''))": "((s ILIKE 'a%') OR (s = ''))",
		"equals(v, v)": "(equals(v, v))", "(equals(v, v))": "(equals(v, v))",
		"greater(v, 0)": "(greater(v, 0))", "(greater(v, 0))": "(greater(v, 0))",
		"v != 7": "(v != 7)", "(v != 7)": "(v != 7)",
	}
	printed2410 = map[string]string{
		"CAST(v AS Int64) > 0": "CAST(v, 'Int64') > 0", "v::Int64 > 0": "CAST(v, 'Int64') > 0",
		"CAST(v, 'Int64') > 0":   "CAST(v, 'Int64') > 0",
		"s ILIKE 'a%' OR s = ''": "(s ILIKE 'a%') OR (s = '')", "(s ILIKE 'a%') OR (s = '')": "(s ILIKE 'a%') OR (s = '')",
		"equals(v, v)": "v = v", "v = v": "v = v", "greater(v, 0)": "v > 0", "v > 0": "v > 0",
		"v != 7": "v != 7", "v != 5": "v != 5", "v < 10": "v < 10", "V > 0": "V > 0",
	}
)

// formattingServer is a ClickHouse server that prints each condition as
// printed holds it for the formatting query the comparison sends, and fails
// any other query, so a statement the comparison runs unmeasured fails the
// test. One that lacks the formatter finds none in system.functions and fails
// each call to it, as a release older than the function does.
type formattingServer struct {
	printed        map[string]string
	lacksFormatter bool
}

func (s formattingServer) query(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	if query == "SELECT count() FROM system.functions WHERE name = 'formatQuerySingleLineOrNull'" {
		found := map[bool]uint64{false: 1, true: 0}[s.lacksFormatter]
		return dbtest.QueryResult{Columns: []string{"count()"}, Rows: [][]driver.Value{{found}}}, nil
	}
	if s.lacksFormatter {
		return dbtest.QueryResult{}, errors.New("code: 46, message: Unknown function formatQuerySingleLineOrNull")
	}
	if !strings.Contains(query, "formatQuerySingleLine") || len(args) != 1 {
		return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
	}
	condition, _ := args[0].Value.(string)
	printed, found := s.printed[condition]
	if !found {
		return dbtest.QueryResult{}, fmt.Errorf("no measured formatting of %q", condition)
	}
	return dbtest.QueryResult{Columns: []string{"formatted"}, Rows: [][]driver.Value{{"SELECT " + printed}}}, nil
}

// serverConnection is a connection to the ClickHouse server db answers for.
// A declined one runs no probe, as a session with no transaction to run one
// in, and the comparison reads each CHECK as text.
type serverConnection struct {
	db       *sql.DB
	declined bool
}

func (serverConnection) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: "clickhouse"}
}

func (serverConnection) ResolveIdentifierSemantics(ctx context.Context, _ []string) (identifier.Semantics, error) {
	return identifier.ForDialect("clickhouse"), ctx.Err()
}

func (s serverConnection) WithRolledBackTransaction(ctx context.Context, _ string, body func(context.Context, *sql.Tx) error) (bool, error) {
	if s.declined {
		return false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	return true, body(ctx, tx)
}

// serverCheckStatements plans desired against current as a plan against the
// connected server, or against one that runs no probe where declined.
func serverCheckStatements(c *qt.C, server formattingServer, declined bool, desired *schemamodel.Database, current *catalog.Database) []string {
	runtime := must.Must(builtin.New())
	db := dbtest.Open(c, server.query)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), serverConnection{db: db.SQL, declined: declined}, desired, current, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	return statements
}

// The server stores a CHECK reformatted, each line in its own way, and the
// declaration it came from plans nothing: a connected server formats both
// sides, and a comparison with no probe, or against a server without
// formatQuerySingleLineOrNull, reads the grouping the operators' precedence
// gives.
func TestGenerateMigrationAST_ACheckTheServerReformatsPlansNothing(t *testing.T) {
	textual := func() (*schemamodel.Database, *catalog.Database) {
		return checkTable(
				[]schemamodel.Field{int32Field("id", ""), int32Field("v", "v != 7"), {StructName: "T", Name: "s", Type: "String"}},
				tableCheck("", "v > 0"), tableCheck("", "v<>5 AND v BETWEEN 1 AND 100"), tableCheck("t_named", "length(s) < 10 OR s LIKE 'x%'"),
			),
			observedCheckTable([]string{"id", "v", "s"},
				reportedCheck("t_v_check", "v != 7"), reportedCheck("t_check", "v > 0"),
				reportedCheck("t_check1", "(v != 5) AND ((v >= 1) AND (v <= 100))"), reportedCheck("t_named", "(length(s) < 10) OR (s LIKE 'x%')"),
			)
	}
	noProbeDesired, noProbeCurrent := textual()
	noFormatterDesired, noFormatterCurrent := textual()
	for _, test := range []struct {
		name     string
		server   formattingServer
		declined bool
		desired  *schemamodel.Database
		current  *catalog.Database
	}{
		{
			name: "26.10", server: formattingServer{printed: printed2610},
			desired: checkTable(
				[]schemamodel.Field{int32Field("id", ""), int32Field("v", "v != 7"), {StructName: "T", Name: "s", Type: "String"}},
				tableCheck("", "CAST(v AS Int64) > 0"), tableCheck("", "v::Int64 > 0"), tableCheck("", "s ILIKE 'a%' OR s = ''"),
				tableCheck("", "equals(v, v)"), tableCheck("", "greater(v, 0)"),
			),
			current: observedCheckTable([]string{"id", "v", "s"}, reportedCheck("t_v_check", "(v != 7)"),
				reportedCheck("t_check", "(CAST(v, 'Int64') > 0)"), reportedCheck("t_check1", "(CAST(v, 'Int64') > 0)"),
				reportedCheck("t_check2", "((s ILIKE 'a%') OR (s = ''))"), reportedCheck("t_check3", "(equals(v, v))"),
				reportedCheck("t_check4", "(greater(v, 0))"),
			),
		},
		{
			name: "24.10", server: formattingServer{printed: printed2410},
			desired: checkTable(
				[]schemamodel.Field{int32Field("id", ""), int32Field("v", "v != 7"), {StructName: "T", Name: "s", Type: "String"}},
				tableCheck("", "CAST(v AS Int64) > 0"), tableCheck("", "v::Int64 > 0"), tableCheck("", "s ILIKE 'a%' OR s = ''"),
				tableCheck("", "equals(v, v)"), tableCheck("", "greater(v, 0)"),
			),
			current: observedCheckTable([]string{"id", "v", "s"}, reportedCheck("t_v_check", "v != 7"),
				reportedCheck("t_check", "CAST(v, 'Int64') > 0"), reportedCheck("t_check1", "CAST(v, 'Int64') > 0"),
				reportedCheck("t_check2", "(s ILIKE 'a%') OR (s = '')"), reportedCheck("t_check3", "v = v"),
				reportedCheck("t_check4", "v > 0"),
			),
		},
		{name: "no probe", declined: true, desired: noProbeDesired, current: noProbeCurrent},
		{
			name: "a server without formatQuerySingleLineOrNull", server: formattingServer{lacksFormatter: true},
			desired: noFormatterDesired, current: noFormatterCurrent,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := serverCheckStatements(c, test.server, test.declined, test.desired, test.current)

			c.Assert(got, qt.HasLen, 0)
		})
	}
}

// A comparison against a server without formatQuerySingleLineOrNull warns,
// when it returns and so before the plan it feeds is printed, that the CHECK
// it pairs is compared as written: once, though the naming and the
// comparison both ask about it.
func TestCompareWithDatabase_WarnsBeforeThePlanOnAServerWithoutTheFormatter(t *testing.T) {
	c := qt.New(t)
	var stream bytes.Buffer
	ctx := rundiag.With(c.Context(), rundiag.New(rundiag.Options{Stream: &stream}))
	db := dbtest.Open(c, formattingServer{lacksFormatter: true}.query)
	desired := checkTable([]schemamodel.Field{int32Field("id", ""), int32Field("v", "")}, tableCheck("", "greater(v, 0)"))
	current := observedCheckTable([]string{"id", "v"}, reportedCheck("t_check", "v > 0"))

	_, err := schemadiff.CompareWithDatabase(ctx, serverConnection{db: db.SQL}, desired, current, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(stream.String(), qt.Equals, "warning: CHECK t_check on t is compared as written, not in the form the server stores: "+
		"the server has no formatQuerySingleLineOrNull function\n")
}

// An unnamed CHECK keeps the name the server holds it under. A swap of two
// unnamed CHECKs plans nothing, and so does a swap of two that only the
// server reads as the declared ones. A named CHECK that takes the first name
// replaces only the CHECK it displaces. A CHECK of the same condition on
// another table lends no name. A column in another case is another column on
// ClickHouse, so a CHECK moved to it is replaced. The server and the text
// comparison agree on each row both of them can decide.
func TestGenerateMigrationAST_AnUnnamedCheckKeepsTheNameTheServerHoldsItUnder(t *testing.T) {
	swapped := func() (*schemamodel.Database, *catalog.Database) {
		return checkTable([]schemamodel.Field{int32Field("id", ""), int32Field("v", "")}, tableCheck("", "v != 5"), tableCheck("", "v < 10")),
			observedCheckTable([]string{"id", "v"}, reportedCheck("t_check", "v < 10"), reportedCheck("t_check1", "v != 5"))
	}
	neighbor := func() (*schemamodel.Database, *catalog.Database) {
		return checkTable([]schemamodel.Field{int32Field("id", ""), int32Field("v", "")},
				tableCheck("t_check", "v != 7"), tableCheck("", "v < 10"), tableCheck("", "v != 5")),
			observedCheckTable([]string{"id", "v"}, reportedCheck("t_check", "v < 10"), reportedCheck("t_check1", "v != 5"))
	}
	recased := func() (*schemamodel.Database, *catalog.Database) {
		return checkTable([]schemamodel.Field{int32Field("id", ""), int32Field("v", ""), int32Field("V", "")}, tableCheck("t_check", "V > 0")),
			observedCheckTable([]string{"id", "v", "V"}, reportedCheck("t_check", "v > 0"))
	}
	neighborPlan := []string{
		"ALTER TABLE t DROP CONSTRAINT t_check",
		heldRows + "SELECT throwIf(count() > 0, 't_check is broken by rows t holds') FROM t WHERE NOT ifNull((v != 7), 0)",
		"ALTER TABLE t ADD CONSTRAINT t_check CHECK (v != 7)",
		"SELECT throwIf(count() > 0, 't_check is broken by rows t holds') FROM t WHERE NOT ifNull((v != 7), 0)",
		heldRows + "SELECT throwIf(count() > 0, 't_check2 is broken by rows t holds') FROM t WHERE NOT ifNull((v < 10), 0)",
		"ALTER TABLE t ADD CONSTRAINT t_check2 CHECK (v < 10)",
		"SELECT throwIf(count() > 0, 't_check2 is broken by rows t holds') FROM t WHERE NOT ifNull((v < 10), 0)",
	}
	reformatted := func() (*schemamodel.Database, *catalog.Database) {
		return checkTable([]schemamodel.Field{int32Field("id", ""), int32Field("v", "")},
				tableCheck("", "equals(v, v)"), tableCheck("", "CAST(v AS Int64) > 0")),
			observedCheckTable([]string{"id", "v"}, reportedCheck("t_check", "CAST(v, 'Int64') > 0"), reportedCheck("t_check1", "v = v"))
	}
	elsewhere := func() (*schemamodel.Database, *catalog.Database) {
		current := observedCheckTable([]string{"id", "v"})
		other := current.Tables[0]
		other.Name = "u"
		current.Tables = append(current.Tables, other)
		current.Constraints = []catalog.Constraint{{Name: "u_check", TableName: "u", Type: "CHECK", CheckClause: new("v < 10")}}
		return checkTable([]schemamodel.Field{int32Field("id", ""), int32Field("v", "")}, tableCheck("", "v < 10")), current
	}
	elsewherePlan := []string{
		heldRows + "SELECT throwIf(count() > 0, 't_check is broken by rows t holds') FROM t WHERE NOT ifNull((v < 10), 0)",
		"ALTER TABLE t ADD CONSTRAINT t_check CHECK (v < 10)",
		"SELECT throwIf(count() > 0, 't_check is broken by rows t holds') FROM t WHERE NOT ifNull((v < 10), 0)",
		"-- WARNING: dropping table will delete all data\nDROP TABLE IF EXISTS u",
	}
	noStatements := make([]string, 0)
	recasedPlan := []string{
		"ALTER TABLE t DROP CONSTRAINT t_check",
		heldRows + "SELECT throwIf(count() > 0, 't_check is broken by rows t holds') FROM t WHERE NOT ifNull((V > 0), 0)",
		"ALTER TABLE t ADD CONSTRAINT t_check CHECK (V > 0)",
		"SELECT throwIf(count() > 0, 't_check is broken by rows t holds') FROM t WHERE NOT ifNull((V > 0), 0)",
	}
	for _, test := range []struct {
		name     string
		schemas  func() (*schemamodel.Database, *catalog.Database)
		declined bool
		want     []string
	}{
		{name: "a swap, by the server", schemas: swapped, want: noStatements},
		{name: "a swap, by the text", schemas: swapped, declined: true, want: noStatements},
		{name: "a swap of CHECKs the server reformats, by the server", schemas: reformatted, want: noStatements},
		{name: "a named neighbor, by the server", schemas: neighbor, want: neighborPlan},
		{name: "a named neighbor, by the text", schemas: neighbor, declined: true, want: neighborPlan},
		{name: "a CHECK another table holds, by the server", schemas: elsewhere, want: elsewherePlan},
		{name: "a CHECK another table holds, by the text", schemas: elsewhere, declined: true, want: elsewherePlan},
		{name: "a column in another case, by the server", schemas: recased, want: recasedPlan},
		{name: "a column in another case, by the text", schemas: recased, declined: true, want: recasedPlan},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := test.schemas()

			got := serverCheckStatements(c, formattingServer{printed: printed2410}, test.declined, desired, current)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A table the plan creates takes its CHECKs in CREATE TABLE, and a table it
// drops takes them away with it.
func TestGenerateMigrationAST_CreatesAndDropsATableWithItsChecks(t *testing.T) {
	c := qt.New(t)
	desired := checkTable([]schemamodel.Field{int32Field("id", ""), int32Field("v", "v != 7")}, tableCheck("", "v > 0"))
	current := observedCheckTable(nil)
	current.Tables[0].Name = "gone"
	current.Tables[0].Columns = []catalog.Column{{Name: "id", DataType: "Int32", ColumnType: "Int32", IsNullable: "NO"}}
	current.Constraints = []catalog.Constraint{{Name: "gone_check", TableName: "gone", Type: "CHECK", CheckClause: new("id > 0")}}

	got := checkStatements(c, desired, current)

	c.Assert(got, qt.DeepEquals, []string{
		"CREATE TABLE t (\n  id Int32,\n  v Int32,\n  CONSTRAINT t_v_check CHECK (v != 7),\n  CONSTRAINT t_check CHECK (v > 0)\n) ENGINE = MergeTree ORDER BY (tuple()) PRIMARY KEY (tuple())",
		"-- WARNING: dropping table will delete all data\nDROP TABLE IF EXISTS gone",
	})
}
