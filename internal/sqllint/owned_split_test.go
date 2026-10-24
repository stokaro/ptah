package sqllint_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/sqllint"
)

// ownedStatement is one statement of an owner's split, before its position
// in the source is known.
type ownedStatement struct {
	sql     string
	effects []lintext.Effect
}

// splittingOwner answers a split of one source with the statements it holds,
// one per line of the source, judging the facts it lists.
type splittingOwner struct {
	judged     []lintext.Fact
	statements []ownedStatement
	err        error
}

func (o splittingOwner) SplitStatements(_ context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	if o.err != nil {
		return lintext.SplitResult{}, o.err
	}
	var statements []lintext.Statement
	start := 0
	for i, statement := range o.statements {
		statements = append(statements, lintext.Statement{Start: start, End: start + len(statement.sql), Line: i + 1, SQL: statement.sql,
			Effects: statement.effects})
		start += len(statement.sql) + len(";\n")
	}
	files := make([][]lintext.Statement, len(request.Sources))
	files[0] = statements
	return lintext.SplitResult{Judged: o.judged, Files: files}, nil
}

// source joins the owner's statements into the file it splits.
func (o splittingOwner) source() sqllint.Source {
	var lines []string
	for _, statement := range o.statements {
		lines = append(lines, statement.sql+";")
	}
	return sqllint.Source{Name: "owned.sql", SQL: strings.Join(lines, "\n")}
}

func kind(class lintext.StatementClass, name, object string) lintext.Effect {
	return lintext.Effect{Fact: lintext.FactStatementKind, Class: class, Kind: name, Object: object}
}

var (
	kindsOnly       = []lintext.Fact{lintext.FactStatementKind}
	everyStatement  = []lintext.Fact{lintext.FactStatementKind, lintext.FactMissingPrimaryKey, lintext.FactRequiredCapability}
	createPrice     = ownedStatement{sql: "CREATE TABLE a (id Uint64, price Decimal(22,9), PRIMARY KEY (id))", effects: []lintext.Effect{kind(lintext.ClassCreateTable, "CREATE TABLE", "a"), decimalPrice}}
	decimalPrice    = lintext.Effect{Fact: lintext.FactRequiredCapability, Capability: capability.ParameterizedDecimal, Kind: "COLUMN", Object: "price", Table: "a"}
	createWithoutPK = ownedStatement{sql: "CREATE TABLE a (id Uint64)", effects: []lintext.Effect{
		kind(lintext.ClassCreateTable, "CREATE TABLE", "a"), {Fact: lintext.FactMissingPrimaryKey, Table: "a"}}}
)

func lintOwned(c *qt.C, owner splittingOwner, caps capability.Capabilities) []string {
	c.Helper()
	findings, err := sqllint.LintSource(c.Context(), owner.source(), sqllint.Options{Dialect: "fixture", Capabilities: caps, Owner: owner})
	c.Assert(err, qt.IsNil)
	return yqlFindings(findings)
}

// A source an owner splits is read from the facts of the split: what each
// statement is, the table it creates without a primary key, and the
// capabilities it needs, which the host compares with the target's.
func TestLintSource_OwnedSplit_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		judged []lintext.Fact
		caps   capability.Capabilities
		owned  []ownedStatement
		want   []string
	}{
		{name: "a statement the linter does not lint", judged: everyStatement,
			owned: []ownedStatement{{sql: "UPSERT INTO a (id) VALUES (1)", effects: []lintext.Effect{kind(lintext.ClassOther, "UPSERT", "a")}}},
			want:  []string{"1:1 SQL002 ptah sql lint does not lint UPSERT statements yet"}},
		{name: "a table without a primary key", judged: everyStatement, owned: []ownedStatement{createWithoutPK},
			want: []string{`1:1 DDL001 table "a" has no primary key`}},
		{name: "a capability the target refuses", judged: everyStatement, caps: capability.YDB251(), owned: []ownedStatement{createPrice},
			want: []string{"1:1 CAP001 COLUMN price of table a requires target capability parameterized_decimal, unavailable on this target"}},
		{name: "a capability the target has", judged: everyStatement, caps: capability.YDB262(), owned: []ownedStatement{createPrice}},
		{name: "a capability the target does not answer", judged: everyStatement, owned: []ownedStatement{createPrice},
			want: []string{"1:1 SQL004 no rule examined the parameterized_decimal requirement of COLUMN price of table a in this file"}},
		{name: "a schema statement no fact examines", judged: everyStatement,
			owned: []ownedStatement{createWithoutPK, {sql: "DROP TABLE a", effects: []lintext.Effect{kind(lintext.ClassSchema, "DROP TABLE", "a")}}},
			want:  []string{`1:1 DDL001 table "a" has no primary key`, "1:1 SQL004 no rule examined DROP TABLE in this file"}},
		{name: "a schema statement with a requirement", judged: everyStatement, caps: capability.YDB251(),
			owned: []ownedStatement{{sql: "CREATE INDEX v ON a (price)", effects: []lintext.Effect{kind(lintext.ClassSchema, "CREATE INDEX", "v"), decimalPrice}}},
			want:  []string{"1:1 CAP001 COLUMN price of table a requires target capability parameterized_decimal, unavailable on this target"}},
		{name: "a table when only kinds are judged", judged: kindsOnly,
			owned: []ownedStatement{{sql: "CREATE TABLE a (id Uint64)", effects: []lintext.Effect{kind(lintext.ClassCreateTable, "CREATE TABLE", "a")}}},
			want:  []string{"1:1 SQL004 no rule examined CREATE TABLE in this file"}},
		{name: "an altered table when only kinds are judged", judged: kindsOnly,
			owned: []ownedStatement{{sql: "ALTER TABLE a ADD COLUMN b Utf8", effects: []lintext.Effect{kind(lintext.ClassAlterTable, "ALTER TABLE", "a")}}},
			want:  []string{"1:1 SQL004 no rule examined ALTER TABLE in this file"}},
		{name: "an altered table when capabilities are judged", judged: []lintext.Fact{lintext.FactStatementKind, lintext.FactRequiredCapability},
			owned: []ownedStatement{{sql: "ALTER TABLE a ADD COLUMN b Utf8", effects: []lintext.Effect{kind(lintext.ClassAlterTable, "ALTER TABLE", "a")}}}},
		{name: "a created table when only primary keys are judged", judged: []lintext.Fact{lintext.FactStatementKind, lintext.FactMissingPrimaryKey},
			owned: []ownedStatement{{sql: "CREATE TABLE a (id Uint64, PRIMARY KEY (id))", effects: []lintext.Effect{kind(lintext.ClassCreateTable, "CREATE TABLE", "a")}}}},
		{name: "a created table when only capabilities are judged", judged: []lintext.Fact{lintext.FactStatementKind, lintext.FactRequiredCapability},
			owned: []ownedStatement{{sql: "CREATE TABLE a (id Uint64)", effects: []lintext.Effect{kind(lintext.ClassCreateTable, "CREATE TABLE", "a")}}}},
		{name: "a finding on a later line", judged: everyStatement,
			owned: []ownedStatement{{sql: "ALTER TABLE a ADD COLUMN b Utf8", effects: []lintext.Effect{kind(lintext.ClassAlterTable, "ALTER TABLE", "a")}}, createWithoutPK},
			want:  []string{`2:1 DDL001 table "a" has no primary key`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := lintOwned(c, splittingOwner{judged: test.judged, statements: test.owned}, test.caps)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// The owner's split is read alone: a dialect the parser would read is not
// parsed, so a statement the parser refuses draws no SQL001.
func TestLintSource_OwnedSplit_ReadsNoGrammar(t *testing.T) {
	c := qt.New(t)
	owner := splittingOwner{judged: everyStatement, statements: []ownedStatement{{sql: "CREATE TABLE a (id ???, PRIMARY KEY (id))",
		effects: []lintext.Effect{kind(lintext.ClassCreateTable, "CREATE TABLE", "a")}}}}

	findings, err := sqllint.LintSource(c.Context(), owner.source(), sqllint.Options{Dialect: "postgres", Owner: owner})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

// The selectors and severities a run sets apply to what the owner's facts
// draw.
func TestLintSource_OwnedSplit_Policy(t *testing.T) {
	c := qt.New(t)
	owner := splittingOwner{judged: everyStatement, statements: []ownedStatement{createWithoutPK,
		{sql: "DROP TABLE a", effects: []lintext.Effect{kind(lintext.ClassSchema, "DROP TABLE", "a")}}}}

	findings, err := sqllint.LintSource(c.Context(), owner.source(), sqllint.Options{Dialect: "fixture", Owner: owner,
		DisabledRules: []string{sqllint.RuleStatementsNotAnalyzed}, Severities: map[string]sqllint.Severity{sqllint.RuleTableWithoutPrimaryKey: sqllint.SeverityError}})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, sqllint.RuleTableWithoutPrimaryKey)
	c.Assert(findings[0].Severity, qt.Equals, sqllint.SeverityError)
}

func TestLintSource_OwnedSplit_FailurePath(t *testing.T) {
	refused := errors.New("the owner refused the split")
	tests := []struct {
		name    string
		owner   splittingOwner
		wantErr string
	}{
		{name: "an owner that does not judge statement kinds", owner: splittingOwner{judged: []lintext.Fact{lintext.FactStatefulDrop},
			statements: []ownedStatement{{sql: "DROP TABLE a"}}},
			wantErr: `the owner of fixture does not report what each statement is, so ptah sql lint cannot lint its SQL`},
		{name: "a split that fails", owner: splittingOwner{err: refused, statements: []ownedStatement{{sql: "DROP TABLE a"}}},
			wantErr: `split owned.sql through the owner of fixture: the owner refused the split`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(c.Context(), test.owner.source(), sqllint.Options{Dialect: "fixture", Owner: test.owner})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(findings, qt.IsNil)
		})
	}
}

// ownedEmissions holds one owner split per identifier OwnerCatalogIDs
// declares, and its keys are compared with the declaration, so the documented
// list cannot claim an identifier the owner path does not emit or omit one it
// does.
var ownedEmissions = map[string]ownedStatement{
	"SQL002": {sql: "UPSERT INTO a (id) VALUES (1)", effects: []lintext.Effect{kind(lintext.ClassOther, "UPSERT", "a")}},
	"DDL001": createWithoutPK,
	"CAP001": createPrice,
	"SQL004": {sql: "DROP TABLE a", effects: []lintext.Effect{kind(lintext.ClassSchema, "DROP TABLE", "a")}},
}

func TestOwnerCatalogIDs_AreWhatTheOwnerPathEmits(t *testing.T) {
	c := qt.New(t)

	var emitted []string
	for code := range ownedEmissions {
		emitted = append(emitted, code)
	}
	slices.Sort(emitted)
	declared := sqllint.OwnerCatalogIDs()
	slices.Sort(declared)

	c.Assert(declared, qt.DeepEquals, emitted)
}

func TestOwnerCatalogIDs_EachIsEmitted(t *testing.T) {
	for code, emission := range ownedEmissions {
		t.Run(code, func(t *testing.T) {
			c := qt.New(t)
			owner := splittingOwner{judged: everyStatement, statements: []ownedStatement{emission}}

			findings, err := sqllint.LintSource(c.Context(), owner.source(),
				sqllint.Options{Dialect: "fixture", Capabilities: capability.YDB251(), Owner: owner})

			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Rule, qt.Equals, code)
			c.Assert(findings[0].Title, qt.Equals, sqllint.CatalogTitle(code))
			c.Assert(findings[0].Severity, qt.Equals, sqllint.CatalogSeverity(code))
		})
	}
}
