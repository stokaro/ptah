package sqllint_test

import (
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqllint"
)

// lintAs lints sql as one file of dialect with opts' policy.
func lintAs(c *qt.C, dialect, sql string, opts sqllint.Options) []sqllint.Finding {
	c.Helper()
	opts.SQL, opts.Bodies, opts.Dialect = builtintest.Runtime().SQL(), builtintest.Runtime(), dialect
	findings, err := sqllint.LintSource(c.Context(), sqllint.Source{Name: "schema.sql", SQL: sql}, opts)
	c.Assert(err, qt.IsNil)
	return findings
}

// ofRule is the findings of one rule.
func ofRule(findings []sqllint.Finding, rule string) []sqllint.Finding {
	var kept []sqllint.Finding
	for _, finding := range findings {
		if finding.Rule == rule {
			kept = append(kept, finding)
		}
	}
	return kept
}

// identifierFindings is the DDL003 and DDL004 findings, in order.
func identifierFindings(findings []sqllint.Finding) []sqllint.Finding {
	var kept []sqllint.Finding
	for _, finding := range findings {
		if finding.Rule == sqllint.RuleIdentifierTruncated || finding.Rule == sqllint.RuleIdentifierCollision {
			kept = append(kept, finding)
		}
	}
	return kept
}

// storedAs is the name each DDL003 finding says the server stores.
func storedAs(findings []sqllint.Finding) []string {
	var stored []string
	for _, finding := range ofRule(findings, sqllint.RuleIdentifierTruncated) {
		stored = append(stored, storedAsPattern.FindStringSubmatch(finding.Message)[1])
	}
	return stored
}

var storedAsPattern = regexp.MustCompile(`it is stored as "([^"]*)"`)

// positions is the line and column of each finding of rule.
func positions(findings []sqllint.Finding, rule string) [][2]int {
	var at [][2]int
	for _, finding := range ofRule(findings, rule) {
		at = append(at, [2]int{finding.Line, finding.Column})
	}
	return at
}

// TestLintSource_IdentifierTruncatedBoundaries holds DDL003 to the limits
// measured on PostgreSQL 15.19: 62 and 63 bytes are kept whole, 64 and more
// are cut to 63, a two-byte character that straddles the limit is dropped
// whole, and an unquoted name is folded before it is measured.
func TestLintSource_IdentifierTruncatedBoundaries(t *testing.T) {
	rows := []struct {
		name  string
		index string
		want  []string
	}{
		{name: "62 bytes", index: `"` + strings.Repeat("a", 62) + `"`},
		{name: "63 bytes", index: `"` + strings.Repeat("a", 63) + `"`},
		{name: "64 bytes", index: `"` + strings.Repeat("a", 64) + `"`, want: []string{strings.Repeat("a", 63)}},
		{name: "far past the limit", index: `"` + strings.Repeat("a", 100) + `"`, want: []string{strings.Repeat("a", 63)}},
		{name: "a character ending at the limit", index: `"` + strings.Repeat("a", 61) + `é"`},
		{name: "a character straddling the limit", index: `"` + strings.Repeat("a", 62) + `é"`, want: []string{strings.Repeat("a", 62)}},
		{name: "unquoted capitals", index: strings.Repeat("A", 64), want: []string{strings.Repeat("a", 63)}},
		{name: "quoted capitals", index: `"` + strings.Repeat("A", 64) + `"`, want: []string{strings.Repeat("A", 63)}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings := lintAs(c, platform.Postgres, "CREATE TABLE t (a int PRIMARY KEY);\nCREATE INDEX "+row.index+" ON t (a);\n", sqllint.Options{})

			c.Assert(storedAs(findings), qt.DeepEquals, row.want)
		})
	}
}

// TestLintSource_IdentifierTruncatedFinding holds what one DDL003 finding
// says: the kind, the written and the stored name, the length and the limit,
// and a fix that names what the server stores.
func TestLintSource_IdentifierTruncatedFinding(t *testing.T) {
	c := qt.New(t)
	long := strings.Repeat("k", 70)

	findings := ofRule(lintAs(c, platform.Postgres, "CREATE TABLE p (id int PRIMARY KEY);\n"+
		`CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT "`+long+`" FOREIGN KEY (a) REFERENCES p (id));`, sqllint.Options{}),
		sqllint.RuleIdentifierTruncated)

	c.Assert(findings, qt.DeepEquals, []sqllint.Finding{{
		Rule:     sqllint.RuleIdentifierTruncated,
		Title:    "Identifier longer than the server keeps",
		Severity: sqllint.SeverityWarning,
		File:     "schema.sql",
		Line:     2,
		Column:   55,
		Dialect:  platform.Postgres,
		Message: `foreign key "` + long + `" in table "public.c" is 70 bytes, longer than the 63 bytes the server keeps; ` +
			`it is stored as "` + strings.Repeat("k", 63) + `"`,
		Rationale: "The server cuts the name with a notice and no error, so the database holds a name the SQL never wrote, " +
			"and later SQL, a schema diff or a second name of the same first bytes meets that one.",
		SuggestedFix: `Write a name of at most 63 bytes, such as "` + strings.Repeat("k", 63) + `", so the SQL names what the database holds.`,
	}})
}

// TestLintSource_IdentifierPositions holds that a finding points at the name
// it reports, read from the statement's tokens: past a string literal and a
// comment that spell it, on the line the name is written, in its quoted or
// unquoted spelling.
func TestLintSource_IdentifierPositions(t *testing.T) {
	long := strings.Repeat("n", 64)
	rows := []struct {
		name string
		sql  string
		want [][2]int
	}{
		{
			name: "a constraint on the third line of its statement",
			sql:  "CREATE TABLE t (\n  a int PRIMARY KEY,\n  CONSTRAINT " + long + " CHECK (a > 0)\n);",
			want: [][2]int{{3, 14}},
		},
		{
			name: "past a literal and a comment spelling the name",
			sql:  "CREATE TABLE t (a text DEFAULT '" + long + "' PRIMARY KEY, -- " + long + "\n CONSTRAINT \"" + long + "\" CHECK (a <> ''));",
			want: [][2]int{{2, 13}},
		},
		{
			name: "an unquoted name written in capitals",
			sql:  "CREATE TABLE t (a int PRIMARY KEY);\n\n   CREATE INDEX " + strings.ToUpper(long) + " ON t (a);",
			want: [][2]int{{3, 17}},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings := lintAs(c, platform.Postgres, row.sql, sqllint.Options{})

			c.Assert(positions(findings, sqllint.RuleIdentifierTruncated), qt.DeepEquals, row.want)
		})
	}
}

// TestLintSource_IdentifierCollisions holds DDL004 to the verdicts of
// PostgreSQL 15.19: a cut name the server refuses is reported where the second
// declaration writes it, and the same cut name in another namespace, which
// the server accepts, is not.
func TestLintSource_IdentifierCollisions(t *testing.T) {
	long := strings.Repeat("k", 63)
	rows := []struct {
		name string
		sql  string
		want [][2]int
	}{
		{
			name: "two indexes in one schema",
			sql:  "CREATE TABLE t (a int PRIMARY KEY, b int);\nCREATE INDEX \"" + long + "_alpha\" ON t (a);\nCREATE INDEX \"" + long + "_beta\" ON t (b);",
			want: [][2]int{{3, 14}},
		},
		{
			name: "two foreign keys on one table",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, a int, b int,\n CONSTRAINT \"" + long +
				"_alpha\" FOREIGN KEY (a) REFERENCES p (id),\n CONSTRAINT \"" + long + "_beta\" FOREIGN KEY (b) REFERENCES p (id));",
			want: [][2]int{{4, 13}},
		},
		{
			name: "an index cut to the name of a table",
			sql:  "CREATE TABLE \"" + long + "\" (a int PRIMARY KEY);\nCREATE INDEX \"" + long + "_x\" ON \"" + long + "\" (a);",
			want: [][2]int{{2, 14}},
		},
		{
			name: "a unique constraint and an index",
			sql:  "CREATE TABLE t (a int PRIMARY KEY, b int, CONSTRAINT \"" + long + "_alpha\" UNIQUE (b));\nCREATE INDEX \"" + long + "_beta\" ON t (b);",
			want: [][2]int{{2, 14}},
		},
		{
			name: "two columns of one table",
			sql:  "CREATE TABLE t (id int PRIMARY KEY, \"" + long + "_alpha\" int, \"" + long + "_beta\" int);",
			want: [][2]int{{1, 114}},
		},
		{
			name: "a name written as another name cut",
			sql:  "CREATE TABLE t (a int PRIMARY KEY, b int);\nCREATE INDEX \"" + long + "_alpha\" ON t (a);\nCREATE INDEX \"" + long + "\" ON t (b);",
			want: [][2]int{{3, 14}},
		},
		{
			name: "a name cut to another name written",
			sql:  "CREATE TABLE t (a int PRIMARY KEY, b int);\nCREATE INDEX \"" + long + "\" ON t (a);\nCREATE INDEX \"" + long + "_beta\" ON t (b);",
			want: [][2]int{{3, 14}},
		},
		{
			name: "a rename onto a cut name",
			sql:  "CREATE TABLE t (a int PRIMARY KEY, b int);\nCREATE INDEX \"" + long + "_alpha\" ON t (a);\nCREATE INDEX ix ON t (b);\nALTER INDEX ix RENAME TO \"" + long + "_beta\";",
			want: [][2]int{{4, 26}},
		},
		{
			name: "foreign keys on two tables",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				"CREATE TABLE c1 (id int PRIMARY KEY, a int, CONSTRAINT \"" + long + "_alpha\" FOREIGN KEY (a) REFERENCES p (id));\n" +
				"CREATE TABLE c2 (id int PRIMARY KEY, a int, CONSTRAINT \"" + long + "_beta\" FOREIGN KEY (a) REFERENCES p (id));",
		},
		{
			name: "indexes in two schemas",
			sql: "CREATE TABLE s1.t (a int PRIMARY KEY);\nCREATE TABLE s2.t (a int PRIMARY KEY);\n" +
				"CREATE INDEX \"" + long + "_alpha\" ON s1.t (a);\nCREATE INDEX \"" + long + "_beta\" ON s2.t (a);",
		},
		{
			name: "an index dropped before its name is taken again",
			sql: "CREATE TABLE t (a int PRIMARY KEY, b int);\nCREATE INDEX \"" + long + "_alpha\" ON t (a);\n" +
				"DROP INDEX \"" + long + "\";\nCREATE INDEX \"" + long + "_beta\" ON t (b);",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings := lintAs(c, platform.Postgres, row.sql, sqllint.Options{})

			c.Assert(positions(findings, sqllint.RuleIdentifierCollision), qt.DeepEquals, row.want)
		})
	}
}

// TestLintSource_IdentifierCollisionFinding holds what one DDL004 finding
// says: both names as written, the namespace, the stored name and the
// server's verdict, at error severity.
func TestLintSource_IdentifierCollisionFinding(t *testing.T) {
	c := qt.New(t)
	long := strings.Repeat("k", 63)

	findings := ofRule(lintAs(c, platform.Postgres, "CREATE TABLE t (a int PRIMARY KEY, b int);\n"+
		`CREATE INDEX "`+long+`_alpha" ON t (a);`+"\n"+`CREATE INDEX "`+long+`_beta" ON t (b);`, sqllint.Options{}),
		sqllint.RuleIdentifierCollision)

	c.Assert(findings, qt.DeepEquals, []sqllint.Finding{{
		Rule:     sqllint.RuleIdentifierCollision,
		Title:    "Identifiers the server stores as one",
		Severity: sqllint.SeverityError,
		File:     "schema.sql",
		Line:     3,
		Column:   14,
		Dialect:  platform.Postgres,
		Message: `index "` + long + `_alpha" and index "` + long + `_beta" in schema "public" are both stored as "` + long +
			`", and the server refuses the second`,
		Rationale: "The server cuts a long name to its first bytes, so two names that differ only after them take one name, " +
			"and the second statement fails when it runs.",
		SuggestedFix: "Rename one of them so the two differ within the first 63 bytes.",
	}})
}

// TestLintSource_GuardedCollisionsAreCutNames holds what a guarded statement
// gets. The server accepts it, so it is no DDL004: IF NOT EXISTS skips the
// second object and OR REPLACE replaces the first. A name it writes past the
// limit is a DDL003 that says one of the two objects is kept; a name written
// as the server stores it names the object holding the name, and is clean.
func TestLintSource_GuardedCollisionsAreCutNames(t *testing.T) {
	long := strings.Repeat("k", 63)
	rows := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "two indexes guarded by IF NOT EXISTS",
			sql: "CREATE TABLE t (a int PRIMARY KEY, b int);\nCREATE INDEX IF NOT EXISTS \"" + long + "_alpha\" ON t (a);\n" +
				"CREATE INDEX IF NOT EXISTS \"" + long + "_beta\" ON t (b);",
			want: []string{"", `; index "` + long + `_alpha" already holds that name, so the server keeps one of the two objects`},
		},
		{
			name: "two views written OR REPLACE",
			sql:  "CREATE OR REPLACE VIEW \"" + long + "_alpha\" AS SELECT 1 AS a;\nCREATE OR REPLACE VIEW \"" + long + "_beta\" AS SELECT 2 AS a;",
			want: []string{"", `; view "` + long + `_alpha" already holds that name, so the server keeps one of the two objects`},
		},
		{
			name: "the stored name under IF NOT EXISTS",
			sql: "CREATE TABLE t (a int PRIMARY KEY, b int);\nCREATE INDEX \"" + long + "_alpha\" ON t (a);\n" +
				"CREATE INDEX IF NOT EXISTS \"" + long + "\" ON t (b);",
			want: []string{""},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings := lintAs(c, platform.Postgres, row.sql, sqllint.Options{})

			var notes []string
			for _, finding := range ofRule(findings, sqllint.RuleIdentifierTruncated) {
				_, note, _ := strings.Cut(finding.Message, `it is stored as "`+long+`"`)
				notes = append(notes, note)
			}
			c.Assert(notes, qt.DeepEquals, row.want)
			c.Assert(ofRule(findings, sqllint.RuleIdentifierCollision), qt.HasLen, 0)
		})
	}
}

// TestLintSource_IdentifierRulesFollowTheDescriptor holds that the rules read
// the descriptor fact rather than a dialect's name: YugabyteDB cuts long names
// as PostgreSQL does, CockroachDB keeps them whole, and MySQL refuses them.
func TestLintSource_IdentifierRulesFollowTheDescriptor(t *testing.T) {
	long := strings.Repeat("k", 63)
	sql := "CREATE TABLE t (a int PRIMARY KEY, b int);\nCREATE INDEX " + long + "_alpha ON t (a);\nCREATE INDEX " + long + "_beta ON t (b);"
	rows := []struct {
		dialect string
		want    []string
	}{
		{dialect: platform.Postgres, want: []string{sqllint.RuleIdentifierTruncated, sqllint.RuleIdentifierCollision}},
		{dialect: platform.YugabyteDB, want: []string{sqllint.RuleIdentifierTruncated, sqllint.RuleIdentifierCollision}},
		{dialect: platform.CockroachDB},
		{dialect: platform.MySQL},
	}
	for _, row := range rows {
		t.Run(row.dialect, func(t *testing.T) {
			c := qt.New(t)

			var rules []string
			for _, finding := range identifierFindings(lintAs(c, row.dialect, sql, sqllint.Options{})) {
				rules = append(rules, finding.Rule)
			}
			c.Assert(rules, qt.DeepEquals, row.want)
		})
	}
}

// TestLintSource_IdentifierRulesTakeThePolicy holds that the rules answer the
// policy every rule answers: a severity promotes a cut name to an error, and a
// code or a family selector disables them.
func TestLintSource_IdentifierRulesTakeThePolicy(t *testing.T) {
	long := strings.Repeat("k", 63)
	sql := "CREATE TABLE t (a int PRIMARY KEY, b int);\nCREATE INDEX " + long + "_alpha ON t (a);\nCREATE INDEX " + long + "_beta ON t (b);"
	rows := []struct {
		name string
		opts sqllint.Options
		want []string
	}{
		{name: "the defaults", want: []string{"DDL003 warning", "DDL004 error"}},
		{
			name: "a cut name promoted to an error",
			opts: sqllint.Options{Severities: map[string]sqllint.Severity{sqllint.RuleIdentifierTruncated: sqllint.SeverityError}},
			want: []string{"DDL003 error", "DDL004 error"},
		},
		{name: "DDL003 disabled", opts: sqllint.Options{DisabledRules: []string{"DDL003"}}, want: []string{"DDL004 error"}},
		{name: "the family disabled", opts: sqllint.Options{DisabledRules: []string{"DDL"}}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			var got []string
			for _, finding := range identifierFindings(lintAs(c, platform.Postgres, sql, row.opts)) {
				got = append(got, finding.Rule+" "+string(finding.Severity))
			}
			c.Assert(got, qt.DeepEquals, row.want)
		})
	}
}

// TestLintSource_DeclaredNamesAreAnalyzedWhereTheServerCutsThem holds SQL004
// to what DDL003 and DDL004 read on PostgreSQL: a statement that declares a
// name is analyzed, and a drop, which declares none, is still reported.
func TestLintSource_DeclaredNamesAreAnalyzedWhereTheServerCutsThem(t *testing.T) {
	rows := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "view", sql: "CREATE VIEW v AS SELECT 1;"},
		{name: "materialized view", sql: "CREATE MATERIALIZED VIEW mv AS SELECT 1;"},
		{name: "sequence", sql: "CREATE SEQUENCE s;"},
		{name: "alter table", sql: "ALTER TABLE users ADD COLUMN x int;"},
		{name: "drop table", sql: "DROP TABLE users;", want: []string{"SQL004 no rule examined DROP TABLE in this file"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			var got []string
			for _, finding := range lintAs(c, platform.Postgres, row.sql, sqllint.Options{}) {
				got = append(got, finding.Rule+" "+finding.Message)
			}
			c.Assert(got, qt.DeepEquals, row.want)
		})
	}
}
