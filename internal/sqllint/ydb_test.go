package sqllint_test

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/sqllint"
)

// yqlFindings renders each finding as line:column rule message.
func yqlFindings(findings []sqllint.Finding) []string {
	var out []string
	for _, finding := range findings {
		out = append(out, fmt.Sprintf("%d:%d %s %s", finding.Line, finding.Column, finding.Rule, finding.Message))
	}
	return out
}

// A YDB source is read as YQL and judged by the rules that reading answers:
// the key YDB requires, and the capabilities a line lacks.
func TestLintSource_YDB_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		version string
		want    []string
	}{
		{
			name: "a table without a key",
			sql:  "CREATE TABLE `shop/notes` (id Uint64 NOT NULL, body Utf8);",
			want: []string{`1:1 DDL001 table "shop/notes" has no primary key, which YDB refuses (Primary key is required for ydb tables)`},
		},
		{
			name: "a unique index added to an existing table on the newest line",
			sql:  "CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id));\nALTER TABLE t ADD INDEX t_v GLOBAL UNIQUE ON (v);",
			want: []string{"2:1 CAP001 ADD INDEX t_v, a unique index added to the existing table t, requires target capability " +
				"unique_index_on_existing_table, unavailable on this target"},
		},
		{
			name:    "a column default on a line without one",
			sql:     "ALTER TABLE t ADD COLUMN a Int64 DEFAULT 7;",
			version: "25.1.4.7",
			want: []string{"1:1 CAP001 ADD COLUMN a with a default on table t requires target capability add_column_with_default, " +
				"unavailable on this target"},
		},
		{
			name: "a vector index on a line that keeps vector indexes behind a flag",
			sql: "CREATE TABLE d (id Uint64 NOT NULL, emb String, PRIMARY KEY (id), INDEX d_e GLOBAL USING vector_kmeans_tree " +
				"ON (emb) WITH (distance=cosine, vector_type=\"float\", vector_dimension=3, levels=1, clusters=2));\n" +
				"ALTER TABLE d ADD INDEX d_f GLOBAL USING vector_kmeans_tree ON (emb) " +
				"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2);",
			version: "25.1.4.7",
			want: []string{
				"1:1 CAP001 INDEX d_e, a vector index of table d, requires target capability vector_indexes, unavailable on this target",
				"2:1 CAP001 ADD INDEX d_f, a vector index added to table d, requires target capability vector_indexes, " +
					"unavailable on this target",
			},
		},
		{
			name: "bit vectors on a line that does not build them",
			sql: "ALTER TABLE d ADD INDEX d_b GLOBAL USING vector_kmeans_tree ON (emb) " +
				"WITH (distance=manhattan, vector_type=\"BIT\", vector_dimension=8, levels=1, clusters=2);",
			version: "25.4.1.15",
			want: []string{"1:1 CAP001 ADD INDEX d_b, a vector index added to table d, requires target capability vector_bit_type, " +
				"unavailable on this target"},
		},
		{
			name: "a vector index of bit vectors on the newest line",
			sql: "CREATE TABLE d (id Uint64 NOT NULL, emb String, PRIMARY KEY (id), INDEX d_e GLOBAL USING vector_kmeans_tree " +
				"ON (emb) WITH (distance=cosine, vector_type=bit, vector_dimension=8, levels=1, clusters=2));",
			want: nil,
		},
		{
			name: "a string holding a semicolon and a statement is one literal",
			sql:  "CREATE TABLE t (id Uint64 NOT NULL, note Utf8 DEFAULT \"x\\\"; CREATE TABLE u (id Uint64);\"u, PRIMARY KEY (id));",
			want: nil,
		},
		{
			name: "statement kinds no rule examines",
			sql:  "DROP TABLE t;\nCREATE VIEW v WITH (security_invoker = TRUE) AS SELECT 1 AS one;\nDROP TABLE u;",
			want: []string{"1:1 SQL004 no rule examined DROP TABLE, CREATE VIEW in this file"},
		},
		{
			name: "statements this linter does not lint",
			sql:  "UPSERT INTO t (id) VALUES (1);\n$x = 1;",
			want: []string{
				"1:1 SQL002 ptah sql lint does not lint UPSERT statements yet",
				"2:1 SQL002 ptah sql lint does not lint named expression statements yet",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(sqllint.Source{Name: "x.sql", SQL: test.sql},
				sqllint.Options{Dialect: "ydb", Version: test.version})

			c.Assert(err, qt.IsNil)
			c.Assert(yqlFindings(findings), qt.DeepEquals, test.want)
		})
	}
}

// What a line accepts is not reported, and a cluster whose capabilities say
// it adds a unique index to an existing table is judged by them.
func TestLintSource_YDB_LeavesWhatTheTargetAccepts(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		version string
		caps    capability.Capabilities
	}{
		{name: "a table with its key and a unique index", sql: "CREATE TABLE t (id Uint64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX t_v GLOBAL UNIQUE ON (v));"},
		{name: "a column default on a line that takes one", sql: "ALTER TABLE t ADD COLUMN a Int64 NOT NULL DEFAULT 0;", version: "26.2.1.14"},
		{
			name: "a unique index on a cluster that allows it",
			sql:  "ALTER TABLE t ADD INDEX t_v GLOBAL UNIQUE ON (v);",
			caps: capability.YDB262().With(capability.UniqueIndexOnExistingTable, true),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(sqllint.Source{Name: "x.sql", SQL: test.sql},
				sqllint.Options{Dialect: "ydbs", Version: test.version, Capabilities: test.caps})

			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 0)
		})
	}
}

// Every other name is left to the checks that already judge it.
func TestLintSource_OtherDialectsKeepTheParser(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(sqllint.Source{Name: "x.sql", SQL: `CREATE TABLE t (id INT, "a;b" TEXT);`},
		sqllint.Options{Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(yqlFindings(findings), qt.DeepEquals, []string{`1:14 DDL001 table "t" has no primary key`})
}

// yqlEmission is a YDB source that draws one identifier, with what the run
// needs to draw it.
type yqlEmission struct {
	sql     string
	version string
}

// yqlEmissions holds one fixture per identifier YQLCatalogIDs declares, and
// its keys are compared with the declaration, so the documented list cannot
// claim an identifier the YQL path does not emit or omit one it does.
var yqlEmissions = map[string]yqlEmission{
	"SQL002": {sql: "UPSERT INTO t (id) VALUES (1);"},
	"DDL001": {sql: "CREATE TABLE t (id Uint64 NOT NULL);"},
	"CAP001": {sql: "ALTER TABLE t ADD COLUMN a Int64 DEFAULT 7;", version: "25.1.4.7"},
	"SQL004": {sql: "DROP TABLE t;"},
}

func TestYQLCatalogIDs_AreWhatTheYQLPathEmits(t *testing.T) {
	c := qt.New(t)

	var emitted []string
	for code := range yqlEmissions {
		emitted = append(emitted, code)
	}
	slices.Sort(emitted)
	declared := sqllint.YQLCatalogIDs()
	slices.Sort(declared)

	c.Assert(declared, qt.DeepEquals, emitted)
}

func TestYQLCatalogIDs_EachIsEmitted(t *testing.T) {
	for code, emission := range yqlEmissions {
		t.Run(code, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(sqllint.Source{Name: "x.sql", SQL: emission.sql},
				sqllint.Options{Dialect: "ydb", Version: emission.version})

			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Rule, qt.Equals, code)
			c.Assert(findings[0].Title, qt.Equals, sqllint.CatalogTitle(code))
			c.Assert(findings[0].Severity, qt.Equals, sqllint.CatalogSeverity(code))
		})
	}
}
