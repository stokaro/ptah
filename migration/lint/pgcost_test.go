package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// pgFS is the fixture the PostgreSQL cost cases analyze: one table, then one
// migration that alters it. What each column is before the change is in
// file 1 and in the dev database, never in file 2.
func pgFS(alter string) map[string]string {
	return map[string]string{
		"1_base.sql":   "CREATE TABLE orders (id integer, total integer, note varchar(10), body text, placed timestamp(3));",
		"2_change.sql": alter,
	}
}

// pgColumn is one baseline column of orders, spelled the way the report
// composes it from the PostgreSQL catalog.
func pgColumn(name, columnType string, notNull bool) lint.BaselineColumn {
	return lint.BaselineColumn{Version: 2, Table: "orders", Name: name, ColumnType: columnType, NotNull: notNull}
}

func analyzePG(c *qt.C, alter string, baseline ...lint.BaselineColumn) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(pgFS(alter)), lint.Options{
		Dialect:   "postgres",
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:  baseline,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// pgCostCodes keeps the codes of this family, so a quiet case asserts that
// the cost rules said nothing rather than that DS103 and PG104 fell silent
// on a statement they still describe.
func pgCostCodes(codes []string) []string {
	var kept []string
	for _, code := range codes {
		if code == "PG301" || code == "PG304" {
			kept = append(kept, code)
		}
	}
	return kept
}

// TestTypeRewriteRule_ReportsTheChangesPostgreSQLRewritesFor pins PG301 to
// the changes measured to rewrite the table, each with the reason.
func TestTypeRewriteRule_ReportsTheChangesPostgreSQLRewritesFor(t *testing.T) {
	tests := []struct {
		name   string
		alter  string
		column lint.BaselineColumn
		why    string
	}{
		{
			name:   "a wider integer",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE bigint;",
			column: pgColumn("total", "integer", false),
			why:    "ALTER COLUMN orders.total TYPE bigint changes the type from integer to bigint; PostgreSQL rewrites the whole table",
		},
		{
			name:   "a narrower integer names the abort",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE smallint;",
			column: pgColumn("total", "integer", false),
			why:    "aborts the statement (smallint out of range)",
		},
		{
			name:   "SET DATA TYPE is the same clause",
			alter:  "ALTER TABLE orders ALTER total SET DATA TYPE numeric;",
			column: pgColumn("total", "integer", false),
			why:    "changes the type from integer to numeric",
		},
		{
			name:   "integer to text",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE text;",
			column: pgColumn("total", "integer", false),
			why:    "changes the type from integer to text",
		},
		{
			name:   "a narrower varchar",
			alter:  "ALTER TABLE orders ALTER COLUMN note TYPE varchar(5);",
			column: pgColumn("note", "character varying(10)", false),
			why:    "changes the type from character varying(10) to character varying(5), which checks every value against the shorter limit",
		},
		{
			name:   "a limit put on an unlimited varchar",
			alter:  "ALTER TABLE orders ALTER COLUMN note TYPE varchar(5);",
			column: pgColumn("note", "character varying", false),
			why:    "checks every value against the new limit",
		},
		{
			name:   "text to a limited varchar",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE varchar(10);",
			column: pgColumn("body", "text", false),
			why:    "changes the type from text to character varying(10), which checks every value against the new limit",
		},
		{
			name:   "a wider char is a rewrite, unlike a wider varchar",
			alter:  "ALTER TABLE orders ALTER COLUMN note TYPE char(20);",
			column: pgColumn("note", "character(10)", false),
			why:    "changes the type from character(10) to character(20)",
		},
		{
			name:   "char to varchar",
			alter:  "ALTER TABLE orders ALTER COLUMN note TYPE varchar(10);",
			column: pgColumn("note", "character(10)", false),
			why:    "changes the type from character(10) to character varying(10)",
		},
		{
			name:   "a numeric scale change",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE numeric(10,3);",
			column: pgColumn("total", "numeric(10,2)", false),
			why:    "a change of scale rounds every stored value",
		},
		{
			name:   "a lower numeric precision",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE numeric(10,2);",
			column: pgColumn("total", "numeric(12,2)", false),
			why:    "checks every value against the lower precision",
		},
		{
			name:   "a precision put on an unlimited numeric",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE numeric(10,2);",
			column: pgColumn("total", "numeric", false),
			why:    "checks and rounds every value to the new precision and scale",
		},
		{
			name:   "numeric(10) drops the scale to zero",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE numeric(10);",
			column: pgColumn("total", "numeric(10,2)", false),
			why:    "changes the type from numeric(10,2) to numeric(10,0)",
		},
		{
			name:   "a shorter fractional-seconds precision",
			alter:  "ALTER TABLE orders ALTER COLUMN placed TYPE timestamp(3);",
			column: pgColumn("placed", "timestamp(6) without time zone", false),
			why:    "changes the type from timestamp without time zone to timestamp(3) without time zone, which rounds every stored value",
		},
		{
			name:   "a precision put on an interval",
			alter:  "ALTER TABLE orders ALTER COLUMN placed TYPE interval(3);",
			column: pgColumn("placed", "interval", false),
			why:    "changes the type from interval to interval(3)",
		},
		{
			name:   "an array element change",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE varchar(20)[];",
			column: pgColumn("total", "character varying(10)[]", false),
			why:    "PostgreSQL rewrites an array column for any change of its element type",
		},
		{
			name:   "a fixed-length bit",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE bit(16);",
			column: pgColumn("total", "bit(8)", false),
			why:    "aborts the statement (bit string length does not match type)",
		},
		{
			name:   "json to jsonb",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE jsonb;",
			column: pgColumn("body", "json", false),
			why:    "changes the type from json to jsonb",
		},
		{
			name:   "a USING expression on a change that rewrites anyway",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE bigint USING total * 2;",
			column: pgColumn("total", "integer", false),
			why:    "changes the type from integer to bigint",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzePG(c, test.alter, test.column)
			c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"PG301"})
			message := messageOf(analysis.Findings(), "PG301")
			c.Assert(message, qt.Contains, test.why)
			c.Assert(message, qt.Contains, "ACCESS EXCLUSIVE lock")
		})
	}
}

func TestTypeRewriteRule_NamesTheTimeZoneCondition(t *testing.T) {
	c := qt.New(t)

	analysis := analyzePG(c, "ALTER TABLE orders ALTER COLUMN placed TYPE timestamptz;", pgColumn("placed", "timestamp(6) without time zone", false))

	c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"PG301"})
	message := messageOf(analysis.Findings(), "PG301")
	c.Assert(message, qt.Contains, "changes the type from timestamp without time zone to timestamp with time zone")
	c.Assert(message, qt.Contains, "rewrites the whole table unless TimeZone is UTC when the statement runs, and rebuilds every index on the column either way")
}

// TestTypeRewriteRule_StaysQuietWhereTheCatalogEditSuffices holds the other
// half: the widenings PostgreSQL proves safe, spellings of one type, and the
// shapes whose outcome the baseline cannot decide.
func TestTypeRewriteRule_StaysQuietWhereTheCatalogEditSuffices(t *testing.T) {
	tests := []struct {
		name   string
		alter  string
		column lint.BaselineColumn
	}{
		{
			name:   "a wider varchar",
			alter:  "ALTER TABLE orders ALTER COLUMN note TYPE varchar(20);",
			column: pgColumn("note", "character varying(10)", false),
		},
		{
			name:   "the limit dropped",
			alter:  "ALTER TABLE orders ALTER COLUMN note TYPE varchar;",
			column: pgColumn("note", "character varying(10)", false),
		},
		{
			name:   "varchar to text",
			alter:  "ALTER TABLE orders ALTER COLUMN note TYPE text;",
			column: pgColumn("note", "character varying(10)", false),
		},
		{
			name:   "text to an unlimited varchar",
			alter:  "ALTER TABLE orders ALTER COLUMN body TYPE character varying;",
			column: pgColumn("body", "text", false),
		},
		{
			name:   "a higher numeric precision at the same scale",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE numeric(12,2);",
			column: pgColumn("total", "numeric(10,2)", false),
		},
		{
			name:   "the numeric precision dropped",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE numeric;",
			column: pgColumn("total", "numeric(10,2)", false),
		},
		{
			name:   "a longer fractional-seconds precision",
			alter:  "ALTER TABLE orders ALTER COLUMN placed TYPE timestamp(6);",
			column: pgColumn("placed", "timestamp(3) without time zone", false),
		},
		{
			name:   "timestamp is timestamp(6)",
			alter:  "ALTER TABLE orders ALTER COLUMN placed TYPE timestamp;",
			column: pgColumn("placed", "timestamp(6) without time zone", false),
		},
		{
			name:   "WITHOUT TIME ZONE spelled out",
			alter:  "ALTER TABLE orders ALTER COLUMN placed TYPE timestamp(6) without time zone;",
			column: pgColumn("placed", "timestamp(3) without time zone", false),
		},
		{
			name:   "a wider bit varying",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE varbit(16);",
			column: pgColumn("total", "bit varying(8)", false),
		},
		{
			name:   "the same type",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE integer;",
			column: pgColumn("total", "integer", false),
		},
		{
			name:   "INT4 is integer",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE int4;",
			column: pgColumn("total", "integer", false),
		},
		{
			name:   "DECIMAL is numeric",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE decimal(10,2);",
			column: pgColumn("total", "numeric(10,2)", false),
		},
		{
			name:   "CHAR is character(1)",
			alter:  "ALTER TABLE orders ALTER COLUMN note TYPE char;",
			column: pgColumn("note", "character(1)", false),
		},
		{
			name:   "a collation alone on a column no index reads, which is a catalog edit",
			alter:  `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "C";`,
			column: pgColumn("body", "text", false),
		},
		{
			name:   "a USING expression on a change that would be in place",
			alter:  "ALTER TABLE orders ALTER COLUMN note TYPE varchar(20) USING note;",
			column: pgColumn("note", "character varying(10)", false),
		},
		{
			name:   "a domain the file does not know",
			alter:  "ALTER TABLE orders ALTER COLUMN total TYPE integer;",
			column: pgColumn("total", "posint", false),
		},
		{
			name:   "a column the baseline does not know",
			alter:  "ALTER TABLE orders ALTER COLUMN other TYPE bigint;",
			column: pgColumn("total", "integer", false),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzePG(c, test.alter, test.column)
			c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestPrimaryKeyScanRule_NamesTheNullableColumns pins PG304 to the columns
// the key sets NOT NULL, which is what adds the second scan.
func TestPrimaryKeyScanRule_NamesTheNullableColumns(t *testing.T) {
	tests := []struct {
		name     string
		alter    string
		baseline []lint.BaselineColumn
		want     []string
	}{
		{
			name:     "one nullable column",
			alter:    "ALTER TABLE orders ADD PRIMARY KEY (id);",
			baseline: []lint.BaselineColumn{pgColumn("id", "integer", false)},
			want: []string{
				"ADD PRIMARY KEY (id) on orders sets id NOT NULL, so besides building the unique index PostgreSQL scans every row to check it",
				"CHECK (id IS NOT NULL) added NOT VALID and then validated",
				"ADD PRIMARY KEY USING INDEX: measured, that path scans nothing",
			},
		},
		{
			name:     "a named constraint",
			alter:    "ALTER TABLE orders ADD CONSTRAINT orders_pk PRIMARY KEY (id);",
			baseline: []lint.BaselineColumn{pgColumn("id", "integer", false)},
			want:     []string{"sets id NOT NULL"},
		},
		{
			name:     "only the nullable half of a composite key",
			alter:    "ALTER TABLE orders ADD PRIMARY KEY (total, id);",
			baseline: []lint.BaselineColumn{pgColumn("total", "integer", true), pgColumn("id", "integer", false)},
			want:     []string{"ADD PRIMARY KEY (total, id) on orders sets id NOT NULL"},
		},
		{
			name:     "both halves",
			alter:    "ALTER TABLE orders ADD PRIMARY KEY (total, id);",
			baseline: []lint.BaselineColumn{pgColumn("total", "integer", false), pgColumn("id", "integer", false)},
			want:     []string{"sets total, id NOT NULL", "scans every row to check them"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzePG(c, test.alter, test.baseline...)
			c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"PG304"})
			c.Assert(rulesOf(analysis.Findings()), qt.Contains, "PG104")
			message := messageOf(analysis.Findings(), "PG304")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

func TestPrimaryKeyScanRule_StaysQuietWhereNothingIsSetNotNull(t *testing.T) {
	tests := []struct {
		name     string
		alter    string
		baseline []lint.BaselineColumn
	}{
		{
			name:     "every column already NOT NULL",
			alter:    "ALTER TABLE orders ADD PRIMARY KEY (id);",
			baseline: []lint.BaselineColumn{pgColumn("id", "integer", true)},
		},
		{
			name:     "USING INDEX names no columns",
			alter:    "ALTER TABLE orders ADD PRIMARY KEY USING INDEX orders_id_idx;",
			baseline: []lint.BaselineColumn{pgColumn("id", "integer", false)},
		},
		{
			name:     "a column the baseline does not know",
			alter:    "ALTER TABLE orders ADD PRIMARY KEY (other);",
			baseline: []lint.BaselineColumn{pgColumn("id", "integer", false)},
		},
		{
			name:     "a unique constraint is not a primary key",
			alter:    "ALTER TABLE orders ADD UNIQUE (id);",
			baseline: []lint.BaselineColumn{pgColumn("id", "integer", false)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzePG(c, test.alter, test.baseline...)
			c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestPostgresCostRules_NameTheirInputWhenTheRunSuppliesNone: without the
// schema state the generic rules keep the statement, and the run says which
// rules could have said more.
func TestPostgresCostRules_NameTheirInputWhenTheRunSuppliesNone(t *testing.T) {
	c := qt.New(t)

	analysis := analyzePG(c, "ALTER TABLE orders ALTER COLUMN total TYPE bigint;\nALTER TABLE orders ADD PRIMARY KEY (id);")

	c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"DS103", "PG104"})
	var unmet []string
	for _, input := range analysis.UnmetInputs() {
		unmet = append(unmet, input.Rule)
	}
	c.Assert(unmet, qt.Contains, "PG301")
	c.Assert(unmet, qt.Contains, "PG304")
	c.Assert(analysis.BaselineVersions(), qt.DeepEquals, []int64{2})
}

func TestPostgresCostRules_SubsumeAndKeepWhatTheyShould(t *testing.T) {
	t.Run("PG301 replaces DS103", func(t *testing.T) {
		c := qt.New(t)
		analysis := analyzePG(c, "ALTER TABLE orders ALTER COLUMN total TYPE bigint;", pgColumn("total", "integer", false))
		c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"PG301"})
		c.Assert(analysis.Findings()[0].Context.Subjects, qt.DeepEquals, []lint.Subject{{
			Kind: lint.SubjectColumn, Name: "total", Parent: "orders", DataType: "bigint",
		}})
	})

	t.Run("PG304 stays beside PG104, which names the lock", func(t *testing.T) {
		c := qt.New(t)
		analysis := analyzePG(c, "ALTER TABLE orders ADD PRIMARY KEY (id);", pgColumn("id", "integer", false))
		c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"PG104", "PG304"})
	})

	t.Run("a change PostgreSQL applies in place is PG301P's, and DS103 goes with it", func(t *testing.T) {
		c := qt.New(t)
		analysis := analyzePG(c, "ALTER TABLE orders ALTER COLUMN note TYPE varchar(20);", pgColumn("note", "character varying(10)", false))
		c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"PG301P"})
	})

	t.Run("a change the rules cannot judge leaves DS103 to describe the statement", func(t *testing.T) {
		c := qt.New(t)
		analysis := analyzePG(c, "ALTER TABLE orders ALTER COLUMN total TYPE posint;", pgColumn("total", "integer", false))
		c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"DS103"})
	})
}

func TestPostgresCostRules_RunOnlyForPostgreSQL(t *testing.T) {
	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(fixture(pgFS("ALTER TABLE orders ADD PRIMARY KEY (id);")), lint.Options{
		Dialect:   "mysql",
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:  []lint.BaselineColumn{pgColumn("id", "int", false)},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(pgCostCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
}
