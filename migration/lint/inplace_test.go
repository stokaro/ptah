package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// analyzeInPlace lints one migration against the cost fixture with the
// state and keys the row supplies.
func analyzeInPlace(c *qt.C, dialect, alter string, columns []lint.BaselineColumn, indexes []lint.BaselineIndex) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(costFS(alter)), lint.Options{
		Dialect:         dialect,
		DirFormat:       migrationfile.DirFormatAtlas,
		Selection:       lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:        columns,
		BaselineIndexes: indexes,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

func inPlaceCodes(codes []string) []string {
	var kept []string
	for _, code := range codes {
		if code == "MY130P" || code == "PG301P" {
			kept = append(kept, code)
		}
	}
	return kept
}

// nullableTotal is the cost fixture's total column as a nullable INT with
// no default, and notNullTotal the same column NOT NULL.
func nullableTotal() lint.BaselineColumn { return costColumn("total", "int", "", "latin1") }

func notNullTotal() lint.BaselineColumn {
	column := costColumn("total", "int", "", "latin1")
	column.NotNull = true
	return column
}

// TestInPlaceRule_NamesWhatMySQLAppliesWithoutACopy pins MY130P to the
// statements the measurement classifies as in place, with the algorithm
// each part takes.
func TestInPlaceRule_NamesWhatMySQLAppliesWithoutACopy(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		alter   string
		columns []lint.BaselineColumn
		want    []string
	}{
		{
			name:    "a VARCHAR widened within one length-prefix class",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20);",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "latin1", "latin1")},
			want: []string{
				"MODIFY COLUMN orders.note widens a VARCHAR within one length-prefix class, in place with writes allowed (INPLACE with LOCK=NONE on MySQL, INSTANT on MariaDB)",
				"Nothing in the statement copies the table or blocks writes",
			},
		},
		{
			name:    "NOT NULL added on a nullable column rebuilds in place",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL;",
			columns: []lint.BaselineColumn{nullableTotal()},
			want:    []string{"MODIFY COLUMN orders.total sets NOT NULL, which rebuilds the table in place with writes allowed (INPLACE, LOCK=NONE) and fails on a NULL row"},
		},
		{
			name:    "NOT NULL dropped by a restatement rebuilds in place",
			dialect: "mariadb",
			alter:   "ALTER TABLE orders MODIFY total INT;",
			columns: []lint.BaselineColumn{notNullTotal()},
			want:    []string{"drops NOT NULL, which rebuilds the table in place with writes allowed (INPLACE, LOCK=NONE)"},
		},
		{
			name:    "DEFAULT, COMMENT and position are metadata",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT DEFAULT 0 COMMENT 'c' AFTER id;",
			columns: []lint.BaselineColumn{nullableTotal()},
			want:    []string{"changes the DEFAULT, COMMENT, position as metadata (ALGORITHM=INSTANT)"},
		},
		{
			name:    "a parenthesized DEFAULT expression",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT DEFAULT (1 + 2) INVISIBLE;",
			columns: []lint.BaselineColumn{nullableTotal()},
			want:    []string{"changes the DEFAULT, visibility as metadata (ALGORITHM=INSTANT)"},
		},
		{
			name:    "a CHANGE that renames",
			dialect: "mysql",
			alter:   "ALTER TABLE orders CHANGE total amount INT;",
			columns: []lint.BaselineColumn{nullableTotal()},
			want:    []string{"CHANGE COLUMN orders.amount changes the name as metadata (ALGORITHM=INSTANT)"},
		},
		{
			name:    "two clauses, each in place",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL, MODIFY note VARCHAR(20);",
			columns: []lint.BaselineColumn{nullableTotal(), costColumn("note", "varchar(10)", "latin1", "latin1")},
			want:    []string{"sets NOT NULL", "widens a VARCHAR within one length-prefix class"},
		},
		{
			name:    "utf8mb3 to utf8mb4 on a short VARCHAR no key covers",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) CHARACTER SET utf8mb4;",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3")},
			want:    []string{"converts varchar(10) from utf8mb3 to utf8mb4", "in place with writes allowed"},
		},
		{
			name:    "a CONVERT whose every column converts in place",
			dialect: "mysql",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3"), costColumn("body", "char(10)", "utf8mb3", "utf8mb3")},
			want:    []string{"CONVERT TO CHARACTER SET utf8mb4 on orders converts 2 columns in place, with writes allowed", "note (converts varchar(10) from utf8mb3 to utf8mb4"},
		},
		{
			name:    "a CONVERT that re-encodes no column",
			dialect: "mariadb",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			columns: []lint.BaselineColumn{nullableTotal()},
			want:    []string{"re-encodes no column, so both servers apply it as a metadata change (ALGORITHM=INSTANT)"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeInPlace(c, test.dialect, test.alter, test.columns, nil)
			c.Assert(inPlaceCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MY130P"})
			message := messageOf(analysis.Findings(), "MY130P")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
			c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "DS103")
			c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "MY101")
		})
	}
}

// TestInPlaceRule_StaysQuietWhereTheStatementIsNotJudged pins the fail-closed
// side: a copy, an attribute outside the measured set, a clause of another
// kind, a keyed conversion, an engine-dependent answer with no engine, and
// no state.
func TestInPlaceRule_StaysQuietWhereTheStatementIsNotJudged(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		alter   string
		columns []lint.BaselineColumn
		indexes []lint.BaselineIndex
	}{
		{
			name:    "a copy",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total BIGINT;",
			columns: []lint.BaselineColumn{nullableTotal()},
		},
		{
			name:    "a copy that also changes a DEFAULT",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total BIGINT DEFAULT 0;",
			columns: []lint.BaselineColumn{nullableTotal()},
		},
		{
			name:    "NOT NULL restated on a NOT NULL column changes nothing",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL;",
			columns: []lint.BaselineColumn{notNullTotal()},
		},
		{
			name:    "AUTO_INCREMENT is outside the measured set",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT AUTO_INCREMENT;",
			columns: []lint.BaselineColumn{nullableTotal()},
		},
		{
			name:    "ON UPDATE is outside the measured set",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT DEFAULT 0 ON UPDATE 1;",
			columns: []lint.BaselineColumn{nullableTotal()},
		},
		{
			name:    "an in-place clause beside a clause of another kind",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20), ADD INDEX k_note (note);",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "latin1", "latin1")},
		},
		{
			name:    "an in-place clause beside a copy",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20), MODIFY total BIGINT;",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "latin1", "latin1"), nullableTotal()},
		},
		{
			name:    "a fully restated column changes nothing the measurement covers",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY total INT;",
			columns: []lint.BaselineColumn{nullableTotal()},
		},
		{
			name:    "a member-list change belongs to the member rules",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY kind ENUM('a','b','c');",
			columns: []lint.BaselineColumn{costColumn("kind", "enum('a','b')", "latin1", "latin1")},
		},
		{
			name:    "a keyed conversion MySQL copies for",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) CHARACTER SET utf8mb4;",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3")},
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
		},
		{
			name:    "a CONVERT with one column MySQL copies for",
			dialect: "mysql",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			columns: []lint.BaselineColumn{costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3"), costColumn("body", "text", "utf8mb3", "utf8mb3")},
		},
		{
			name:    "a keyed collation change with no engine to judge it by",
			dialect: "",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) COLLATE latin1_bin;",
			columns: []lint.BaselineColumn{costCollated("note", "varchar(10)", "latin1", "latin1", "latin1_swedish_ci", "latin1_swedish_ci")},
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
		},
		{
			name:    "a column the state does not know",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY other INT NOT NULL;",
			columns: []lint.BaselineColumn{nullableTotal()},
		},
		{
			name:    "no state",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20);",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeInPlace(c, test.dialect, test.alter, test.columns, test.indexes)
			c.Assert(inPlaceCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestInPlaceRule_NamesWhatPostgreSQLAppliesAsACatalogEdit pins PG301P to
// the statements whose every clause is a catalog edit.
func TestInPlaceRule_NamesWhatPostgreSQLAppliesAsACatalogEdit(t *testing.T) {
	tests := []struct {
		name    string
		alter   string
		columns []lint.BaselineColumn
		indexes []lint.BaselineIndex
		want    []string
	}{
		{
			name:    "a widening",
			alter:   "ALTER TABLE orders ALTER COLUMN note TYPE varchar(20);",
			columns: []lint.BaselineColumn{pgColumn("note", "character varying(10)", false)},
			want: []string{
				"ALTER COLUMN orders.note TYPE character varying(20) widens a varchar; PostgreSQL applies the statement as a catalog edit: no table rewrite, no index rebuild, and no scan of the rows",
			},
		},
		{
			name:    "a widening beside SET DEFAULT and DROP NOT NULL",
			alter:   "ALTER TABLE orders ALTER COLUMN note TYPE varchar(20), ALTER COLUMN note SET DEFAULT 'n', ALTER COLUMN total DROP NOT NULL;",
			columns: []lint.BaselineColumn{pgColumn("note", "character varying(10)", false), pgColumn("total", "integer", true)},
			want:    []string{"widens a varchar", "the 2 SET DEFAULT, DROP DEFAULT or DROP NOT NULL clauses in the same statement included"},
		},
		{
			name:    "a collation change on a column no index reads",
			alter:   `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "C";`,
			columns: []lint.BaselineColumn{pgCollated("body", "text", "")},
			indexes: []lint.BaselineIndex{pgIndex("orders_note", "note")},
			want:    []string{`changes the collation from the database default to "C" on a column no index reads, which is a catalog edit`},
		},
		{
			name:    "a restated type beside a DROP DEFAULT",
			alter:   "ALTER TABLE orders ALTER COLUMN total TYPE integer, ALTER COLUMN total DROP DEFAULT;",
			columns: []lint.BaselineColumn{pgColumn("total", "integer", false)},
			want:    []string{"ALTER TABLE orders restates its column types; PostgreSQL applies the statement as a catalog edit"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeInPlace(c, "postgres", test.alter, test.columns, test.indexes)
			c.Assert(inPlaceCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"PG301P"})
			message := messageOf(analysis.Findings(), "PG301P")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
			c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "DS103")
		})
	}
}

// TestInPlaceRule_StaysQuietWherePostgreSQLScansOrRewrites pins the
// PostgreSQL fail-closed side.
func TestInPlaceRule_StaysQuietWherePostgreSQLScansOrRewrites(t *testing.T) {
	tests := []struct {
		name    string
		alter   string
		columns []lint.BaselineColumn
		indexes []lint.BaselineIndex
	}{
		{
			name:    "a rewrite",
			alter:   "ALTER TABLE orders ALTER COLUMN total TYPE bigint;",
			columns: []lint.BaselineColumn{pgColumn("total", "integer", false)},
		},
		{
			name:    "a widening beside SET NOT NULL, which scans",
			alter:   "ALTER TABLE orders ALTER COLUMN note TYPE varchar(20), ALTER COLUMN note SET NOT NULL;",
			columns: []lint.BaselineColumn{pgColumn("note", "character varying(10)", false)},
		},
		{
			name:    "a widening beside an ADD COLUMN",
			alter:   "ALTER TABLE orders ALTER COLUMN note TYPE varchar(20), ADD COLUMN extra text;",
			columns: []lint.BaselineColumn{pgColumn("note", "character varying(10)", false)},
		},
		{
			name:    "a collation change on an indexed column rebuilds the index",
			alter:   `ALTER TABLE orders ALTER COLUMN body TYPE text COLLATE "C";`,
			columns: []lint.BaselineColumn{pgCollated("body", "text", "")},
			indexes: []lint.BaselineIndex{pgIndex("orders_body", "body")},
		},
		{
			name:    "a domain the comparison does not know",
			alter:   "ALTER TABLE orders ALTER COLUMN total TYPE posint;",
			columns: []lint.BaselineColumn{pgColumn("total", "integer", false)},
		},
		{
			name:    "SET DEFAULT alone changes no type",
			alter:   "ALTER TABLE orders ALTER COLUMN total SET DEFAULT 0;",
			columns: []lint.BaselineColumn{pgColumn("total", "integer", false)},
		},
		{
			name:  "no state",
			alter: "ALTER TABLE orders ALTER COLUMN note TYPE varchar(20);",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeInPlace(c, "postgres", test.alter, test.columns, test.indexes)
			c.Assert(inPlaceCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestInPlaceRule_IsInformationalAndNamesItsInput: the finding never gates,
// and a run without the state names the rule as unmet like the cost rules.
func TestInPlaceRule_IsInformationalAndNamesItsInput(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeInPlace(c, "mysql", "ALTER TABLE orders MODIFY note VARCHAR(20);",
		[]lint.BaselineColumn{costColumn("note", "varchar(10)", "latin1", "latin1")}, nil)
	c.Assert(analysis.Findings(), qt.HasLen, 1)
	c.Assert(analysis.Findings()[0].Rule, qt.Equals, "MY130P")
	c.Assert(analysis.Findings()[0].Severity, qt.Equals, lint.SeverityInfo)

	bare := analyzeInPlace(c, "mysql", "ALTER TABLE orders MODIFY note VARCHAR(20);", nil, nil)
	c.Assert(rulesOf(bare.Findings()), qt.DeepEquals, []string{"DS103", "MY101"})
	var unmet []string
	for _, entry := range bare.UnmetInputs() {
		unmet = append(unmet, entry.Rule)
	}
	c.Assert(unmet, qt.Contains, "MY130P")
}
