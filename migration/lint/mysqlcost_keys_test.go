package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// costCollated is one baseline column of orders with its collation and the
// table's default collation, the two facts a MODIFY that names no COLLATE
// is judged by.
func costCollated(name, columnType, charset, tableCharset, collation, tableCollation string) lint.BaselineColumn {
	column := costColumn(name, columnType, charset, tableCharset)
	column.Collation = collation
	column.TableCollation = tableCollation
	return column
}

// costKey is one baseline key of orders over whole columns; costPrefixKey
// keys the leading characters of one column, MySQL's `KEY (note(5))`.
func costKey(name string, columns ...string) lint.BaselineIndex {
	parts := make([]lint.BaselineIndexPart, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, lint.BaselineIndexPart{Column: column})
	}
	return lint.BaselineIndex{Version: 2, Table: "orders", Name: name, Parts: parts}
}

func costPrefixKey(name, column string, prefix int) lint.BaselineIndex {
	return lint.BaselineIndex{Version: 2, Table: "orders", Name: name, Parts: []lint.BaselineIndexPart{{Column: column, Prefix: prefix}}}
}

func analyzeCostState(c *qt.C, dialect, alter string, columns []lint.BaselineColumn, indexes []lint.BaselineIndex) lint.Analysis {
	c.Helper()
	opts := costOptions(dialect, columns)
	opts.BaselineIndexes = indexes
	analysis, err := lint.AnalyzeFS(fixture(costFS(alter)), opts)
	c.Assert(err, qt.IsNil)
	return analysis
}

// latin1Note is the column the collation rows change: latin1_swedish_ci on a
// table whose default is the same, so a MODIFY with no COLLATE keeps it.
func latin1Note(collation string) lint.BaselineColumn {
	return costCollated("note", "varchar(10)", "latin1", "latin1", collation, "latin1_swedish_ci")
}

// TestColumnTypeCopyRule_ReadsTheKeysOnTheColumn pins MY130 to the changes
// measured to copy only because of a key on the column: a collation change
// on MySQL, and the utf8mb3 to utf8mb4 conversion of a short VARCHAR or
// CHAR on MySQL with any key and on both engines with a prefix key.
func TestColumnTypeCopyRule_ReadsTheKeysOnTheColumn(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		alter   string
		column  lint.BaselineColumn
		indexes []lint.BaselineIndex
		want    []string
	}{
		{
			name:    "MySQL copies for a collation change on a keyed column",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) COLLATE latin1_bin;",
			column:  latin1Note("latin1_swedish_ci"),
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
			want: []string{
				"changes the collation of varchar(10) from latin1_swedish_ci to latin1_bin on a column a key covers",
				"the same change on a column no key covers is applied in place",
			},
		},
		{
			name:    "a prefix key is a key",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) COLLATE latin1_bin;",
			column:  latin1Note("latin1_swedish_ci"),
			indexes: []lint.BaselineIndex{costPrefixKey("k_note", "note", 5)},
			want:    []string{"on a column a key covers"},
		},
		{
			name:    "a composite key that reads the column is a key",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) COLLATE latin1_bin;",
			column:  latin1Note("latin1_swedish_ci"),
			indexes: []lint.BaselineIndex{costKey("k_total_note", "total", "note")},
			want:    []string{"on a column a key covers"},
		},
		{
			name:    "a MODIFY that names no COLLATE resets a keyed column to the table default",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10);",
			column:  latin1Note("latin1_bin"),
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
			want:    []string{"from latin1_bin to latin1_swedish_ci"},
		},
		{
			name:    "MySQL copies for utf8mb3 to utf8mb4 on a keyed VARCHAR",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20) CHARACTER SET utf8mb4;",
			column:  costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3"),
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
			want:    []string{"converts varchar(10) from utf8mb3 to utf8mb4", "but not, on MySQL, for one a key covers"},
		},
		{
			name:    "MySQL copies for utf8mb3 to utf8mb4 on a keyed CHAR",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note CHAR(10) CHARACTER SET utf8mb4;",
			column:  costColumn("note", "char(10)", "utf8mb3", "utf8mb3"),
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
			want:    []string{"but not, on MySQL, for one a key covers"},
		},
		{
			name:    "both engines copy for utf8mb3 to utf8mb4 under a prefix key",
			dialect: "mariadb",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20) CHARACTER SET utf8mb4;",
			column:  costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3"),
			indexes: []lint.BaselineIndex{costPrefixKey("k_note", "note", 5)},
			want:    []string{"but not for one a prefix key covers, which both servers copy for"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeCostState(c, test.dialect, test.alter, []lint.BaselineColumn{test.column}, test.indexes)
			c.Assert(costCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MY130"})
			message := messageOf(analysis.Findings(), "MY130")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

// TestColumnTypeCopyRule_StaysQuietWhereTheKeysAllow pins the measured
// in-place cases and the fail-closed ones: a restated collation, a change on
// a column no key covers, MariaDB's in-place collation change and whole-key
// conversion, MySQL's keyed ENUM, and the two shapes the file cannot judge.
func TestColumnTypeCopyRule_StaysQuietWhereTheKeysAllow(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		alter   string
		column  lint.BaselineColumn
		indexes []lint.BaselineIndex
	}{
		{
			name:    "a collation change on a column no key covers",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) COLLATE latin1_bin;",
			column:  latin1Note("latin1_swedish_ci"),
			indexes: []lint.BaselineIndex{costKey("k_total", "total")},
		},
		{
			name:    "the collation restated on a keyed column",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) COLLATE latin1_bin;",
			column:  latin1Note("latin1_bin"),
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
		},
		{
			name:    "no COLLATE on a column already at the table default",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10);",
			column:  latin1Note("latin1_swedish_ci"),
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
		},
		{
			name:    "MariaDB applies a collation change in place, key or not",
			dialect: "mariadb",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) COLLATE latin1_bin;",
			column:  latin1Note("latin1_swedish_ci"),
			indexes: []lint.BaselineIndex{costPrefixKey("k_note", "note", 5)},
		},
		{
			name:    "MariaDB converts a whole-keyed VARCHAR in place",
			dialect: "mariadb",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20) CHARACTER SET utf8mb4;",
			column:  costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3"),
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
		},
		{
			name:    "MySQL converts a keyed ENUM in place",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY kind ENUM('a','b') CHARACTER SET utf8mb4;",
			column:  costColumn("kind", "enum('a','b')", "utf8mb3", "utf8mb3"),
			indexes: []lint.BaselineIndex{costKey("k_kind", "kind")},
		},
		{
			name:    "a character set named without a collation takes a default the file cannot name",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) CHARACTER SET latin1;",
			column:  latin1Note("latin1_bin"),
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
		},
		{
			name:    "a keyed collation change with no engine to judge it by",
			dialect: "",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) COLLATE latin1_bin;",
			column:  latin1Note("latin1_swedish_ci"),
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
		},
		{
			name:    "a keyed whole-column conversion with no engine to judge it by",
			dialect: "",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20) CHARACTER SET utf8mb4;",
			column:  costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3"),
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeCostState(c, test.dialect, test.alter, []lint.BaselineColumn{test.column}, test.indexes)
			c.Assert(costCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestCharsetConversionCopyRule_ReadsTheKeys pins MY136 to the keyed
// columns a CONVERT TO utf8mb4 copies for, and to MariaDB's whole-key
// exemption.
func TestCharsetConversionCopyRule_ReadsTheKeys(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		indexes []lint.BaselineIndex
		want    []string
	}{
		{
			name:    "MySQL copies for the keyed VARCHAR",
			dialect: "mysql",
			indexes: []lint.BaselineIndex{costKey("k_note", "note")},
			want:    []string{"for 1 column: note (", "but not, on MySQL, for one a key covers", "The other 1 character column of the table would have converted in place"},
		},
		{
			name:    "MariaDB copies for the prefix-keyed VARCHAR",
			dialect: "mariadb",
			indexes: []lint.BaselineIndex{costPrefixKey("k_note", "note", 5)},
			want:    []string{"for 1 column: note (", "which both servers copy for"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			columns := []lint.BaselineColumn{
				costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3"),
				costColumn("body", "char(10)", "utf8mb3", "utf8mb3"),
			}
			analysis := analyzeCostState(c, test.dialect, "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;", columns, test.indexes)
			c.Assert(costCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MY136"})
			message := messageOf(analysis.Findings(), "MY136")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

// TestCharsetConversionCopyRule_MariaDBConvertsAWholeKeyedColumnInPlace is
// the control for the row above: the same conversion under a whole-column
// key is in place on MariaDB.
func TestCharsetConversionCopyRule_MariaDBConvertsAWholeKeyedColumnInPlace(t *testing.T) {
	c := qt.New(t)
	columns := []lint.BaselineColumn{costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3")}
	analysis := analyzeCostState(c, "mariadb", "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;", columns,
		[]lint.BaselineIndex{costKey("k_note", "note")})
	c.Assert(costCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
}
