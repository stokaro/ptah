package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// costFS is the fixture the cost cases analyze: one table, then one
// migration that alters it. The column's type and character set before the
// change are in file 1 and in the dev database, never in file 2, which is
// why the rules read the schema state the version starts from.
func costFS(alter string) map[string]string {
	return map[string]string{
		"1_base.sql":   "CREATE TABLE orders (id int NOT NULL, total int, note varchar(10), body text, kind enum('a','b')) CHARSET=latin1;",
		"2_change.sql": alter,
	}
}

// costColumn is one baseline column of orders, spelled the way the
// dev-database read reports it: COLUMN_TYPE, the column's character set, and
// the table's default.
func costColumn(name, columnType, charset, tableCharset string) lint.BaselineColumn {
	return lint.BaselineColumn{
		Version:      2,
		Table:        "orders",
		Name:         name,
		ColumnType:   columnType,
		Charset:      charset,
		TableCharset: tableCharset,
	}
}

func costOptions(dialect string, baseline []lint.BaselineColumn) lint.Options {
	return lint.Options{
		Dialect:   dialect,
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:  baseline,
	}
}

func analyzeCost(c *qt.C, dialect, alter string, baseline ...lint.BaselineColumn) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(costFS(alter)), costOptions(dialect, baseline))
	c.Assert(err, qt.IsNil)
	return analysis
}

// costCodes keeps the codes of this family, so a quiet case asserts that
// the cost rules said nothing rather than that DS103 and MY101 fell silent
// on a statement they still describe.
func costCodes(codes []string) []string {
	var kept []string
	for _, code := range codes {
		if code == "MY130" || code == "MY133" || code == "MY136" {
			kept = append(kept, code)
		}
	}
	return kept
}

// TestColumnTypeCopyRule_ReportsTheChangesInnoDBCopiesFor pins MY130 to the
// changes both servers refuse ALGORITHM=INSTANT and INPLACE for, each named
// in the message with the reason the copy happens.
func TestColumnTypeCopyRule_ReportsTheChangesInnoDBCopiesFor(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		alter   string
		column  lint.BaselineColumn
		why     string
	}{
		{
			dialect: "mysql",
			name:    "a wider integer",
			alter:   "ALTER TABLE orders MODIFY COLUMN total BIGINT NOT NULL;",
			column:  costColumn("total", "int", "", "latin1"),
			why:     "MODIFY COLUMN orders.total changes the type from int to bigint;",
		},
		{
			dialect: "mysql",
			name:    "MariaDB spells the old integer with a display width",
			alter:   "ALTER TABLE orders MODIFY total BIGINT;",
			column:  costColumn("total", "int(11)", "", "latin1"),
			why:     "changes the type from int to bigint",
		},
		{
			dialect: "mysql",
			name:    "signedness",
			alter:   "ALTER TABLE orders MODIFY total INT UNSIGNED;",
			column:  costColumn("total", "int", "", "latin1"),
			why:     "changes the type from int to int unsigned",
		},
		{
			dialect: "mysql",
			name:    "ZEROFILL alone implies UNSIGNED",
			alter:   "ALTER TABLE orders MODIFY total INT ZEROFILL;",
			column:  costColumn("total", "int", "", "latin1"),
			why:     "changes the type from int to int unsigned",
		},
		{
			dialect: "mysql",
			name:    "decimal precision",
			alter:   "ALTER TABLE orders MODIFY total DECIMAL(12,2);",
			column:  costColumn("total", "decimal(10,2)", "", "latin1"),
			why:     "changes the type from decimal(10,2) to decimal(12,2)",
		},
		{
			dialect: "mysql",
			name:    "fractional seconds",
			alter:   "ALTER TABLE orders MODIFY total DATETIME(6);",
			column:  costColumn("total", "datetime", "", "latin1"),
			why:     "changes the type from datetime to datetime(6)",
		},
		{
			dialect: "mysql",
			name:    "a longer text family member",
			alter:   "ALTER TABLE orders MODIFY body LONGTEXT;",
			column:  costColumn("body", "text", "latin1", "latin1"),
			why:     "changes the type from text to longtext",
		},
		{
			dialect: "mysql",
			name:    "a narrower VARCHAR",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(5);",
			column:  costColumn("note", "varchar(10)", "latin1", "latin1"),
			why:     "narrows varchar(10) to varchar(5)",
		},
		{
			dialect: "mysql",
			name:    "VARCHAR to CHAR",
			alter:   "ALTER TABLE orders MODIFY note CHAR(10);",
			column:  costColumn("note", "varchar(10)", "latin1", "latin1"),
			why:     "changes the type from varchar(10) to char(10)",
		},
		{
			dialect: "mysql",
			name:    "a wider CHAR is fixed-width, so it is not the VARCHAR exception",
			alter:   "ALTER TABLE orders MODIFY note CHAR(20);",
			column:  costColumn("note", "char(10)", "latin1", "latin1"),
			why:     "changes the type from char(10) to char(20)",
		},
		{
			dialect: "mysql",
			name:    "a VARCHAR widened across the 255-byte length-prefix line",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(70);",
			column:  costColumn("note", "varchar(60)", "utf8mb4", "utf8mb4"),
			why:     "widens varchar(60) to varchar(70), which in utf8mb4 takes the longest encoding from 240 to 280 bytes across the 255-byte line",
		},
		{
			dialect: "mysql",
			name:    "the same line in a one-byte character set",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(300);",
			column:  costColumn("note", "varchar(200)", "latin1", "latin1"),
			why:     "from 200 to 300 bytes across the 255-byte line",
		},
		{
			dialect: "mysql",
			name:    "a character set named in the clause",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) CHARACTER SET utf8mb4;",
			column:  costColumn("note", "varchar(10)", "latin1", "latin1"),
			why:     "changes the character set of varchar(10) from latin1 to utf8mb4, which re-encodes every stored value",
		},
		{
			dialect: "mysql",
			name:    "CHARSET is the same clause",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) CHARSET utf8mb4;",
			column:  costColumn("note", "varchar(10)", "latin1", "latin1"),
			why:     "from latin1 to utf8mb4",
		},
		{
			dialect: "mysql",
			name:    "a clause naming no character set gives the column the table default",
			// Measured: the column was latin1 in a utf8mb4 table, the widening
			// alone would be in place, and the server copied.
			alter:  "ALTER TABLE orders MODIFY note VARCHAR(20);",
			column: costColumn("note", "varchar(10)", "latin1", "utf8mb4"),
			why:    "changes the character set of varchar(10) from latin1 to utf8mb4",
		},
		{
			dialect: "mysql",
			name:    "a table default changed earlier in the same file counts",
			alter:   "ALTER TABLE orders DEFAULT CHARACTER SET utf8mb4;\nALTER TABLE orders MODIFY note VARCHAR(20);",
			column:  costColumn("note", "varchar(10)", "latin1", "latin1"),
			why:     "from latin1 to utf8mb4",
		},
		{
			dialect: "mysql",
			name:    "utf8 is the old spelling of utf8mb3, and ascii is not a subset of it as far as InnoDB is concerned",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) CHARACTER SET utf8;",
			column:  costColumn("note", "varchar(10)", "ascii", "ascii"),
			why:     "from ascii to utf8mb3",
		},
		{
			dialect: "mysql",
			name:    "utf8mb3 to utf8mb4 on a TEXT column",
			alter:   "ALTER TABLE orders MODIFY body TEXT CHARACTER SET utf8mb4;",
			column:  costColumn("body", "text", "utf8mb3", "utf8mb3"),
			why:     "converts text from utf8mb3 to utf8mb4, a conversion both servers apply in place for a short VARCHAR or a CHAR but not for a TEXT column",
		},
		{
			dialect: "mysql",
			name:    "utf8mb3 to utf8mb4 on a VARCHAR whose longest encoding crosses 255 bytes",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(64) CHARACTER SET utf8mb4;",
			column:  costColumn("note", "varchar(64)", "utf8mb3", "utf8mb3"),
			why:     "but not for one whose longest encoding crosses 255 bytes (192 to 256 here)",
		},
		{
			name:    "utf8mb3 to utf8mb4 on an ENUM, which MariaDB copies for",
			dialect: "mariadb",
			alter:   "ALTER TABLE orders MODIFY kind ENUM('a','b') CHARACTER SET utf8mb4;",
			column:  costColumn("kind", "enum('a','b')", "utf8mb3", "utf8mb3"),
			why:     "while MariaDB copies the table for an ENUM",
		},
		{
			dialect: "mysql",
			name:    "a list column becoming a string",
			alter:   "ALTER TABLE orders MODIFY kind VARCHAR(10);",
			column:  costColumn("kind", "enum('a','b')", "latin1", "latin1"),
			why:     "changes the type from enum('a', 'b') to varchar(10)",
		},
		{
			dialect: "mysql",
			name:    "CHANGE renames and retypes at once",
			alter:   "ALTER TABLE orders CHANGE COLUMN total amount BIGINT;",
			column:  costColumn("total", "int", "", "latin1"),
			why:     "CHANGE COLUMN orders.amount changes the type from int to bigint",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeCost(c, test.dialect, test.alter, test.column)
			c.Assert(costCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MY130"})
			message := messageOf(analysis.Findings(), "MY130")
			c.Assert(message, qt.Contains, test.why)
			c.Assert(message, qt.Contains, "both MySQL and MariaDB refuse ALGORITHM=INSTANT and INPLACE")
			c.Assert(message, qt.Contains, "Out of range value or Data truncated")
		})
	}
}

// TestColumnTypeCopyRule_StaysQuietWhereTheServerDoesNotCopy holds the other
// half of the measurement: spellings of one storage type, the VARCHAR
// widening InnoDB applies in place, and the conversions it applies in place.
func TestColumnTypeCopyRule_StaysQuietWhereTheServerDoesNotCopy(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		alter   string
		column  lint.BaselineColumn
	}{
		{
			dialect: "mysql",
			name:    "attributes alone",
			alter:   "ALTER TABLE orders MODIFY total INT NOT NULL DEFAULT 0 COMMENT 'x';",
			column:  costColumn("total", "int", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "a display width",
			alter:   "ALTER TABLE orders MODIFY total INT(5);",
			column:  costColumn("total", "int(11)", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "INTEGER",
			alter:   "ALTER TABLE orders MODIFY total INTEGER;",
			column:  costColumn("total", "int", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "BOOLEAN is tinyint(1)",
			alter:   "ALTER TABLE orders MODIFY total BOOLEAN;",
			column:  costColumn("total", "tinyint(1)", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "tinyint(1) is tinyint",
			alter:   "ALTER TABLE orders MODIFY total TINYINT;",
			column:  costColumn("total", "tinyint(1)", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "a decimal's default scale",
			alter:   "ALTER TABLE orders MODIFY total DECIMAL(10);",
			column:  costColumn("total", "decimal(10,0)", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "a bare DECIMAL is decimal(10,0)",
			alter:   "ALTER TABLE orders MODIFY total NUMERIC;",
			column:  costColumn("total", "decimal(10,0)", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "DOUBLE PRECISION and REAL are double",
			alter:   "ALTER TABLE orders MODIFY total DOUBLE PRECISION;",
			column:  costColumn("total", "double", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "ZEROFILL on an unsigned column",
			alter:   "ALTER TABLE orders MODIFY total INT UNSIGNED ZEROFILL;",
			column:  costColumn("total", "int unsigned", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "TIMESTAMP(0) is timestamp",
			alter:   "ALTER TABLE orders MODIFY total TIMESTAMP(0) NULL;",
			column:  costColumn("total", "timestamp", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "CHARACTER VARYING is varchar",
			alter:   "ALTER TABLE orders MODIFY note CHARACTER VARYING(10);",
			column:  costColumn("note", "varchar(10)", "latin1", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "a VARCHAR widened within the one-byte length-prefix class",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20);",
			column:  costColumn("note", "varchar(10)", "utf8mb4", "utf8mb4"),
		},
		{
			dialect: "mysql",
			name:    "a VARCHAR widened within the two-byte class",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(200);",
			column:  costColumn("note", "varchar(100)", "utf8mb4", "utf8mb4"),
		},
		{
			dialect: "mysql",
			name:    "a VARBINARY widened",
			alter:   "ALTER TABLE orders MODIFY note VARBINARY(20);",
			column:  costColumn("note", "varbinary(10)", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "the same character set spelled out",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) CHARACTER SET latin1;",
			column:  costColumn("note", "varchar(10)", "latin1", "utf8mb4"),
		},
		{
			dialect: "mysql",
			name:    "utf8mb3 to utf8mb4 on a short VARCHAR",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(20) CHARACTER SET utf8mb4;",
			column:  costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3"),
		},
		{
			dialect: "mysql",
			name:    "utf8mb3 to utf8mb4 on a CHAR",
			alter:   "ALTER TABLE orders MODIFY note CHAR(10) CHARACTER SET utf8mb4;",
			column:  costColumn("note", "char(10)", "utf8mb3", "utf8mb3"),
		},
		{
			name:    "utf8mb3 to utf8mb4 on an ENUM, which MySQL converts in place",
			dialect: "mysql",
			alter:   "ALTER TABLE orders MODIFY kind ENUM('a','b') CHARACTER SET utf8mb4;",
			column:  costColumn("kind", "enum('a','b')", "utf8mb3", "utf8mb3"),
		},
		{
			dialect: "mysql",
			name:    "a collation change on a column whose current collation the state does not carry",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(10) COLLATE latin1_bin;",
			column:  costColumn("note", "varchar(10)", "latin1", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "a member list change is the member rules' finding, not this one's",
			alter:   "ALTER TABLE orders MODIFY kind ENUM('a','b','c');",
			column:  costColumn("kind", "enum('a','b')", "latin1", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "CHANGE that only renames",
			alter:   "ALTER TABLE orders CHANGE total amount INT;",
			column:  costColumn("total", "int", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "a column the baseline does not know",
			alter:   "ALTER TABLE orders MODIFY other BIGINT;",
			column:  costColumn("total", "int", "", "latin1"),
		},
		{
			dialect: "mysql",
			name:    "a VARCHAR widened in a character set the table cannot name",
			alter:   "ALTER TABLE orders MODIFY note VARCHAR(70);",
			column:  costColumn("note", "varchar(60)", "", ""),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeCost(c, test.dialect, test.alter, test.column)
			c.Assert(costCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestPrimaryKeyDropCopyRule_NeedsNoBaseline pins MY133 to the shape that
// decides the copy: whether the same statement adds the replacement key.
func TestPrimaryKeyDropCopyRule_NeedsNoBaseline(t *testing.T) {
	t.Run("a drop with no replacement", func(t *testing.T) {
		c := qt.New(t)
		analysis := analyzeCost(c, "mysql", "ALTER TABLE orders DROP PRIMARY KEY;")
		c.Assert(costCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MY133"})
		c.Assert(rulesOf(analysis.Findings()), qt.Contains, "CD103")
		message := messageOf(analysis.Findings(), "MY133")
		c.Assert(message, qt.Contains, "Dropping a primary key is not allowed without also adding a new primary key")
		c.Assert(message, qt.Contains, "MariaDB does the same unless another NOT NULL UNIQUE key exists")
		c.Assert(message, qt.Contains, "ALGORITHM=INPLACE, LOCK=NONE")
	})

	quiet := []struct {
		name  string
		alter string
	}{
		{name: "the replacement in the same statement", alter: "ALTER TABLE orders DROP PRIMARY KEY, ADD PRIMARY KEY (total);"},
		{name: "the replacement as a named constraint", alter: "ALTER TABLE orders DROP PRIMARY KEY, ADD CONSTRAINT pk PRIMARY KEY (total);"},
		{name: "a key added without a drop", alter: "ALTER TABLE orders ADD PRIMARY KEY (total);"},
		{name: "a foreign key dropped", alter: "ALTER TABLE orders DROP FOREIGN KEY fk_customer;"},
	}
	for _, test := range quiet {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeCost(c, "mysql", test.alter)
			c.Assert(costCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestCharsetConversionCopyRule_NamesTheColumnsThatForceTheCopy pins MY136
// to what CONVERT TO CHARACTER SET does to each column of the table.
func TestCharsetConversionCopyRule_NamesTheColumnsThatForceTheCopy(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		alter    string
		baseline []lint.BaselineColumn
		want     []string
	}{
		{
			dialect: "mysql",
			name:    "latin1 to utf8mb4 re-encodes every character column",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			baseline: []lint.BaselineColumn{
				costColumn("total", "int", "", "latin1"),
				costColumn("note", "varchar(10)", "latin1", "latin1"),
				costColumn("body", "text", "latin1", "latin1"),
			},
			want: []string{
				"CONVERT TO CHARACTER SET utf8mb4 on orders cannot be applied in place for 2 columns: note (changes the character set of varchar(10) from latin1 to utf8mb4, which re-encodes every stored value); body (",
				"both MySQL and MariaDB refuse ALGORITHM=INSTANT and INPLACE",
				"DEFAULT CHARACTER SET",
			},
		},
		{
			dialect: "mysql",
			name:    "CHARSET is the same clause, and a collation may follow",
			alter:   "ALTER TABLE orders CONVERT TO CHARSET utf8mb4 COLLATE utf8mb4_bin;",
			baseline: []lint.BaselineColumn{
				costColumn("note", "varchar(10)", "latin1", "latin1"),
			},
			want: []string{"for 1 column: note ("},
		},
		{
			dialect: "mysql",
			name:    "utf8mb3 to utf8mb4 copies for the TEXT and converts the short VARCHAR in place",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			baseline: []lint.BaselineColumn{
				costColumn("note", "varchar(10)", "utf8mb3", "utf8mb3"),
				costColumn("body", "text", "utf8mb3", "utf8mb3"),
			},
			want: []string{
				"for 1 column: body (converts text from utf8mb3 to utf8mb4, a conversion both servers apply in place for a short VARCHAR or a CHAR but not for a TEXT column)",
				"The other 1 character column of the table would have converted in place",
			},
		},
		{
			dialect: "mysql",
			name:    "a column already in the target set is not re-encoded",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			baseline: []lint.BaselineColumn{
				costColumn("note", "varchar(10)", "utf8mb4", "latin1"),
				costColumn("body", "text", "latin1", "latin1"),
			},
			want: []string{"for 1 column: body ("},
		},
		{
			name:    "utf8mb3 to utf8mb4 on MariaDB copies for the ENUM",
			dialect: "mariadb",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			baseline: []lint.BaselineColumn{
				costColumn("kind", "enum('a','b')", "utf8mb3", "utf8mb3"),
			},
			want: []string{"for 1 column: kind (", "while MariaDB copies the table for an ENUM"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeCost(c, test.dialect, test.alter, test.baseline...)
			c.Assert(costCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"MY136"})
			message := messageOf(analysis.Findings(), "MY136")
			for _, want := range test.want {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

func TestCharsetConversionCopyRule_StaysQuietWhereTheServerConvertsInPlace(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		alter    string
		baseline []lint.BaselineColumn
	}{
		{
			dialect: "mysql",
			name:    "no character column",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			baseline: []lint.BaselineColumn{
				costColumn("total", "int", "", "latin1"),
				costColumn("note", "varbinary(10)", "", "latin1"),
			},
		},
		{
			dialect: "mysql",
			name:    "every column already in the target set",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			baseline: []lint.BaselineColumn{
				costColumn("note", "varchar(10)", "utf8mb4", "utf8mb4"),
			},
		},
		{
			dialect: "mysql",
			name:    "utf8mb3 to utf8mb4 on short VARCHARs and CHARs",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			baseline: []lint.BaselineColumn{
				costColumn("note", "varchar(63)", "utf8mb3", "utf8mb3"),
				costColumn("body", "char(10)", "utf8mb3", "utf8mb3"),
			},
		},
		{
			name:    "utf8mb3 to utf8mb4 on an ENUM on MySQL",
			dialect: "mysql",
			alter:   "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;",
			baseline: []lint.BaselineColumn{
				costColumn("kind", "enum('a','b')", "utf8mb3", "utf8mb3"),
			},
		},
		{
			dialect: "mysql",
			name:    "the table default alone",
			alter:   "ALTER TABLE orders DEFAULT CHARACTER SET = utf8mb4;",
			baseline: []lint.BaselineColumn{
				costColumn("note", "varchar(10)", "latin1", "latin1"),
			},
		},
		{
			dialect: "mysql",
			name:    "a table the baseline does not know",
			alter:   "ALTER TABLE customers CONVERT TO CHARACTER SET utf8mb4;",
			baseline: []lint.BaselineColumn{
				costColumn("note", "varchar(10)", "latin1", "latin1"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeCost(c, test.dialect, test.alter, test.baseline...)
			c.Assert(costCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestCostRules_NameTheirInputWhenTheRunSuppliesNone: without the schema
// state, the statement is still reported by the generic rules, and the run
// says which rules could have said more.
func TestCostRules_NameTheirInputWhenTheRunSuppliesNone(t *testing.T) {
	c := qt.New(t)

	analysis := analyzeCost(c, "mysql", "ALTER TABLE orders MODIFY total BIGINT;\nALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;")

	c.Assert(costCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
	c.Assert(rulesOf(analysis.Findings()), qt.Contains, "DS103")
	c.Assert(rulesOf(analysis.Findings()), qt.Contains, "MY101")
	var unmet []string
	for _, input := range analysis.UnmetInputs() {
		unmet = append(unmet, input.Rule)
	}
	c.Assert(unmet, qt.Contains, "MY130")
	c.Assert(unmet, qt.Contains, "MY136")
	c.Assert(analysis.BaselineVersions(), qt.DeepEquals, []int64{2})
}

// TestCostRules_SubsumeTheGenericFindings: a finding that says which copy
// the server performs leaves nothing for "a column type changed" and "this
// ALTER usually rebuilds the table" to add on the same statement.
func TestCostRules_SubsumeTheGenericFindings(t *testing.T) {
	t.Run("MY130 replaces DS103 and MY101", func(t *testing.T) {
		c := qt.New(t)
		analysis := analyzeCost(c, "mysql", "ALTER TABLE orders MODIFY total BIGINT;", costColumn("total", "int", "", "latin1"))
		c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"MY130"})
		c.Assert(analysis.Findings()[0].Context.Subjects, qt.DeepEquals, []lint.Subject{{
			Kind: lint.SubjectColumn, Name: "total", Parent: "orders", DataType: "bigint",
		}})
	})

	t.Run("MY136 replaces MY101", func(t *testing.T) {
		c := qt.New(t)
		analysis := analyzeCost(c, "mysql", "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;", costColumn("note", "varchar(10)", "latin1", "latin1"))
		c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"MY136"})
		c.Assert(analysis.Findings()[0].Context.Subjects, qt.DeepEquals, []lint.Subject{{Kind: lint.SubjectTable, Name: "orders"}})
	})

	t.Run("a change applied in place is MY130P's, and the generic findings go with it", func(t *testing.T) {
		c := qt.New(t)
		analysis := analyzeCost(c, "mysql", "ALTER TABLE orders MODIFY note VARCHAR(20);", costColumn("note", "varchar(10)", "utf8mb4", "utf8mb4"))
		c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"MY130P"})
	})

	t.Run("a change the rules cannot judge leaves the generic findings in place", func(t *testing.T) {
		c := qt.New(t)
		analysis := analyzeCost(c, "mysql", "ALTER TABLE orders MODIFY note VARCHAR(70);", costColumn("note", "varchar(60)", "", ""))
		c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"DS103", "MY101"})
	})
}

func TestCostRules_RunOnlyForTheMySQLFamily(t *testing.T) {
	for _, dialect := range []string{"postgres", "sqlite"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeCost(c, dialect, "ALTER TABLE orders DROP PRIMARY KEY;\nALTER TABLE orders MODIFY total BIGINT;", costColumn("total", "int", "", "latin1"))
			c.Assert(costCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestPrimaryKeyAddedRule_SaysWhatWasMeasured pins the MY132 wording to the
// measurement in mysqlcost.go rather than to the older claim that the
// rebuild blocks DML.
func TestPrimaryKeyAddedRule_SaysWhatWasMeasured(t *testing.T) {
	c := qt.New(t)

	analysis := analyzeCost(c, "mysql", "ALTER TABLE orders ADD PRIMARY KEY (total);")

	message := messageOf(analysis.Findings(), "MY132")
	c.Assert(message, qt.Contains, "in place with writes allowed (ALGORITHM=INPLACE, LOCK=NONE)")
	c.Assert(message, qt.Not(qt.Contains), "blocks DML")
}
