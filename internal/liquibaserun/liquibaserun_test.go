package liquibaserun_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/liquibaserun"
)

// Attribute recognizes each changeset attribute in any case, and says what its
// value asks of an import. An empty value, and a value that spells out
// Liquibase's default, asks nothing.
func TestAttribute_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		value  string
		effect liquibaserun.Effect
	}{
		{name: "id", key: "id", value: "1", effect: liquibaserun.NoEffect},
		{name: "created", key: "created", value: "2026-01-01", effect: liquibaserun.NoEffect},
		{name: "logicalFilePath", key: "logicalFilePath", value: "db/changelog.xml", effect: liquibaserun.NoEffect},
		{name: "onValidationFail", key: "onValidationFail", value: "MARK_RAN", effect: liquibaserun.NoEffect},
		{name: "validCheckSum", key: "validCheckSum", value: "ANY", effect: liquibaserun.NoEffect},
		{name: "splitStatements", key: "splitStatements", value: "false", effect: liquibaserun.NoEffect},
		{name: "dbms", key: "dbms", value: "mysql", effect: liquibaserun.Selector},
		{name: "empty dbms", key: "dbms", value: " ", effect: liquibaserun.NoEffect},
		{name: "contextFilter", key: "contextFilter", value: "prod", effect: liquibaserun.Selector},
		{name: "labels", key: "labels", value: "nightly", effect: liquibaserun.Selector},
		{name: "preconditions whatever it holds", key: "preConditions", value: "", effect: liquibaserun.Selector},
		{name: "upper-case dbms", key: "DBMS", value: "mysql", effect: liquibaserun.Selector},
		{name: "runAlways true", key: "runAlways", value: "TRUE", effect: liquibaserun.Repeat},
		{name: "alwaysRun true", key: "alwaysRun", value: "true", effect: liquibaserun.Repeat},
		{name: "runOnChange false", key: "runOnChange", value: "false", effect: liquibaserun.NoEffect},
		{name: "runInTransaction false", key: "runInTransaction", value: "false", effect: liquibaserun.NoTransaction},
		{name: "runInTransaction true", key: "runInTransaction", value: "true", effect: liquibaserun.NoEffect},
		{name: "ignore true", key: "ignore", value: "true", effect: liquibaserun.Skip},
		{name: "ignore false", key: "ignore", value: "false", effect: liquibaserun.NoEffect},
		{name: "failOnError false", key: "failOnError", value: "false", effect: liquibaserun.Unsupported},
		{name: "failOnError true", key: "failOnError", value: "true", effect: liquibaserun.NoEffect},
		{name: "runOrder", key: "runOrder", value: "last", effect: liquibaserun.Unsupported},
		{name: "runWith jdbc", key: "runWith", value: "JDBC", effect: liquibaserun.NoEffect},
		{name: "runWith psql", key: "runWith", value: "psql", effect: liquibaserun.Unsupported},
		{name: "runWithSpoolFile", key: "runWithSpoolFile", value: "out.spool", effect: liquibaserun.Unsupported},
		{name: "objectQuotingStrategy LEGACY", key: "objectQuotingStrategy", value: "LEGACY", effect: liquibaserun.NoEffect},
		{name: "objectQuotingStrategy QUOTE_ALL_OBJECTS", key: "objectQuotingStrategy", value: "QUOTE_ALL_OBJECTS", effect: liquibaserun.Unsupported},
		{name: "endDelimiter semicolon", key: "endDelimiter", value: ";", effect: liquibaserun.NoEffect},
		{name: "endDelimiter slash", key: "endDelimiter", value: "/", effect: liquibaserun.Unsupported},
		{name: "rollbackEndDelimiter GO", key: "rollbackEndDelimiter", value: "GO", effect: liquibaserun.Unsupported},
		{name: "a name Ptah does not read", key: "tag", value: "v1", effect: liquibaserun.Unknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			effect, err := liquibaserun.Attribute(test.key, test.value)

			c.Assert(err, qt.IsNil)
			c.Assert(effect, qt.Equals, test.effect)
		})
	}
}

// A boolean attribute whose value is not a boolean is refused: what the author
// meant by it is unknown.
func TestAttribute_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		message string
	}{
		{name: "runAlways", key: "runAlways", value: "yes", message: `runAlways "yes" is not true or false`},
		{name: "ignore", key: "ignore", value: "maybe", message: `ignore "maybe" is not true or false`},
		{name: "runInTransaction", key: "runInTransaction", value: "no", message: `runInTransaction "no" is not true or false`},
		{name: "failOnError", key: "failOnError", value: "0", message: `failOnError "0" is not true or false`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			effect, err := liquibaserun.Attribute(test.key, test.value)

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(effect, qt.Equals, liquibaserun.NoEffect)
		})
	}
}

// The zero value asks nothing; the attributes that convert rather than refuse
// are reported, not refused; and a name is recorded once however often it is
// noted.
func TestChangeset_HappyPath(t *testing.T) {
	c := qt.New(t)
	var none liquibaserun.Changeset
	var converted liquibaserun.Changeset
	converted.Note("runInTransaction", "false")
	converted.Note("ignore", "true")
	var repeated liquibaserun.Changeset
	repeated.NoteKnown("preconditions", "")
	repeated.NoteKnown("preconditions", "")
	repeated.AddSelector("<sql> dbms")
	repeated.AddSelector("<sql> dbms")

	c.Assert(none.Err("s:1", "c.sql"), qt.IsNil)
	c.Assert(none.Skipped(), qt.IsFalse)
	c.Assert(none.NoTransaction(), qt.IsFalse)
	c.Assert(converted.Err("s:1", "c.sql"), qt.IsNil)
	c.Assert(converted.Skipped(), qt.IsTrue)
	c.Assert(converted.NoTransaction(), qt.IsTrue)
	c.Assert(repeated.Err("s:1", "c.sql"), qt.ErrorMatches,
		`liquibase changeset s:1 in "c.sql" is conditional on preconditions, <sql> dbms; .*`)
}

// A clone shares nothing with its original, so a changelog's own attributes can
// start every changeset in it without one changeset's attributes reaching
// another.
func TestChangeset_Clone_HappyPath(t *testing.T) {
	c := qt.New(t)
	var inherited liquibaserun.Changeset
	inherited.Note("context", "prod")
	inherited.Note("labels", "nightly")
	inherited.Note("dbms", "mysql")
	first := inherited.Clone()
	second := inherited.Clone()
	first.AddSelector("<sql> dbms")
	second.AddSelector("<insert> dbms")

	c.Assert(first.Err("s:1", "c.xml"), qt.ErrorMatches,
		`liquibase changeset s:1 in "c.xml" is conditional on context, labels, dbms, <sql> dbms; .*`)
	c.Assert(second.Err("s:2", "c.xml"), qt.ErrorMatches,
		`liquibase changeset s:2 in "c.xml" is conditional on context, labels, dbms, <insert> dbms; .*`)
	c.Assert(inherited.Err("s:3", "c.xml"), qt.ErrorMatches,
		`liquibase changeset s:3 in "c.xml" is conditional on context, labels, dbms; .*`)
}

// An attribute with no form in a Ptah migration, and one Ptah does not read,
// are refused by name in one message, each with what it would have changed.
func TestChangeset_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		attributes [][2]string
		message    string
	}{
		{
			name:       "failOnError false",
			attributes: [][2]string{{"failOnError", "false"}},
			message: `liquibase changeset s:1 in "c.xml" sets failOnError=false, which Ptah cannot carry because ` +
				`Liquibase records the changeset as run when it fails, and a failed Ptah migration stops the apply ` +
				`-- import it by hand`,
		},
		{
			name:       "runOrder and a name Ptah does not read",
			attributes: [][2]string{{"runOrder", "last"}, {"tag", "v1"}},
			message: `liquibase changeset s:1 in "c.xml" sets runOrder=last, which Ptah cannot carry because ` +
				`Liquibase moves the changeset to the start or the end of the update; tag, which Ptah does not read ` +
				`-- import it by hand`,
		},
		{
			// The selector decides the remedy and is named first.
			name:       "a selector beside runWith",
			attributes: [][2]string{{"runWith", "psql"}, {"dbms", "postgresql"}},
			message:    `liquibase changeset s:1 in "c.xml" is conditional on dbms; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var changeset liquibaserun.Changeset
			for _, attribute := range test.attributes {
				changeset.Note(attribute[0], attribute[1])
			}

			c.Assert(changeset.Err("s:1", "c.xml"), qt.ErrorMatches, test.message)
		})
	}
}

// Attributes skips the author:id, however it is spelled, and reads a quoted
// value and one written after a blank.
func TestAttributes_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args string
		want [][2]string
	}{
		{name: "author:id only", args: " dbms:1", want: nil},
		{name: "author named like an attribute", args: " dbms:1 runInTransaction:false", want: [][2]string{{"runInTransaction", "false"}}},
		{name: "quoted value", args: ` s:1 context:"prod or staging"`, want: [][2]string{{"context", "prod or staging"}}},
		{name: "value after a blank", args: " s:1 dbms: mysql labels:x", want: [][2]string{{"dbms", "mysql"}, {"labels", "x"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(liquibaserun.Attributes(test.args), qt.DeepEquals, test.want)
		})
	}
}

// ScanFormattedSQL refuses a file holding a changeset whose attributes a copy
// cannot carry, naming the first one, whichever changeset in the file it is.
func TestScanFormattedSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		message string
	}{
		{
			name:    "dbms",
			content: "--liquibase formatted sql\n--changeset s:1 dbms:mysql\nCREATE TABLE t (id int);\n",
			message: `liquibase changeset s:1 in "1_x.sql" is conditional on dbms; a migration directory has no ` +
				`equivalent, so importing it would turn a conditional history into an unconditional one -- ` +
				`split the changelog or import it by hand`,
		},
		{
			name:    "dbms on the first of two changesets",
			content: "--liquibase formatted sql\n--changeset s:1 dbms:mysql\nSELECT 1;\n--changeset s:2\nSELECT 2;\n",
			message: `liquibase changeset s:1 in "1_x.sql" is conditional on dbms; .*`,
		},
		{
			name:    "labels on the last changeset",
			content: "--liquibase formatted sql\n--changeset s:1\nSELECT 1;\n--changeset s:2 labels:nightly\nSELECT 2;\n",
			message: `liquibase changeset s:2 in "1_x.sql" is conditional on labels; .*`,
		},
		{
			name: "preconditions",
			content: "--liquibase formatted sql\n--changeset s:1\n--preconditions onFail:MARK_RAN\n" +
				"--precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM t\nSELECT 1;\n",
			message: `liquibase changeset s:1 in "1_x.sql" is conditional on preconditions; .*`,
		},
		{
			name:    "runOnChange",
			content: "--liquibase formatted sql\n--changeset s:1 runOnChange:true\nCREATE VIEW v AS SELECT 1;\n",
			message: `liquibase changeset s:1 in "1_x.sql" sets runOnChange, so Liquibase can run it again .*`,
		},
		{
			name:    "a repeat that is not a boolean",
			content: "--liquibase formatted sql\n--changeset s:1 runOnChange:maybe\nSELECT 1;\n",
			message: `liquibase changeset s:1 in "1_x.sql": runOnChange "maybe" is not true or false`,
		},
		{
			name:    "failOnError false",
			content: "--liquibase formatted sql\n--changeset s:1 failOnError:false\nSELECT 1;\n",
			message: `liquibase changeset s:1 in "1_x.sql" sets failOnError=false, which Ptah cannot carry .*`,
		},
		{
			name:    "endDelimiter",
			content: "--liquibase formatted sql\n--changeset s:1 endDelimiter:/\nBEGIN NULL; END;\n/\n",
			message: `liquibase changeset s:1 in "1_x.sql" sets endDelimiter=/, which Ptah cannot carry because ` +
				`Ptah does not know the delimiter, so it would stay in the SQL -- import it by hand`,
		},
		{
			name:    "a name Ptah does not read",
			content: "--liquibase formatted sql\n--changeset s:1 tag:v1\nSELECT 1;\n",
			message: `liquibase changeset s:1 in "1_x.sql" sets tag, which Ptah does not read -- import it by hand`,
		},
		{
			name:    "ignore",
			content: "--liquibase formatted sql\n--changeset s:1\nSELECT 1;\n--changeset s:2 ignore:true\nSELECT 2;\n",
			message: `liquibase changeset s:2 in "1_x.sql" sets ignore, so Liquibase never runs it, and a file ` +
				`copied whole cannot leave one changeset out -- remove the changeset or import it by hand`,
		},
		{
			name:    "transaction modes mixed",
			content: "--liquibase formatted sql\n--changeset s:1 runInTransaction:false\nVACUUM;\n--changeset s:2\nSELECT 2;\n",
			message: `liquibase file "1_x.sql" sets runInTransaction to false on some changesets and not on others, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			scanned, err := liquibaserun.ScanFormattedSQL("1_x.sql", test.content)

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(scanned, qt.DeepEquals, liquibaserun.FormattedCopy{})
		})
	}
}

// ScanFormattedSQL reads attributes and nothing else. A layout the changeset
// parser refuses -- a header with no changeset, SQL before the first one -- is
// carried whole by a copy and passes here, and a file whose changesets all set
// runInTransaction to false is a no-transaction copy.
func TestScanFormattedSQL_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		content       string
		noTransaction bool
	}{
		{name: "header only", content: "--liquibase formatted sql\n"},
		{name: "no header", content: "CREATE TABLE t (id int);\n"},
		{name: "sql before the first changeset", content: "--liquibase formatted sql\nSELECT 0;\n--changeset s:1\nSELECT 1;\n"},
		{name: "defaults spelled out", content: "--liquibase formatted sql\n--changeset s:1 runAlways:false ignore:false failOnError:true dbms:\nSELECT 1;\n"},
		{name: "a comment that mentions a precondition", content: "--liquibase formatted sql\n--changeset s:1\n-- precondition checked by the deploy job\nSELECT 1;\n"},
		{name: "a precondition line before any changeset", content: "--liquibase formatted sql\n--preconditions onFail:HALT\n--changeset s:1\nSELECT 1;\n"},
		{name: "one no-transaction changeset", content: "--liquibase formatted sql\n--changeset atlas:1-1 runInTransaction:false\nVACUUM;\n", noTransaction: true},
		{
			name:          "every changeset no-transaction",
			content:       "--liquibase formatted sql\n--changeset s:1 runInTransaction:false\nVACUUM;\n--changeset s:2 runInTransaction:false\nVACUUM;\n",
			noTransaction: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			scanned, err := liquibaserun.ScanFormattedSQL("1_x.sql", test.content)

			c.Assert(err, qt.IsNil)
			c.Assert(scanned.NoTransaction, qt.Equals, test.noTransaction)
		})
	}
}

// MatchDBMS follows Liquibase's DatabaseList.definitionMatches: `all` wins,
// then `none`, then an excluded name, and otherwise a list naming no database
// without `!` matches every one it does not exclude.
func TestMatchDBMS_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		shortName  string
		matches    bool
	}{
		{name: "empty", definition: " ", shortName: "postgresql", matches: true},
		{name: "the named database", definition: "postgresql", shortName: "postgresql", matches: true},
		{name: "another database", definition: "mysql", shortName: "postgresql", matches: false},
		{name: "a list naming it", definition: "mysql, postgresql", shortName: "postgresql", matches: true},
		{name: "a list not naming it", definition: "mysql,mariadb", shortName: "postgresql", matches: false},
		{name: "it excluded", definition: "!postgresql", shortName: "postgresql", matches: false},
		{name: "another excluded", definition: "!mysql", shortName: "postgresql", matches: true},
		{name: "another excluded beside it", definition: "!mysql,postgresql", shortName: "postgresql", matches: true},
		{name: "another excluded beside a third", definition: "!mysql,mariadb", shortName: "postgresql", matches: false},
		{name: "all", definition: "all", shortName: "postgresql", matches: true},
		{name: "all outranks its exclusion", definition: "all,!postgresql", shortName: "postgresql", matches: true},
		{name: "none", definition: "none", shortName: "postgresql", matches: false},
		{name: "none outranks naming it", definition: "none,postgresql", shortName: "postgresql", matches: false},
		{name: "names compare exactly", definition: "PostgreSQL", shortName: "postgresql", matches: false},
		{name: "an unknown name excluded is not checked", definition: "!notadb", shortName: "postgresql", matches: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			matches, err := liquibaserun.MatchDBMS(test.definition, test.shortName)

			c.Assert(err, qt.IsNil)
			c.Assert(matches, qt.Equals, test.matches)
		})
	}
}

// A name Liquibase does not know is refused, as Liquibase's validation refuses
// it. Ptah's own dialect names are not Liquibase's.
func TestMatchDBMS_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		message    string
	}{
		{name: "a Ptah dialect name", definition: "postgres", message: `dbms "postgres" names "postgres", which is not a database Liquibase knows`},
		{name: "one unknown name in a list", definition: "mysql,spanner", message: `dbms "mysql,spanner" names "spanner", which is not a database Liquibase knows`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			matches, err := liquibaserun.MatchDBMS(test.definition, "postgresql")

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(matches, qt.IsFalse)
		})
	}
}

// ResolveDBMS lowercases a changeset's dbms, as Liquibase does, answers whether
// the changeset runs on the named database, and stops dbms from refusing the
// changeset. Other selectors still refuse it.
func TestChangeset_ResolveDBMS_HappyPath(t *testing.T) {
	c := qt.New(t)
	var matching liquibaserun.Changeset
	matching.Note("dbms", "MySQL, PostgreSQL")
	var other liquibaserun.Changeset
	other.Note("dbms", "mysql")
	other.Note("context", "prod")
	var none liquibaserun.Changeset

	c.Assert(matching.DBMS(), qt.Equals, "MySQL, PostgreSQL")
	runs, err := matching.ResolveDBMS("postgresql")
	c.Assert(err, qt.IsNil)
	c.Assert(runs, qt.IsTrue)
	c.Assert(matching.Err("s:1", "c.xml"), qt.IsNil)

	runs, err = other.ResolveDBMS("postgresql")
	c.Assert(err, qt.IsNil)
	c.Assert(runs, qt.IsFalse)
	c.Assert(other.Err("s:2", "c.xml"), qt.ErrorMatches, `liquibase changeset s:2 in "c.xml" is conditional on context; .*`)

	runs, err = none.ResolveDBMS("postgresql")
	c.Assert(err, qt.IsNil)
	c.Assert(runs, qt.IsTrue)
}

// KnownDBMS accepts Liquibase's names in any case and nothing else.
func TestKnownDBMS_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		known bool
	}{
		{name: "postgresql", known: true},
		{name: "MSSQL", known: true},
		{name: "yugabytedb", known: true},
		{name: "cloudspanner", known: true},
		{name: "postgres", known: false},
		{name: "sqlserver", known: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(liquibaserun.KnownDBMS(test.name), qt.Equals, test.known)
		})
	}
}

// FormattedLines keeps what Liquibase reads and drops an --ignoreLines
// directive with the lines it skips. Each row was checked against Liquibase
// 5.0.4 where it concerns a changelog Liquibase runs (stokaro/ptah#3727).
func TestFormattedLines_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    []string
	}{
		{
			name:    "a block",
			content: "a\n--ignoreLines:start\nskipped\n--ignoreLines:end\nb",
			want:    []string{"a", "b"},
		},
		{
			name:    "a count",
			content: "a\n--ignoreLines:2\nskipped\nskipped\nb",
			want:    []string{"a", "b"},
		},
		{
			name:    "a blank after the dashes, and the name in any case",
			content: "a\n-- IGNORELINES:1\nskipped\nb",
			want:    []string{"a", "b"},
		},
		{
			name:    "a block with no end runs to the end of the file",
			content: "a\n--ignoreLines:start\nskipped\n--changeset s:2\nskipped",
			want:    []string{"a"},
		},
		{
			// Liquibase ends a block on end in lower case only.
			name:    "END inside a block does not end it",
			content: "a\n--ignoreLines:start\n--ignoreLines:END\nskipped\n--ignoreLines:end\nb",
			want:    []string{"a", "b"},
		},
		{
			// Inside a block Liquibase reads no directive but end, not even
			// one it refuses outside a block.
			name:    "a count and a misspelling inside a block",
			content: "a\n--ignoreLines:start\n--ignoreLines:3\n--ignore:true\nskipped\n--ignoreLines:end\nb",
			want:    []string{"a", "b"},
		},
		{
			name:    "a count past the end of the file",
			content: "a\n--ignoreLines:99\nskipped",
			want:    []string{"a"},
		},
		{
			name:    "a count of zero",
			content: "a\n--ignoreLines:0\nb",
			want:    []string{"a", "b"},
		},
		{
			// The directive must fill its line, so these are comments.
			name:    "text after the word, and a blank before the dashes",
			content: "--ignoreLines:1 skip one\n --ignoreLines:1\nb",
			want:    []string{"--ignoreLines:1 skip one", " --ignoreLines:1", "b"},
		},
		{
			name:    "a carriage return before the newline",
			content: "a\r\n--ignoreLines:start\r\nskipped\r\n--ignoreLines:end\r\nb\r",
			want:    []string{"a\r", "b\r"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			lines, err := liquibaserun.FormattedLines("c.sql", test.content)

			c.Assert(err, qt.IsNil)
			c.Assert(lines, qt.DeepEquals, test.want)
		})
	}
}

// The spellings Liquibase refuses are refused, with the line that holds them.
func TestFormattedLines_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		message string
	}{
		{
			name:    "start in upper case",
			content: "a\n--ignoreLines:START\nb",
			message: `liquibase changelog "c.sql" line 2: "--ignoreLines:START" names neither start nor a number of lines, so Liquibase refuses it`,
		},
		{
			name:    "a word that is not a number",
			content: "--ignoreLines:1_0",
			message: `liquibase changelog "c.sql" line 1: "--ignoreLines:1_0" names neither start nor a number of lines, so Liquibase refuses it`,
		},
		{
			name:    "one dash",
			content: "a\n-ignoreLines:2",
			message: `liquibase changelog "c.sql" line 2: "-ignoreLines:2" is not a directive Liquibase reads, and Liquibase refuses it -- write --ignoreLines:<count\|start\|end>`,
		},
		{
			name:    "ignore for ignoreLines",
			content: "--ignore:2",
			message: `liquibase changelog "c.sql" line 1: "--ignore:2" is not a directive Liquibase reads, .*`,
		},
		{
			name:    "one dash inside a block",
			content: "--ignoreLines:start\nskipped\n-ignoreLines:end\n--ignoreLines:end",
			message: `liquibase changelog "c.sql" line 3: "-ignoreLines:end" is not a directive Liquibase reads, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			lines, err := liquibaserun.FormattedLines("c.sql", test.content)

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(lines, qt.IsNil)
		})
	}
}

// PropertyReferences finds what Liquibase's expander fills in, each once.
func TestPropertyReferences_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{name: "none", text: "SELECT 1", want: nil},
		{name: "one", text: "CREATE TABLE ${schema}.t (id int)", want: []string{"${schema}"}},
		{name: "each once, in order", text: "${b} ${a} ${b}", want: []string{"${b}", "${a}"}},
		{name: "one inside another", text: "x ${a${b}} y", want: []string{"${a${b}}"}},
		{name: "a dollar before it", text: "$${a}", want: []string{"${a}"}},
		{name: "no closing brace", text: "SELECT '${a' || '}'", want: []string{"${a' || '}"}},
		{name: "no closing brace at all", text: "SELECT '${a'", want: nil},
		{name: "a dollar alone", text: "SELECT $1, $$body$$", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(liquibaserun.PropertyReferences(test.text), qt.DeepEquals, test.want)
		})
	}
}

// PropertyReferencesErr names every reference across its texts and says why
// the value cannot be taken from the changelog.
func TestPropertyReferencesErr_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := liquibaserun.PropertyReferencesErr("CREATE TABLE ${schema}.t", "DROP TABLE ${schema}.${t}")

	c.Assert(err, qt.ErrorMatches, `uses the property reference \$\{schema\}, \$\{t\}; Liquibase fills it in when it runs, `+
		`and an environment variable, a Java system property or a command-line parameter of that name wins over `+
		`any property the changelog defines, so the changelog does not record the value that ran -- write the `+
		`value in, or import the changeset by hand`)
}

// Text without a reference passes.
func TestPropertyReferencesErr_HappyPath(t *testing.T) {
	c := qt.New(t)

	c.Assert(liquibaserun.PropertyReferencesErr("SELECT 1", "", "SELECT '$'"), qt.IsNil)
}

// A copied file carries only the lines Liquibase reads, so a changeset inside
// an ignored block is not read and does not refuse the file.
func TestScanFormattedSQL_IgnoreLines_HappyPath(t *testing.T) {
	c := qt.New(t)
	content := "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE a (id int);\n" +
		"--ignoreLines:start\n--changeset s:9 dbms:mysql\nCREATE TABLE hidden (id int);\n--ignoreLines:end\n" +
		"--ignoreLines:1\nCREATE TABLE skipped (id int);\n"

	scanned, err := liquibaserun.ScanFormattedSQL("1_x.sql", content)

	c.Assert(err, qt.IsNil)
	c.Assert(scanned.Lines, qt.DeepEquals, []string{
		"--liquibase formatted sql", "--changeset s:1", "CREATE TABLE a (id int);", "",
	})
}

// A property reference in a copied changeset, in its SQL or its rollback,
// refuses the file; so does a directive Liquibase refuses.
func TestScanFormattedSQL_PropertyReference_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		message string
	}{
		{
			name:    "in the SQL",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE ${t} (id int);\n",
			message: `liquibase changeset s:1 in "1_x.sql" uses the property reference \$\{t\}; .*`,
		},
		{
			name:    "in the rollback",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n--rollback DROP TABLE ${t};\n",
			message: `liquibase changeset s:1 in "1_x.sql" uses the property reference \$\{t\}; .*`,
		},
		{
			name:    "a directive Liquibase refuses",
			content: "--liquibase formatted sql\n--changeset s:1\n--ignoreLines:START\nSELECT 1;\n",
			message: `liquibase changelog "1_x.sql" line 3: "--ignoreLines:START" names neither start nor a number of lines, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			scanned, err := liquibaserun.ScanFormattedSQL("1_x.sql", test.content)

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(scanned, qt.DeepEquals, liquibaserun.FormattedCopy{})
		})
	}
}
