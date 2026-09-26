package liquibaserun_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/liquibaserun"
)

// readChangeset reads a changelog holding one changeset, s:1, whose lines
// follow its marker.
func readChangeset(c *qt.C, changeset string) liquibaserun.FormattedChangeset {
	c.Helper()
	read, err := liquibaserun.ReadFormattedSQL("c.sql", "--liquibase formatted sql\n--changeset s:1\n"+changeset)
	c.Assert(err, qt.IsNil)
	c.Assert(read.Changesets, qt.HasLen, 1)
	return read.Changesets[0]
}

// ReadFormattedSQL splits a changeset into its body and its rollback as
// Liquibase's parser does. Each row was run by Liquibase 5.0.4 on SQLite
// (stokaro/ptah#3735): the body is the SQL update ran, and the rollback is the
// text rollback-count-sql printed.
func TestReadFormattedSQL_Rollback_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		changeset string
		body      []string
		rollback  string
	}{
		{
			name:      "rollback lines",
			changeset: "CREATE TABLE t (id int);\n--rollback DROP TABLE t;\n--rollback DROP TABLE u;",
			body:      []string{"CREATE TABLE t (id int);"},
			rollback:  "DROP TABLE t;\nDROP TABLE u;\n",
		},
		{
			name:      "a rollback line in any case, with blanks after the dashes",
			changeset: "SELECT 1;\n--  ROLLBACK DROP TABLE t;",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE t;\n",
		},
		{
			name:      "a rollback line keeps the blanks after its one blank",
			changeset: "SELECT 1;\n--rollback   DROP TABLE t;",
			body:      []string{"SELECT 1;"},
			rollback:  "  DROP TABLE t;\n",
		},
		{
			name:      "a block, with SQL after it",
			changeset: "CREATE TABLE t (id int);\n/* liquibase rollback\nDROP TABLE t;\n*/\nCREATE TABLE u (id int);",
			body:      []string{"CREATE TABLE t (id int);", "CREATE TABLE u (id int);"},
			rollback:  "DROP TABLE t;",
		},
		{
			name:      "the lines of a block join with nothing between them",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE u;\nDROP TABLE t;\n*/",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE u;DROP TABLE t;",
		},
		{
			name:      "a block joins the rollback line after it",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE u\n*/\n--rollback ;DROP TABLE t;",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE u;DROP TABLE t;\n",
		},
		{
			name:      "a rollback line ends before the block after it",
			changeset: "SELECT 1;\n--rollback DROP TABLE u;\n/* liquibase rollback\nDROP TABLE t;\n*/",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE u;\nDROP TABLE t;",
		},
		{
			name:      "two blocks",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE u;\n*/\n/* liquibase rollback\nDROP TABLE t;\n*/",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE u;DROP TABLE t;",
		},
		{
			name:      "an opening with no blanks",
			changeset: "SELECT 1;\n/*liquibaserollback\nDROP TABLE t;\n*/",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE t;",
		},
		{
			name:      "an opening in any case",
			changeset: "SELECT 1;\n/* LIQUIBASE Rollback\nDROP TABLE t;\n*/",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE t;",
		},
		{
			name:      "an opening among blanks",
			changeset: "SELECT 1;\n  \t/*   liquibase \t rollback  \t\nDROP TABLE t;\n*/",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE t;",
		},
		{
			name:      "text before the closing */",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE u;\nDROP TABLE t; */",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE u;DROP TABLE t; ",
		},
		{
			name:      "blanks before the closing */ add nothing",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE\n   */\n--rollback t;",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLEt;\n",
		},
		{
			name:      "a line that goes on after */ does not close the block",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE u;\n*/ -- done\nDROP TABLE t;\n*/",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE u;*/ -- doneDROP TABLE t;",
		},
		{
			name:      "a blank line in a block adds nothing",
			changeset: "SELECT 1;\n/* liquibase rollback\n\nDROP TABLE t;\n\n*/",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE t;",
		},
		{
			name:      "a carriage return before each line feed",
			changeset: "SELECT 1;\r\n/* liquibase rollback\r\nDROP TABLE u;\r\nDROP TABLE t; */\r\n--rollback DROP TABLE v;\r",
			body:      []string{"SELECT 1;\r"},
			rollback:  "DROP TABLE u;DROP TABLE t; DROP TABLE v;\n",
		},
		{
			name:      "an --ignoreLines directive inside a block is rollback text",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE u;\n--ignoreLines:1\nDROP TABLE t;\n*/",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE u;--ignoreLines:1DROP TABLE t;",
		},
		{
			name:      "a --changeset marker inside a block is rollback text",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE t;\n--changeset s:2\nCREATE TABLE u (id int);\n*/",
			body:      []string{"SELECT 1;"},
			rollback:  "DROP TABLE t;--changeset s:2CREATE TABLE u (id int);",
		},
		{
			name:      "an --ignoreLines count that skips the opening",
			changeset: "SELECT 1;\n--ignoreLines:1\n/* liquibase rollback\nDROP TABLE t;\n*/",
			body:      []string{"SELECT 1;", "DROP TABLE t;", "*/"},
		},
		{
			name:      "an opening with text after it is SQL",
			changeset: "SELECT 1;\n/* liquibase rollback:\nDROP TABLE t;\n*/",
			body:      []string{"SELECT 1;", "/* liquibase rollback:", "DROP TABLE t;", "*/"},
		},
		{
			name:      "a block on one line is SQL",
			changeset: "SELECT 1;\n/* liquibase rollback DROP TABLE t; */",
			body:      []string{"SELECT 1;", "/* liquibase rollback DROP TABLE t; */"},
		},
		{
			name:      "rollback with no blank after it is SQL",
			changeset: "SELECT 1;\n--rollback\tDROP TABLE u;\n--rollback;DROP TABLE t;\n--rollback",
			body:      []string{"SELECT 1;", "--rollback\tDROP TABLE u;", "--rollback;DROP TABLE t;", "--rollback"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changeset := readChangeset(c, test.changeset)

			c.Assert(changeset.Body, qt.DeepEquals, test.body)
			c.Assert(changeset.Rollback.Text(), qt.Equals, test.rollback)
		})
	}
}

// Before the first changeset Liquibase reads no rollback: a block there is
// preamble, and Liquibase 5.0.4 ignored it.
func TestReadFormattedSQL_RollbackBeforeTheFirstChangeset_HappyPath(t *testing.T) {
	c := qt.New(t)

	read, err := liquibaserun.ReadFormattedSQL("c.sql",
		"--liquibase formatted sql\n/* liquibase rollback\nDROP TABLE x;\n*/\n--changeset s:1\nSELECT 1;")

	c.Assert(err, qt.IsNil)
	c.Assert(read.Preamble, qt.DeepEquals,
		[]string{"--liquibase formatted sql", "/* liquibase rollback", "DROP TABLE x;", "*/"})
	c.Assert(read.Changesets, qt.HasLen, 1)
	c.Assert(read.Changesets[0].Rollback.Text(), qt.Equals, "")
}

// A changelog Liquibase refuses to parse is refused.
func TestReadFormattedSQL_Rollback_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		changeset string
		message   string
	}{
		{
			name:      "a block no line closes",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE t;\n",
			message: `liquibase changeset s:1 in "c.sql" opens a /\* liquibase rollback block that no line closes, and ` +
				`Liquibase refuses the changelog -- end the block with a line that ends in \*/`,
		},
		{
			name:      "a block that runs over the next changeset to the end",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE t;\n--changeset s:2\nSELECT 2;\n*/ -- not a close",
			message:   `liquibase changeset s:1 in "c.sql" opens a /\* liquibase rollback block that no line closes, .*`,
		},
		{
			name:      "rollback with one dash",
			changeset: "SELECT 1;\n-rollback DROP TABLE t;",
			message: `liquibase changelog "c.sql" line 4: "-rollback DROP TABLE t;" is not a directive Liquibase ` +
				`reads, and Liquibase refuses it -- write --rollback <SQL>`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			read, err := liquibaserun.ReadFormattedSQL("c.sql", "--liquibase formatted sql\n--changeset s:1\n"+test.changeset)

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(read.Preamble, qt.IsNil)
			c.Assert(read.Changesets, qt.IsNil)
		})
	}
}

// Kind follows Liquibase's handleRollbackSequence. Each row was run by
// Liquibase 5.0.4 on SQLite: an empty rollback made rollback-count run nothing,
// and a rollback naming changesetId stopped the parse.
func TestRollbackKind_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		changeset string
		kind      liquibaserun.RollbackKind
	}{
		{name: "none", changeset: "SELECT 1;", kind: liquibaserun.NoRollback},
		{name: "a blank block", changeset: "SELECT 1;\n/* liquibase rollback\n   \n*/", kind: liquibaserun.NoRollback},
		{name: "not required", changeset: "SELECT 1;\n--rollback not required", kind: liquibaserun.EmptyRollback},
		{name: "in upper case", changeset: "SELECT 1;\n--rollback NOT REQUIRED", kind: liquibaserun.EmptyRollback},
		{name: "among blanks", changeset: "SELECT 1;\n--rollback   not required  ", kind: liquibaserun.EmptyRollback},
		{name: "text after empty", changeset: "SELECT 1;\n--rollback emptyfoo;", kind: liquibaserun.EmptyRollback},
		{name: "empty in a block", changeset: "SELECT 1;\n/* liquibase rollback\nEMPTY\n*/", kind: liquibaserun.EmptyRollback},
		{
			name:      "empty and a second line",
			changeset: "SELECT 1;\n--rollback empty\n--rollback DROP TABLE t;",
			kind:      liquibaserun.SQLRollback,
		},
		{
			name:      "changesetId",
			changeset: "SELECT 1;\n--rollback changesetId:1 changesetAuthor:s",
			kind:      liquibaserun.ChangesetRollback,
		},
		{
			name:      "changesetId in SQL",
			changeset: "SELECT 1;\n/* liquibase rollback\nDELETE FROM a WHERE ChangeSetId = 1;\n*/",
			kind:      liquibaserun.ChangesetRollback,
		},
		{name: "sql", changeset: "SELECT 1;\n--rollback DROP TABLE t;", kind: liquibaserun.SQLRollback},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changeset := readChangeset(c, test.changeset)

			c.Assert(changeset.Rollback.Kind(), qt.Equals, test.kind)
		})
	}
}

// SQL is what Liquibase runs, where joining two lines leaves the SQL as it was
// written, and nothing where Liquibase runs nothing.
func TestRollbackSQL_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		changeset string
		sql       string
	}{
		{name: "none", changeset: "SELECT 1;"},
		{name: "not required", changeset: "SELECT 1;\n--rollback not required"},
		{
			name:      "rollback lines",
			changeset: "SELECT 1;\n--rollback DROP TABLE u;\n--rollback DROP TABLE t;",
			sql:       "DROP TABLE u;\nDROP TABLE t;",
		},
		{
			name:      "a statement per line of a block",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE u;\nDROP TABLE t;\n*/",
			sql:       "DROP TABLE u;DROP TABLE t;",
		},
		{
			name:      "a continued line that begins with a blank",
			changeset: "SELECT 1;\n/* liquibase rollback\nDELETE FROM t\n  WHERE id = 1;\n*/",
			sql:       "DELETE FROM t  WHERE id = 1;",
		},
		{
			name:      "a line that ends after a comma or a parenthesis",
			changeset: "SELECT 1;\n/* liquibase rollback\nINSERT INTO t (a,\nb) VALUES (\n1);\n*/",
			sql:       "INSERT INTO t (a,b) VALUES (1);",
		},
		{
			name:      "a quoted string that ends before the join",
			changeset: "SELECT 1;\n/* liquibase rollback\nDELETE FROM t WHERE k = 'x';\nDROP TABLE t;\n*/",
			sql:       "DELETE FROM t WHERE k = 'x';DROP TABLE t;",
		},
		{
			name:      "a join inside a block comment",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE u; /* c\nd */ DROP TABLE t;\n*/",
			sql:       "DROP TABLE u; /* cd */ DROP TABLE t;",
		},
		{
			name:      "a line comment that ends the block",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE t;\n-- done\n*/",
			sql:       "DROP TABLE t;-- done",
		},
		{
			name:      "a dollar-quoted body that ends before the join",
			changeset: "SELECT 1;\n/* liquibase rollback\nDO $body$ BEGIN NULL; END $body$;\nDROP TABLE t;\n*/",
			sql:       "DO $body$ BEGIN NULL; END $body$;DROP TABLE t;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changeset := readChangeset(c, test.changeset)

			sql, err := changeset.Rollback.SQL()

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.sql)
		})
	}
}

// SQL refuses a rollback Liquibase does not run as it is written, and one it
// takes from another changeset.
func TestRollbackSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		changeset string
		message   string
	}{
		{
			// Liquibase 5.0.4 ran `DELETE FROM aWHERE id = 1` and failed.
			name:      "two words run together",
			changeset: "SELECT 1;\n/* liquibase rollback\nDELETE FROM a\nWHERE id = 1;\n*/",
			message: `has a rollback Liquibase does not run as written: Liquibase joins the lines of a /\* liquibase ` +
				`rollback block, and the block and a --rollback line after it, with nothing between them, so ` +
				`"DELETE FROM a" and "WHERE id = 1;" run as "DELETE FROM aWHERE id = 1;" -- write the rollback as ` +
				`--rollback lines`,
		},
		{
			// Liquibase 5.0.4 ran `-- drop bDROP TABLE b;`, a comment.
			name:      "a line comment runs on over the next line",
			changeset: "SELECT 1;\n/* liquibase rollback\n-- drop b\nDROP TABLE b;\n*/",
			message:   `has a rollback .* so "-- drop b" and "DROP TABLE b;" run as "-- drop bDROP TABLE b;" -- .*`,
		},
		{
			name:      "a line comment runs on over an indented line",
			changeset: "SELECT 1;\n/* liquibase rollback\n-- drop b\n  DROP TABLE b;\n*/",
			message:   `has a rollback .* so "-- drop b" and "  DROP TABLE b;" run as "-- drop b  DROP TABLE b;" -- .*`,
		},
		{
			// A blank line joins nothing, and the comment still runs on.
			name:      "a blank line between a line comment and the next statement",
			changeset: "SELECT 1;\n/* liquibase rollback\n-- drop b\n   \nDROP TABLE b;\n*/",
			message:   `has a rollback .* so "-- drop b" and "DROP TABLE b;" run as "-- drop b   DROP TABLE b;" -- .*`,
		},
		{
			// Liquibase 5.0.4 inserted 'xy'.
			name:      "a quoted string loses its line break",
			changeset: "SELECT 1;\n/* liquibase rollback\nINSERT INTO s VALUES ('x\ny');\n*/",
			message:   `has a rollback .* so "INSERT INTO s VALUES \('x" and "y'\);" run as .*`,
		},
		{
			name:      "the block runs into the rollback line after it",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE\n   */\n--rollback t;",
			message:   `has a rollback .* so "DROP TABLE" and "t;" run as "DROP TABLEt;" -- .*`,
		},
		{
			name:      "go that begins a line",
			changeset: "SELECT 1;\n/* liquibase rollback\nDROP TABLE t;\ngo\n*/",
			message:   `has a rollback .* so "DROP TABLE t;" and "go" run as "DROP TABLE t;go" -- .*`,
		},
		{
			name:      "a slash that begins a line",
			changeset: "SELECT 1;\n/* liquibase rollback\nBEGIN NULL; END;\n/\n*/",
			message:   `has a rollback .* so "BEGIN NULL; END;" and "/" run as "BEGIN NULL; END;/" -- .*`,
		},
		{
			name:      "a string a backslash keeps open",
			changeset: "SELECT 1;\n/* liquibase rollback\nINSERT INTO t VALUES ('a\\');\nDROP TABLE t;\n*/",
			message:   `has a rollback .* so "INSERT INTO t VALUES \('a\\\\'\);" and "DROP TABLE t;" run as .*`,
		},
		{
			name:      "a dollar-quoted body loses its line break",
			changeset: "SELECT 1;\n/* liquibase rollback\nDO $$ BEGIN\n  NULL; END $$;\n*/",
			message:   `has a rollback .* so "DO \$\$ BEGIN" and "  NULL; END \$\$;" run as .*`,
		},
		{
			name:      "a changeset reference",
			changeset: "SELECT 1;\n--rollback changesetId:1 changesetAuthor:s",
			message: `has a rollback that names changesetId, which Liquibase reads as a reference to another ` +
				`changeset whose changes are the rollback; Ptah does not follow the reference, so write the ` +
				`rollback out`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changeset := readChangeset(c, test.changeset)

			sql, err := changeset.Rollback.SQL()

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
