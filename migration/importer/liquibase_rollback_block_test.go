package importer_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/importer"
)

// A formatted-SQL changeset's rollback is the down, whether it is written as
// --rollback lines or as a /* liquibase rollback block, and it is what
// Liquibase runs. Each changelog here was applied by Liquibase 5.0.4 on SQLite
// and rolled back with rollback-count (stokaro/ptah#3735): the down migration
// holds the SQL rollback-count ran, and none where it ran nothing.
func TestLiquibaseRollbackBlock_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    []importer.SourceMigration
	}{
		{
			name: "a block",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n/* liquibase rollback\n" +
				"DROP TABLE t;\n*/\n",
			want: []importer.SourceMigration{
				{Version: 1, Name: "s_1", UpSQL: "CREATE TABLE t (id int);", DownSQL: "DROP TABLE t;"},
			},
		},
		{
			name: "a block of several statements, in the middle of the changeset",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE a (id int);\n/* liquibase rollback\n" +
				"DROP TABLE b;\nDROP TABLE a;\n*/\nCREATE TABLE b (id int);\n",
			want: []importer.SourceMigration{{
				Version: 1, Name: "s_1", UpSQL: "CREATE TABLE a (id int);\nCREATE TABLE b (id int);",
				DownSQL: "DROP TABLE b;DROP TABLE a;",
			}},
		},
		{
			name: "a block beside rollback lines",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE a (id int);\nCREATE TABLE b (id int);\n" +
				"CREATE TABLE c (id int);\n--rollback DROP TABLE c;\n/* liquibase rollback\nDROP TABLE b;\n*/\n" +
				"--rollback DROP TABLE a;\n",
			want: []importer.SourceMigration{{
				Version: 1, Name: "s_1",
				UpSQL:   "CREATE TABLE a (id int);\nCREATE TABLE b (id int);\nCREATE TABLE c (id int);",
				DownSQL: "DROP TABLE c;\nDROP TABLE b;DROP TABLE a;",
			}},
		},
		{
			name: "each changeset keeps its own block",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE a (id int);\n/* liquibase rollback\n" +
				"DROP TABLE a;\n*/\n--changeset s:2\nCREATE TABLE b (id int);\n/* liquibase rollback\nDROP TABLE b;\n*/\n",
			want: []importer.SourceMigration{
				{Version: 1, Name: "s_1", UpSQL: "CREATE TABLE a (id int);", DownSQL: "DROP TABLE a;"},
				{Version: 2, Name: "s_2", UpSQL: "CREATE TABLE b (id int);", DownSQL: "DROP TABLE b;"},
			},
		},
		{
			name:    "a rollback that is not required",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n--rollback not required\n",
			want:    []importer.SourceMigration{{Version: 1, Name: "s_1", UpSQL: "CREATE TABLE t (id int);"}},
		},
		{
			name:    "an empty rollback",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n--rollback empty\n",
			want:    []importer.SourceMigration{{Version: 1, Name: "s_1", UpSQL: "CREATE TABLE t (id int);"}},
		},
		{
			// Liquibase 5.0.4 read `--rollback;` as a comment in the SQL, and
			// rollback-count dropped only a.
			name: "rollback with no blank after it",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE a (id int);\n--rollback;DROP TABLE b;\n" +
				"--rollback DROP TABLE a;\n",
			want: []importer.SourceMigration{{
				Version: 1, Name: "s_1", UpSQL: "CREATE TABLE a (id int);\n--rollback;DROP TABLE b;",
				DownSQL: "DROP TABLE a;",
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parser, err := importer.ParserByName("liquibase")
			c.Assert(err, qt.IsNil)

			parsed, err := parser.Parse(fstest.MapFS{"changelog.sql": {Data: []byte(test.content)}})

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Migrations, qt.DeepEquals, test.want)
		})
	}
}

// A rollback the down cannot carry as Liquibase runs it is refused by name, and
// so is a changelog Liquibase refuses to read.
func TestLiquibaseRollbackBlock_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		message string
	}{
		{
			// Liquibase 5.0.4 stopped with "Liquibase rollback comment is not
			// closed."
			name:    "a block no line closes",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n/* liquibase rollback\nDROP TABLE t;\n",
			message: `liquibase changeset s:1 in "changelog.sql" opens a /\* liquibase rollback block that no line ` +
				`closes, and Liquibase refuses the changelog -- end the block with a line that ends in \*/`,
		},
		{
			// Liquibase 5.0.4 ran `DELETE FROM aWHERE id = 1` and failed.
			name: "two lines of a block run together",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE a (id int);\n/* liquibase rollback\n" +
				"DELETE FROM a\nWHERE id = 1;\n*/\n",
			message: `liquibase changeset s:1 in "changelog.sql" has a rollback Liquibase does not run as written: ` +
				`Liquibase joins the lines of a /\* liquibase rollback block, and the block and a --rollback line ` +
				`after it, with nothing between them, so "DELETE FROM a" and "WHERE id = 1;" run as ` +
				`"DELETE FROM aWHERE id = 1;" -- write the rollback as --rollback lines`,
		},
		{
			name: "a rollback naming another changeset",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE a (id int);\n--changeset s:2\n" +
				"CREATE TABLE b (id int);\n--rollback changesetId:1 changesetAuthor:s\n",
			message: `liquibase changeset s:2 in "changelog.sql" has a rollback that names changesetId, which ` +
				`Liquibase reads as a reference to another changeset whose changes are the rollback; Ptah does ` +
				`not follow the reference, so write the rollback out`,
		},
		{
			name:    "rollback with one dash",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n-rollback DROP TABLE t;\n",
			message: `liquibase changelog "changelog.sql" line 4: "-rollback DROP TABLE t;" is not a directive ` +
				`Liquibase reads, and Liquibase refuses it -- write --rollback <SQL>`,
		},
		{
			name: "a property reference in a block",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n/* liquibase rollback\n" +
				"DROP TABLE ${tbl};\n*/\n",
			message: `liquibase changeset s:1 in "changelog.sql" uses the property reference \$\{tbl\}; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parser, err := importer.ParserByName("liquibase")
			c.Assert(err, qt.IsNil)

			parsed, err := parser.Parse(fstest.MapFS{"changelog.sql": {Data: []byte(test.content)}})

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(parsed, qt.IsNil)
		})
	}
}
