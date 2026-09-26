package importer_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/importer"
)

// The lines an --ignoreLines directive skips never run under Liquibase, and
// never reach an imported migration either (stokaro/ptah#3727). Each changelog
// here was applied by Liquibase 5.0.4 on SQLite, and the migrations below hold
// exactly the tables it created.
func TestLiquibaseIgnoreLines_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    []importer.SourceMigration
	}{
		{
			name: "a block and a count inside changesets",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t1 (id int);\n--ignoreLines:start\n" +
				"CREATE TABLE ignored_block (id int);\n--ignoreLines:end\nCREATE TABLE t1b (id int);\n--changeset s:2\n" +
				"--ignoreLines:1\nCREATE TABLE ignored_count (id int);\nCREATE TABLE t2 (id int);\n",
			want: []importer.SourceMigration{
				{Version: 1, Name: "s_1", UpSQL: "CREATE TABLE t1 (id int);\nCREATE TABLE t1b (id int);"},
				{Version: 2, Name: "s_2", UpSQL: "CREATE TABLE t2 (id int);"},
			},
		},
		{
			name: "a block holding a whole changeset, before the first one",
			content: "--liquibase formatted sql\n--ignoreLines:start\n--changeset s:9\nCREATE TABLE hidden_changeset (id int);\n" +
				"--ignoreLines:end\n--changeset s:1\nCREATE TABLE b1 (id int);\n",
			want: []importer.SourceMigration{{Version: 1, Name: "s_1", UpSQL: "CREATE TABLE b1 (id int);"}},
		},
		{
			name: "a block with no end, over the next changeset",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE c1 (id int);\n-- ignoreLines:2\n" +
				"CREATE TABLE skipped_a (id int);\nCREATE TABLE skipped_b (id int);\n--ignoreLines:start\n" +
				"CREATE TABLE never_ended (id int);\n--changeset s:2\nCREATE TABLE never_ended_2 (id int);\n",
			want: []importer.SourceMigration{{Version: 1, Name: "s_1", UpSQL: "CREATE TABLE c1 (id int);"}},
		},
		{
			name: "a rollback line inside a block",
			content: "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n--ignoreLines:1\n" +
				"--rollback DROP TABLE ignored;\n--rollback DROP TABLE t;\n",
			want: []importer.SourceMigration{{Version: 1, Name: "s_1", UpSQL: "CREATE TABLE t (id int);", DownSQL: "DROP TABLE t;"}},
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

// A directive Liquibase refuses is refused: Liquibase 5.0.4 stops with
// NumberFormatException on --ignoreLines:START, so the changelog never ran.
func TestLiquibaseIgnoreLines_FailurePath(t *testing.T) {
	c := qt.New(t)
	parser, err := importer.ParserByName("liquibase")
	c.Assert(err, qt.IsNil)
	content := "--liquibase formatted sql\n--changeset s:1\nCREATE TABLE d1 (id int);\n--ignoreLines:START\n" +
		"CREATE TABLE d2 (id int);\n--ignoreLines:END\n"

	parsed, err := parser.Parse(fstest.MapFS{"changelog.sql": {Data: []byte(content)}})

	c.Assert(err, qt.ErrorMatches, `liquibase changelog "changelog.sql" line 4: "--ignoreLines:START" names neither `+
		`start nor a number of lines, so Liquibase refuses it`)
	c.Assert(parsed, qt.IsNil)
}

// A property reference is refused wherever Liquibase fills one in, in every
// serialization: the value that ran depends on the environment Liquibase ran in,
// which the changelog does not record (stokaro/ptah#3727).
func TestLiquibasePropertyReference_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		files   fstest.MapFS
		message string
	}{
		{
			name: "formatted sql",
			files: fstest.MapFS{"changelog.sql": {Data: []byte(
				"--liquibase formatted sql\n--property name:tbl value:users\n--changeset s:1\nCREATE TABLE ${tbl} (id int);\n",
			)}},
			message: `liquibase changeset s:1 in "changelog.sql" uses the property reference \$\{tbl\}; Liquibase fills it ` +
				`in when it runs, and an environment variable, a Java system property or a command-line parameter of ` +
				`that name wins over any property the changelog defines, so the changelog does not record the value ` +
				`that ran -- write the value in, or import the changeset by hand`,
		},
		{
			name: "formatted sql rollback",
			files: fstest.MapFS{"changelog.sql": {Data: []byte(
				"--liquibase formatted sql\n--changeset s:1\nCREATE TABLE t (id int);\n--rollback DROP TABLE ${schema}.t;\n",
			)}},
			message: `liquibase changeset s:1 in "changelog.sql" uses the property reference \$\{schema\}; .*`,
		},
		{
			name: "xml sql text",
			files: fstest.MapFS{"changelog.xml": {Data: []byte(
				`<databaseChangeLog><property name="tbl" value="users"/><changeSet id="1" author="s">` +
					`<sql>CREATE TABLE ${tbl} (id int);</sql></changeSet></databaseChangeLog>`,
			)}},
			message: `liquibase changeset s_1 in "changelog.xml" uses the property reference \$\{tbl\}; .*`,
		},
		{
			name: "xml rollback text",
			files: fstest.MapFS{"changelog.xml": {Data: []byte(
				`<databaseChangeLog><changeSet id="1" author="s"><sql>CREATE TABLE t (id int);</sql>` +
					`<rollback>DROP TABLE ${tbl};</rollback></changeSet></databaseChangeLog>`,
			)}},
			message: `liquibase changeset s_1 in "changelog.xml" uses the property reference \$\{tbl\}; .*`,
		},
		{
			name: "yaml sql attribute",
			files: fstest.MapFS{"changelog.yaml": {Data: []byte(
				"databaseChangeLog:\n  - changeSet: {id: \"1\", author: s, changes: [{sql: {sql: \"CREATE TABLE ${tbl} (id int);\"}}]}\n",
			)}},
			message: `liquibase changeset s_1 in "changelog.yaml" uses the property reference \$\{tbl\}; .*`,
		},
		{
			name: "json bare sql",
			files: fstest.MapFS{"changelog.json": {Data: []byte(
				`{"databaseChangeLog":[{"changeSet":{"id":"1","author":"s","changes":[{"sql":"INSERT INTO t VALUES ('${v}');"}]}}]}`,
			)}},
			message: `liquibase changeset s_1 in "changelog.json" uses the property reference \$\{v\}; .*`,
		},
		{
			name: "sqlFile path",
			files: fstest.MapFS{"changelog.xml": {Data: []byte(
				`<databaseChangeLog><changeSet id="1" author="s"><sqlFile path="${dir}/t.sql"/></changeSet></databaseChangeLog>`,
			)}},
			message: `liquibase changeset s_1 in "changelog.xml" uses the property reference \$\{dir\}; .*`,
		},
		{
			name: "sqlFile content",
			files: fstest.MapFS{
				"changelog.xml": {Data: []byte(`<databaseChangeLog><changeSet id="1" author="s"><sqlFile path="t.sql"/></changeSet></databaseChangeLog>`)},
				"t.sql":         {Data: []byte("CREATE TABLE ${tbl} (id int);\n")},
			},
			message: `liquibase changeset s_1 in "changelog.xml": <sqlFile> names "t.sql", which uses the property reference \$\{tbl\}; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parser, err := importer.ParserByName("liquibase")
			c.Assert(err, qt.IsNil)

			parsed, err := parser.Parse(test.files)

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(parsed, qt.IsNil)
		})
	}
}

// A reference in a value Liquibase reads as a typed change's is refused too,
// including one no converter reads by name, such as a table's remarks.
func TestLiquibasePropertyReference_Typed_FailurePath(t *testing.T) {
	c := qt.New(t)

	parsed, err := parseLiquibaseFor(c, "postgres", liquibaseXMLOneChangeSet(
		`<createTable tableName="t" remarks="owned by ${team}"><column name="id" type="int"/></createTable>`))

	c.Assert(err, qt.ErrorMatches, `liquibase changeset simon_1 in "changelog.xml" uses the property reference \$\{team\}; .*`)
	c.Assert(parsed, qt.IsNil)
}

// What Liquibase does not fill in, or never runs, imports: a property nothing
// references, a `${` with no closing brace, a change's comment, and a change
// the named database does not run.
func TestLiquibasePropertyReference_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		files fstest.MapFS
		dbms  string
		up    string
	}{
		{
			name: "formatted sql property nothing references",
			files: fstest.MapFS{"changelog.sql": {Data: []byte(
				"--liquibase formatted sql\n--property name:tbl value:users\n--changeset s:1\nSELECT 1;\n",
			)}},
			dbms: "sqlite",
			up:   "SELECT 1;",
		},
		{
			name: "xml property nothing references, and a comment with one",
			files: fstest.MapFS{"changelog.xml": {Data: []byte(
				`<databaseChangeLog><property name="tbl" value="users"/><changeSet id="1" author="s">` +
					`<sql><comment>creates ${tbl}</comment>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			)}},
			dbms: "sqlite",
			up:   "SELECT 1;",
		},
		{
			name: "no closing brace",
			files: fstest.MapFS{"changelog.sql": {Data: []byte(
				"--liquibase formatted sql\n--changeset s:1\nSELECT '${not closed';\n",
			)}},
			dbms: "sqlite",
			up:   "SELECT '${not closed';",
		},
		{
			name: "a change the named database does not run",
			files: fstest.MapFS{"changelog.xml": {Data: []byte(
				`<databaseChangeLog><changeSet id="1" author="s"><sql>SELECT 1;</sql>` +
					`<sql dbms="mysql">SELECT ${mysql_only};</sql></changeSet></databaseChangeLog>`,
			)}},
			dbms: "sqlite",
			up:   "SELECT 1;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := parseLiquibaseOn(c, test.dbms, test.files)

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Migrations, qt.HasLen, 1)
			c.Assert(parsed.Migrations[0].UpSQL, qt.Equals, test.up)
		})
	}
}
