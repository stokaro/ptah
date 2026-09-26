package importer_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/importer"
)

// A changeset Liquibase runs on some databases only, or runs again after its
// first run, has no equivalent in a migration directory. Imported as an ordinary
// migration it runs everywhere, once, and the imported history is not the one
// Liquibase applied (stokaro/ptah#3631). Each row names the attribute in the
// message, and each serialization has a row, because every reader hands its
// attributes to the same recognizer and a reader that stopped doing so would
// drop them again.
func TestLiquibaseRunConditions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
		message string
	}{
		{
			name:    "xml changeset dbms",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" dbms="mysql"><sql>CREATE TABLE t (id int);</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" is conditional on dbms; a migration directory has no ` +
				`equivalent, so importing it would turn a conditional history into an unconditional one -- ` +
				`split the changelog or import it by hand`,
		},
		{
			name: "yaml changeset dbms",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, dbms: mysql, changes: [{sql: {sql: \"CREATE TABLE t (id int);\"}}]}\n",
			message: `liquibase changeset s_1 in "changelog.yaml" is conditional on dbms;`,
		},
		{
			name: "json changeset dbms",
			file: "changelog.json",
			content: `{"databaseChangeLog":[{"changeSet":{"id":"1","author":"s","dbms":"!h2",` +
				`"changes":[{"sql":{"sql":"CREATE TABLE t (id int);"}}]}}]}`,
			message: `liquibase changeset s_1 in "changelog.json" is conditional on dbms;`,
		},
		{
			name:    "formatted sql changeset dbms",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 dbms:mysql\nCREATE TABLE t (id int);\n",
			message: `liquibase changeset s:1 in "changelog.sql" is conditional on dbms;`,
		},
		{
			// Liquibase's own dbms pattern accepts blanks after the colon.
			name:    "formatted sql changeset dbms after a blank",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 dbms: mysql\nCREATE TABLE t (id int);\n",
			message: `liquibase changeset s:1 in "changelog.sql" is conditional on dbms;`,
		},
		{
			name:    "formatted sql context",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 context:\"prod or staging\"\nCREATE TABLE t (id int);\n",
			message: `liquibase changeset s:1 in "changelog.sql" is conditional on context;`,
		},
		{
			name:    "formatted sql labels",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 labels:nightly\nCREATE TABLE t (id int);\n",
			message: `liquibase changeset s:1 in "changelog.sql" is conditional on labels;`,
		},
		{
			name: "formatted sql preconditions",
			file: "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1\n--preconditions onFail:MARK_RAN\n" +
				"--precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM t\nCREATE TABLE t (id int);\n",
			message: `liquibase changeset s:1 in "changelog.sql" is conditional on preconditions;`,
		},
		{
			name:    "xml contextFilter",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" contextFilter="prod"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `is conditional on contextFilter;`,
		},
		{
			name:    "xml preconditions spelled in lower case",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><preconditions><dbms type="mysql"/></preconditions><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `is conditional on preconditions;`,
		},
		{
			name: "yaml preconditions spelled in lower case",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, preconditions: [{dbms: {type: mysql}}], changes: [{sql: {sql: \"SELECT 1;\"}}]}\n",
			message: `is conditional on preconditions;`,
		},
		{
			// Liquibase reads a changeset attribute written as a nested element.
			name:    "xml dbms written as an element",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><dbms>mysql</dbms><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `is conditional on dbms;`,
		},
		{
			// Liquibase matches attribute names in any case.
			name:    "xml dbms in upper case",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" DBMS="mysql"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `is conditional on DBMS;`,
		},
		{
			name:    "xml sql dbms",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sql dbms="mysql">CREATE TABLE t (id int);</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" is conditional on <sql> dbms;`,
		},
		{
			name:    "xml sql dbms in upper case",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sql DBMS="mysql">CREATE TABLE t (id int);</sql></changeSet></databaseChangeLog>`,
			message: `is conditional on <sql> DBMS;`,
		},
		{
			name: "yaml sql dbms",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, changes: [{sql: {dbms: mysql, sql: \"CREATE TABLE t (id int);\"}}]}\n",
			message: `liquibase changeset s_1 in "changelog.yaml" is conditional on sql dbms;`,
		},
		{
			name:    "xml sqlFile dbms",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sqlFile dbms="mysql" path="t.sql"/></changeSet></databaseChangeLog>`,
			message: `is conditional on <sqlFile> dbms;`,
		},
		{
			name: "xml insert dbms",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><insert dbms="mysql" tableName="t">` +
				`<column name="id" valueNumeric="1"/></insert></changeSet></databaseChangeLog>`,
			message: `is conditional on <insert> dbms;`,
		},
		{
			name: "xml createProcedure dbms",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><createProcedure dbms="oracle">` +
				`CREATE PROCEDURE p AS BEGIN NULL; END;</createProcedure></changeSet></databaseChangeLog>`,
			message: `is conditional on <createProcedure> dbms;`,
		},
		{
			// Liquibase skips a rollback change on another database as well.
			name: "xml rollback sql dbms",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sql>CREATE TABLE t (id int);</sql>` +
				`<rollback><sql dbms="mysql">DROP TABLE t;</sql></rollback></changeSet></databaseChangeLog>`,
			message: `is conditional on <sql> dbms;`,
		},
		{
			name:    "xml runAlways",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" runAlways="true"><sql>INSERT INTO t VALUES (1);</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" sets runAlways, so Liquibase can run it again on a ` +
				`later update; a Ptah migration runs once, so importing it would turn a repeated changeset into a ` +
				`one-time one -- import it by hand`,
		},
		{
			name:    "xml alwaysRun",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" alwaysRun="true"><sql>INSERT INTO t VALUES (1);</sql></changeSet></databaseChangeLog>`,
			message: `sets alwaysRun, so Liquibase can run it again`,
		},
		{
			name:    "xml runOnChange",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" runOnChange="true"><sql>CREATE VIEW v AS SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `sets runOnChange, so Liquibase can run it again`,
		},
		{
			name: "yaml runAlways",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, runAlways: true, changes: [{sql: {sql: \"SELECT 1;\"}}]}\n",
			message: `liquibase changeset s_1 in "changelog.yaml" sets runAlways,`,
		},
		{
			name: "json runOnChange",
			file: "changelog.json",
			content: `{"databaseChangeLog":[{"changeSet":{"id":"1","author":"s","runOnChange":true,` +
				`"changes":[{"sql":{"sql":"SELECT 1;"}}]}}]}`,
			message: `liquibase changeset s_1 in "changelog.json" sets runOnChange,`,
		},
		{
			name:    "formatted sql runAlways",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 runAlways:true\nINSERT INTO t VALUES (1);\n",
			message: `liquibase changeset s:1 in "changelog.sql" sets runAlways,`,
		},
		{
			name:    "formatted sql runOnChange",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 runOnChange:true\nCREATE VIEW v AS SELECT 1;\n",
			message: `liquibase changeset s:1 in "changelog.sql" sets runOnChange,`,
		},
		{
			name:    "a repeat that is not a boolean",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" runAlways="yes"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml": runAlways "yes" is not true or false`,
		},
		{
			// The selector decides the remedy, and the repeat is named in the
			// same message so removing one does not reveal the other.
			name:    "a selector beside a repeat",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" dbms="mysql" runOnChange="true"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `is conditional on dbms; a migration directory has no equivalent, so importing it would turn a ` +
				`conditional history into an unconditional one -- split the changelog or import it by hand; it also ` +
				`sets runOnChange, which a migration directory cannot express either`,
		},
		{
			// modifySql carries its own dbms and rewrites the SQL the changes
			// produce. The XML walk reads it as a change Ptah does not convert,
			// and so does this one.
			name: "yaml modifySql",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, changes: [{sql: {sql: \"CREATE TABLE t (id int)\"}}], " +
				"modifySql: {dbms: mysql, append: {value: \" ENGINE=InnoDB\"}}}\n",
			message: `uses modifySql, which is not SQL text and which Ptah does not convert`,
		},
		{
			name:    "sql endDelimiter",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sql endDelimiter="/">SELECT 1</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml": <sql> sets endDelimiter, which Ptah does not convert`,
		},
		{
			name:    "sql element nothing reads",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sql><where>x</where>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `<sql> contains <where>, which Ptah does not convert`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parser, err := importer.ParserByName("liquibase")
			c.Assert(err, qt.IsNil)

			parsed, err := parser.Parse(fstest.MapFS{test.file: {Data: []byte(test.content)}})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.message)
			c.Assert(parsed, qt.IsNil)
		})
	}
}

// A run attribute that selects nothing is Liquibase's default spelled out, and
// the changeset imports as it would without it. Refusing these would refuse a
// changelog for writing down what Ptah already does.
func TestLiquibaseRunConditions_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
		up      string
	}{
		{
			name:    "xml runAlways and runOnChange false",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" runAlways="false" runOnChange="false"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			up:      "SELECT 1;",
		},
		{
			name:    "xml alwaysRun false",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" alwaysRun="FALSE"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			up:      "SELECT 1;",
		},
		{
			name: "yaml runAlways and runOnChange false",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, runAlways: false, runOnChange: false, changes: [{sql: {sql: \"SELECT 1;\"}}]}\n",
			up: "SELECT 1;",
		},
		{
			name: "json runAlways and runOnChange false",
			file: "changelog.json",
			content: `{"databaseChangeLog":[{"changeSet":{"id":"1","author":"s","runAlways":false,"runOnChange":false,` +
				`"changes":[{"sql":{"sql":"SELECT 1;"}}]}}]}`,
			up: "SELECT 1;",
		},
		{
			name:    "formatted sql runAlways and runOnChange false",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 runAlways:false runOnChange:false\nSELECT 1;\n",
			up:      "SELECT 1;",
		},
		{
			// A key written with no value is Liquibase's default, as an empty
			// attribute is.
			name: "yaml dbms and runAlways with no value",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet:\n" +
				"      id: \"1\"\n" +
				"      author: s\n" +
				"      dbms:\n" +
				"      runAlways:\n" +
				"      changes: [{sql: {sql: \"SELECT 1;\"}}]\n",
			up: "SELECT 1;",
		},
		{
			// An empty dbms selects every database, which is what the imported
			// migration does.
			name:    "xml empty dbms on the changeset and the change",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" dbms=""><sql dbms=" ">SELECT 1;</sql></changeSet></databaseChangeLog>`,
			up:      "SELECT 1;",
		},
		{
			// Ptah writes this attribute into a Liquibase directory itself, and
			// it decides nothing about whether the changeset runs.
			name:    "formatted sql attribute that is not a run condition",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset atlas:20240102030405-1 runInTransaction:false\nSELECT 1;\n",
			up:      "SELECT 1;",
		},
		{
			// The author:id is not an attribute, whatever the author is called.
			// The second attribute makes the reader look past the first field.
			name:    "formatted sql author named like an attribute",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset dbms:1 runInTransaction:false\nSELECT 1;\n",
			up:      "SELECT 1;",
		},
		{
			name:    "formatted sql comment that mentions a precondition",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1\n-- precondition checked by the deploy job\nSELECT 1;\n",
			up:      "-- precondition checked by the deploy job\nSELECT 1;",
		},
		{
			name: "sql attributes and comment it accepts",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sql splitStatements="false" stripComments="true">` +
				`<comment>one statement</comment>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			up: "SELECT 1;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parser, err := importer.ParserByName("liquibase")
			c.Assert(err, qt.IsNil)

			parsed, err := parser.Parse(fstest.MapFS{test.file: {Data: []byte(test.content)}})

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Migrations, qt.HasLen, 1)
			c.Assert(parsed.Migrations[0].UpSQL, qt.Equals, test.up)
		})
	}
}

// A target dialect does not make dbms convert. Honoring it would mean keeping
// the changesets whose dbms names the target, and the name Liquibase gives some
// targets depends on how it reached the server: YugabyteDB is `yugabytedb` with
// the Liquibase extension and `postgresql` without it. The first row is the
// case honoring would have kept, and it is still refused.
func TestLiquibaseRunConditions_WithDialect_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		changes string
		message string
	}{
		{
			name:    "dbms naming the target",
			dialect: "mysql",
			changes: `<sql dbms="mysql">CREATE TABLE t (id int);</sql>`,
			message: `is conditional on <sql> dbms;`,
		},
		{
			name:    "dbms naming another database",
			dialect: "postgres",
			changes: `<sql dbms="mysql">CREATE TABLE t (id int);</sql>`,
			message: `is conditional on <sql> dbms;`,
		},
		{
			// createTable is not a change Liquibase selects by dbms, so the
			// attribute is one its converter does not read.
			name:    "dbms on a change type that does not take it",
			dialect: "postgres",
			changes: `<createTable tableName="t" dbms="mysql"><column name="id" type="int"/></createTable>`,
			message: `<createTable> sets dbms, which Ptah does not convert`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := parseLiquibaseFor(c, test.dialect, liquibaseXMLOneChangeSet(test.changes))

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.message)
			c.Assert(parsed, qt.IsNil)
		})
	}
}

// The changeset-level refusal also holds with a dialect: the dialect renders
// typed changes and selects nothing.
func TestLiquibaseRunConditions_ChangesetDbmsWithDialect_FailurePath(t *testing.T) {
	c := qt.New(t)
	files := fstest.MapFS{"changelog.xml": {Data: []byte(
		`<databaseChangeLog><changeSet id="1" author="s" dbms="mysql">` +
			`<createTable tableName="t"><column name="id" type="int"/></createTable></changeSet></databaseChangeLog>`,
	)}}

	parsed, err := parseLiquibaseFor(c, "mysql", files)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `liquibase changeset s_1 in "changelog.xml" is conditional on dbms;`)
	c.Assert(parsed, qt.IsNil)
}
