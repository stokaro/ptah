package importer_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/importer"
)

// A changeset attribute with no form in a Ptah migration, and one Ptah does not
// read at all, are refused by name in every serialization (stokaro/ptah#3714).
// Read by nothing, each would be dropped: failOnError="false" imported as a
// migration whose failure stops the apply, runOrder as one in file order.
func TestLiquibaseChangesetAttributes_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
		message string
	}{
		{
			name:    "xml failOnError false",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" failOnError="false"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" sets failOnError=false, which Ptah cannot carry because ` +
				`Liquibase records the changeset as run when it fails, and a failed Ptah migration stops the apply ` +
				`-- import it by hand`,
		},
		{
			name: "yaml failOnError false",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, failOnError: false, changes: [{sql: {sql: \"SELECT 1;\"}}]}\n",
			message: `liquibase changeset s_1 in "changelog.yaml" sets failOnError=false, which Ptah cannot carry .*`,
		},
		{
			name:    "xml runOrder",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" runOrder="last"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" sets runOrder=last, which Ptah cannot carry because ` +
				`Liquibase moves the changeset to the start or the end of the update -- import it by hand`,
		},
		{
			name:    "xml runWith",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" runWith="psql"><sql>\dt</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" sets runWith=psql, which Ptah cannot carry .*`,
		},
		{
			name:    "json runWithSpoolFile",
			file:    "changelog.json",
			content: `{"databaseChangeLog":[{"changeSet":{"id":"1","author":"s","runWithSpoolFile":"out.spool","changes":[{"sql":{"sql":"SELECT 1;"}}]}}]}`,
			message: `liquibase changeset s_1 in "changelog.json" sets runWithSpoolFile=out.spool, which Ptah cannot carry .*`,
		},
		{
			name:    "xml objectQuotingStrategy",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" objectQuotingStrategy="QUOTE_ALL_OBJECTS"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" sets objectQuotingStrategy=QUOTE_ALL_OBJECTS, which Ptah cannot carry .*`,
		},
		{
			name:    "formatted sql endDelimiter",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 endDelimiter:GO\nSELECT 1\nGO\n",
			message: `liquibase changeset s:1 in "changelog.sql" sets endDelimiter=GO, which Ptah cannot carry because ` +
				`Ptah does not know the delimiter, so it would stay in the SQL -- import it by hand`,
		},
		{
			name:    "xml attribute Ptah does not read",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" tag="v1"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" sets tag, which Ptah does not read -- import it by hand`,
		},
		{
			name: "yaml key Ptah does not read",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, tag: v1, changes: [{sql: {sql: \"SELECT 1;\"}}]}\n",
			message: `liquibase changeset s_1 in "changelog.yaml" sets tag, which Ptah does not read -- import it by hand`,
		},
		{
			name:    "formatted sql attribute Ptah does not read",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 tag:v1\nSELECT 1;\n",
			message: `liquibase changeset s:1 in "changelog.sql" sets tag, which Ptah does not read -- import it by hand`,
		},
		{
			// An attribute of the changelog applies to every changeset in it.
			name: "xml changelog context",
			file: "changelog.xml",
			content: `<databaseChangeLog context="prod"><changeSet id="1" author="s"><sql>SELECT 1;</sql></changeSet>` +
				`</databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" is conditional on context; .*`,
		},
		{
			name: "xml changelog objectQuotingStrategy",
			file: "changelog.xml",
			content: `<databaseChangeLog objectQuotingStrategy="QUOTE_ONLY_RESERVED_WORDS"><changeSet id="1" author="s">` +
				`<sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" sets objectQuotingStrategy=QUOTE_ONLY_RESERVED_WORDS, .*`,
		},
		{
			name:    "xml ignore that is not a boolean",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" ignore="maybe"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml": ignore "maybe" is not true or false`,
		},
		{
			name:    "every changeset ignored",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" ignore="true"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase source holds no changeset Liquibase runs here: every one was left out ` +
				`\(changelog.xml s:1: ignore="true": Liquibase never runs this changeset\)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parser, err := importer.ParserByName("liquibase")
			c.Assert(err, qt.IsNil)

			parsed, err := parser.Parse(fstest.MapFS{test.file: {Data: []byte(test.content)}})

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(parsed, qt.IsNil)
		})
	}
}

// runInTransaction="false" converts: the migration it becomes runs outside a
// transaction in both directions, because Liquibase runs the rollback in the
// changeset's own mode.
func TestLiquibaseChangesetAttributes_NoTransaction_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
	}{
		{
			name:    "xml",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" runInTransaction="false"><sql>VACUUM;</sql></changeSet></databaseChangeLog>`,
		},
		{
			name: "yaml",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, runInTransaction: false, changes: [{sql: {sql: \"VACUUM;\"}}]}\n",
		},
		{
			name:    "formatted sql",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 runInTransaction:false\nVACUUM;\n",
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
			c.Assert(parsed.Migrations[0].UpSQL, qt.Equals, "VACUUM;")
			c.Assert(parsed.Migrations[0].UpNoTransaction, qt.IsTrue)
			c.Assert(parsed.Migrations[0].DownNoTransaction, qt.IsTrue)
		})
	}
}

// ignore="true" means Liquibase never runs the changeset and never records it,
// so the import leaves it out and names it in [importer.ParseResult.Skipped].
// What the ignored changeset holds does not matter: a typed change there needs
// no dialect, since nothing converts it.
func TestLiquibaseChangesetAttributes_Ignore_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
		skipped importer.SkippedChangeset
	}{
		{
			name: "xml",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sql>SELECT 1;</sql></changeSet>` +
				`<changeSet id="2" author="s" ignore="true"><createTable tableName="t"/></changeSet></databaseChangeLog>`,
			skipped: importer.SkippedChangeset{
				Path: "changelog.xml", Changeset: "s:2", Reason: `ignore="true": Liquibase never runs this changeset`,
			},
		},
		{
			name: "yaml",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, changes: [{sql: {sql: \"SELECT 1;\"}}]}\n" +
				"  - changeSet: {id: \"2\", author: s, ignore: true, changes: [{sql: {sql: \"SELECT 2;\"}}]}\n",
			skipped: importer.SkippedChangeset{
				Path: "changelog.yaml", Changeset: "s:2", Reason: `ignore="true": Liquibase never runs this changeset`,
			},
		},
		{
			name:    "formatted sql",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1\nSELECT 1;\n--changeset s:2 ignore:true\nSELECT 2;\n",
			skipped: importer.SkippedChangeset{
				Path: "changelog.sql", Changeset: "s:2", Reason: `ignore="true": Liquibase never runs this changeset`,
			},
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
			c.Assert(parsed.Migrations[0].UpSQL, qt.Equals, "SELECT 1;")
			c.Assert(parsed.Skipped, qt.DeepEquals, []importer.SkippedChangeset{test.skipped})
		})
	}
}

// An attribute that decides nothing a migration has to carry -- an identifier,
// a note, a checksum override, a value that spells out the default, or an XML
// declaration on the changelog -- imports as if it were absent.
func TestLiquibaseChangesetAttributes_NoEffect_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
	}{
		{
			name: "xml changelog declarations and logicalFilePath",
			file: "changelog.xml",
			content: `<databaseChangeLog xmlns="http://www.liquibase.org/xml/ns/dbchangelog" ` +
				`xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ` +
				`xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog dbchangelog-latest.xsd" ` +
				`logicalFilePath="db/changelog.xml" objectQuotingStrategy="LEGACY">` +
				`<changeSet id="1" author="s"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
		},
		{
			name: "xml changeset attributes and elements",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" created="2026-01-01" logicalFilePath="x.xml" ` +
				`onValidationFail="MARK_RAN" runInTransaction="true" failOnError="true" ignore="false" runWith="jdbc">` +
				`<validCheckSum>ANY</validCheckSum><comment>one</comment><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
		},
		{
			name: "yaml changeset keys",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, comment: one, validCheckSum: ANY, created: x, " +
				"runInTransaction: true, changes: [{sql: {sql: \"SELECT 1;\"}}]}\n",
		},
		{
			name:    "formatted sql attributes",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 splitStatements:false stripComments:true endDelimiter:; logicalFilePath:x.sql\nSELECT 1;\n",
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
			c.Assert(parsed.Migrations[0].UpSQL, qt.Equals, "SELECT 1;")
			c.Assert(parsed.Migrations[0].UpNoTransaction, qt.IsFalse)
			c.Assert(parsed.Skipped, qt.HasLen, 0)
		})
	}
}

// Import hands the skipped changesets to its caller, which owes the user the
// list.
func TestImport_ReportsSkippedChangesets_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := fstest.MapFS{"changelog.sql": {Data: []byte(
		"--liquibase formatted sql\n--changeset s:1\nSELECT 1;\n--changeset s:2 ignore:true\nSELECT 2;\n",
	)}}
	parser, err := importer.ParserByName("liquibase")
	c.Assert(err, qt.IsNil)

	result, err := importer.Import(source, parser, c.TempDir(), importer.Options{DryRun: true})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 2)
	c.Assert(result.Skipped, qt.DeepEquals, []importer.SkippedChangeset{{
		Path: "changelog.sql", Changeset: "s:2", Reason: `ignore="true": Liquibase never runs this changeset`,
	}})
}
