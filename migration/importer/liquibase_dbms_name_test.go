package importer_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/importer"
)

// parseLiquibaseOn parses files with the Liquibase parser set to keep what
// Liquibase runs on the database it calls dbms.
func parseLiquibaseOn(c *qt.C, dbms string, files fstest.MapFS) (*importer.ParseResult, error) {
	c.Helper()
	parser, err := importer.ParserByName("liquibase")
	c.Assert(err, qt.IsNil)
	selecting, err := importer.WithLiquibaseDBMS(parser, dbms)
	c.Assert(err, qt.IsNil)
	return selecting.Parse(files)
}

// With the database named, a changeset's dbms decides whether it imports, by
// Liquibase's rule, instead of refusing it (stokaro/ptah#3715). A changeset left
// out is named in Skipped with the dbms that left it out.
func TestWithLiquibaseDBMS_Changeset_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dbms     string
		imported bool
	}{
		{name: "the named database", dbms: "postgresql", imported: true},
		{name: "another database", dbms: "mysql", imported: false},
		{name: "a list naming it, in any case", dbms: "MySQL, PostgreSQL", imported: true},
		{name: "another database excluded", dbms: "!mysql", imported: true},
		{name: "it excluded", dbms: "!postgresql", imported: false},
		{name: "all", dbms: "all", imported: true},
		{name: "none", dbms: "none", imported: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			files := fstest.MapFS{"changelog.xml": {Data: []byte(
				`<databaseChangeLog><changeSet id="1" author="s"><sql>SELECT 1;</sql></changeSet>` +
					`<changeSet id="2" author="s" dbms="` + test.dbms + `"><sql>SELECT 2;</sql></changeSet></databaseChangeLog>`,
			)}}

			parsed, err := parseLiquibaseOn(c, "postgresql", files)

			c.Assert(err, qt.IsNil)
			c.Assert(len(parsed.Migrations) == 2, qt.Equals, test.imported)
			c.Assert(len(parsed.Skipped) == 1, qt.Equals, !test.imported)
		})
	}
}

// The report names the file, the changeset and the dbms that left it out, in
// every serialization.
func TestWithLiquibaseDBMS_ChangesetReport_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
		skipped importer.SkippedChangeset
	}{
		{
			name: "xml",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" dbms="postgresql"><sql>SELECT 1;</sql></changeSet>` +
				`<changeSet id="2" author="s" dbms="mysql"><sql>SELECT 2;</sql></changeSet></databaseChangeLog>`,
			skipped: importer.SkippedChangeset{Path: "changelog.xml", Changeset: "s:2", Reason: `dbms="mysql" does not select postgresql`},
		},
		{
			name: "yaml",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, dbms: postgresql, changes: [{sql: {sql: \"SELECT 1;\"}}]}\n" +
				"  - changeSet: {id: \"2\", author: s, dbms: mysql, changes: [{sql: {sql: \"SELECT 2;\"}}]}\n",
			skipped: importer.SkippedChangeset{Path: "changelog.yaml", Changeset: "s:2", Reason: `dbms="mysql" does not select postgresql`},
		},
		{
			name:    "formatted sql",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 dbms:postgresql\nSELECT 1;\n--changeset s:2 dbms:mysql\nSELECT 2;\n",
			skipped: importer.SkippedChangeset{Path: "changelog.sql", Changeset: "s:2", Reason: `dbms="mysql" does not select postgresql`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := parseLiquibaseOn(c, "postgresql", fstest.MapFS{test.file: {Data: []byte(test.content)}})

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Migrations, qt.HasLen, 1)
			c.Assert(parsed.Migrations[0].UpSQL, qt.Equals, "SELECT 1;")
			c.Assert(parsed.Skipped, qt.DeepEquals, []importer.SkippedChangeset{test.skipped})
		})
	}
}

// A change's own dbms decides whether that change imports, in the up and the
// rollback, and is compared as written, as Liquibase compares it. A changeset
// whose every change names another database is left out whole.
func TestWithLiquibaseDBMS_Change_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		changes  string
		skipped  []importer.SkippedChangeset
		imported int
	}{
		{
			name: "one change for each database",
			changes: `<sql dbms="mysql">SELECT 'mysql';</sql><sql dbms="postgresql">SELECT 'postgresql';</sql>` +
				`<rollback><sql dbms="mysql">SELECT 'undo mysql';</sql><sql dbms="postgresql">SELECT 'undo postgresql';</sql></rollback>`,
			skipped: []importer.SkippedChangeset{
				{Path: "changelog.xml", Changeset: "simon:1", Change: "<sql>", Reason: `dbms="mysql" does not select postgresql`},
				{Path: "changelog.xml", Changeset: "simon:1", Change: "<sql>", Reason: `dbms="mysql" does not select postgresql`},
			},
			imported: 1,
		},
		{
			// Liquibase lowercases a changeset's dbms and not a change's, so
			// PostgreSQL on a change selects nothing on "postgresql".
			name:    "a change's dbms compared as written",
			changes: `<sql>SELECT 1;</sql><sql dbms="PostgreSQL">SELECT 2;</sql>`,
			skipped: []importer.SkippedChangeset{
				{Path: "changelog.xml", Changeset: "simon:1", Change: "<sql>", Reason: `dbms="PostgreSQL" does not select postgresql`},
			},
			imported: 1,
		},
		{
			name:    "every change for another database",
			changes: `<sql dbms="mysql">SELECT 1;</sql><sql dbms="mariadb">SELECT 2;</sql></changeSet><changeSet id="2" author="simon"><sql>SELECT 3;</sql>`,
			skipped: []importer.SkippedChangeset{
				{Path: "changelog.xml", Changeset: "simon:1", Reason: "every change in it has a dbms that does not select postgresql"},
			},
			imported: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := parseLiquibaseOn(c, "postgresql", liquibaseXMLOneChangeSet(test.changes))

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Migrations, qt.HasLen, test.imported)
			c.Assert(parsed.Skipped, qt.DeepEquals, test.skipped)
		})
	}
}

// The SQL a kept change carries is the SQL the migration runs, without the
// changes the named database does not run.
func TestWithLiquibaseDBMS_ChangeSQL_HappyPath(t *testing.T) {
	c := qt.New(t)
	changes := `<sql dbms="mysql">SELECT 'mysql';</sql><sql dbms="postgresql">SELECT 'postgresql';</sql>` +
		`<rollback><sql dbms="mysql">SELECT 'undo mysql';</sql><sql dbms="postgresql">SELECT 'undo postgresql';</sql></rollback>`

	parsed, err := parseLiquibaseOn(c, "postgresql", liquibaseXMLOneChangeSet(changes))

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Migrations, qt.HasLen, 1)
	c.Assert(parsed.Migrations[0].UpSQL, qt.Equals, "SELECT 'postgresql';")
	c.Assert(parsed.Migrations[0].DownSQL, qt.Equals, "SELECT 'undo postgresql';")
}

// The name composes with a target dialect: a typed change in a kept changeset is
// rendered for the dialect, and one in a changeset left out needs none.
func TestWithLiquibaseDBMS_WithDialect_HappyPath(t *testing.T) {
	c := qt.New(t)
	parser, err := importer.ParserByName("liquibase")
	c.Assert(err, qt.IsNil)
	rendering, err := importer.WithDialect(parser, "postgres")
	c.Assert(err, qt.IsNil)
	selecting, err := importer.WithLiquibaseDBMS(rendering, "postgresql")
	c.Assert(err, qt.IsNil)
	files := fstest.MapFS{"changelog.xml": {Data: []byte(
		`<databaseChangeLog><changeSet id="1" author="s" dbms="postgresql"><createTable tableName="t">` +
			`<column name="id" type="int"/></createTable></changeSet>` +
			`<changeSet id="2" author="s" dbms="mysql"><loadData tableName="t" file="t.csv"/></changeSet></databaseChangeLog>`,
	)}}

	parsed, err := selecting.Parse(files)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Migrations, qt.HasLen, 1)
	c.Assert(parsed.Migrations[0].UpSQL, qt.Contains, `CREATE TABLE "t"`)
	c.Assert(parsed.Skipped, qt.HasLen, 1)
}

// With the database named, dbms stops refusing; nothing else does. A dbms
// naming a database Liquibase does not know is refused, as Liquibase's
// validation refuses it -- including Ptah's own dialect names.
func TestWithLiquibaseDBMS_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
		message string
	}{
		{
			name:    "a Ptah dialect name in a changeset dbms",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" dbms="postgres"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml": dbms "postgres" names "postgres", which is not a database Liquibase knows`,
		},
		{
			name:    "an unknown name in a change dbms",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sql dbms="sqlserver">SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml": <sql> dbms "sqlserver" names "sqlserver", which is not a database Liquibase knows`,
		},
		{
			name:    "an unknown name in formatted sql",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 dbms:pg\nSELECT 1;\n",
			message: `liquibase changeset s:1 in "changelog.sql": dbms "pg" names "pg", which is not a database Liquibase knows`,
		},
		{
			name:    "every changeset for another database",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" dbms="mysql"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase source holds no changeset Liquibase runs here: every one was left out ` +
				`\(changelog.xml s:1: dbms="mysql" does not select postgresql\)`,
		},
		{
			name:    "another selector beside a dbms that selects it",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" dbms="postgresql" context="prod"><sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `liquibase changeset s_1 in "changelog.xml" is conditional on context; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := parseLiquibaseOn(c, "postgresql", fstest.MapFS{test.file: {Data: []byte(test.content)}})

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(parsed, qt.IsNil)
		})
	}
}

// Whatever else a changeset carries, a dbms that leaves it out leaves it out:
// Liquibase does not run it there, so its context, or a change Ptah cannot
// convert, does not matter.
func TestWithLiquibaseDBMS_LeftOutWhatever_HappyPath(t *testing.T) {
	c := qt.New(t)
	files := fstest.MapFS{"changelog.xml": {Data: []byte(
		`<databaseChangeLog><changeSet id="1" author="s"><sql>SELECT 1;</sql></changeSet>` +
			`<changeSet id="2" author="s" dbms="mysql" context="prod"><loadData tableName="t" file="t.csv"/></changeSet>` +
			`</databaseChangeLog>`,
	)}}

	parsed, err := parseLiquibaseOn(c, "postgresql", files)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Migrations, qt.HasLen, 1)
	c.Assert(parsed.Skipped, qt.DeepEquals, []importer.SkippedChangeset{
		{Path: "changelog.xml", Changeset: "s:2", Reason: `dbms="mysql" does not select postgresql`},
	})
}

// WithLiquibaseDBMS refuses a name Liquibase does not know, a source tool whose
// changesets have no dbms, and a missing parser.
func TestWithLiquibaseDBMS_Option_FailurePath(t *testing.T) {
	c := qt.New(t)
	liquibase, err := importer.ParserByName("liquibase")
	c.Assert(err, qt.IsNil)
	goose, err := importer.ParserByName("goose")
	c.Assert(err, qt.IsNil)
	tests := []struct {
		name    string
		parser  importer.Parser
		dbms    string
		message string
	}{
		{name: "a Ptah dialect name", parser: liquibase, dbms: "postgres", message: `"postgres" is not a database name Liquibase knows \(known: .*\)`},
		{name: "another source tool", parser: goose, dbms: "postgresql", message: `a Liquibase database name applies only to a Liquibase source, not to goose`},
		{name: "no parser", parser: nil, dbms: "postgresql", message: `a Liquibase database name needs a source tool: choose or detect the parser first`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := importer.WithLiquibaseDBMS(test.parser, test.dbms)

			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(got, qt.IsNil)
		})
	}
}
