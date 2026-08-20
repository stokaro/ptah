package importer_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/importer"
)

// The same changelog written three ways. Liquibase serializes one model into
// XML, YAML and JSON, so importing one of them and refusing the others left a
// team on the other two with nothing (stokaro/ptah#1629).
const (
	liquibaseChangelogXML = `<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog>
  <changeSet id="create-users" author="alice">
    <sql>CREATE TABLE users (id integer PRIMARY KEY);</sql>
    <rollback><sql>DROP TABLE users;</sql></rollback>
  </changeSet>
  <changeSet id="add-email" author="bob">
    <comment>widen the profile</comment>
    <sql>ALTER TABLE users ADD email text;</sql>
  </changeSet>
</databaseChangeLog>
`

	liquibaseChangelogYAML = `databaseChangeLog:
  - changeSet:
      id: create-users
      author: alice
      changes:
        - sql:
            sql: "CREATE TABLE users (id integer PRIMARY KEY);"
      rollback:
        - sql:
            sql: "DROP TABLE users;"
  - changeSet:
      id: add-email
      author: bob
      changes:
        - sql:
            sql: "ALTER TABLE users ADD email text;"
`

	liquibaseChangelogJSON = `{
  "databaseChangeLog": [
    {"changeSet": {
      "id": "create-users",
      "author": "alice",
      "changes": [{"sql": {"sql": "CREATE TABLE users (id integer PRIMARY KEY);"}}],
      "rollback": [{"sql": {"sql": "DROP TABLE users;"}}]
    }},
    {"changeSet": {
      "id": "add-email",
      "author": "bob",
      "changes": [{"sql": {"sql": "ALTER TABLE users ADD email text;"}}]
    }}
  ]
}
`
)

// parseLiquibaseChangelogFile runs one changelog through the shared parser.
func parseLiquibaseChangelogFile(c *qt.C, name, content string) []importer.SourceMigration {
	c.Helper()
	parser, err := importer.ParserByName("liquibase")
	c.Assert(err, qt.IsNil)

	migrations, err := parser.Parse(fstest.MapFS{name: {Data: []byte(content)}})
	c.Assert(err, qt.IsNil)
	normalized, err := importer.Normalize(migrations)
	c.Assert(err, qt.IsNil)
	return normalized
}

// parseLiquibaseChangelogError is the paired helper for the refusals: it returns
// the message rather than the migrations.
func parseLiquibaseChangelogError(c *qt.C, name, content string) error {
	c.Helper()
	parser, err := importer.ParserByName("liquibase")
	c.Assert(err, qt.IsNil)

	_, parseErr := parser.Parse(fstest.MapFS{name: {Data: []byte(content)}})
	return parseErr
}

// TestLiquibaseChangelog_ThreeSerializationsOneResult is the acceptance case:
// the three files above are the same changelog, so they must import to the same
// migrations. Asserting each separately would let two of them drift.
func TestLiquibaseChangelog_ThreeSerializationsOneResult(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
	}{
		{name: "xml", file: "changelog.xml", content: liquibaseChangelogXML},
		{name: "yaml", file: "changelog.yaml", content: liquibaseChangelogYAML},
		{name: "json", file: "changelog.json", content: liquibaseChangelogJSON},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			normalized := parseLiquibaseChangelogFile(c, test.file, test.content)

			c.Assert(normalized, qt.HasLen, 2)
			c.Assert(normalized[0].Version, qt.Equals, int64(1))
			c.Assert(normalized[0].Name, qt.Equals, "alice_create_users")
			c.Assert(normalized[0].UpSQL, qt.Equals, "CREATE TABLE users (id integer PRIMARY KEY);")
			// The rollback is the down direction, not a dropped construct.
			c.Assert(normalized[0].DownSQL, qt.Equals, "DROP TABLE users;")

			c.Assert(normalized[1].Version, qt.Equals, int64(2))
			c.Assert(normalized[1].Name, qt.Equals, "bob_add_email")
			c.Assert(normalized[1].UpSQL, qt.Equals, "ALTER TABLE users ADD email text;")
			// No rollback in the source: an empty down, and the emit path writes
			// the placeholder.
			c.Assert(normalized[1].DownSQL, qt.Equals, "")
		})
	}
}

// TestLiquibaseChangelog_YMLExtension covers the second spelling Liquibase
// accepts for YAML. A reader keyed on ".yaml" alone refuses half the YAML
// changelogs in the wild for a reason that has nothing to do with content.
func TestLiquibaseChangelog_YMLExtension(t *testing.T) {
	c := qt.New(t)

	normalized := parseLiquibaseChangelogFile(c, "changelog.yml", liquibaseChangelogYAML)

	c.Assert(normalized, qt.HasLen, 2)
	c.Assert(normalized[0].Name, qt.Equals, "alice_create_users")
}

// TestLiquibaseChangelog_XMLRollbackText covers a rollback written as text
// rather than as a nested <sql>, which is how Liquibase's own examples write a
// one-liner.
func TestLiquibaseChangelog_XMLRollbackText(t *testing.T) {
	c := qt.New(t)
	changelog := `<databaseChangeLog>
  <changeSet id="create-users" author="alice">
    <sql>CREATE TABLE users (id integer PRIMARY KEY);</sql>
    <rollback>DROP TABLE users;</rollback>
  </changeSet>
</databaseChangeLog>
`

	normalized := parseLiquibaseChangelogFile(c, "changelog.xml", changelog)

	c.Assert(normalized, qt.HasLen, 1)
	c.Assert(normalized[0].DownSQL, qt.Equals, "DROP TABLE users;")
}

// TestLiquibaseChangelog_DocumentRollbackString covers the same shorthand in
// YAML, where a rollback may be a bare SQL string instead of a change list.
func TestLiquibaseChangelog_DocumentRollbackString(t *testing.T) {
	c := qt.New(t)
	changelog := `databaseChangeLog:
  - changeSet:
      id: create-users
      author: alice
      changes:
        - sql:
            sql: "CREATE TABLE users (id integer PRIMARY KEY);"
      rollback: "DROP TABLE users;"
`

	normalized := parseLiquibaseChangelogFile(c, "changelog.yaml", changelog)

	c.Assert(normalized, qt.HasLen, 1)
	c.Assert(normalized[0].DownSQL, qt.Equals, "DROP TABLE users;")
}

// TestLiquibaseChangelog_BareSQLChange covers `sql` written as a bare string
// rather than a mapping, which both YAML and JSON allow.
func TestLiquibaseChangelog_BareSQLChange(t *testing.T) {
	c := qt.New(t)
	changelog := `{"databaseChangeLog":[{"changeSet":{"id":"a","author":"alice",` +
		`"changes":[{"sql":"CREATE TABLE users (id integer);"}]}}]}`

	normalized := parseLiquibaseChangelogFile(c, "changelog.json", changelog)

	c.Assert(normalized, qt.HasLen, 1)
	c.Assert(normalized[0].UpSQL, qt.Equals, "CREATE TABLE users (id integer);")
}

// TestLiquibaseChangelog_FilesAreReadInNameOrder pins the ordering rule.
//
// Absent a master changelog naming an order, the file name is the only stable
// one. Map iteration order would reorder history between runs, which is the
// failure that shows up as a migration applying before the table it alters.
func TestLiquibaseChangelog_FilesAreReadInNameOrder(t *testing.T) {
	c := qt.New(t)
	first := `<databaseChangeLog><changeSet id="a" author="alice">` +
		`<sql>CREATE TABLE users (id integer);</sql></changeSet></databaseChangeLog>`
	second := `<databaseChangeLog><changeSet id="b" author="bob">` +
		`<sql>ALTER TABLE users ADD email text;</sql></changeSet></databaseChangeLog>`
	parser, err := importer.ParserByName("liquibase")
	c.Assert(err, qt.IsNil)

	// Written second-then-first so a reader honoring insertion order would fail.
	migrations, err := parser.Parse(fstest.MapFS{
		"02-add-email.xml":    {Data: []byte(second)},
		"01-create-users.xml": {Data: []byte(first)},
	})
	c.Assert(err, qt.IsNil)
	normalized, err := importer.Normalize(migrations)
	c.Assert(err, qt.IsNil)

	c.Assert(normalized, qt.HasLen, 2)
	c.Assert(normalized[0].Name, qt.Equals, "alice_a")
	c.Assert(normalized[1].Name, qt.Equals, "bob_b")
}

// TestLiquibaseChangelog_RefusesUnconvertibleConstructs is the other half of the
// contract, and the more important one.
//
// A migration directory that is not the changelog it claims to have imported is
// worse than an import that did not happen: it applies cleanly and leaves out
// the changesets an <include> named, or runs unconditionally what `contexts`
// selected. Each row names the construct in the message, so the operator learns
// what to do rather than that something went wrong.
func TestLiquibaseChangelog_RefusesUnconvertibleConstructs(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
		message string
	}{
		{
			name:    "xml include",
			file:    "changelog.xml",
			content: `<databaseChangeLog><include file="other.xml"/></databaseChangeLog>`,
			message: `uses <include>, which composes other changelog files`,
		},
		{
			name:    "xml includeAll",
			file:    "changelog.xml",
			content: `<databaseChangeLog><includeAll path="changes"/></databaseChangeLog>`,
			message: `uses <includeAll>, which composes other changelog files`,
		},
		{
			name: "document include",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - include: {file: other.yaml}\n",
			message: `uses "include", which composes other changelog files`,
		},
		{
			name: "typed refactoring",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="a" author="alice">` +
				`<createTable tableName="users"/></changeSet></databaseChangeLog>`,
			message: `uses <createTable>, which is not SQL text`,
		},
		{
			name: "document typed refactoring",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: a, author: alice, changes: [{addColumn: {tableName: users}}]}\n",
			message: `uses addColumn, which is not SQL text`,
		},
		{
			name: "xml preConditions",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="a" author="alice">` +
				`<preConditions><dbms type="postgresql"/></preConditions>` +
				`<sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `carries preConditions, which decide whether the changeset runs`,
		},
		{
			name: "document preConditions",
			file: "changelog.json",
			content: `{"databaseChangeLog":[{"changeSet":{"id":"a","author":"alice",` +
				`"preConditions":[{"dbms":{"type":"postgresql"}}],` +
				`"changes":[{"sql":{"sql":"SELECT 1;"}}]}}]}`,
			message: `carries preConditions, which decide whether the changeset runs`,
		},
		{
			name: "xml contexts",
			file: "changelog.xml",
			content: `<databaseChangeLog><changeSet id="a" author="alice" contexts="prod">` +
				`<sql>SELECT 1;</sql></changeSet></databaseChangeLog>`,
			message: `carries contexts, which decide whether the changeset runs`,
		},
		{
			name: "document labels",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: a, author: alice, labels: nightly, changes: [{sql: {sql: \"SELECT 1;\"}}]}\n",
			message: `carries labels, which decide whether the changeset runs`,
		},
		{
			name:    "top-level element",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeLogPropertyDefined/></databaseChangeLog>`,
			message: `uses <changeLogPropertyDefined> at the top level`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := parseLiquibaseChangelogError(c, test.file, test.content)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.message)
		})
	}
}

// TestLiquibaseChangelog_SelectorsAreNamedBeforeChangeTypes pins which refusal
// wins when a changeset carries both.
//
// The remedies differ and only one of them is available: a typed change can be
// rewritten as `sql`, and a selector cannot be rewritten at all. Reporting the
// change type first would send the operator to rewrite the changeset and then
// refuse it again for the selector.
func TestLiquibaseChangelog_SelectorsAreNamedBeforeChangeTypes(t *testing.T) {
	c := qt.New(t)
	changelog := `<databaseChangeLog><changeSet id="a" author="alice" contexts="prod">` +
		`<createTable tableName="users"/></changeSet></databaseChangeLog>`

	err := parseLiquibaseChangelogError(c, "changelog.xml", changelog)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "carries contexts")
	c.Assert(err.Error(), qt.Not(qt.Contains), "createTable")
}

// TestLiquibaseChangelog_NamesEveryUnconvertibleConstruct is the "rather than
// dropped" half stated directly: a changeset carrying two of them reports both,
// so a second import does not fail for a construct the first message withheld.
func TestLiquibaseChangelog_NamesEveryUnconvertibleConstruct(t *testing.T) {
	c := qt.New(t)
	changelog := `<databaseChangeLog><changeSet id="a" author="alice">` +
		`<createTable tableName="users"/><addColumn tableName="users"/>` +
		`</changeSet></databaseChangeLog>`

	err := parseLiquibaseChangelogError(c, "changelog.xml", changelog)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "<createTable>, <addColumn>")
}

// TestLiquibaseChangelog_EmptyChangesetIsRefused keeps a changeset that
// converted to nothing from becoming an empty migration file. An empty file
// applies cleanly and records a version, which is how a skipped changeset
// becomes permanent.
func TestLiquibaseChangelog_EmptyChangesetIsRefused(t *testing.T) {
	c := qt.New(t)
	changelog := `<databaseChangeLog><changeSet id="a" author="alice">` +
		`<comment>nothing here</comment></changeSet></databaseChangeLog>`

	err := parseLiquibaseChangelogError(c, "changelog.xml", changelog)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `changeset alice_a in "changelog.xml" has no SQL`)
}

// TestLiquibaseChangelog_MalformedDocumentNamesTheFile covers the parse
// failures, which must say which file rather than surfacing a bare decoder
// error against a directory of many.
func TestLiquibaseChangelog_MalformedDocumentNamesTheFile(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
	}{
		{name: "xml", file: "changelog.xml", content: `<databaseChangeLog><changeSet></databaseChangeLog>`},
		{name: "json", file: "changelog.json", content: `{"databaseChangeLog": [`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := parseLiquibaseChangelogError(c, test.file, test.content)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, `parse liquibase changelog "`+test.file+`"`)
		})
	}
}

// TestLiquibaseChangelog_DocumentWithoutRootIsRefused covers the gap between how
// a changelog is SELECTED and how it is READ.
//
// Selection is a textual match for the root name, because a file has to be
// classified before it is decoded. This document mentions the name but does not
// have it at the root, so it is handed to the reader and must be refused there
// rather than decoded into zero changesets -- which would import an empty
// directory and call it that file's history.
func TestLiquibaseChangelog_DocumentWithoutRootIsRefused(t *testing.T) {
	c := qt.New(t)

	err := parseLiquibaseChangelogError(c, "changelog.json",
		`{"notes": "databaseChangeLog lives one level down", "wrapper": {"databaseChangeLog": []}}`)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "has no databaseChangeLog")
}

// TestLiquibaseChangelog_RootMustBeAList is the paired shape check: a mapping
// where a list belongs decodes without error and holds no changesets.
func TestLiquibaseChangelog_RootMustBeAList(t *testing.T) {
	c := qt.New(t)

	err := parseLiquibaseChangelogError(c, "changelog.json", `{"databaseChangeLog": {"changeSet": {}}}`)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "databaseChangeLog must be a list")
}

// TestLiquibaseChangelog_MixedDirectoryIsRefused covers a directory holding both
// shapes.
//
// The two readers order changesets by different rules -- a formatted-SQL file is
// read in directive order, a changelog directory in file-name order -- so
// merging them would produce an order neither tool would have run.
func TestLiquibaseChangelog_MixedDirectoryIsRefused(t *testing.T) {
	c := qt.New(t)
	parser, err := importer.ParserByName("liquibase")
	c.Assert(err, qt.IsNil)

	_, parseErr := parser.Parse(fstest.MapFS{
		"changelog.sql": {Data: []byte(liquibaseFormattedSQL)},
		"changelog.xml": {Data: []byte(liquibaseChangelogXML)},
	})

	c.Assert(parseErr, qt.IsNotNil)
	c.Assert(parseErr.Error(), qt.Contains, "formatted-SQL changelogs")
}

// TestLiquibaseChangelog_ImportEndToEnd is the Ptah-native destination, where
// the rollback is not merely parsed but written.
//
// The Atlas destination has no down file to put it in; this layout does, so
// this is where the down direction survives an import (stokaro/ptah#1629).
func TestLiquibaseChangelog_ImportEndToEnd(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()

	result, err := importer.Import(
		fstest.MapFS{"changelog.xml": {Data: []byte(liquibaseChangelogXML)}}, nil, out, importer.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 4) // 2 changesets -> 2 up + 2 down
	c.Assert(result.Files, qt.Contains, "0000000001_alice_create_users.up.sql")
	c.Assert(result.Files, qt.Contains, "0000000002_bob_add_email.up.sql")

	up, err := os.ReadFile(filepath.Join(out, "0000000001_alice_create_users.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Contains, "CREATE TABLE users (id integer PRIMARY KEY);")

	down, err := os.ReadFile(filepath.Join(out, "0000000001_alice_create_users.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Contains, "DROP TABLE users;")

	// The second changeset declared no rollback, so it gets the placeholder
	// rather than an empty file that would look like a rollback of nothing.
	placeholder, err := os.ReadFile(filepath.Join(out, "0000000002_bob_add_email.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(placeholder), qt.Contains, "No rollback")
}

// TestLiquibaseChangelog_RollbackConstructsAreRefusedToo covers the down
// direction of the refusal rule.
//
// A typed refactoring inside `rollback` is as unconvertible as one inside
// `changes`, and dropping it is worse there: the up would import, the migration
// would look complete, and the down would be missing the statement that undoes
// it. That is only discovered when someone rolls back.
func TestLiquibaseChangelog_RollbackConstructsAreRefusedToo(t *testing.T) {
	c := qt.New(t)
	changelog := `databaseChangeLog:
  - changeSet:
      id: create-users
      author: alice
      changes:
        - sql:
            sql: "CREATE TABLE users (id integer PRIMARY KEY);"
      rollback:
        - dropTable:
            tableName: users
`

	err := parseLiquibaseChangelogError(c, "changelog.yaml", changelog)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "uses dropTable, which is not SQL text")
}
