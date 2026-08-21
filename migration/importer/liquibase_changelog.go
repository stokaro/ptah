package importer

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	yaml "go.yaml.in/yaml/v3"
)

// Liquibase serializes one changeset model four ways: formatted SQL, XML, YAML
// and JSON. The formatted-SQL reader lives in liquibase.go; this file reads the
// other three into the same [SourceMigration] shape (stokaro/ptah#1629).
//
// # What converts, and what is refused
//
// A changeset converts when its changes are SQL: the `sql` change, and
// `rollback` holding SQL for the down direction. Everything else Liquibase can
// put in a changelog is refused BY NAME before anything is written:
//
//   - file composition -- `include` and `includeAll` -- because the imported
//     directory would silently be missing the changesets those files hold;
//   - `preConditions`, because a changeset that ran conditionally there would
//     run unconditionally here;
//   - `contexts` and `labels`, because they select WHICH changesets run and
//     Ptah's directory has no equivalent, so importing them would flatten a
//     conditional history into an unconditional one;
//   - typed refactorings such as `createTable` and `addColumn`, because they
//     are not SQL text at all and rendering them would mean reimplementing
//     Liquibase's generator per dialect.
//
// Refusing by name rather than dropping is the rule this repository already
// applies to unconvertible constructs: a migration directory that is not the
// changelog it claims to have imported is worse than an import that did not
// happen.

// liquibaseChangeSet is one changeset, however it was serialized.
type liquibaseChangeSet struct {
	id      string
	author  string
	upSQL   []string
	downSQL []string
	// unsupported names the constructs this changeset carries that do not
	// convert, in the order they were found.
	unsupported []string
	// selectors are the subset that decide WHETHER a changeset runs rather than
	// what it does. They are separated because the remedy differs: a change
	// type can be rewritten as `sql`, and a selector cannot be rewritten at all
	// -- it has no equivalent in a migration directory.
	selectors []string
}

// parseLiquibaseChangelogFiles reads XML, YAML and JSON changelogs in name
// order and returns their changesets.
//
// Name order is the same rule the formatted-SQL reader applies for the same
// reason: absent a master changelog naming an order, the file name is the only
// stable one, and inventing a different one would reorder history.
func parseLiquibaseChangelogFiles(fsys fs.FS, names []string) ([]SourceMigration, error) {
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)

	var migrations []SourceMigration
	for _, name := range sorted {
		content, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", name, err)
		}
		changesets, err := parseLiquibaseChangelog(name, content)
		if err != nil {
			return nil, err
		}
		for _, changeset := range changesets {
			migration, err := liquibaseMigrationFrom(name, changeset)
			if err != nil {
				return nil, err
			}
			migrations = append(migrations, migration)
		}
	}
	return migrations, nil
}

// parseLiquibaseChangelog dispatches on the file extension.
func parseLiquibaseChangelog(name string, content []byte) ([]liquibaseChangeSet, error) {
	switch strings.ToLower(path.Ext(name)) {
	case ".xml":
		return parseLiquibaseXML(name, content)
	case ".yaml", ".yml":
		return parseLiquibaseYAML(name, content)
	case ".json":
		return parseLiquibaseJSON(name, content)
	default:
		return nil, fmt.Errorf("liquibase changelog %q: unsupported extension", name)
	}
}

// liquibaseMigrationFrom turns one changeset into a migration, or reports what
// stopped it.
func liquibaseMigrationFrom(fileName string, changeset liquibaseChangeSet) (SourceMigration, error) {
	name := liquibaseChangesetName(changeset.author, changeset.id)
	if len(changeset.selectors) > 0 {
		return SourceMigration{}, fmt.Errorf(
			"liquibase changeset %s in %q carries %s, which decide whether the changeset runs; "+
				"a migration directory has no equivalent, so importing it would turn a conditional "+
				"history into an unconditional one -- split the changelog or import it by hand",
			name, fileName, strings.Join(changeset.selectors, ", "))
	}
	if len(changeset.unsupported) > 0 {
		return SourceMigration{}, fmt.Errorf(
			"liquibase changeset %s in %q uses %s, which is not SQL text and which Ptah does not "+
				"generate per dialect; rewrite it as a `sql` change or import it by hand",
			name, fileName, strings.Join(changeset.unsupported, ", "))
	}
	upSQL := strings.TrimSpace(strings.Join(changeset.upSQL, "\n"))
	if upSQL == "" {
		return SourceMigration{}, fmt.Errorf(
			"liquibase changeset %s in %q has no SQL",
			liquibaseChangesetName(changeset.author, changeset.id), fileName)
	}
	return SourceMigration{
		Name:    liquibaseChangesetName(changeset.author, changeset.id),
		UpSQL:   upSQL,
		DownSQL: strings.TrimSpace(strings.Join(changeset.downSQL, "\n")),
	}, nil
}

// ---------------------------------------------------------------------- XML

type liquibaseXMLRoot struct {
	XMLName xml.Name          `xml:"databaseChangeLog"`
	Nodes   []liquibaseXMLAny `xml:",any"`
}

// liquibaseXMLAny captures any element with its name, so an unconvertible one
// can be refused by the name the author wrote rather than by a position.
type liquibaseXMLAny struct {
	XMLName  xml.Name
	ID       string            `xml:"id,attr"`
	Author   string            `xml:"author,attr"`
	Context  string            `xml:"context,attr"`
	Contexts string            `xml:"contexts,attr"`
	Labels   string            `xml:"labels,attr"`
	Children []liquibaseXMLAny `xml:",any"`
	Text     string            `xml:",chardata"`
}

func parseLiquibaseXML(name string, content []byte) ([]liquibaseChangeSet, error) {
	var root liquibaseXMLRoot
	if err := xml.Unmarshal(content, &root); err != nil {
		return nil, fmt.Errorf("parse liquibase changelog %q: %w", name, err)
	}

	var changesets []liquibaseChangeSet
	for _, node := range root.Nodes {
		switch node.XMLName.Local {
		case "changeSet":
			changesets = append(changesets, liquibaseXMLChangeSet(node))
		case "include", "includeAll":
			// Composition: the changesets those files hold would be missing
			// from the imported directory.
			return nil, fmt.Errorf(
				"liquibase changelog %q uses <%s>, which composes other changelog files; "+
					"Ptah imports one changelog at a time, so import the referenced files instead",
				name, node.XMLName.Local)
		case "property", "":
			// A property substitutes into values Ptah does not interpret, and
			// an empty name is chardata between elements.
		default:
			return nil, fmt.Errorf(
				"liquibase changelog %q uses <%s> at the top level, which Ptah does not convert",
				name, node.XMLName.Local)
		}
	}
	return changesets, nil
}

func liquibaseXMLChangeSet(node liquibaseXMLAny) liquibaseChangeSet {
	changeset := liquibaseChangeSet{id: node.ID, author: node.Author}
	if strings.TrimSpace(node.Context) != "" {
		changeset.selectors = append(changeset.selectors, "context")
	}
	if strings.TrimSpace(node.Contexts) != "" {
		changeset.selectors = append(changeset.selectors, "contexts")
	}
	if strings.TrimSpace(node.Labels) != "" {
		changeset.selectors = append(changeset.selectors, "labels")
	}
	for _, child := range node.Children {
		switch child.XMLName.Local {
		case "sql":
			changeset.upSQL = append(changeset.upSQL, strings.TrimSpace(child.Text))
		case "rollback":
			changeset.downSQL = append(changeset.downSQL, liquibaseXMLRollbackSQL(child))
		case "preConditions":
			changeset.selectors = append(changeset.selectors, "preConditions")
		case "comment", "":
			// A comment documents the changeset, and an empty name is the
			// chardata between elements.
		default:
			changeset.unsupported = append(changeset.unsupported, "<"+child.XMLName.Local+">")
		}
	}
	return changeset
}

// liquibaseXMLRollbackSQL reads a rollback's SQL, whether written as text or as
// a nested <sql> element.
func liquibaseXMLRollbackSQL(node liquibaseXMLAny) string {
	var parts []string
	for _, child := range node.Children {
		if child.XMLName.Local == "sql" {
			parts = append(parts, strings.TrimSpace(child.Text))
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, "\n")
	}
	return strings.TrimSpace(node.Text)
}

// -------------------------------------------------------------- YAML / JSON

// YAML and JSON serialize the same document, so they decode into the same
// generic shape and share one walk:
//
//	databaseChangeLog:
//	  - changeSet:
//	      id: "1"
//	      author: ada
//	      changes:
//	        - sql: { sql: "CREATE TABLE ..." }
//	      rollback:
//	        - sql: { sql: "DROP TABLE ..." }
func parseLiquibaseYAML(name string, content []byte) ([]liquibaseChangeSet, error) {
	var document any
	if err := yaml.Unmarshal(content, &document); err != nil {
		return nil, fmt.Errorf("parse liquibase changelog %q: %w", name, err)
	}
	return liquibaseDocumentChangeSets(name, document)
}

func parseLiquibaseJSON(name string, content []byte) ([]liquibaseChangeSet, error) {
	var document any
	if err := json.Unmarshal(content, &document); err != nil {
		return nil, fmt.Errorf("parse liquibase changelog %q: %w", name, err)
	}
	return liquibaseDocumentChangeSets(name, document)
}

func liquibaseDocumentChangeSets(name string, document any) ([]liquibaseChangeSet, error) {
	root, ok := liquibaseMapValue(document, "databaseChangeLog")
	if !ok {
		return nil, fmt.Errorf("liquibase changelog %q has no databaseChangeLog", name)
	}
	entries, ok := root.([]any)
	if !ok {
		return nil, fmt.Errorf("liquibase changelog %q: databaseChangeLog must be a list", name)
	}

	var changesets []liquibaseChangeSet
	for _, entry := range entries {
		key, value, ok := liquibaseSingleKey(entry)
		if !ok {
			continue
		}
		switch key {
		case "changeSet":
			changesets = append(changesets, liquibaseDocumentChangeSet(value))
		case "include", "includeAll":
			return nil, fmt.Errorf(
				"liquibase changelog %q uses %q, which composes other changelog files; "+
					"Ptah imports one changelog at a time, so import the referenced files instead",
				name, key)
		case "property":
		default:
			return nil, fmt.Errorf(
				"liquibase changelog %q uses %q at the top level, which Ptah does not convert", name, key)
		}
	}
	return changesets, nil
}

func liquibaseDocumentChangeSet(value any) liquibaseChangeSet {
	changeset := liquibaseChangeSet{
		id:     liquibaseStringValue(value, "id"),
		author: liquibaseStringValue(value, "author"),
	}
	for _, key := range []string{"context", "contexts", "labels"} {
		if liquibaseStringValue(value, key) != "" {
			changeset.selectors = append(changeset.selectors, key)
		}
	}
	if _, present := liquibaseMapValue(value, "preConditions"); present {
		changeset.selectors = append(changeset.selectors, "preConditions")
	}
	changes, _ := liquibaseMapValue(value, "changes")
	upSQL, upUnsupported := liquibaseDocumentSQL(changes)
	rollback, _ := liquibaseMapValue(value, "rollback")
	downSQL, downUnsupported := liquibaseDocumentSQL(rollback)

	changeset.upSQL = upSQL
	changeset.downSQL = downSQL
	changeset.unsupported = append(append(changeset.unsupported, upUnsupported...), downUnsupported...)
	return changeset
}

// liquibaseDocumentSQL splits a changes or rollback list into the SQL it holds
// and the names of the change types that are not SQL.
//
// The second result is what the caller refuses by name; returning it rather
// than dropping it here is what keeps an unconvertible change from becoming a
// migration file that silently omits it.
func liquibaseDocumentSQL(node any) (statements, unsupported []string) {
	for _, entry := range liquibaseList(node) {
		key, value, ok := liquibaseSingleKey(entry)
		if !ok {
			// A rollback may be a bare SQL string rather than a list of
			// changes, which is how Liquibase's own examples write a one-liner.
			if text, isText := entry.(string); isText && strings.TrimSpace(text) != "" {
				statements = append(statements, strings.TrimSpace(text))
			}
			continue
		}
		if key != "sql" {
			unsupported = append(unsupported, key)
			continue
		}
		statements = append(statements, liquibaseSQLText(value))
	}
	return statements, unsupported
}

// liquibaseSQLText reads a `sql` change, which is either a bare string or a
// mapping whose own `sql` key holds the statement.
func liquibaseSQLText(value any) string {
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	if inner, ok := liquibaseMapValue(value, "sql"); ok {
		if text, isText := inner.(string); isText {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

// liquibaseList treats a bare value as a one-element list, which is how both
// serializations allow a single change to be written without a sequence.
func liquibaseList(node any) []any {
	switch typed := node.(type) {
	case nil:
		return nil
	case []any:
		return typed
	default:
		return []any{typed}
	}
}

// liquibaseMapValue reads one key from a YAML or JSON mapping. YAML decodes
// into map[string]any for string keys, and JSON always does.
func liquibaseMapValue(node any, key string) (any, bool) {
	mapping, ok := node.(map[string]any)
	if !ok {
		return nil, false
	}
	value, present := mapping[key]
	return value, present
}

func liquibaseStringValue(node any, key string) string {
	value, ok := liquibaseMapValue(node, key)
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return fmt.Sprintf("%v", value)
	}
	return strings.TrimSpace(text)
}

// liquibaseSingleKey reads a one-key mapping, which is how both serializations
// spell a tagged union: `- changeSet: {...}`, `- sql: {...}`.
func liquibaseSingleKey(node any) (string, any, bool) {
	mapping, ok := node.(map[string]any)
	if !ok || len(mapping) != 1 {
		return "", nil, false
	}
	for key, value := range mapping {
		return key, value, true
	}
	return "", nil, false
}
