package importer

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"slices"
	"sort"
	"strings"

	yaml "go.yaml.in/yaml/v3"

	"ptah.run/core/platform/capability"
)

// Liquibase serializes one changeset model four ways: formatted SQL, XML, YAML
// and JSON. The formatted-SQL reader lives in liquibase.go; this file reads the
// other three into the same [SourceMigration] shape (stokaro/ptah#1629).
//
// # What converts, and what is refused
//
// A changeset converts when every change in it converts. The `sql` and
// `sqlFile` changes carry SQL and convert as they are. The typed changes listed
// in [liquibaseConverters] -- `createTable`, `addColumn` and the others -- are
// rendered for the target dialect the caller names, and liquibase_typed.go says
// how. Everything else Liquibase can put in a changelog is refused BY NAME
// before anything is written:
//
//   - file composition -- `include` and `includeAll` -- because the imported
//     directory would silently be missing the changesets those files hold;
//   - `preConditions`, because a changeset that ran conditionally there would
//     run unconditionally here;
//   - `context`, `contexts`, `labels` and `dbms`, because they select WHICH
//     changesets run and Ptah's directory has no equivalent, so importing them
//     would flatten a conditional history into an unconditional one. `dbms`
//     selects a change too, on the change types listed in
//     [liquibaseDbmsTargeted];
//   - `runAlways` and `runOnChange` set to true, because Liquibase runs such a
//     changeset again on a later update and a Ptah migration runs once;
//   - a typed change with no target dialect, because its SQL does not exist
//     until a dialect is chosen;
//   - every change type outside [liquibaseConverters].
//
// A target dialect does not make `dbms` convert. Keeping the changesets whose
// `dbms` names the target needs the name Liquibase gave that target, and for
// some targets that name depends on how Liquibase reached the server rather
// than on the server: YugabyteDB is `yugabytedb` with the Liquibase extension
// installed and `postgresql` without it, and Spanner is `cloudspanner` through
// its JDBC driver and `postgresql` through PGAdapter.
//
// Refusing by name rather than dropping is the rule this repository already
// applies to unconvertible constructs: a migration directory that is not the
// changelog it claims to have imported is worse than an import that did not
// happen.

// liquibaseChangeSet is one changeset, however it was serialized.
type liquibaseChangeSet struct {
	id     string
	author string
	// changes are the changes the changeset applies, in order.
	changes []liquibaseChange
	// rollback holds the changes an explicit rollback declared.
	// rollbackDeclared separates an empty rollback, which says undoing the
	// changeset takes nothing, from no rollback at all, which leaves Ptah to
	// derive one.
	rollback         []liquibaseChange
	rollbackDeclared bool
	// rollbackRefusal is set when the rollback is written in a form that does
	// not convert, such as one naming another changeset's rollback.
	rollbackRefusal string
	// selectors are the subset that decide WHETHER a changeset runs rather than
	// what it does. They are separated because the remedy differs: a change
	// type can be rewritten as `sql`, and a selector cannot be rewritten at all
	// -- it has no equivalent in a migration directory.
	selectors []string
	// repeats are the attributes that make Liquibase run the changeset again
	// after its first run. A migration directory has no equivalent for them
	// either.
	repeats []string
	// conditionErr is set when a run attribute holds a value that does not
	// parse, so what it decides is unknown.
	conditionErr error
}

// liquibaseRunKind says what a changeset attribute decides about running the
// changeset.
type liquibaseRunKind int

const (
	// liquibaseNoRunCondition is an attribute that decides nothing about
	// running the changeset.
	liquibaseNoRunCondition liquibaseRunKind = iota
	// liquibaseSelector decides WHETHER the changeset runs.
	liquibaseSelector
	// liquibaseRepeat decides whether Liquibase runs it again after its first
	// run.
	liquibaseRepeat
)

// liquibaseRunCondition is the one place that recognizes what decides whether,
// or how often, Liquibase runs a changeset. The XML walk, the YAML and JSON walk
// and the formatted-SQL reader hand it every attribute and element a changeset
// carries, and [liquibaseChange.withoutRunCondition] hands it the `dbms` of a
// change, so a condition added here is refused in all four serializations and at
// both levels. A list per reader would drop, in the readers that were not told,
// whatever the others learned to refuse.
//
// kind is reported for every name the function recognizes, and binds reports
// whether the value makes the condition hold: an empty value is the default, so
// an empty selector selects nothing, and `runAlways="false"` spells out the
// default. A repeat whose value is not a boolean is an error, because what the
// author meant by it is unknown. Names match in any case, as Liquibase matches
// them.
func liquibaseRunCondition(key, value string) (kind liquibaseRunKind, binds bool, err error) {
	switch strings.ToLower(key) {
	// contextFilter is the newer spelling of context, and Liquibase reads
	// either.
	case "context", "contextfilter", "contexts", "labels", "dbms":
		return liquibaseSelector, strings.TrimSpace(value) != "", nil
	// A precondition is a selector whatever it holds; Liquibase accepts both
	// spellings of the element.
	case "preconditions":
		return liquibaseSelector, true, nil
	// alwaysRun is the older spelling of runAlways, and Liquibase reads either.
	case "runalways", "alwaysrun", "runonchange":
		if strings.TrimSpace(value) == "" {
			return liquibaseRepeat, false, nil
		}
		repeat, ok := liquibaseParseBool(value)
		if !ok {
			return liquibaseRepeat, false, fmt.Errorf("%s %q is not true or false", key, value)
		}
		return liquibaseRepeat, repeat, nil
	default:
		return liquibaseNoRunCondition, false, nil
	}
}

// noteRunCondition records key when it names a run condition that holds, and
// reports whether key names a run condition at all, so a reader can tell one
// from a change. A name is recorded once: formatted SQL writes one
// `--precondition-<type>` line per precondition.
func (cs *liquibaseChangeSet) noteRunCondition(key, value string) bool {
	kind, binds, err := liquibaseRunCondition(key, value)
	switch {
	case kind == liquibaseNoRunCondition:
		return false
	case err != nil:
		if cs.conditionErr == nil {
			cs.conditionErr = err
		}
	case binds && kind == liquibaseSelector && !slices.Contains(cs.selectors, key):
		cs.selectors = append(cs.selectors, key)
	case binds && kind == liquibaseRepeat && !slices.Contains(cs.repeats, key):
		cs.repeats = append(cs.repeats, key)
	}
	return true
}

// runConditionErr refuses a changeset that Liquibase runs conditionally or more
// than once, naming every attribute that makes it so.
//
// A selector is named first and decides the remedy: a changeset that runs on
// some databases or in some environments has to be split in Liquibase, and
// removing a repeat attribute does not change that.
func (cs liquibaseChangeSet) runConditionErr(name, file string) error {
	switch {
	case cs.conditionErr != nil:
		return fmt.Errorf("liquibase changeset %s in %q: %w", name, file, cs.conditionErr)
	case len(cs.selectors) > 0:
		message := fmt.Sprintf(
			"liquibase changeset %s in %q is conditional on %s; a migration directory has no "+
				"equivalent, so importing it would turn a conditional history into an "+
				"unconditional one -- split the changelog or import it by hand",
			name, file, strings.Join(cs.selectors, ", "))
		if len(cs.repeats) > 0 {
			message += fmt.Sprintf("; it also sets %s, which a migration directory cannot express either",
				strings.Join(cs.repeats, ", "))
		}
		return errors.New(message)
	case len(cs.repeats) > 0:
		return fmt.Errorf(
			"liquibase changeset %s in %q sets %s, so Liquibase can run it again on a later update; "+
				"a Ptah migration runs once, so importing it would turn a repeated changeset into a "+
				"one-time one -- import it by hand",
			name, file, strings.Join(cs.repeats, ", "))
	default:
		return nil
	}
}

// liquibaseDbmsTargeted are the change types that run only on the databases
// their own `dbms` attribute names, whatever their changeset says: the change
// types that implement Liquibase's DbmsTargetedChange. Liquibase reads `dbms` on
// no other change type, so there the attribute stays on the change and its
// converter refuses it as one it does not read.
var liquibaseDbmsTargeted = []string{"sql", "sqlFile", "insert", "createProcedure"}

// withoutRunCondition takes the `dbms` attribute off a change Liquibase targets
// by it, and returns the selector it names, if any.
//
// The converter then never sees the attribute. An empty one runs everywhere,
// which is what the converted change does, and a non-empty one refuses the
// changeset before any converter runs.
func (ch liquibaseChange) withoutRunCondition() (liquibaseChange, []string) {
	if !slices.Contains(liquibaseDbmsTargeted, ch.name) {
		return ch, nil
	}
	stripped := ch
	stripped.attrs = make(map[string]string, len(ch.attrs))
	var selectors []string
	for _, key := range slices.Sorted(maps.Keys(ch.attrs)) {
		value := ch.attrs[key]
		if strings.EqualFold(key, "dbms") {
			if kind, binds, _ := liquibaseRunCondition(key, value); kind == liquibaseSelector {
				if binds {
					selectors = append(selectors, ch.display+" "+key)
				}
				continue
			}
		}
		stripped.attrs[key] = value
	}
	return stripped, selectors
}

// liquibaseWithoutRunConditions applies [liquibaseChange.withoutRunCondition]
// to each change.
func liquibaseWithoutRunConditions(changes []liquibaseChange) ([]liquibaseChange, []string) {
	stripped := make([]liquibaseChange, 0, len(changes))
	var selectors []string
	for _, change := range changes {
		change, named := change.withoutRunCondition()
		stripped = append(stripped, change)
		selectors = append(selectors, named...)
	}
	return stripped, selectors
}

// parseLiquibaseChangelogFiles reads XML, YAML and JSON changelogs in name
// order and returns their changesets.
//
// Name order is the same rule the formatted-SQL reader applies for the same
// reason: absent a master changelog naming an order, the file name is the only
// stable one, and inventing a different one would reorder history.
func parseLiquibaseChangelogFiles(
	fsys fs.FS,
	names []string,
	dialect string,
	caps capability.Capabilities,
) ([]SourceMigration, []string, error) {
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)

	var migrations []SourceMigration
	var consumed []string
	for _, name := range sorted {
		content, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, nil, fmt.Errorf("read %q: %w", name, err)
		}
		changesets, err := parseLiquibaseChangelog(name, content)
		if err != nil {
			return nil, nil, err
		}
		converter := &liquibaseConverter{fsys: fsys, file: name, dialect: dialect, caps: caps}
		for _, changeset := range changesets {
			migration, err := liquibaseMigrationFrom(converter, changeset)
			if err != nil {
				return nil, nil, err
			}
			migrations = append(migrations, migration)
		}
		consumed = append(consumed, converter.consumed...)
	}
	return migrations, consumed, nil
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
//
// Every change is tried before anything is refused, so one message names every
// construct that stopped the changeset: a second import must not fail for a
// construct the first message withheld.
func liquibaseMigrationFrom(converter *liquibaseConverter, changeset liquibaseChangeSet) (SourceMigration, error) {
	name := liquibaseChangesetName(changeset.author, changeset.id)
	// A rollback change selected by dbms is refused like an up change: Liquibase
	// skips it on another database when it rolls back.
	changes, changeSelectors := liquibaseWithoutRunConditions(changeset.changes)
	rollback, rollbackSelectors := liquibaseWithoutRunConditions(changeset.rollback)
	changeset.selectors = slices.Concat(changeset.selectors, changeSelectors, rollbackSelectors)
	if err := changeset.runConditionErr(name, converter.file); err != nil {
		return SourceMigration{}, err
	}
	if changeset.rollbackRefusal != "" {
		return SourceMigration{}, fmt.Errorf("liquibase changeset %s in %q: %s", name, converter.file, changeset.rollbackRefusal)
	}

	var refusal liquibaseRefusal
	up := refusal.convertAll(converter, changes)
	var down []liquibaseConverted
	if changeset.rollbackDeclared {
		down = refusal.convertAll(converter, rollback)
	}
	if err := refusal.err(name, converter.file); err != nil {
		return SourceMigration{}, err
	}

	upSQL := liquibaseJoin(up, func(converted liquibaseConverted) string { return converted.up })
	if upSQL == "" {
		return SourceMigration{}, fmt.Errorf("liquibase changeset %s in %q has no SQL", name, converter.file)
	}
	var downSQL string
	if changeset.rollbackDeclared {
		downSQL = liquibaseJoin(down, func(converted liquibaseConverted) string { return converted.up })
	} else {
		downSQL = liquibaseDerivedRollback(up)
	}
	return SourceMigration{Name: name, UpSQL: upSQL, DownSQL: downSQL}, nil
}

// liquibaseRefusal collects what stopped a changeset's conversion.
type liquibaseRefusal struct {
	// unconverted are change types Ptah does not convert.
	unconverted []string
	// needDialect are typed changes that convert once a dialect is chosen.
	needDialect []string
	// failed is the first change that was recognized and could not be read.
	failed error
}

// convertAll converts changes in order, recording each one that did not
// convert instead of stopping at it.
func (r *liquibaseRefusal) convertAll(converter *liquibaseConverter, changes []liquibaseChange) []liquibaseConverted {
	converted := make([]liquibaseConverted, 0, len(changes))
	for _, change := range changes {
		result, err := converter.convert(change)
		switch {
		case err == nil:
			converted = append(converted, result)
		case errors.Is(err, errLiquibaseNotConverted):
			r.unconverted = append(r.unconverted, change.display)
		case errors.Is(err, errLiquibaseNeedsDialect):
			r.needDialect = append(r.needDialect, change.display)
		case r.failed == nil:
			r.failed = err
		}
	}
	return converted
}

// err states the refusal, naming every construct that caused it.
//
// A change type Ptah does not convert is named first, because no dialect makes
// it convert; the typed changes that are waiting only for a dialect are named in
// the same message so that choosing one does not reveal a second refusal.
func (r *liquibaseRefusal) err(name, file string) error {
	switch {
	case len(r.unconverted) > 0:
		message := fmt.Sprintf(
			"liquibase changeset %s in %q uses %s, which is not SQL text and which Ptah does not "+
				"convert; rewrite it as a `sql` change or import it by hand",
			name, file, strings.Join(r.unconverted, ", "))
		if len(r.needDialect) > 0 {
			message += fmt.Sprintf("; %s would also need --dialect", strings.Join(r.needDialect, ", "))
		}
		return errors.New(message)
	case len(r.needDialect) > 0:
		return fmt.Errorf(
			"liquibase changeset %s in %q uses %s, which is not SQL text; Ptah renders it for one "+
				"target dialect, so pass --dialect to choose it, or rewrite it as a `sql` change",
			name, file, strings.Join(r.needDialect, ", "))
	case r.failed != nil:
		return fmt.Errorf("liquibase changeset %s in %q: %w", name, file, r.failed)
	default:
		return nil
	}
}

// liquibaseDerivedRollback is the rollback Liquibase derives when a changeset
// declares none: each change's inverse, last change first. One change that
// cannot be undone from the changelog alone leaves the whole changeset without
// a derived rollback, because undoing the rest would leave that one applied.
func liquibaseDerivedRollback(up []liquibaseConverted) string {
	inverse := make([]liquibaseConverted, 0, len(up))
	for _, converted := range slices.Backward(up) {
		if !converted.reversible {
			return ""
		}
		inverse = append(inverse, liquibaseConverted{up: converted.down})
	}
	return liquibaseJoin(inverse, func(converted liquibaseConverted) string { return converted.up })
}

func liquibaseJoin(converted []liquibaseConverted, text func(liquibaseConverted) string) string {
	parts := make([]string, 0, len(converted))
	for _, entry := range converted {
		if part := strings.TrimSpace(text(entry)); part != "" {
			parts = append(parts, part)
		}
	}
	return strings.Join(parts, "\n")
}

// ---------------------------------------------------------------------- XML

type liquibaseXMLRoot struct {
	XMLName xml.Name          `xml:"databaseChangeLog"`
	Nodes   []liquibaseXMLAny `xml:",any"`
}

// liquibaseXMLAny captures any element with its name, so an unconvertible one
// can be refused by the name the author wrote rather than by a position.
//
// Only id and author have a field. Every other attribute lands in Attrs, so the
// changeset walk hands each one to [liquibaseRunCondition] and a change's
// converter sees each one it does not read. A field of its own would hide an
// attribute from both.
type liquibaseXMLAny struct {
	XMLName xml.Name
	ID      string `xml:"id,attr"`
	Author  string `xml:"author,attr"`
	// Attrs are the attributes no field above claims, which on a change are
	// all of them.
	Attrs    []xml.Attr        `xml:",any,attr"`
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
	for _, attr := range node.Attrs {
		changeset.noteRunCondition(attr.Name.Local, attr.Value)
	}
	for _, child := range node.Children {
		switch child.XMLName.Local {
		case "rollback":
			changeset.rollbackDeclared = true
			changeset.rollback, changeset.rollbackRefusal = liquibaseXMLRollback(child)
		case "comment", "":
			// A comment documents the changeset, and an empty name is the
			// chardata between elements.
		default:
			// Liquibase reads a changeset attribute written as a nested element
			// as well, and <preConditions> is one of these.
			if !changeset.noteRunCondition(child.XMLName.Local, strings.TrimSpace(child.Text)) {
				changeset.changes = append(changeset.changes, liquibaseXMLChange(child))
			}
		}
	}
	return changeset
}

// liquibaseXMLRollback reads a rollback's changes: nested changes, or SQL
// written directly as its text.
//
// A rollback with attributes names another changeset whose rollback to reuse.
// Ptah cannot follow the reference, and reading the element as empty would
// write a down migration that undoes nothing, so it is refused.
func liquibaseXMLRollback(node liquibaseXMLAny) ([]liquibaseChange, string) {
	if len(node.Attrs) > 0 {
		names := make([]string, 0, len(node.Attrs))
		for _, attr := range node.Attrs {
			names = append(names, attr.Name.Local)
		}
		return nil, fmt.Sprintf("<rollback> refers to another changeset's rollback (%s); "+
			"Ptah does not follow the reference, so write the rollback out", strings.Join(names, ", "))
	}
	rollback := liquibaseXMLChange(node)
	var changes []liquibaseChange
	for _, child := range rollback.children {
		if child.name == "comment" {
			continue
		}
		changes = append(changes, child)
	}
	if len(changes) == 0 && rollback.text != "" {
		changes = append(changes, liquibaseSQLChange(rollback.text, "<rollback>"))
	}
	return changes, ""
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
	mapping, _ := value.(map[string]any)
	for _, key := range slices.Sorted(maps.Keys(mapping)) {
		changeset.noteRunCondition(key, liquibaseStringValue(value, key))
	}
	changes, _ := liquibaseMapValue(value, "changes")
	changeset.changes = liquibaseDocumentChanges(changes)
	// modifySql rewrites the SQL the changes produce, on the databases its own
	// dbms names. The XML walk reads it as a change, which Ptah does not
	// convert, and reading it the same way here refuses it by name rather than
	// dropping it.
	if modify, present := liquibaseMapValue(value, "modifySql"); present {
		changeset.changes = append(changeset.changes, liquibaseDocumentChange("modifySql", modify))
	}
	if rollback, present := liquibaseMapValue(value, "rollback"); present {
		changeset.rollbackDeclared = true
		changeset.rollback, changeset.rollbackRefusal = liquibaseDocumentRollback(rollback)
	}
	return changeset
}

// liquibaseDocumentChanges reads a `changes` list.
//
// An entry that is not a one-key mapping names no change type. It is kept as a
// change named for what it is, so the converter refuses it rather than the walk
// skipping it.
func liquibaseDocumentChanges(node any) []liquibaseChange {
	var changes []liquibaseChange
	for _, entry := range liquibaseList(node) {
		key, value, ok := liquibaseSingleKey(entry)
		if !ok {
			changes = append(changes, liquibaseChange{
				name: "", display: fmt.Sprintf("a change entry that names no change type (%v)", entry),
				attrs: make(map[string]string),
			})
			continue
		}
		changes = append(changes, liquibaseDocumentChange(key, value))
	}
	return changes
}

// liquibaseDocumentRollback reads a rollback, which is a SQL string, a list of
// changes and SQL strings, or a mapping.
//
// A mapping carrying changeSetId names another changeset whose rollback to
// reuse, and is refused for the reason the XML reader gives.
func liquibaseDocumentRollback(node any) ([]liquibaseChange, string) {
	if mapping, ok := node.(map[string]any); ok {
		if _, reference := mapping["changeSetId"]; reference {
			return nil, "rollback refers to another changeset's rollback (changeSetId); " +
				"Ptah does not follow the reference, so write the rollback out"
		}
	}
	var changes []liquibaseChange
	for _, entry := range liquibaseList(node) {
		// A rollback may be a bare SQL string rather than a list of changes,
		// which is how Liquibase's own examples write a one-liner.
		if text, isText := entry.(string); isText {
			if strings.TrimSpace(text) != "" {
				changes = append(changes, liquibaseSQLChange(text, "rollback"))
			}
			continue
		}
		changes = append(changes, liquibaseDocumentChanges(entry)...)
	}
	return changes, ""
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

// liquibaseStringValue reads one key as text. A key written with no value, which
// YAML decodes as nil, reads as empty, the way an empty XML attribute does.
func liquibaseStringValue(node any, key string) string {
	value, ok := liquibaseMapValue(node, key)
	if !ok || value == nil {
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
