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

	"ptah.run/internal/liquibaserun"
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
//   - a changeset attribute Ptah has no form for, such as `failOnError="false"`
//     or `runOrder`, and one Ptah does not read at all;
//   - a typed change with no target dialect, because its SQL does not exist
//     until a dialect is chosen;
//   - every change type outside [liquibaseConverters].
//
// Two changeset attributes convert rather than refuse. `runInTransaction="false"`
// becomes a no-transaction migration in both directions, since Liquibase runs
// the rollback in the same mode. `ignore="true"` means Liquibase never runs the
// changeset and never records it, so the changeset is left out and reported in
// [ParseResult.Skipped]. [liquibaserun.Attribute] says what every changeset
// attribute asks for.
//
// A target dialect does not make `dbms` convert. Keeping the changesets whose
// `dbms` names the target needs the name Liquibase gave that target, and for
// some targets that name depends on how Liquibase reached the server rather
// than on the server: YugabyteDB is `yugabytedb` with the Liquibase extension
// installed and `postgresql` without it, and Spanner is `cloudspanner` through
// its JDBC driver and `postgresql` through PGAdapter. [WithLiquibaseDBMS] takes
// that name from the caller instead, and with it `dbms` keeps or leaves out
// each changeset and change by Liquibase's rule, [liquibaserun.MatchDBMS].
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
	// run holds what decides WHETHER, or how often, the changeset runs rather
	// than what it does. It is kept apart from the changes because the remedy
	// differs: a change type can be rewritten as `sql`, and a run condition
	// cannot be rewritten at all -- it has no equivalent in a migration
	// directory.
	run liquibaserun.Changeset
}

// liquibaseDbmsTargeted are the change types that run only on the databases
// their own `dbms` attribute names, whatever their changeset says: the change
// types that implement Liquibase's DbmsTargetedChange. Liquibase reads `dbms` on
// no other change type, so there the attribute stays on the change and its
// converter refuses it as one it does not read.
var liquibaseDbmsTargeted = []string{"sql", "sqlFile", "insert", "createProcedure"}

// liquibaseChangeDBMS is one change with its `dbms` taken off, and what the
// `dbms` said.
type liquibaseChangeDBMS struct {
	change liquibaseChange
	// key and definition are the `dbms` attribute as written, when it selects
	// anything.
	key, definition string
	// runs reports that Liquibase runs the change on the database the caller
	// named. Without a name it is true, and a definition refuses the changeset.
	runs bool
}

// withoutRunCondition takes the `dbms` attribute off a change Liquibase targets
// by it. The converter then never sees the attribute. An empty one runs
// everywhere, which is what the converted change does. A non-empty one refuses
// the changeset when the caller named no database; when it named one, the
// change is kept or left out by Liquibase's rule, [liquibaserun.MatchDBMS],
// with the value compared as written, as Liquibase compares a change's dbms.
func (ch liquibaseChange) withoutRunCondition(shortName string) (liquibaseChangeDBMS, error) {
	result := liquibaseChangeDBMS{change: ch, runs: true}
	if !slices.Contains(liquibaseDbmsTargeted, ch.name) {
		return result, nil
	}
	result.change.attrs = make(map[string]string, len(ch.attrs))
	for _, key := range slices.Sorted(maps.Keys(ch.attrs)) {
		value := ch.attrs[key]
		if strings.EqualFold(key, "dbms") {
			if effect, _ := liquibaserun.Attribute(key, value); effect == liquibaserun.Selector || effect == liquibaserun.NoEffect {
				if effect == liquibaserun.Selector {
					result.key, result.definition = key, value
				}
				continue
			}
		}
		result.change.attrs[key] = value
	}
	if result.definition != "" && shortName != "" {
		runs, err := liquibaserun.MatchDBMS(result.definition, shortName)
		if err != nil {
			return liquibaseChangeDBMS{}, fmt.Errorf("%s %w", ch.display, err)
		}
		result.runs = runs
	}
	return result, nil
}

// liquibaseChangesDBMS is a list of changes sorted by their own `dbms`.
type liquibaseChangesDBMS struct {
	// kept are the changes that run, without their `dbms`.
	kept []liquibaseChange
	// selectors name the changes whose `dbms` refuses the changeset, when the
	// caller named no database.
	selectors []string
	// dropped are the changes Liquibase does not run on the named database.
	dropped []liquibaseChangeDBMS
}

// liquibaseChangesByDBMS applies [liquibaseChange.withoutRunCondition] to each
// change.
func liquibaseChangesByDBMS(changes []liquibaseChange, shortName string) (liquibaseChangesDBMS, error) {
	var sorted liquibaseChangesDBMS
	for _, change := range changes {
		result, err := change.withoutRunCondition(shortName)
		if err != nil {
			return liquibaseChangesDBMS{}, err
		}
		switch {
		case !result.runs:
			sorted.dropped = append(sorted.dropped, result)
		case result.definition != "" && shortName == "":
			sorted.selectors = append(sorted.selectors, change.display+" "+result.key)
			sorted.kept = append(sorted.kept, result.change)
		default:
			sorted.kept = append(sorted.kept, result.change)
		}
	}
	return sorted, nil
}

// liquibaseChangelogRead is what the XML, YAML and JSON changelogs of a source
// yielded.
type liquibaseChangelogRead struct {
	migrations []SourceMigration
	// consumed are the source files a sqlFile change read.
	consumed []string
	// skipped are the changesets left out because Liquibase never runs them.
	skipped []SkippedChangeset
}

// parseLiquibaseChangelogFiles reads XML, YAML and JSON changelogs in name
// order.
//
// Name order is the same rule the formatted-SQL reader applies for the same
// reason: absent a master changelog naming an order, the file name is the only
// stable one, and inventing a different one would reorder history.
func parseLiquibaseChangelogFiles(fsys fs.FS, names []string, parser liquibaseParser) (liquibaseChangelogRead, error) {
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)

	var read liquibaseChangelogRead
	for _, name := range sorted {
		content, err := fs.ReadFile(fsys, name)
		if err != nil {
			return liquibaseChangelogRead{}, fmt.Errorf("read %q: %w", name, err)
		}
		changesets, err := parseLiquibaseChangelog(name, content)
		if err != nil {
			return liquibaseChangelogRead{}, err
		}
		converter := &liquibaseConverter{fsys: fsys, file: name, dialect: parser.dialect, caps: parser.caps, dbms: parser.dbms}
		for _, changeset := range changesets {
			conversion, err := liquibaseMigrationFrom(converter, changeset)
			if err != nil {
				return liquibaseChangelogRead{}, err
			}
			read.skipped = append(read.skipped, conversion.skipped...)
			if conversion.imported {
				read.migrations = append(read.migrations, conversion.migration)
			}
		}
		read.consumed = append(read.consumed, converter.consumed...)
	}
	return read, nil
}

// liquibaseIgnored reports a changeset left out because its `ignore` is true.
// Liquibase never runs such a changeset and never records it, so leaving it out
// is what Liquibase does; saying so is what keeps it from being a silent drop.
func liquibaseIgnored(file, changeset string) SkippedChangeset {
	return SkippedChangeset{
		Path:      file,
		Changeset: changeset,
		Reason:    `ignore="true": Liquibase never runs this changeset`,
	}
}

// liquibaseOtherDatabase reports a changeset, or a change inside one, left out
// because its `dbms` does not select the database the caller named. Liquibase
// does not run it on that database, so the history there does not hold it.
func liquibaseOtherDatabase(file, changeset, change, definition, shortName string) SkippedChangeset {
	return SkippedChangeset{
		Path:      file,
		Changeset: changeset,
		Change:    change,
		Reason:    fmt.Sprintf("dbms=%q does not select %s", definition, shortName),
	}
}

// liquibaseConversion is what one changeset became.
type liquibaseConversion struct {
	migration SourceMigration
	// imported is false when the whole changeset was left out.
	imported bool
	// skipped names what was left out: the changeset, or changes inside it.
	skipped []SkippedChangeset
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
func liquibaseMigrationFrom(converter *liquibaseConverter, changeset liquibaseChangeSet) (liquibaseConversion, error) {
	name := liquibaseChangesetName(changeset.author, changeset.id)
	id := changeset.author + ":" + changeset.id
	if changeset.run.Skipped() {
		return liquibaseConversion{skipped: []SkippedChangeset{liquibaseIgnored(converter.file, id)}}, nil
	}
	if converter.dbms != "" {
		definition := changeset.run.DBMS()
		runs, err := changeset.run.ResolveDBMS(converter.dbms)
		if err != nil {
			return liquibaseConversion{}, fmt.Errorf("liquibase changeset %s in %q: %w", name, converter.file, err)
		}
		if !runs {
			return liquibaseConversion{skipped: []SkippedChangeset{
				liquibaseOtherDatabase(converter.file, id, "", definition, converter.dbms),
			}}, nil
		}
	}
	// A rollback change is sorted by its dbms like an up change: Liquibase
	// skips it on another database when it rolls back.
	up, err := liquibaseChangesByDBMS(changeset.changes, converter.dbms)
	if err != nil {
		return liquibaseConversion{}, fmt.Errorf("liquibase changeset %s in %q: %w", name, converter.file, err)
	}
	rollback, err := liquibaseChangesByDBMS(changeset.rollback, converter.dbms)
	if err != nil {
		return liquibaseConversion{}, fmt.Errorf("liquibase changeset %s in %q: %w", name, converter.file, err)
	}
	for _, selector := range slices.Concat(up.selectors, rollback.selectors) {
		changeset.run.AddSelector(selector)
	}
	if err := changeset.run.Err(name, converter.file); err != nil {
		return liquibaseConversion{}, err
	}
	if changeset.rollbackRefusal != "" {
		return liquibaseConversion{}, fmt.Errorf("liquibase changeset %s in %q: %s", name, converter.file, changeset.rollbackRefusal)
	}
	// Every change targets another database, so Liquibase runs no SQL for the
	// changeset on this one.
	if len(changeset.changes) > 0 && len(up.kept) == 0 {
		return liquibaseConversion{skipped: []SkippedChangeset{{
			Path: converter.file, Changeset: id,
			Reason: fmt.Sprintf("every change in it has a dbms that does not select %s", converter.dbms),
		}}}, nil
	}
	// Liquibase fills in a property reference in every value it reads, so one
	// anywhere in a change that runs refuses the changeset.
	if err := liquibaserun.PropertyReferencesErr(liquibaseChangeTexts(slices.Concat(up.kept, rollback.kept))...); err != nil {
		return liquibaseConversion{}, fmt.Errorf("liquibase changeset %s in %q %w", name, converter.file, err)
	}
	var skipped []SkippedChangeset
	for _, dropped := range slices.Concat(up.dropped, rollback.dropped) {
		skipped = append(skipped, liquibaseOtherDatabase(
			converter.file, id, dropped.change.display, dropped.definition, converter.dbms))
	}
	migration, err := liquibaseConvertChangeset(converter, changeset, up.kept, rollback.kept)
	if err != nil {
		return liquibaseConversion{}, err
	}
	return liquibaseConversion{migration: migration, imported: true, skipped: skipped}, nil
}

// liquibaseChangeTexts lists every value Liquibase reads in changes: each
// attribute value and text, down through the nested elements. A `comment`
// documents a change and runs nowhere, so it is not listed.
func liquibaseChangeTexts(changes []liquibaseChange) []string {
	var texts []string
	for _, change := range changes {
		if change.name == "comment" {
			continue
		}
		for _, key := range slices.Sorted(maps.Keys(change.attrs)) {
			if key != "comment" {
				texts = append(texts, change.attrs[key])
			}
		}
		texts = append(texts, change.text)
		texts = append(texts, liquibaseChangeTexts(change.children)...)
	}
	return texts
}

// liquibaseConvertChangeset converts a changeset's changes and rollback into a
// migration, once nothing about the changeset refuses it.
func liquibaseConvertChangeset(
	converter *liquibaseConverter,
	changeset liquibaseChangeSet,
	changes, rollback []liquibaseChange,
) (SourceMigration, error) {
	name := liquibaseChangesetName(changeset.author, changeset.id)
	// Liquibase runs a rollback in the changeset's own transaction mode.
	noTransaction := changeset.run.NoTransaction()

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
	return SourceMigration{
		Name: name, UpSQL: upSQL, DownSQL: downSQL,
		UpNoTransaction: noTransaction, DownNoTransaction: noTransaction,
		Path: converter.file, Changeset: changeset.author + ":" + changeset.id,
	}, nil
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
	Attrs   []xml.Attr        `xml:",any,attr"`
	Nodes   []liquibaseXMLAny `xml:",any"`
}

// liquibaseXMLAny captures any element with its name, so an unconvertible one
// can be refused by the name the author wrote rather than by a position.
//
// Only id and author have a field. Every other attribute lands in Attrs, so the
// changeset walk hands each one to [liquibaserun.Condition] and a change's
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

	// An attribute of the changelog itself -- `context`, `objectQuotingStrategy`
	// -- applies to every changeset in it, so each changeset starts from it.
	var inherited liquibaserun.Changeset
	for _, attr := range root.Attrs {
		if !liquibaseXMLDeclaration(attr) {
			inherited.Note(attr.Name.Local, attr.Value)
		}
	}
	var changesets []liquibaseChangeSet
	for _, node := range root.Nodes {
		switch node.XMLName.Local {
		case "changeSet":
			changesets = append(changesets, liquibaseXMLChangeSet(node, inherited))
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

func liquibaseXMLChangeSet(node liquibaseXMLAny, inherited liquibaserun.Changeset) liquibaseChangeSet {
	changeset := liquibaseChangeSet{id: node.ID, author: node.Author, run: inherited.Clone()}
	for _, attr := range node.Attrs {
		if !liquibaseXMLDeclaration(attr) {
			changeset.run.Note(attr.Name.Local, attr.Value)
		}
	}
	for _, child := range node.Children {
		switch child.XMLName.Local {
		case "rollback":
			changeset.rollbackDeclared = true
			changeset.rollback, changeset.rollbackRefusal = liquibaseXMLRollback(child)
		case "":
			// The chardata between elements.
		default:
			// Liquibase reads a changeset attribute written as a nested element
			// as well, and <preConditions>, <comment> and <validCheckSum> are
			// among them. Any other element is a change.
			if !changeset.run.NoteKnown(child.XMLName.Local, strings.TrimSpace(child.Text)) {
				changeset.changes = append(changeset.changes, liquibaseXMLChange(child))
			}
		}
	}
	return changeset
}

// liquibaseXMLDeclaration reports an attribute that belongs to XML rather than
// to Liquibase: a namespace declaration, or an attribute in another namespace
// such as xsi:schemaLocation.
func liquibaseXMLDeclaration(attr xml.Attr) bool {
	return attr.Name.Space != "" || attr.Name.Local == "xmlns"
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
		switch key {
		case "id", "author", "changes", "rollback", "modifySql":
			// The changeset's identity and contents, read below.
		default:
			changeset.run.Note(key, liquibaseStringValue(value, key))
		}
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
