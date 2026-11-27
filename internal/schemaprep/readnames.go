package schemaprep

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlqualifier"
	"ptah.run/internal/tableref"
)

// ReadNames is what a read found, which the database and table names of a
// document are matched against.
type ReadNames struct {
	// Session is the database a name written without one is in: the session's
	// database on a connection to one database, and empty on a whole server.
	Session string
	// Databases are the databases the read found.
	Databases []string
	// Relations are the tables, views and sequences the read found.
	Relations []ReadRelation
	// Dialect is the dialect a view body is read in, for its quoting. Empty
	// leaves every body as written.
	Dialect string
}

// ReadRelation is a table, a view or a sequence a read found. An empty
// Database is [ReadNames.Session].
type ReadRelation struct {
	// Database is the database the relation is in.
	Database string
	// Name is the relation's name, as the read spells it.
	Name string
}

// ReadNamesOf is what database, a read, found: its schemas and the schema and
// the name of each table, view and sequence. session is the schema a name
// written without one is in, empty on a whole MySQL-family server, and
// dialect the dialect a view body is read in. A nil database found nothing.
func ReadNamesOf(database *catalog.Database, session, dialect string) ReadNames {
	read := ReadNames{Session: session, Dialect: dialect}
	if database == nil {
		return read
	}
	for _, schema := range database.Schemas {
		read.Databases = append(read.Databases, schema.Name)
	}
	found := func(schema, name string) {
		read.Databases = append(read.Databases, schema)
		read.Relations = append(read.Relations, ReadRelation{Database: schema, Name: name})
	}
	for _, table := range database.Tables {
		found(table.Schema, table.Name)
	}
	for _, view := range database.Views {
		found(view.Schema, view.Name)
	}
	for _, sequence := range database.Sequences {
		found(sequence.Schema, sequence.Name)
	}
	return read
}

// DeclaredNamesOf is what a document declares, read as [ReadNamesOf] reads a
// database: its schemas and the schema and the name of each table, view and
// sequence, a qualified name standing in the schema it names. session and
// dialect are as [ReadNamesOf] takes them. A nil document declares nothing.
//
// It stands for the side a comparison of two documents holds no read of, so
// that a name of the other document is matched against it as against a read.
func DeclaredNamesOf(database *schemamodel.Database, session, dialect string) ReadNames {
	read := ReadNames{Session: session, Dialect: dialect}
	if database == nil {
		return read
	}
	for _, schema := range database.Schemas {
		read.Databases = append(read.Databases, schema.Name)
	}
	declared := func(schema, name string) {
		if ref, ok := tableref.Parse(name); ok && ref.Qualified {
			schema, name = ref.Schema, ref.Name
		}
		if schema = strings.TrimSpace(schema); schema != "" {
			read.Databases = append(read.Databases, schema)
		}
		read.Relations = append(read.Relations, ReadRelation{Database: schema, Name: strings.TrimSpace(name)})
	}
	for _, table := range database.Tables {
		declared(table.Schema, table.Name)
	}
	for _, view := range database.Views {
		declared("", view.Name)
	}
	for _, sequence := range database.Sequences {
		declared(sequence.Schema, sequence.Name)
	}
	return read
}

// RespellReadNames returns a copy of database in which every database and
// table name the document spells in another case than the read does is
// spelled as the read does. folding is how the server compares the names, from
// [catalog.ServerInfo.NameFolding]. database is not modified, a nil database
// is returned as it is, and so is any database on a server that keeps names
// apart by case.
//
// A server that folds names, a MySQL or MariaDB server started with
// lower_case_table_names 1 or 2, describes every object under its own
// spelling: a table of the session's database carries no database, and the
// reader respells each grant it reads to DATABASE(). A schema file may spell a
// name in any case. Without this step, measured on MySQL 26.10.0 and MariaDB
// 12.3.3 with lower_case_table_names=1:
//
//   - `REVOKE SELECT ON FA.items` or `REVOKE SELECT ON Items` against a
//     connection to fa names no grant the read found, nothing is revoked, and
//     the role keeps SELECT;
//   - on a whole server, a document that declares FA beside fa plans CREATE
//     SCHEMA IF NOT EXISTS FA, which the server reads as fa and skips, and
//     then DROP DATABASE fa;
//   - a declared GRANT on FA.items or Items is revoked and granted again on
//     every run, and a foreign key that references either is dropped and
//     added again;
//   - a declared table Items beside items plans CREATE TABLE Items and DROP
//     TABLE items.
//
// A database name is matched against the databases the read found. A table
// name is matched against the tables, views and sequences the read found in
// its database, and a reference also against the tables the document
// declares, so a foreign key may spell a declared table in another case. A
// name matches under [mysqlname.Names]: a name the read or the document
// spells the same way stays as written, and a name that matches one spelling
// only without regard to case takes that spelling.
//
// A server that folds names holds no two databases or tables of one database
// that differ only in case. Where a name matches more than one, the step
// refuses with an [*AmbiguousNameError] rather than pick one, and so it does
// where the document declares two databases, or two tables, views or sequences
// of one database, that the server reads as one: planned, the second CREATE
// fails on the object the first made.
//
// On a server whose folding no connection read, [catalog.NameFoldingUnknown],
// nothing is respelled. A name that would be is refused with an
// [*UnknownFoldingError], and so are two declarations that differ only in
// case: whether the two spellings name one object decides the plan, and the
// server's setting answers it.
//
// What is respelled: the databases the document declares; the database and
// the name of a table, a sequence, an enum and a declared row; the qualified
// names of functions, views and materialized views; every reference
// [placeReferences] lists; and each database qualifier in the body of a view
// and a materialized view. The server stores a view body with its own
// database spelled as DATABASE() spells it, so without the last a body that
// names `APP4.t` for a view of app4 reads as another query than the one the
// server holds, and every plan replaces the view. A qualifier is respelled
// only on a server known to fold names: where no connection read the setting,
// a body keeps what it was written with and is not refused for it. Any other
// part of a body, an expression and a default keep what they were written
// with, and so does the identity of a feature object.
func RespellReadNames(database *schemamodel.Database, read ReadNames, folding catalog.NameFolding) (*schemamodel.Database, error) {
	return RespellReadNamesBy(database, read, FoldingRule(folding))
}

// RespellReadNamesBy is [RespellReadNames] under rule: a name that matches
// one spelling only by rule takes that spelling, where rule says a connection
// answered, and is refused where it says none did. A database the rule
// compares as bytes is returned as it is. A view body's qualifiers are
// respelled only where rule says the server stores them as it holds the
// names; a SQL Server body keeps what it was written with, since the server
// stores the body as written. A refusal names a schema as rule names it: a
// database on the MySQL family, a schema on SQL Server.
func RespellReadNamesBy(database *schemamodel.Database, read ReadNames, rule NameRule) (*schemamodel.Database, error) {
	if database == nil || rule.Exact() {
		return database, nil
	}
	r := newReadRespeller(read, rule)
	respelled := r.declarations(database)
	r.declaredDatabases(database, &respelled)
	relation := r.qualifiedBy(r.declaredRelations(database, &respelled))
	respelled.ManagedData = mapSlice(database.ManagedData, func(data schemamodel.ManagedData) schemamodel.ManagedData {
		data.Schema = r.database(data.Schema)
		data.Table = relation(data.Table)
		return data
	})
	placeReferences(&respelled, referenceMaps{
		relation: relation, sequence: relation, routine: r.routine, anyDeclared: r.routine,
		database: r.database, object: r.databaseOnly,
	})
	if r.match.err != nil {
		return nil, r.match.err
	}
	return &respelled, nil
}

// readRespeller respells the names of one document against one read.
type readRespeller struct {
	match     *nameMatch
	dialect   string
	session   string
	databases []string
	// found is the relations the read found, by the database they are in.
	found map[string][]string
}

// newReadRespeller is a respeller for read on a server whose names compare
// by rule.
func newReadRespeller(read ReadNames, rule NameRule) *readRespeller {
	r := &readRespeller{
		match:     &nameMatch{rule: rule},
		dialect:   read.Dialect,
		session:   read.Session,
		databases: distinctNames(append([]string{read.Session}, read.Databases...)),
		found:     make(map[string][]string),
	}
	for _, relation := range read.Relations {
		in := strings.TrimSpace(relation.Database)
		if in == "" {
			in = read.Session
		}
		r.found[in] = distinctNames(append(r.found[in], relation.Name))
	}
	return r
}

// database respells a database qualifier among the databases the read found.
func (r *readRespeller) database(qualifier string) string {
	return r.match.respell(r.match.rule.schema, qualifier, r.databases)
}

// in is the database a qualifier names once respelled, and the session's
// database for an empty one.
func (r *readRespeller) in(qualifier string) string {
	if trimmed := strings.TrimSpace(qualifier); trimmed != "" {
		return r.database(trimmed)
	}
	return r.session
}

// qualifiedBy is a reference map that respells the database of a name and the
// name itself among relations, the relations of each database.
func (r *readRespeller) qualifiedBy(relations map[string][]string) func(string) string {
	return func(name string) string {
		ref, ok := tableref.Parse(name)
		if !ok {
			return name
		}
		if !ref.Qualified {
			respelled := r.match.respell("table", ref.Name, relations[r.session])
			if respelled == ref.Name {
				return name
			}
			return respelled
		}
		schema := r.database(ref.Schema)
		respelled := r.match.respell("table", ref.Name, relations[r.in(schema)])
		if schema == ref.Schema && respelled == ref.Name {
			return name
		}
		return tableref.Canonical(schema, respelled)
	}
}

// databaseOnly respells the database a qualified name names, and leaves the
// name as written.
func (r *readRespeller) databaseOnly(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok || !ref.Qualified {
		return name
	}
	schema := r.database(ref.Schema)
	if schema == ref.Schema {
		return name
	}
	return tableref.Canonical(schema, ref.Name)
}

// relationIn respells a declared name among the relations the read found: in
// the database a qualified name names, and otherwise in schema, or in the
// session's database where schema is empty.
func (r *readRespeller) relationIn(schema, name string) string {
	if ref, ok := tableref.Parse(name); ok && ref.Qualified {
		return r.qualifiedBy(r.found)(name)
	}
	return r.match.respell("table", name, r.found[r.in(schema)])
}

// declaredAt is the database in and the name a declared relation has.
func (r *readRespeller) declaredAt(schema, written string) (in, name string) {
	if ref, ok := tableref.Parse(written); ok && ref.Qualified {
		return r.in(ref.Schema), ref.Name
	}
	return r.in(schema), strings.TrimSpace(written)
}

// writtenAt is the database in and the name a declared relation is written
// with, nothing respelled: the session's database for a name written with
// none.
func (r *readRespeller) writtenAt(schema, written string) (in, name string) {
	if ref, ok := tableref.Parse(written); ok && ref.Qualified {
		return ref.Schema, ref.Name
	}
	if trimmed := strings.TrimSpace(schema); trimmed != "" {
		return trimmed, strings.TrimSpace(written)
	}
	return r.session, strings.TrimSpace(written)
}

// routine respells the database of a routine written as `name` or
// `name(arguments)`.
func (r *readRespeller) routine(reference string) string {
	trimmed := strings.TrimSpace(reference)
	name, arguments, called := strings.Cut(trimmed, "(")
	name = strings.TrimSpace(name)
	respelled := r.databaseOnly(name)
	if respelled == name {
		return reference
	}
	if !called {
		return respelled
	}
	return respelled + "(" + arguments
}

// declarations is a copy of database with each object it declares respelled
// against the read.
func (r *readRespeller) declarations(database *schemamodel.Database) schemamodel.Database {
	respelled := *database
	respelled.Schemas = mapSlice(database.Schemas, func(schema schemamodel.Schema) schemamodel.Schema {
		schema.Name = r.database(schema.Name)
		return schema
	})
	respelled.Tables = mapSlice(database.Tables, func(table schemamodel.Table) schemamodel.Table {
		table.Schema = r.database(table.Schema)
		table.Name = r.relationIn(table.Schema, table.Name)
		return table
	})
	respelled.Sequences = mapSlice(database.Sequences, func(sequence schemamodel.Sequence) schemamodel.Sequence {
		sequence.Schema = r.database(sequence.Schema)
		sequence.Name = r.relationIn(sequence.Schema, sequence.Name)
		return sequence
	})
	respelled.Enums = mapSlice(database.Enums, func(enum schemamodel.Enum) schemamodel.Enum {
		enum.Schema = r.database(enum.Schema)
		enum.Name = r.databaseOnly(enum.Name)
		return enum
	})
	respelled.Functions = mapSlice(database.Functions, func(function schemamodel.Function) schemamodel.Function {
		function.Name = r.databaseOnly(function.Name)
		return function
	})
	respelled.Views = mapSlice(database.Views, func(view schemamodel.View) schemamodel.View {
		view.Name = r.relationIn("", view.Name)
		view.Body = r.body(view.Body)
		return view
	})
	respelled.MaterializedViews = mapSlice(database.MaterializedViews, func(view schemamodel.MaterializedView) schemamodel.MaterializedView {
		view.Name = r.databaseOnly(view.Name)
		return view
	})
	return respelled
}

// body is a view body with each database qualifier the read found in another
// case spelled as the read spells it, on a server known to fold names. A
// qualifier that matches no database the read found, or more than one, keeps
// what it was written with: a body is compared, never refused, for how it
// spells a name.
func (r *readRespeller) body(body string) string {
	if !r.match.rule.known || !r.match.rule.bodies || r.dialect == "" {
		return body
	}
	return sqlqualifier.RespellViewBody(body, r.dialect, func(name string) string {
		if slices.Contains(r.databases, name) {
			return name
		}
		var matches []string
		for _, database := range r.databases {
			if r.match.rule.same(name, database) {
				matches = append(matches, database)
			}
		}
		if len(matches) != 1 {
			return name
		}
		return matches[0]
	})
}

// declaredRelations is the relations a reference may name: those the read
// found and those respelled declares, by database. Two tables, views or
// sequences of one database that database spells in two ways a server that
// folds names reads as one are refused; such a server keeps the three kinds
// in one namespace.
func (r *readRespeller) declaredRelations(database, respelled *schemamodel.Database) map[string][]string {
	relations := make(map[string][]string, len(r.found))
	for in, names := range r.found {
		relations[in] = slices.Clone(names)
	}
	declared := &declaredSpellings{kind: "table", key: r.match.rule.key}
	declare := func(schema, name, writtenSchema, written string) {
		in, respelledName := r.declaredAt(schema, name)
		relations[in] = distinctNames(append(relations[in], respelledName))
		writtenIn, writtenName := r.writtenAt(writtenSchema, written)
		declared.add(declaredSpelling{in: writtenIn, name: writtenName, respelledIn: in, respelled: respelledName})
	}
	for i, table := range respelled.Tables {
		declare(table.Schema, table.Name, database.Tables[i].Schema, database.Tables[i].Name)
	}
	for i, view := range respelled.Views {
		declare("", view.Name, "", database.Views[i].Name)
	}
	for i, sequence := range respelled.Sequences {
		declare(sequence.Schema, sequence.Name, database.Sequences[i].Schema, database.Sequences[i].Name)
	}
	declared.refuse(r.match)
	return relations
}

// declaredDatabases refuses two databases database declares that a server
// that folds names reads as one.
func (r *readRespeller) declaredDatabases(database, respelled *schemamodel.Database) {
	declared := &declaredSpellings{kind: r.match.rule.schema, key: r.match.rule.key}
	for i, schema := range respelled.Schemas {
		declared.add(declaredSpelling{name: strings.TrimSpace(database.Schemas[i].Name), respelled: strings.TrimSpace(schema.Name)})
	}
	declared.refuse(r.match)
}

// declaredSpelling is one name a document declares: the database it is in and
// the name, as written and once respelled. A database declares no database.
type declaredSpelling struct {
	in, name               string
	respelledIn, respelled string
}

// declaredSpellings groups the names a document declares of one kind by the
// key the server's rule gives them, to find two spellings of one name.
type declaredSpellings struct {
	kind   string
	key    func(string) string
	keys   []string
	groups map[string][]declaredSpelling
}

// add records spelling, once for each way it is written.
func (d *declaredSpellings) add(spelling declaredSpelling) {
	if spelling.name == "" {
		return
	}
	key := d.key(spelling.in) + "\x00" + d.key(spelling.name)
	if d.groups == nil {
		d.groups = make(map[string][]declaredSpelling)
	}
	group := d.groups[key]
	if len(group) == 0 {
		d.keys = append(d.keys, key)
	}
	if !slices.ContainsFunc(group, func(kept declaredSpelling) bool {
		return kept.in == spelling.in && kept.name == spelling.name
	}) {
		d.groups[key] = append(group, spelling)
	}
}

// refuse records in match a refusal for each name written in more than one
// way: an [*UnknownFoldingError] where the folding is unknown, which names the
// first two spellings, and an [*AmbiguousNameError] naming every spelling
// otherwise.
func (d *declaredSpellings) refuse(match *nameMatch) {
	for _, key := range d.keys {
		group := d.groups[key]
		if len(group) < 2 {
			continue
		}
		written := make([]string, len(group))
		for i, spelling := range group {
			written[i] = shownIn(group, spelling.in, spelling.name)
		}
		if !match.rule.known {
			match.fail(&UnknownFoldingError{Kind: d.kind, Written: written[1], Found: written[0], Setting: match.rule.setting})
			continue
		}
		first := group[0]
		match.fail(&AmbiguousNameError{
			Kind: d.kind, Name: shownIn(group, first.respelledIn, first.respelled), Matches: written, Reading: match.rule.reading,
		})
	}
}

// shownIn is name in database in as a refusal names it: alone where every
// spelling of group is written in one database, and qualified otherwise.
func shownIn(group []declaredSpelling, in, name string) string {
	for _, other := range group[1:] {
		if other.in != group[0].in {
			return tableref.Canonical(in, name)
		}
	}
	return name
}

// RespellName is written as the one of spellings rule reads as the same
// schema name, as [RespellReadNamesBy] respells a schema. A name spelled as
// one of spellings, or matching none, stays as written. A name matching more
// than one spelling is refused with an [*AmbiguousNameError], and one that
// would be respelled under a rule no connection answered with an
// [*UnknownFoldingError].
func RespellName(rule NameRule, written string, spellings []string) (string, error) {
	if rule.Exact() {
		return written, nil
	}
	match := &nameMatch{rule: rule}
	respelled := match.respell(rule.schema, written, distinctNames(spellings))
	if match.err != nil {
		return "", match.err
	}
	return respelled, nil
}

// nameMatch matches the names of a document against the spellings a read
// found under a rule, and keeps the first refusal it meets. A rule no
// connection answered refuses a name rather than respell it.
type nameMatch struct {
	rule NameRule
	err  error
}

// respell is written as the one of spellings it names under the rule. A name
// spelled as one of them, or matching none, stays as written.
func (m *nameMatch) respell(kind, written string, spellings []string) string {
	trimmed := strings.TrimSpace(written)
	if trimmed == "" || slices.Contains(spellings, trimmed) {
		return written
	}
	var matches []string
	for _, spelling := range spellings {
		if m.rule.same(trimmed, spelling) {
			matches = append(matches, spelling)
		}
	}
	switch {
	case len(matches) == 0:
		return written
	case len(matches) > 1:
		m.fail(&AmbiguousNameError{Kind: kind, Name: trimmed, Matches: matches, Reading: m.rule.reading})
		return written
	case !m.rule.known:
		m.fail(&UnknownFoldingError{Kind: kind, Written: trimmed, Found: matches[0], Setting: m.rule.setting})
		return written
	default:
		return matches[0]
	}
}

// fail keeps err unless a refusal is already kept.
func (m *nameMatch) fail(err error) {
	if m.err == nil {
		m.err = err
	}
}

// distinctNames is names trimmed, without empty names and repeats, in order.
func distinctNames(names []string) []string {
	distinct := make([]string, 0, len(names))
	for _, name := range names {
		if name = strings.TrimSpace(name); name != "" && !slices.Contains(distinct, name) {
			distinct = append(distinct, name)
		}
	}
	return distinct
}

// AmbiguousNameError refuses a database or table name that matches more than
// one spelling, on a server that compares database and table names without
// regard to case. Such a server holds no two databases, and no two tables of
// one database, that differ only in case, so no spelling is chosen.
type AmbiguousNameError struct {
	// Kind is what the name names: "database" or "table".
	Kind string
	// Name is the name that matches more than one spelling.
	Name string
	// Matches are the spellings Name matches, in the order the read and the
	// document gave them.
	Matches []string
	// Reading is how the server compares the names, where it does not
	// compare them without regard to case: "under the database collation" on
	// SQL Server. Empty is the MySQL family's rule.
	Reading string
}

// Error implements error.
func (e *AmbiguousNameError) Error() string {
	quoted := make([]string, len(e.Matches))
	for i, name := range e.Matches {
		quoted[i] = strconv.Quote(name)
	}
	if e.Reading != "" {
		return fmt.Sprintf("%s %q matches %s, names the server reads as one %s; no spelling is chosen",
			e.Kind, e.Name, strings.Join(quoted, " and "), e.Reading)
	}
	return fmt.Sprintf("%s %q matches %s, names that differ only in case, "+
		"on a server that compares database and table names without regard to case; no spelling is chosen",
		e.Kind, e.Name, strings.Join(quoted, " and "))
}

// UnknownFoldingError refuses a database or table name that matches a name
// the read found, or one the document declares, only without regard to case,
// in a comparison that does not know how the server compares names.
type UnknownFoldingError struct {
	// Kind is what the names name: "database" or "table".
	Kind string
	// Written is the name as the document spells it.
	Written string
	// Found is the spelling it matches without regard to case.
	Found string
	// Setting is what decides whether the two are one name, where it is not
	// lower_case_table_names: "the database collation" on SQL Server. Empty
	// is the MySQL family's setting.
	Setting string
}

// Error implements error.
func (e *UnknownFoldingError) Error() string {
	if e.Setting != "" {
		return fmt.Sprintf("%s %q and %q differ only in case, and whether they name one %s depends on %s, "+
			"which this comparison does not know; compare against a database connection, which reads it, "+
			"or spell both names the same way",
			e.Kind, e.Written, e.Found, e.Kind, e.Setting)
	}
	return fmt.Sprintf("%s %q and %q differ only in case, and whether they name one %s depends on the server's "+
		"lower_case_table_names, which this comparison does not know; compare against a database connection, "+
		"which reads the setting, or spell both names the same way",
		e.Kind, e.Written, e.Found, e.Kind)
}
