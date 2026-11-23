package schemaprep

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/mysqlname"
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
}

// ReadRelation is a table, a view or a sequence a read found. An empty
// Database is [ReadNames.Session].
type ReadRelation struct {
	// Database is the database the relation is in.
	Database string
	// Name is the relation's name, as the read spells it.
	Name string
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
// where the document declares two tables the server reads as one.
//
// On a server whose folding no connection read, [catalog.NameFoldingUnknown],
// nothing is respelled. A name that would be is refused with an
// [*UnknownFoldingError]: whether the two spellings name one object decides
// the plan, and the server's setting answers it.
//
// What is respelled: the databases the document declares; the database and
// the name of a table, a sequence, an enum and a declared row; the qualified
// names of functions, views and materialized views; and every reference
// [placeReferences] lists. A body, an expression and a default keep what they
// were written with, and so does the identity of a feature object.
func RespellReadNames(database *schemamodel.Database, read ReadNames, folding catalog.NameFolding) (*schemamodel.Database, error) {
	if database == nil || folding == catalog.NameFoldingExact {
		return database, nil
	}
	r := newReadRespeller(read, folding)
	respelled := r.declarations(database)
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
	session   string
	databases []string
	// found is the relations the read found, by the database they are in.
	found map[string][]string
}

// newReadRespeller is a respeller for read on a server whose folding is
// folding.
func newReadRespeller(read ReadNames, folding catalog.NameFolding) *readRespeller {
	r := &readRespeller{
		match:     &nameMatch{unknown: folding != catalog.NameFoldingFolds},
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
	return r.match.respell("database", qualifier, r.databases)
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
		return view
	})
	respelled.MaterializedViews = mapSlice(database.MaterializedViews, func(view schemamodel.MaterializedView) schemamodel.MaterializedView {
		view.Name = r.databaseOnly(view.Name)
		return view
	})
	return respelled
}

// declaredRelations is the relations a reference may name: those the read
// found and those respelled declares, by database. Two tables of database
// that respelling made one are refused.
func (r *readRespeller) declaredRelations(database, respelled *schemamodel.Database) map[string][]string {
	relations := make(map[string][]string, len(r.found))
	for in, names := range r.found {
		relations[in] = slices.Clone(names)
	}
	spellings := make(map[string][]string, len(respelled.Tables))
	for i, table := range respelled.Tables {
		in, name := r.declaredAt(table.Schema, table.Name)
		key := in + "\x00" + name
		spellings[key] = distinctNames(append(spellings[key], database.Tables[i].Name))
		relations[in] = distinctNames(append(relations[in], name))
	}
	for _, table := range respelled.Tables {
		in, name := r.declaredAt(table.Schema, table.Name)
		if names := spellings[in+"\x00"+name]; len(names) > 1 {
			r.match.fail(&AmbiguousNameError{Kind: "table", Name: name, Matches: names})
		}
	}
	for _, view := range respelled.Views {
		in, name := r.declaredAt("", view.Name)
		relations[in] = distinctNames(append(relations[in], name))
	}
	for _, sequence := range respelled.Sequences {
		in, name := r.declaredAt(sequence.Schema, sequence.Name)
		relations[in] = distinctNames(append(relations[in], name))
	}
	return relations
}

// nameMatch matches the names of a document against the spellings a read
// found, and keeps the first refusal it meets. unknown is a server whose
// folding no connection read, on which a name is refused rather than
// respelled.
type nameMatch struct {
	unknown bool
	err     error
}

// respell is written as the one of spellings it names, under the rule of a
// server that folds names. A name spelled as one of them, or matching none,
// stays as written.
func (m *nameMatch) respell(kind, written string, spellings []string) string {
	trimmed := strings.TrimSpace(written)
	if trimmed == "" || slices.Contains(spellings, trimmed) {
		return written
	}
	folds := mysqlname.NamesOf(true)
	var matches []string
	for _, spelling := range spellings {
		if folds.Same(trimmed, spelling) {
			matches = append(matches, spelling)
		}
	}
	switch {
	case len(matches) == 0:
		return written
	case len(matches) > 1:
		m.fail(&AmbiguousNameError{Kind: kind, Name: trimmed, Matches: matches})
		return written
	case m.unknown:
		m.fail(&UnknownFoldingError{Kind: kind, Written: trimmed, Found: matches[0]})
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
}

// Error implements error.
func (e *AmbiguousNameError) Error() string {
	quoted := make([]string, len(e.Matches))
	for i, name := range e.Matches {
		quoted[i] = strconv.Quote(name)
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
}

// Error implements error.
func (e *UnknownFoldingError) Error() string {
	return fmt.Sprintf("%s %q and %q differ only in case, and whether they name one %s depends on the server's "+
		"lower_case_table_names, which this comparison does not know; compare against a database connection, "+
		"which reads the setting, or spell both names the same way",
		e.Kind, e.Written, e.Found, e.Kind)
}
