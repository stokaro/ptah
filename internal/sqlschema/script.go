package sqlschema

import (
	"context"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
)

// Converter is the side of [ConvertScript] that the owner of a target takes
// when its grammar reads the target's documents, as the YDB owner reads YQL.
// It parses each document, and converts the statements and ALTER TABLE
// operations of its own that the shared conversion does not model.
//
// It is in-process API between the owner and this package, not part of the
// SQL provider contract in core/sqlext: it hands the owner Go values and
// pointers into the model being built, which no protocol can carry.
type Converter interface {
	// Parse parses one document of the load into statements.
	Parse(ctx context.Context, file sqlext.SourceFile) (*ast.StatementList, error)
	// Statement converts statement into scope, and reports whether it did. A
	// statement it leaves is converted by the shared conversion, which refuses
	// one it does not model. It is asked about every statement, before the
	// shared conversion.
	Statement(scope Scope, statement ast.Node) (bool, error)
	// AlterOperation applies operation, of an ALTER TABLE the shared
	// conversion resolved to table, and reports whether it did. An operation
	// it leaves is refused.
	AlterOperation(scope Scope, table Table, operation *ast.ExtensionAlterOperation) (bool, error)
}

// AlterClause is what a payload of an owner's ALTER TABLE operation may
// implement to name the clause it is written as, such as `ADD CHANGEFEED
// updates`. A refusal of the operation, such as one for a table the document
// does not declare, names it; one that does not implement it is named
// `extension operation`.
type AlterClause interface {
	AlterClause() string
}

// Scope is what the shared conversion hands a [Converter] for one statement:
// the model the statement's file builds, what the earlier files of the load
// built, and the conversion's own rules for reading the names the statement
// writes. Its methods read through the dialect of the load, so a name the
// owner resolves lands where the shared conversion would put it.
type Scope struct {
	database *schemamodel.Database
	document *Document
	dialect  string
}

// Database is the model of the statement's file, which holds what the file
// declares. A declaration the statement adds goes here.
func (s Scope) Database() *schemamodel.Database {
	return s.database
}

// Base is what the earlier files of the load declared, empty for the first
// file. A change the statement makes to an object an earlier file declared is
// made in place, here.
func (s Scope) Base() *schemamodel.Database {
	return s.document.base
}

// DatabasePath is the database root the load's absolute paths are read
// against, or empty where the load names none.
func (s Scope) DatabasePath() string {
	return s.document.databasePath
}

// Name reads one written name, such as a column or a role, as the shared
// conversion reads it.
func (s Scope) Name(written string) string {
	return normalizeSQLIdentifier(s.dialect, written)
}

// TableName reads a written table name into the qualified name the model
// records it under.
func (s Scope) TableName(written string) string {
	return normalizeSQLTableReference(s.dialect, written)
}

// Table finds the table a written name reaches, in the statement's file or an
// earlier one, as an ALTER TABLE finds it. A name no declared table answers
// returns false, and a Table holding only the qualified name and the
// databases.
func (s Scope) Table(written string) (Table, bool) {
	target, found := findAlterTarget(s.database, s.document.base, written, s.dialect)
	return tableOf(target), found
}

// Column finds the column of table a written name reaches, or nil.
func (s Scope) Column(table Table, written string) *schemamodel.Field {
	return resolveColumn(table.Databases, table.StructName, written, s.dialect)
}

// View finds the view a written name reaches, in the statement's file or an
// earlier one, or nil.
func (s Scope) View(written string) *schemamodel.View {
	qualified := normalizeSQLTableReference(s.dialect, written)
	var views []*schemamodel.View
	var names []string
	for _, database := range declaringDatabases(s.database, s.document.base) {
		for i := range database.Views {
			views = append(views, &database.Views[i])
			names = append(names, database.Views[i].Name)
		}
	}
	if index := resolveDeclaredName(s.dialect, qualified, names); index >= 0 {
		return views[index]
	}
	return nil
}

// Table is a table a statement names, as the shared conversion resolved it.
type Table struct {
	// Declared is the table's declaration, in the database that holds it, and
	// nil when no declared table answers the name.
	Declared *schemamodel.Table
	// StructName joins the table to its columns, indexes and constraints.
	StructName string
	// Qualified is the table's qualified name in the model.
	Qualified string
	// Databases are where the table's objects may be: the statement's file
	// first, then the earlier files of the load.
	Databases []*schemamodel.Database
}

func tableOf(target alterTarget) Table {
	return Table{Declared: target.table, StructName: target.structName, Qualified: target.qualified, Databases: target.databases}
}

// convertOwned hands statement to the owner whose grammar parsed it. A
// document in the shared grammar has none.
func (d *Document) convertOwned(database *schemamodel.Database, statement ast.Node, dialect string) (bool, error) {
	if d.script == nil {
		return false, nil
	}
	return d.script.Statement(Scope{database: database, document: d, dialect: dialect}, statement)
}

// applyOwnedOperation hands operation to the owner whose grammar parsed it. A
// document in the shared grammar has none.
func (d *Document) applyOwnedOperation(database *schemamodel.Database, target alterTarget, operation *ast.ExtensionAlterOperation) (bool, error) {
	if d.script == nil {
		return false, nil
	}
	scope := Scope{database: database, document: d, dialect: target.sourcePlatform}
	return d.script.AlterOperation(scope, tableOf(target), operation)
}
