// Package sqldecl lists what one file of a SQL schema directory declares, in
// the terms of the directory's check that a later file does not declare again
// what an earlier one did: the objects the file leaves declared, the ones it
// declares under IF NOT EXISTS or OR REPLACE, and its CREATE TABLE and DROP
// TABLE statements in order. The schema file loader reads it for the files it
// parses, and an owner that reads a load in its own grammar answers it for the
// files it reads, so the check is one check for every SQL directory.
package sqldecl

import (
	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgpolicy"
)

// The object kinds a declaration names, spelled the way the directory check's
// refusal names them.
const (
	Schema           = "schema"
	Table            = "table"
	View             = "view"
	MaterializedView = "materialized view"
	Index            = "index"
	Trigger          = "trigger"
	Policy           = "policy"
	Type             = "type"
	Sequence         = "sequence"
	Extension        = "extension"
	Role             = "role"
)

// Of describes what one SQL file declares. db is what the file declares once it
// has run and statements what it runs. A SQL file creates its schemas, so a
// schema it declares counts as an object; see [objects].
func Of(path string, db *schemamodel.Database, statements *ast.StatementList) sqlext.FileDeclarations {
	listed := schemas(db)
	listed = append(listed, objects(db)...)
	return sqlext.FileDeclarations{Path: path, Objects: listed, Guards: guards(statements), Tables: tables(statements)}
}

// OfDocument describes what one file that is no script declares, an HCL file.
// Such a file only names the schema it is in, so no schema counts as an
// object.
func OfDocument(path string, db *schemamodel.Database) sqlext.FileDeclarations {
	return sqlext.FileDeclarations{Path: path, Objects: objects(db)}
}

// schemas lists the schemas a SQL file declares.
func schemas(db *schemamodel.Database) []sqlext.Declaration {
	if db == nil {
		return nil
	}
	listed := make([]sqlext.Declaration, 0, len(db.Schemas))
	for _, schema := range db.Schemas {
		listed = append(listed, declaration(Schema, schema.Name))
	}
	return listed
}

// declaration is one object as the file names it.
func declaration(kind, name string) sqlext.Declaration {
	return sqlext.Declaration{Kind: kind, Name: name}
}

// objects lists the named objects one parsed schema file declares, apart from
// its schemas.
//
// The schema split by format is measured rather than a convenience.
// `CREATE SCHEMA app` twice is an engine error, so a SQL directory that repeats
// one is exit 1 on the pinned binary; an HCL directory whose files each open
// with `schema "main" {}` is exit 0 there against a realm-scoped dev database,
// because HCL files are one document and not a script. Refusing that layout
// would remove a capability.
func objects(db *schemamodel.Database) []sqlext.Declaration {
	if db == nil {
		return nil
	}

	listed := make([]sqlext.Declaration, 0, len(db.Tables)+len(db.Indexes)+len(db.Views))
	for _, table := range db.Tables {
		listed = append(listed, declaration(Table, schemamodel.QualifyTableName(table.Schema, table.Name)))
	}
	for _, view := range db.Views {
		listed = append(listed, declaration(View, view.Name))
	}
	for _, view := range db.MaterializedViews {
		listed = append(listed, declaration(MaterializedView, view.Name))
	}
	for _, index := range db.Indexes {
		listed = append(listed, declaration(Index, qualifyWithOwner(indexOwner(index), index.Name)))
	}
	for _, trigger := range db.Triggers {
		listed = append(listed, declaration(Trigger, qualifyWithOwner(trigger.Table, trigger.Name)))
	}
	for _, policy := range db.RLSPolicies {
		listed = append(listed, declaration(Policy, qualifyWithOwner(policy.Table, policy.Name)))
	}
	// An HCL document hands its policies to the row-security owner as it is
	// read; a SQL file's are handed over once the whole directory is read.
	for _, ref := range db.FeatureObjects.Refs() {
		if ref.Kind == objectidentity.Kind(pgpolicy.PolicyKind) {
			table := schemamodel.QualifyTableName(ref.Schema.Authored(), ref.Parent.Source)
			listed = append(listed, declaration(Policy, qualifyWithOwner(table, ref.Name.Source)))
		}
	}
	listed = append(listed, declaredTypes(db)...)
	for _, sequence := range db.Sequences {
		listed = append(listed, declaration(Sequence, schemamodel.QualifyTableName(sequence.Schema, sequence.Name)))
	}
	for _, extension := range db.Extensions {
		listed = append(listed, declaration(Extension, extension.Name))
	}
	for _, role := range db.Roles {
		listed = append(listed, declaration(Role, role.Name))
	}
	return listed
}

// declaredTypes lists the four object kinds that share one type namespace, so a
// domain and an enum spelled the same collide the way the engine collides them.
func declaredTypes(db *schemamodel.Database) []sqlext.Declaration {
	listed := make([]sqlext.Declaration, 0,
		len(db.Enums)+len(db.Domains)+len(db.CompositeTypes)+len(db.Ranges))
	for _, enum := range db.Enums {
		listed = append(listed, declaration(Type, enum.Name))
	}
	for _, domain := range db.Domains {
		listed = append(listed, declaration(Type, schemamodel.QualifyTableName(domain.Schema, domain.Name)))
	}
	for _, composite := range db.CompositeTypes {
		listed = append(listed, declaration(Type, schemamodel.QualifyTableName(composite.Schema, composite.Name)))
	}
	for _, rangeType := range db.Ranges {
		listed = append(listed, declaration(Type, schemamodel.QualifyTableName(rangeType.Schema, rangeType.Name)))
	}
	return listed
}

// indexOwner names the table an index belongs to, preferring the qualified
// spelling the SQL loader records.
func indexOwner(index schemamodel.Index) string {
	if index.TableName != "" {
		return index.TableName
	}
	return index.StructName
}

// qualifyWithOwner prefixes a table-scoped object with its table.
//
// Index and trigger names are unique per TABLE on MySQL and MariaDB, so two
// tables carrying an `idx_name` each is legal there and must not be read as a
// redeclaration.
func qualifyWithOwner(owner, name string) string {
	if owner == "" {
		return name
	}
	return owner + "." + name
}

// guards lists the objects a SQL file declares under its own "may already
// exist" clause.
func guards(statements *ast.StatementList) []sqlext.Declaration {
	if statements == nil {
		return nil
	}
	var guarded []sqlext.Declaration
	for _, statement := range statements.Statements {
		kind, name, ok := guardedDeclaration(statement)
		if !ok {
			continue
		}
		guarded = append(guarded, declaration(kind, name))
	}
	return guarded
}

// guardedDeclaration reports whether one statement declares an object under a
// guard, and which object.
//
// The nodes listed here are exactly those whose guard Ptah's SQL parser keeps.
// A node whose guard it does not model must not appear: reading its absence as
// "unguarded" would refuse a script the engine runs.
func guardedDeclaration(statement ast.Node) (kind, name string, guarded bool) {
	switch node := statement.(type) {
	case *ast.CreateTableNode:
		return Table, node.Name, node.IfNotExists
	case *ast.CreateSchemaNode:
		return Schema, node.Name, node.IfNotExists
	case *ast.IndexNode:
		return Index, node.Name, node.IfNotExists
	case *ast.ExtensionNode:
		return Extension, node.Name, node.IfNotExists
	case *ast.CreateSequenceNode:
		return Sequence, node.Name, node.IfNotExists
	case *ast.CreateViewNode:
		return View, node.Name, node.Replace
	case *ast.CreateTriggerNode:
		return Trigger, node.Name, node.Replace
	case *ast.CreatePolicyNode:
		return Policy, node.Name, node.Replace
	}
	return "", "", false
}

// tables lists a SQL file's CREATE TABLE and DROP TABLE statements in the
// order the file runs them.
func tables(statements *ast.StatementList) []sqlext.TableStatement {
	if statements == nil {
		return nil
	}
	var listed []sqlext.TableStatement
	for _, statement := range statements.Statements {
		switch node := statement.(type) {
		case *ast.CreateTableNode:
			listed = append(listed, sqlext.TableStatement{Name: node.Name, Guarded: node.IfNotExists})
		case *ast.DropTableNode:
			for _, name := range droppedTableNames(node) {
				listed = append(listed, sqlext.TableStatement{Name: name, Drop: true})
			}
		}
	}
	return listed
}

// droppedTableNames answers every table a DROP TABLE names.
func droppedTableNames(node *ast.DropTableNode) []string {
	if len(node.Names) > 0 {
		return node.Names
	}
	return []string{node.Name}
}
