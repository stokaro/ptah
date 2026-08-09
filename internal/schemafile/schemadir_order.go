package schemafile

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
)

// A schema DIRECTORY is an ordered script, not a set of declarations.
//
// The pinned Atlas community binary v1.3.0 reads one by executing every file in
// filename order against the dev database, so a file that declares an object an
// earlier file already declared is an engine error rather than a merge:
//
//	read state from "2_b.sql": executing statement:
//	"CREATE TABLE users (id INTEGER PRIMARY KEY, extra TEXT);":
//	table users already exists
//
// Merging the parsed files instead produced a desired state that appears in
// NEITHER file and exited 0 where that binary exits 1 -- on `schema diff` and,
// worse, on `schema apply`, which really wrote it. The declaration ledger below
// is what makes the second file measurable against the first.
//
// The ledger is not the engine, and two behaviors follow from that:
//
//   - A guard is honored rather than executed. `CREATE TABLE IF NOT EXISTS`
//     twice is exit 0 on that binary, measured, so a guarded redeclaration is
//     admitted instead of refused. Which of the two definitions survives the
//     merge is unchanged by this file and still differs from that binary; the
//     exit code, which is what the compatibility rule is about, now agrees.
//   - An ALTER is not replayed. A file that alters what an earlier file created
//     declares nothing, so it neither refuses nor contributes -- the same as
//     before this file existed.

// Object kinds, spelled the way the refusal names them.
const (
	kindSchema           = "schema"
	kindTable            = "table"
	kindView             = "view"
	kindMaterializedView = "materialized view"
	kindIndex            = "index"
	kindTrigger          = "trigger"
	kindPolicy           = "policy"
	kindType             = "type"
	kindSequence         = "sequence"
	kindExtension        = "extension"
	kindRole             = "role"
)

// objectKey identifies one named object across the files of a schema directory.
//
// Functions are deliberately absent from every producer below. A PostgreSQL
// function's identity includes its argument types, so two overloads share a
// name legally, and `CREATE OR REPLACE FUNCTION` is not modeled as a guard --
// keying them by name would refuse directories the pinned binary accepts.
type objectKey struct {
	kind string
	name string // lower-cased, qualified exactly as the file declared it
}

// guardKey identifies a declaration that carries its own "this may already
// exist" clause: IF NOT EXISTS, or OR REPLACE.
//
// It is keyed on the UNQUALIFIED name while [objectKey] is keyed on the
// qualified one, and the asymmetry is deliberate. A guard that matches too
// widely can only fail to refuse something; a collision key that matches too
// widely refuses a directory the pinned binary accepts, and two schemas each
// holding a `users` table is an ordinary layout rather than a conflict.
type guardKey struct {
	kind string
	base string
}

// declaredObject is one object a file declares, carrying both the identity it
// is compared by and the spelling the refusal prints.
type declaredObject struct {
	key     objectKey
	display string
}

func newDeclaredObject(kind, name string) declaredObject {
	return declaredObject{
		key:     objectKey{kind: kind, name: strings.ToLower(name)},
		display: name,
	}
}

// schemaDirFormat names the format a schema directory's files are written in.
// It decides one thing here: whether a repeated schema declaration is a
// redeclaration.
type schemaDirFormat int

const (
	// schemaDirFormatSQL is a directory of .sql files, executed in order.
	schemaDirFormatSQL schemaDirFormat = iota
	// schemaDirFormatHCL is a directory of .hcl files, read as one document.
	schemaDirFormatHCL
)

// declaredObjects lists the named objects one parsed schema file declares.
//
// The schema split by format is measured rather than a convenience.
// `CREATE SCHEMA app` twice is an engine error, so a SQL directory that repeats
// one is exit 1 on the pinned binary; an HCL directory whose files each open
// with `schema "main" {}` is exit 0 there against a realm-scoped dev database,
// because HCL files are one document and not a script. Refusing that layout
// would remove a capability.
func declaredObjects(db *goschema.Database, format schemaDirFormat) []declaredObject {
	if db == nil {
		return nil
	}

	objects := make([]declaredObject, 0, len(db.Tables)+len(db.Indexes)+len(db.Views))
	if format == schemaDirFormatSQL {
		for _, schema := range db.Schemas {
			objects = append(objects, newDeclaredObject(kindSchema, schema.Name))
		}
	}
	for _, table := range db.Tables {
		objects = append(objects, newDeclaredObject(kindTable, goschema.QualifyTableName(table.Schema, table.Name)))
	}
	for _, view := range db.Views {
		objects = append(objects, newDeclaredObject(kindView, view.Name))
	}
	for _, view := range db.MaterializedViews {
		objects = append(objects, newDeclaredObject(kindMaterializedView, view.Name))
	}
	for _, index := range db.Indexes {
		objects = append(objects, newDeclaredObject(kindIndex, qualifyWithOwner(indexOwner(index), index.Name)))
	}
	for _, trigger := range db.Triggers {
		objects = append(objects, newDeclaredObject(kindTrigger, qualifyWithOwner(trigger.Table, trigger.Name)))
	}
	for _, policy := range db.RLSPolicies {
		objects = append(objects, newDeclaredObject(kindPolicy, qualifyWithOwner(policy.Table, policy.Name)))
	}
	objects = append(objects, declaredTypes(db)...)
	for _, sequence := range db.Sequences {
		objects = append(objects, newDeclaredObject(kindSequence, goschema.QualifyTableName(sequence.Schema, sequence.Name)))
	}
	for _, extension := range db.Extensions {
		objects = append(objects, newDeclaredObject(kindExtension, extension.Name))
	}
	for _, role := range db.Roles {
		objects = append(objects, newDeclaredObject(kindRole, role.Name))
	}
	return objects
}

// declaredTypes lists the four object kinds that share one type namespace, so a
// domain and an enum spelled the same collide the way the engine collides them.
func declaredTypes(db *goschema.Database) []declaredObject {
	objects := make([]declaredObject, 0,
		len(db.Enums)+len(db.Domains)+len(db.CompositeTypes)+len(db.Ranges))
	for _, enum := range db.Enums {
		objects = append(objects, newDeclaredObject(kindType, enum.Name))
	}
	for _, domain := range db.Domains {
		objects = append(objects, newDeclaredObject(kindType, goschema.QualifyTableName(domain.Schema, domain.Name)))
	}
	for _, composite := range db.CompositeTypes {
		objects = append(objects, newDeclaredObject(kindType, goschema.QualifyTableName(composite.Schema, composite.Name)))
	}
	for _, rangeType := range db.Ranges {
		objects = append(objects, newDeclaredObject(kindType, goschema.QualifyTableName(rangeType.Schema, rangeType.Name)))
	}
	return objects
}

// indexOwner names the table an index belongs to, preferring the qualified
// spelling the SQL loader records.
func indexOwner(index goschema.Index) string {
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

// guardedObjects lists the identities a SQL file declares under its own
// "may already exist" clause.
func guardedObjects(statements *ast.StatementList) map[guardKey]struct{} {
	guarded := make(map[guardKey]struct{})
	if statements == nil {
		return guarded
	}
	for _, statement := range statements.Statements {
		kind, name, ok := guardedDeclaration(statement)
		if !ok {
			continue
		}
		guarded[guardKey{kind: kind, base: strings.ToLower(baseName(name))}] = struct{}{}
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
		return kindTable, node.Name, node.IfNotExists
	case *ast.CreateSchemaNode:
		return kindSchema, node.Name, node.IfNotExists
	case *ast.IndexNode:
		return kindIndex, node.Name, node.IfNotExists
	case *ast.ExtensionNode:
		return kindExtension, node.Name, node.IfNotExists
	case *ast.CreateSequenceNode:
		return kindSequence, node.Name, node.IfNotExists
	case *ast.CreateViewNode:
		return kindView, node.Name, node.Replace
	case *ast.CreateTriggerNode:
		return kindTrigger, node.Name, node.Replace
	case *ast.CreatePolicyNode:
		return kindPolicy, node.Name, node.Replace
	}
	return "", "", false
}

// baseName drops a qualifier, so `app.users` and `users` guard alike.
func baseName(name string) string {
	if index := strings.LastIndex(name, "."); index >= 0 {
		return name[index+1:]
	}
	return name
}

// dirDeclarations is the ledger of what the files read so far have declared.
type dirDeclarations struct {
	declared map[objectKey]struct{}
}

func newDirDeclarations() *dirDeclarations {
	return &dirDeclarations{declared: make(map[objectKey]struct{})}
}

// admit records one file's declarations, refusing an object an earlier file
// already declared unless this file declares it under a guard.
//
// file is the entry name rather than the path, because that is what the pinned
// binary names in the same refusal.
func (d *dirDeclarations) admit(file string, objects []declaredObject, guarded map[guardKey]struct{}) error {
	for _, object := range objects {
		if _, seen := d.declared[object.key]; !seen {
			d.declared[object.key] = struct{}{}
			continue
		}
		if _, ok := guarded[guardKey{kind: object.key.kind, base: strings.ToLower(baseName(object.display))}]; ok {
			continue
		}
		return fmt.Errorf("read state from %q: %s %q already exists", file, object.key.kind, object.display)
	}
	return nil
}
