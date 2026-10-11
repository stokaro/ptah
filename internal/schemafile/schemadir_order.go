package schemafile

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/sqldecl"
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
// The ledger is not the engine, so what it replays of the script is stated here:
//
//   - A guard is honored rather than executed. `CREATE TABLE IF NOT EXISTS`
//     twice is exit 0 on that binary, measured, so a guarded redeclaration is
//     admitted instead of refused. Which of the two definitions survives the
//     merge is unchanged by this file and still differs from that binary; the
//     exit code, which is what the compatibility rule is about, now agrees.
//   - An ALTER is not replayed. A file that alters what an earlier file created
//     declares nothing, so it neither refuses nor contributes -- the same as
//     before this file existed.
//   - CREATE TABLE and DROP TABLE are replayed in the order the file runs them,
//     so a file may drop a table an earlier file created and create it again
//     (see [dirDeclarations.admit]).

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
// declared.Tables are the file's CREATE TABLE and DROP TABLE statements in
// order, none for a file that is not a script. They are replayed on the ledger before the
// file's objects are admitted, because a SQL file runs in order: `DROP TABLE a;
// CREATE TABLE a (...)` after a file that created a is exit 0 on the pinned
// binary, measured on PostgreSQL 18.6 (stokaro/ptah#3914). Without the replay
// the file's final state holds a table a, and the ledger refuses it as a second
// declaration. A table the replay decided is not admitted again from the final
// state. `CREATE TABLE a; DROP TABLE a;` still refuses on the CREATE, as the
// engine does, and a file that only drops a takes a out of the ledger.
//
// file is the entry name rather than the path, because that is what the pinned
// binary names in the same refusal.
func (d *dirDeclarations) admit(file string, declared sqlext.FileDeclarations) error {
	objects := make([]declaredObject, 0, len(declared.Objects))
	for _, object := range declared.Objects {
		objects = append(objects, newDeclaredObject(object.Kind, object.Name))
	}
	guarded := make(map[guardKey]struct{}, len(declared.Guards))
	for _, guard := range declared.Guards {
		guarded[guardKey{kind: guard.Kind, base: strings.ToLower(baseName(guard.Name))}] = struct{}{}
	}
	tables := make([]tableStatement, 0, len(declared.Tables))
	for _, statement := range declared.Tables {
		table := newDeclaredObject(sqldecl.Table, statement.Name)
		tables = append(tables, tableStatement{key: table.key, display: table.display, drop: statement.Drop, guarded: statement.Guarded})
	}
	replayed := make(map[objectKey]struct{}, len(tables))
	for _, table := range tables {
		replayed[table.key] = struct{}{}
		if table.drop {
			d.forgetTable(table.key)
			continue
		}
		if _, seen := d.declared[table.key]; seen && !table.guarded {
			return fmt.Errorf("read state from %q: %s %q already exists", file, sqldecl.Table, table.display)
		}
		d.declared[table.key] = struct{}{}
	}
	for _, object := range objects {
		if _, decided := replayed[object.key]; decided {
			continue
		}
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

// forgetTable takes a dropped table out of the ledger, with the indexes,
// triggers and policies keyed under it: the engine drops those with the table,
// so a later CREATE of the same name may declare them again.
func (d *dirDeclarations) forgetTable(table objectKey) {
	delete(d.declared, table)
	for key := range d.declared {
		switch key.kind {
		case sqldecl.Index, sqldecl.Trigger, sqldecl.Policy:
			// The rest must be one name, so dropping a table app leaves the
			// indexes of a table app.users alone.
			if name, owned := strings.CutPrefix(key.name, table.name+"."); owned && !strings.Contains(name, ".") {
				delete(d.declared, key)
			}
		}
	}
}

// tableStatement is one CREATE TABLE or DROP TABLE of a SQL file, keyed as
// [declaredObjects] keys the table.
type tableStatement struct {
	key     objectKey
	display string
	drop    bool
	guarded bool
}

// fileDeclarations is what one file of a directory in format declares: a SQL
// file is a script that creates its schemas, and an HCL file names them.
func fileDeclarations(
	name string, db *schemamodel.Database, statements *ast.StatementList, format schemaDirFormat,
) sqlext.FileDeclarations {
	if format == schemaDirFormatSQL {
		return sqldecl.Of(name, db, statements)
	}
	return sqldecl.OfDocument(name, db)
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
