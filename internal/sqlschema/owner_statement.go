package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqldecl"
)

// applyOwnerStatement applies an owner's statement, which the parse read
// through the owner: it adds the objects the statement declares to database,
// and replaces the ones it amends where they were declared, in database or in
// what the earlier documents declared. handled is false for any other
// statement.
//
// An object is added as a declaration, so declaring an extension a second
// time with the same definition declares nothing new, and one that differs is
// refused; see [schemaext.Objects.Declare]. A child object names its table as
// the statement writes it, and is keyed by the table the schema declares; see
// [declaredChild]. The owner's facets of a table are merged into the table
// the schema declares; see [setTableFacets]. A statement that reports a change
// without objects or table facets is refused with [ErrUnmodeledStatement].
func applyOwnerStatement(database *schemamodel.Database, document *Document, stmt ast.Node, sourcePlatform string) (handled bool, err error) {
	statement, ok := stmt.(*ast.ExtensionStatement)
	if !ok {
		return false, nil
	}
	note, owned := document.ownerStatement(statement)
	if !owned {
		return false, nil
	}
	if changesUndeclared(statement, note) {
		return true, fmt.Errorf("%w: %s", ErrUnmodeledStatement, firstLine(note.Text))
	}
	databases := declaringDatabases(database, document.base)
	for _, object := range note.Objects {
		object.Ref = declaredChild(databases, object.Ref, sourcePlatform)
		objects, err := database.FeatureObjects.Declare(object)
		if err != nil {
			return true, err
		}
		database.FeatureObjects = objects
	}
	for _, object := range note.Replaces {
		object.Ref = declaredChild(databases, object.Ref, sourcePlatform)
		if err := replaceDeclared(database, document.base, object, note.Text); err != nil {
			return true, err
		}
	}
	if note.TableFacets != nil {
		return true, setTableFacets(databases, *note.TableFacets, note.Text, sourcePlatform)
	}
	return true, nil
}

// changesUndeclared reports an owner's statement that changes a named object
// without declared objects, replacements or table facets, such as DROP
// EXTENSION. A schema document has no representation for that change.
func changesUndeclared(statement *ast.ExtensionStatement, note parser.OwnerStatement) bool {
	if len(note.Objects) > 0 || len(note.Replaces) > 0 || note.TableFacets != nil {
		return false
	}
	reporter, ok := statement.Payload.(ast.ExtensionChangeReporter)
	return ok && strings.TrimSpace(reporter.SchemaChange().Name) != ""
}

// declaringDatabases are the models a statement's table names resolve in:
// the document's own and, when there is one, the earlier documents'.
func declaringDatabases(database, base *schemamodel.Database) []*schemamodel.Database {
	if base == nil {
		return []*schemamodel.Database{database}
	}
	return []*schemamodel.Database{database, base}
}

// declaredChild keys a child object, one whose identity names its table as
// its parent, by the table the schema declares under that name; see
// [childTable]. A table the schema does not declare stays as the statement
// wrote it, since a child may name a table the document leaves to the
// database.
func declaredChild(databases []*schemamodel.Database, ref objectidentity.ID, sourcePlatform string) objectidentity.ID {
	table := childTable(databases, ref, sourcePlatform)
	if table == nil {
		return ref
	}
	declared := objectidentity.NewBuilder(identifier.ForDialect(sourcePlatform)).TablePartsVerbatim(table.Schema, table.Name)
	ref.Schema, ref.Parent = declared.Schema, declared.Name
	return ref
}

// childTable is the declared table a child object's identity names as its
// parent, as an ALTER TABLE finds a table, and nil for an object that is no
// child or names no declared table. An identity holds the schema it was
// built with, which defaults an unwritten one, so a parent in the dialect's
// default schema also names the table the schema declares without one.
func childTable(databases []*schemamodel.Database, ref objectidentity.ID, sourcePlatform string) *schemamodel.Table {
	if ref.Parent.Empty() {
		return nil
	}
	return declaredOwnerTable(databases, objectidentity.ID{Schema: ref.Schema, Name: ref.Parent}, sourcePlatform)
}

// declaredOwnerTable resolves a table identity from an owner's declaration.
// An explicit default schema also reaches a table declared without a schema.
func declaredOwnerTable(databases []*schemamodel.Database, ref objectidentity.ID, sourcePlatform string) *schemamodel.Table {
	written := schemamodel.QualifyTableName(ref.Schema.Source, ref.Name.Source)
	if table := resolveTable(databases, normalizeSQLTableReference("", written), sourcePlatform); table != nil {
		return table
	}
	if ref.Schema.Normalized != identifier.ForDialect(sourcePlatform).DefaultSchema {
		return nil
	}
	return resolveTable(databases, normalizeSQLTableReference("", ref.Name.Source), sourcePlatform)
}

// setTableFacets merges an owner's facets into the table the schema declares
// under the name the statement writes, and refuses a table it does not
// declare and a facet the table already holds. written is the statement,
// which a refusal names first.
func setTableFacets(databases []*schemamodel.Database, facets sqlext.TableFacets, written, sourcePlatform string) error {
	schema, name := normalizeSQLTableIdentifier(sourcePlatform, facets.Name)
	ref := objectidentity.NewBuilder(identifier.ForDialect(sourcePlatform)).TablePartsVerbatim(schema, name)
	table := declaredOwnerTable(databases, ref, sourcePlatform)
	if table == nil {
		return undeclared(statementRefusal{
			text: fmt.Sprintf("%s: table %q is not declared in this schema", statementExcerpt(written), facets.Name),
			err:  ErrUnmodeledStatement,
		})
	}
	merged, err := table.Facets.Merge(facets.Facets)
	if err != nil {
		return fmt.Errorf("%s: %w", statementExcerpt(written), err)
	}
	table.Facets = merged
	return nil
}

// replaceDeclared replaces object where it was declared: in database, the
// document's own declarations, or in base, the earlier documents', which a
// change is made to in place; see [ReadOnto]. written is the statement that
// amends it, which a refusal names first.
func replaceDeclared(database, base *schemamodel.Database, object schemaext.Object, written string) error {
	for _, declared := range []*schemamodel.Database{database, base} {
		if declared == nil {
			continue
		}
		_, found, err := declared.FeatureObjects.Get(object.Ref)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		replaced, err := declared.FeatureObjects.Replace(object)
		if err != nil {
			return err
		}
		declared.FeatureObjects = replaced
		return nil
	}
	subject := sqldecl.Owned(object.Ref)
	return undeclared(statementRefusal{
		text: fmt.Sprintf("%s: %s %q is not declared in this schema", statementExcerpt(written), subject.Kind, subject.Name),
		err:  ErrUnmodeledStatement,
	})
}

// statementRefusal is a refusal of a statement that leads with the statement
// rather than with err's text, and still answers errors.Is for err.
type statementRefusal struct {
	text string
	err  error
}

func (e statementRefusal) Error() string { return e.text }

func (e statementRefusal) Unwrap() error { return e.err }

// statementExcerpt is a statement as written, its whitespace runs read as one
// space and cut to a length a refusal can carry.
func statementExcerpt(written string) string {
	const limit = 80
	excerpt := strings.Join(strings.Fields(written), " ")
	if runes := []rune(excerpt); len(runes) > limit {
		return string(runes[:limit]) + "..."
	}
	return excerpt
}
