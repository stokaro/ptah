package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
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
// refused; see [schemaext.Objects.Declare]. A statement that changes an
// object and declares and amends nothing, such as DROP EXTENSION, is refused
// with [ErrUnmodeledStatement], naming the statement.
func applyOwnerStatement(database *schemamodel.Database, document *Document, stmt ast.Node) (handled bool, err error) {
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
	for _, object := range note.Objects {
		objects, err := database.FeatureObjects.Declare(object)
		if err != nil {
			return true, err
		}
		database.FeatureObjects = objects
	}
	for _, object := range note.Replaces {
		if err := replaceDeclared(database, document.base, object, note.Text); err != nil {
			return true, err
		}
	}
	return true, nil
}

// changesUndeclared reports an owner's statement that changes a named object
// and declares and amends none, such as DROP EXTENSION: a change a migration
// states. A schema document describes objects, and the model has no place for
// such a statement. One whose payload reports no change is syntax the owner
// answered, and stays where it stood.
func changesUndeclared(statement *ast.ExtensionStatement, note parser.OwnerStatement) bool {
	if len(note.Objects) > 0 || len(note.Replaces) > 0 {
		return false
	}
	reporter, ok := statement.Payload.(ast.ExtensionChangeReporter)
	return ok && strings.TrimSpace(reporter.SchemaChange().Name) != ""
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
