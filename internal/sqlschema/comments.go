package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

// commentStatement is one COMMENT ON statement as Parser.parseCommentStatement
// spells it: `COMMENT ON <KIND> <name> IS <literal>`. The shape is fixed by
// that function, so this reads a known format rather than arbitrary SQL.
type commentStatement struct {
	kind    string
	name    string
	comment string
}

// parseCommentStatement splits a COMMENT ON text. The name ends at the first
// " IS ": a literal may contain the word, and an unquoted name cannot.
func parseCommentStatement(text string) (commentStatement, bool) {
	rest, found := strings.CutPrefix(text, "COMMENT ON ")
	if !found {
		return commentStatement{}, false
	}
	kind, rest, found := strings.Cut(rest, " ")
	if !found {
		return commentStatement{}, false
	}
	name, literal, found := strings.Cut(rest, " IS ")
	if !found {
		return commentStatement{}, false
	}
	return commentStatement{
		kind:    kind,
		name:    strings.TrimSpace(name),
		comment: unquoteSQLStringLiteral(strings.TrimSpace(literal)),
	}, true
}

// applyComment attaches a COMMENT ON statement to the object it names.
//
// A comment is part of the declaration it describes, and every other source
// carries it there, so a SQL file that sets it with a separate statement has
// to reach the same place. Dropped, the comment was missing from the desired
// schema, and a plan removed every comment a database had -- which is every
// comment in a schema pg_dump wrote (stokaro/ptah#3610).
//
// The object may come from an earlier file of the same document; the comment
// is then set on base, in place, as an ALTER TABLE is. A kind the model keeps
// no comment for, and an object the document does not declare, are refused.
func applyComment(database, base *schemamodel.Database, node *ast.CommentNode, sourcePlatform string) error {
	statement, ok := parseCommentStatement(node.Text)
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnmodeledStatement, node.Text)
	}
	databases := []*schemamodel.Database{database}
	if base != nil {
		databases = append(databases, base)
	}
	if unrenderedCommentKinds[statement.kind] {
		return fmt.Errorf(
			"%w: COMMENT ON %s %s: the model keeps this comment and the plan never writes it "+
				"(stokaro/ptah#3627), so reading it would drop it",
			ErrUnmodeledStatement, statement.kind, statement.name)
	}
	target, found := commentTarget(databases, statement, sourcePlatform)
	switch {
	case target == &unmodeledKind:
		return fmt.Errorf("%w: COMMENT ON %s: Ptah keeps no comment for this kind of object",
			ErrUnmodeledStatement, statement.kind)
	case !found:
		return fmt.Errorf("%w: COMMENT ON %s %s names an object this schema does not declare",
			ErrUnmodeledStatement, statement.kind, statement.name)
	}
	*target = statement.comment
	return nil
}

// unrenderedCommentKinds are the objects whose comment the model has a field
// for and the PostgreSQL plan never writes or compares. A comment read into
// one of them would reach nothing, which is the drop this refusal exists to
// prevent; the entries go when stokaro/ptah#3627 renders them.
var unrenderedCommentKinds = map[string]bool{
	"VIEW": true, "SEQUENCE": true, "DOMAIN": true, "TYPE": true, "EXTENSION": true,
}

// unmodeledKind is the target commentTarget answers with for a comment the
// model has no place for. It is compared by address.
var unmodeledKind string

// commentTarget returns the Comment field the statement sets, and whether the
// document declares the object. Where the model has no place for the comment
// at all it answers with &unmodeledKind.
func commentTarget(
	databases []*schemamodel.Database, statement commentStatement, sourcePlatform string,
) (target *string, found bool) {
	switch statement.kind {
	case "TABLE":
		return tableCommentTarget(databases, normalizeSQLTableReference(sourcePlatform, statement.name))
	case "COLUMN":
		return columnCommentTarget(databases, statement.name, sourcePlatform)
	case "INDEX":
		_, bare := normalizeSQLTableIdentifier(sourcePlatform, statement.name)
		return indexCommentTarget(databases, bare)
	case "SCHEMA":
		return schemaCommentTarget(databases, normalizeSQLIdentifier(sourcePlatform, statement.name))
	case "ROLE":
		return roleCommentTarget(databases, roleName(sourcePlatform, statement.name))
	default:
		return &unmodeledKind, false
	}
}

func tableCommentTarget(databases []*schemamodel.Database, qualified string) (*string, bool) {
	table := commentTable(databases, qualified)
	if table == nil {
		return new(string), false
	}
	return &table.Comment, true
}

// commentTable finds the table a comment names by its qualified name, never by
// its struct name: the struct name camel-cases the table name, so a table
// created as "Docs" and a comment on docs share one, and PostgreSQL resolves
// the two to different tables.
func commentTable(databases []*schemamodel.Database, qualified string) *schemamodel.Table {
	for _, database := range databases {
		for i := range database.Tables {
			if database.Tables[i].QualifiedName() == qualified {
				return &database.Tables[i]
			}
		}
	}
	return nil
}

func indexCommentTarget(databases []*schemamodel.Database, name string) (*string, bool) {
	for _, database := range databases {
		for i := range database.Indexes {
			if database.Indexes[i].Name == name {
				return &database.Indexes[i].Comment, true
			}
		}
	}
	return new(string), false
}

func schemaCommentTarget(databases []*schemamodel.Database, name string) (*string, bool) {
	for _, database := range databases {
		for i := range database.Schemas {
			if database.Schemas[i].Name == name {
				return &database.Schemas[i].Comment, true
			}
		}
	}
	return new(string), false
}

func roleCommentTarget(databases []*schemamodel.Database, name string) (*string, bool) {
	for _, database := range databases {
		for i := range database.Roles {
			if database.Roles[i].Name == name {
				return &database.Roles[i].Comment, true
			}
		}
	}
	return new(string), false
}

// columnCommentTarget finds the column `table.column` or `schema.table.column`
// names.
func columnCommentTarget(databases []*schemamodel.Database, name, sourcePlatform string) (*string, bool) {
	dot := strings.LastIndex(name, ".")
	if dot < 0 {
		return new(string), false
	}
	table := commentTable(databases, normalizeSQLTableReference(sourcePlatform, name[:dot]))
	if table == nil {
		return new(string), false
	}
	column := normalizeSQLIdentifier(sourcePlatform, name[dot+1:])
	for _, database := range databases {
		for i := range database.Fields {
			if database.Fields[i].StructName == table.StructName && database.Fields[i].Name == column {
				return &database.Fields[i].Comment, true
			}
		}
	}
	return new(string), false
}
