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
	target, found := commentTarget(databases, statement, sourcePlatform)
	switch {
	case target == &unmodeledKind:
		return fmt.Errorf("%w: COMMENT ON %s: Ptah keeps no comment for this kind of object",
			ErrUnmodeledStatement, statement.kind)
	case target == &unmodeledEnum:
		return fmt.Errorf("%w: COMMENT ON TYPE %s names an enum type, and Ptah keeps no comment for one "+
			"(stokaro/ptah#3646)",
			ErrUnmodeledStatement, statement.name)
	case !found:
		return fmt.Errorf("%w: COMMENT ON %s %s names an object this schema does not declare",
			ErrUnmodeledStatement, statement.kind, statement.name)
	}
	*target = statement.comment
	return nil
}

// unmodeledKind is the target commentTarget answers with for a comment the
// model has no place for. It is compared by address.
var unmodeledKind string

// unmodeledEnum is the target commentTarget answers with for COMMENT ON TYPE
// naming an enum. The statement is the same for every kind of type and only an
// enum has no comment in the model, so it is refused by name rather than as a
// type the document does not declare.
var unmodeledEnum string

// commentTarget returns the Comment field the statement sets, and whether the
// document declares the object. Where the model has no place for the comment
// at all it answers with &unmodeledKind.
func commentTarget(
	databases []*schemamodel.Database, statement commentStatement, sourcePlatform string,
) (target *string, found bool) {
	switch statement.kind {
	case "TABLE":
		return tableCommentTarget(databases, normalizeSQLTableReference(sourcePlatform, statement.name), sourcePlatform)
	case "COLUMN":
		return columnCommentTarget(databases, statement.name, sourcePlatform)
	case "INDEX":
		_, bare := normalizeSQLTableIdentifier(sourcePlatform, statement.name)
		return indexCommentTarget(databases, bare)
	case "SCHEMA":
		return schemaCommentTarget(databases, normalizeSQLIdentifier(sourcePlatform, statement.name))
	case "ROLE":
		return roleCommentTarget(databases, roleName(sourcePlatform, statement.name))
	case "VIEW":
		return viewCommentTarget(databases, normalizeSQLTableReference(sourcePlatform, statement.name), sourcePlatform)
	case "SEQUENCE":
		return sequenceCommentTarget(databases, normalizeSQLTableReference(sourcePlatform, statement.name), sourcePlatform)
	case "DOMAIN":
		return domainCommentTarget(databases, normalizeSQLTableReference(sourcePlatform, statement.name), sourcePlatform)
	case "TYPE":
		return typeCommentTarget(databases, normalizeSQLTableReference(sourcePlatform, statement.name), sourcePlatform)
	case "EXTENSION":
		return extensionCommentTarget(databases, identifierPart(sourcePlatform, statement.name), sourcePlatform)
	default:
		return &unmodeledKind, false
	}
}

// The finders below resolve a name the way resolveTable resolves a table's:
// exact after the source dialect's fold, and by the dialect's own
// case-insensitive rule where the server has one.

// resolvedCommentTarget answers the comment field of the declared name written
// reaches, and whether one does. An enum's field is &unmodeledEnum, which
// applyComment refuses by name.
func resolvedCommentTarget(sourcePlatform, written string, targets []*string, names []string) (*string, bool) {
	index := resolveDeclaredName(sourcePlatform, written, names)
	if index < 0 {
		return new(string), false
	}
	return targets[index], true
}

func viewCommentTarget(databases []*schemamodel.Database, qualified, sourcePlatform string) (*string, bool) {
	var targets []*string
	var names []string
	for _, database := range databases {
		for i := range database.Views {
			targets = append(targets, &database.Views[i].Comment)
			names = append(names, database.Views[i].Name)
		}
	}
	return resolvedCommentTarget(sourcePlatform, qualified, targets, names)
}

func sequenceCommentTarget(databases []*schemamodel.Database, qualified, sourcePlatform string) (*string, bool) {
	var targets []*string
	var names []string
	for _, database := range databases {
		for i := range database.Sequences {
			targets = append(targets, &database.Sequences[i].Comment)
			names = append(names, database.Sequences[i].QualifiedName())
		}
	}
	return resolvedCommentTarget(sourcePlatform, qualified, targets, names)
}

func domainCommentTarget(databases []*schemamodel.Database, qualified, sourcePlatform string) (*string, bool) {
	var targets []*string
	var names []string
	for _, database := range databases {
		for i := range database.Domains {
			targets = append(targets, &database.Domains[i].Comment)
			names = append(names, database.Domains[i].QualifiedName())
		}
	}
	return resolvedCommentTarget(sourcePlatform, qualified, targets, names)
}

// typeCommentTarget finds the composite or range type COMMENT ON TYPE names,
// and answers &unmodeledEnum for an enum. The three kinds share one namespace,
// so they are resolved together.
func typeCommentTarget(databases []*schemamodel.Database, qualified, sourcePlatform string) (*string, bool) {
	var targets []*string
	var names []string
	for _, database := range databases {
		for i := range database.CompositeTypes {
			targets = append(targets, &database.CompositeTypes[i].Comment)
			names = append(names, database.CompositeTypes[i].QualifiedName())
		}
		for i := range database.Ranges {
			targets = append(targets, &database.Ranges[i].Comment)
			names = append(names, database.Ranges[i].QualifiedName())
		}
		for i := range database.Enums {
			targets = append(targets, &unmodeledEnum)
			names = append(names, database.Enums[i].Name)
		}
	}
	return resolvedCommentTarget(sourcePlatform, qualified, targets, names)
}

// extensionCommentTarget finds an extension by its name, which is
// database-wide and carries no schema.
func extensionCommentTarget(databases []*schemamodel.Database, name, sourcePlatform string) (*string, bool) {
	var targets []*string
	var names []string
	for _, database := range databases {
		for i := range database.Extensions {
			targets = append(targets, &database.Extensions[i].Comment)
			names = append(names, database.Extensions[i].Name)
		}
	}
	return resolvedCommentTarget(sourcePlatform, name, targets, names)
}

// tableCommentTarget finds the table a comment names by the rule an ALTER TABLE
// uses, [resolveTable]: by the name the server resolves, never by the struct
// name, which gives a table created as "Docs" and a comment on docs one key.
func tableCommentTarget(databases []*schemamodel.Database, qualified, sourcePlatform string) (*string, bool) {
	table := resolveTable(databases, qualified, sourcePlatform)
	if table == nil {
		return new(string), false
	}
	return &table.Comment, true
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
// names, by the rules an ALTER TABLE uses for the table and its column.
func columnCommentTarget(databases []*schemamodel.Database, name, sourcePlatform string) (*string, bool) {
	dot := strings.LastIndex(name, ".")
	if dot < 0 {
		return new(string), false
	}
	table := resolveTable(databases, normalizeSQLTableReference(sourcePlatform, name[:dot]), sourcePlatform)
	if table == nil {
		return new(string), false
	}
	field := resolveColumn(databases, table.StructName, name[dot+1:], sourcePlatform)
	if field == nil {
		return new(string), false
	}
	return &field.Comment, true
}
