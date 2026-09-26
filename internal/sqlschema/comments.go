package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/routineargs"
)

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
	case target == &unmodeledDomainConstraint:
		return fmt.Errorf("%w: COMMENT ON CONSTRAINT %s ON DOMAIN %s: Ptah keeps no comment for a domain's constraint",
			ErrUnmodeledStatement, statement.name, statement.table)
	case target == &ambiguousRoutine:
		return fmt.Errorf("%w: COMMENT ON %s %s names more than one declared overload; "+
			"write its argument types to name one",
			ErrUnmodeledStatement, statement.kind, statement.name)
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

// unmodeledDomainConstraint is the target commentTarget answers with for
// COMMENT ON CONSTRAINT ... ON DOMAIN. The model keeps a domain's CHECK as an
// expression, with no name and no comment.
var unmodeledDomainConstraint string

// ambiguousRoutine is the target commentTarget answers with for a function or
// a procedure named without the argument list that would select one of its
// declared overloads.
var ambiguousRoutine string

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
	case "MATERIALIZED VIEW":
		return materializedViewCommentTarget(databases,
			normalizeSQLTableReference(sourcePlatform, statement.name), sourcePlatform)
	case "FUNCTION", "PROCEDURE":
		return routineCommentTarget(databases, statement, sourcePlatform)
	case "TRIGGER", "POLICY", "CONSTRAINT":
		return tableMemberCommentTarget(databases, statement, sourcePlatform)
	default:
		return &unmodeledKind, false
	}
}

// The finders below resolve a name the way resolveTable resolves a table's:
// exact after the source dialect's fold, and by the dialect's own
// case-insensitive rule where the server has one.

// resolvedCommentTarget answers the comment field of the declared name written
// reaches, and whether one does.
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

// typeCommentTarget finds the composite, range or enum type COMMENT ON TYPE
// names. The three kinds share one namespace, so they are resolved together.
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
			targets = append(targets, &database.Enums[i].Comment)
			names = append(names, database.Enums[i].QualifiedName())
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

func materializedViewCommentTarget(databases []*schemamodel.Database, qualified, sourcePlatform string) (*string, bool) {
	var targets []*string
	var names []string
	for _, database := range databases {
		for i := range database.MaterializedViews {
			targets = append(targets, &database.MaterializedViews[i].Comment)
			names = append(names, database.MaterializedViews[i].Name)
		}
	}
	return resolvedCommentTarget(sourcePlatform, qualified, targets, names)
}

// routineCommentTarget finds the function or procedure COMMENT ON names.
//
// A name may carry several overloads, so the argument list is compared by
// input types, the way GRANT and REVOKE name a routine: `f(integer)` and a
// declaration `f(a int DEFAULT 1)` are one routine. Without an argument list
// the name has to carry exactly one declared routine of the kind, which is
// the case PostgreSQL resolves the bare name in too.
func routineCommentTarget(
	databases []*schemamodel.Database, statement commentStatement, sourcePlatform string,
) (*string, bool) {
	written := normalizeSQLTableReference(sourcePlatform, statement.name)
	procedure := statement.kind == "PROCEDURE"
	var candidates []*string
	for _, database := range databases {
		for i := range database.Functions {
			function := &database.Functions[i]
			if function.IsProcedure() != procedure ||
				resolveDeclaredName(sourcePlatform, written, []string{function.Name}) < 0 {
				continue
			}
			if statement.arguments != nil &&
				routineargs.InputTypes(function.Parameters) != routineargs.InputTypes(*statement.arguments) {
				continue
			}
			candidates = append(candidates, &function.Comment)
		}
	}
	switch len(candidates) {
	case 0:
		return new(string), false
	case 1:
		return candidates[0], true
	default:
		return &ambiguousRoutine, false
	}
}

// tableMemberCommentTarget finds the trigger, policy or constraint COMMENT ON
// names ON a table. Each name is scoped to its table, so the table is
// resolved with the name: two tables may each have a trigger called audit.
func tableMemberCommentTarget(
	databases []*schemamodel.Database, statement commentStatement, sourcePlatform string,
) (*string, bool) {
	if statement.onDomain {
		return &unmodeledDomainConstraint, false
	}
	if statement.table == "" {
		return new(string), false
	}
	var targets []*string
	var names []string
	for _, database := range databases {
		switch statement.kind {
		case "TRIGGER":
			for i := range database.Triggers {
				targets = append(targets, &database.Triggers[i].Comment)
				names = append(names, memberName(database.Triggers[i].Table, database.Triggers[i].Name))
			}
		case "POLICY":
			for i := range database.RLSPolicies {
				targets = append(targets, &database.RLSPolicies[i].Comment)
				names = append(names, memberName(database.RLSPolicies[i].Table, database.RLSPolicies[i].Name))
			}
		default:
			for i := range database.Constraints {
				targets = append(targets, &database.Constraints[i].Comment)
				names = append(names, memberName(database.Constraints[i].Table, database.Constraints[i].Name))
			}
		}
	}
	written := memberName(normalizeSQLTableReference(sourcePlatform, statement.table),
		normalizeSQLIdentifier(sourcePlatform, statement.name))
	return resolvedCommentTarget(sourcePlatform, written, targets, names)
}

// memberName spells an object scoped to a table as one name the resolver can
// compare: the table's qualified name, then the object's own. A name cannot
// hold the separator, which no identifier the reader records contains.
func memberName(table, name string) string {
	return table + "\x00" + name
}
