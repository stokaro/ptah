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
func applyComment(database *schemamodel.Database, document *Document, node *ast.CommentNode, sourcePlatform string) error {
	statement, ok := parseCommentStatement(node.Text)
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnmodeledStatement, node.Text)
	}
	return applyCommentStatement(database, document, statement, sourcePlatform)
}

func applyCommentStatement(database *schemamodel.Database, document *Document, statement commentStatement, sourcePlatform string) error {
	base := document.base
	databases := []*schemamodel.Database{database}
	if base != nil {
		databases = append(databases, base)
	}
	if document.ownedConstraintComment(databases, statement, sourcePlatform) {
		return nil
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
	case target == &ambiguousIndex:
		return fmt.Errorf("%w: COMMENT ON INDEX %s names an index more than one declared table holds%s",
			ErrUnmodeledStatement, statement.name, ambiguousIndexHint(sourcePlatform))
	case !found:
		return undeclared(fmt.Errorf("%w: COMMENT ON %s %s names an object this schema does not declare",
			ErrUnmodeledStatement, statement.kind, statement.name))
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

// ambiguousIndex is the target commentTarget answers with for an index name
// that indexes of more than one table in the named schema carry.
var ambiguousIndex string

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
		return indexCommentTarget(databases, statement.name, sourcePlatform)
	case "SCHEMA":
		return schemaCommentTarget(databases, normalizeSQLIdentifier(sourcePlatform, statement.name))
	case "ROLE":
		return roleCommentTarget(databases, roleName(sourcePlatform, statement.name))
	case "VIEW":
		return viewCommentTarget(databases, normalizeSQLTableReference(sourcePlatform, statement.name), sourcePlatform)
	case "SEQUENCE":
		return sequenceCommentTarget(databases, normalizeSQLTableReference(sourcePlatform, statement.name), sourcePlatform)
	case "DOMAIN":
		// A domain is a type owner's object, and its owner claims the comment
		// on one it declared. A domain reaching here is one no owner declared.
		return new(string), false
	case "TYPE":
		return typeCommentTarget(databases, normalizeSQLTableReference(sourcePlatform, statement.name), sourcePlatform)
	case "MATERIALIZED VIEW":
		return materializedViewCommentTarget(databases,
			normalizeSQLTableReference(sourcePlatform, statement.name), sourcePlatform)
	case "FUNCTION", "PROCEDURE":
		return routineCommentTarget(databases, statement, sourcePlatform)
	case "TRIGGER", "CONSTRAINT":
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

// typeCommentTarget finds the enum type COMMENT ON TYPE names. The owner of a
// composite or a range type claims the comment on one it declared.
func typeCommentTarget(databases []*schemamodel.Database, qualified, sourcePlatform string) (*string, bool) {
	var targets []*string
	var names []string
	for _, database := range databases {
		for i := range database.Enums {
			targets = append(targets, &database.Enums[i].Comment)
			names = append(names, database.Enums[i].QualifiedName())
		}
	}
	return resolvedCommentTarget(sourcePlatform, qualified, targets, names)
}

// tableCommentTarget finds the table a comment names by the rule an ALTER TABLE
// uses, [resolveTable]: by the name the server resolves, never by the struct
// name, which gives a table created as "Docs" and a comment on docs one key.
// SQL Server's comment statement has to spell the schema out, and a
// description of `dbo` writes its tables without one, so the rule's default
// schema is what lets the comment find its table there.
func tableCommentTarget(databases []*schemamodel.Database, qualified, sourcePlatform string) (*string, bool) {
	table := resolveTable(databases, qualified, sourcePlatform)
	if table == nil {
		return new(string), false
	}
	return &table.Comment, true
}

// indexCommentTarget finds the index COMMENT ON INDEX names. An index is named
// in the schema of its table, as DROP INDEX names it ([findNamedIndex]):
// `s2.t_a` is an index of a table in s2, and a bare `t_a` one of a table in the
// default schema. Found by its name alone, with s2.t declared before public.t
// and each holding an index t_a, both comments would land on s2.t_a and
// public.t_a would keep none.
//
// CockroachDB names an index within its table, so two tables of one schema may
// each have an index t_a. It names one of them with its table, `t@t_a`, and
// refuses the name alone: measured on v26.4.0, `COMMENT ON INDEX t_a` and
// `COMMENT ON INDEX public.t_a` each answer `index name "t_a" is ambiguous`
// (SQLSTATE 42P08), and a read refuses them too.
func indexCommentTarget(databases []*schemamodel.Database, written, sourcePlatform string) (*string, bool) {
	schema, name := normalizeSQLTableIdentifier(sourcePlatform, written)
	reaches := func(holder indexRelation) bool { return sameSchema(sourcePlatform, schema, holder.schema) }
	if tableName, indexName, onTable := cutIndexTable(sourcePlatform, written); onTable {
		named, found := indexHolder(databases, schemamodel.Index{TableName: normalizeSQLTableReference(sourcePlatform, tableName)}, sourcePlatform)
		if !found {
			return new(string), false
		}
		name = normalizeSQLIdentifier(sourcePlatform, indexName)
		reaches = func(holder indexRelation) bool { return holder.qualified == named.qualified }
	}
	var targets []*string
	for _, database := range databases {
		for i := range database.Indexes {
			index := &database.Indexes[i]
			if index.Name != name {
				continue
			}
			if holder, found := indexHolder(databases, *index, sourcePlatform); found && reaches(holder) {
				targets = append(targets, &index.Comment)
			}
		}
	}
	switch len(targets) {
	case 0:
		return new(string), false
	case 1:
		return targets[0], true
	default:
		return &ambiguousIndex, false
	}
}

// indexRelation is the table or materialized view an index belongs to: its
// name as the reader stores it, schema included, and its schema, empty for the
// default.
type indexRelation struct {
	qualified, schema string
}

// indexHolder answers the relation index belongs to: a table, found by the
// rule [indexOwner] gives, or a materialized view, which PostgreSQL indexes as
// it indexes a table. An index whose table names neither answers false.
func indexHolder(databases []*schemamodel.Database, index schemamodel.Index, sourcePlatform string) (indexRelation, bool) {
	if table := indexOwner(databases, index, sourcePlatform); table != nil {
		return indexRelation{qualified: table.QualifiedName(), schema: table.Schema}, true
	}
	if index.TableName == "" {
		return indexRelation{}, false
	}
	var views, names []string
	for _, database := range databases {
		for _, view := range database.MaterializedViews {
			views = append(views, view.Name)
			names = append(names, unqualified(view.Name, sourcePlatform))
		}
	}
	at := resolveDeclaredName(sourcePlatform, unqualified(normalizeSQLTableReference("", index.TableName), sourcePlatform), names)
	if at < 0 {
		return indexRelation{}, false
	}
	schema, view := normalizeSQLTableIdentifier("", views[at])
	return indexRelation{qualified: schemamodel.QualifyTableName(schema, view), schema: schema}, true
}

// cutIndexTable splits the name of an index written with its table,
// `table@index`, at the `@` outside double quotes. Only a dialect that names an
// index within its table writes one; elsewhere a name holding `@` is a name no
// index carries.
func cutIndexTable(sourcePlatform, written string) (table, index string, found bool) {
	if !factsOf(sourcePlatform).indexNamesPerTable {
		return "", "", false
	}
	quoted := false
	for i := 0; i < len(written); i++ {
		switch {
		case written[i] == '"':
			quoted = !quoted
		case !quoted && written[i] == '@':
			return strings.TrimSpace(written[:i]), strings.TrimSpace(written[i+1:]), true
		}
	}
	return "", "", false
}

// ambiguousIndexHint says how to name one of the indexes an ambiguous name
// reaches, on a dialect that has a spelling for it.
func ambiguousIndexHint(sourcePlatform string) string {
	if factsOf(sourcePlatform).indexNamesPerTable {
		return "; write it as table@index to name one"
	}
	return ""
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
