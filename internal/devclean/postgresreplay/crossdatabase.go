package postgresreplay

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// crossDatabaseName returns, on CockroachDB, the first name a statement
// writes, defines, drops, comments on or grants on that is an object of
// another database than the dev database, as written, or "". CockroachDB
// reads a name of three parts as database, schema and object, and a name of
// two parts whose first part is no schema of the current database as
// database and object of its public schema: measured on 26.4, `INSERT INTO
// other.t` and `INSERT INTO other.public.t` from a session of the dev
// database each write the table t of the database other. The cleanup drops
// the dev database's objects only, so the write outlives it. A schema the
// dev database holds, as the session read it and the replay changed it, and
// the dev database's own name, are this database; a name the guard cannot
// place, from a guard that read no session, is accepted.
//
// PostgreSQL and YugabyteDB refuse a name of another database themselves
// ("cross-database references are not implemented"), so the rule is
// CockroachDB's alone. A run that trusts its SQL lifts it, as it lifts
// dblink, which reaches another database too.
func (p protection) crossDatabaseName(tokens []lexer.Token) string {
	if p.dialect != platform.CockroachDB || p.trustedSQL || p.database == "" {
		return ""
	}
	for _, name := range statementObjectNames(tokens) {
		if p.otherDatabase(name.parts, name.schema) {
			return strings.Join(name.parts, ".")
		}
	}
	return ""
}

// otherDatabase reports whether the dotted folded name parts, of a schema
// when schema is set, names an object of another database.
func (p protection) otherDatabase(parts []string, schema bool) bool {
	switch {
	case schema && len(parts) == 2, len(parts) >= 3:
		return parts[0] != p.database
	case len(parts) == 2 && !schema:
		if parts[0] == p.database || p.isProtectedPostgresNamespace(parts[0]) {
			return false
		}
		known, read := p.graph.knowsSchema(parts[0])
		return read && !known
	}
	return false
}

// objectName is a dotted folded name a statement acts on, and whether it
// names a schema, whose name of two parts is database and schema.
type objectName struct {
	parts  []string
	schema bool
}

// statementObjectNames returns the names a statement writes the rows of
// ([writeTargets]), copies into, creates, alters, drops, comments on or
// grants a privilege on.
func statementObjectNames(tokens []lexer.Token) []objectName {
	var names []objectName
	for _, parts := range writeTargets(tokens) {
		names = append(names, objectName{parts: parts})
	}
	switch wordAt(tokens, 0) {
	case "COPY":
		if parts, _ := foldedNameAt(tokens, 1); len(parts) > 0 {
			names = append(names, objectName{parts: parts})
		}
	case "CREATE", "ALTER", "DROP":
		names = append(names, ddlObjectNames(tokens)...)
	case "COMMENT":
		if wordAt(tokens, 1) == "ON" {
			names = append(names, namesAfterKind(tokens, 2)...)
		}
	case "GRANT", "REVOKE":
		if on := findPostgresKeyword(tokens, "ON", 1); on != replay.MutationTargetNotFound {
			names = append(names, namesAfterKind(tokens, on+1)...)
		}
	}
	return names
}

// ddlObjectNames returns the names a CREATE, an ALTER or a DROP acts on: the
// objects it names, every one of a DROP's list, and the table an index or a
// trigger is on. A database, a role and the other objects of the server are
// read by [isGlobalPostgresDDL].
func ddlObjectNames(tokens []lexer.Token) []objectName {
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return nil
	}
	kind := wordAt(tokens, kindIndex)
	if kind == "DATABASE" || kind == "ROLE" || kind == "USER" {
		return nil
	}
	index := afterIfNotExists(tokens, replay.ObjectNameIndex(tokens, kindIndex))
	if wordAt(tokens, index) == "CONCURRENTLY" {
		index = afterIfNotExists(tokens, index+1)
	}
	if wordAt(tokens, index) == "ONLY" {
		index++
	}
	names := namesAt(tokens, index, shapeOf(kind))
	if kind == "INDEX" || kind == "UNIQUE" || kind == "TRIGGER" {
		names = append(names, tableAfterOn(tokens, kindIndex+1)...)
	}
	return names
}

// namesAfterKind reads the kind of object at index of `COMMENT ON kind name`
// or `GRANT ... ON kind name`, and returns the names after it: a column's
// without the column, and a constraint's, a trigger's, a policy's or a rule's
// table. A privilege written with no kind is on a table. An object of the
// server, a database or a role, and `ALL ... IN SCHEMA`, name nothing here.
func namesAfterKind(tokens []lexer.Token, index int) []objectName {
	kind := wordAt(tokens, index)
	switch kind {
	case "MATERIALIZED", "FOREIGN":
		if wordAt(tokens, index+1) != "VIEW" && wordAt(tokens, index+1) != "TABLE" {
			return nil
		}
		return namesAt(tokens, index+2, objectNames)
	case "OPERATOR":
		if word := wordAt(tokens, index+1); word == "CLASS" || word == "FAMILY" {
			index++
		}
	case "TEXT":
		index += 2
	case "CONSTRAINT", "TRIGGER", "POLICY", "RULE":
		return tableAfterOn(tokens, index+1)
	case "ACCESS", "ALL", "CAST", "DATABASE", "EVENT", "LANGUAGE", "LARGE", "PUBLICATION", "ROLE",
		"SERVER", "SUBSCRIPTION", "TABLESPACE", "TRANSFORM":
		return nil
	case "AGGREGATE", "COLLATION", "COLUMN", "CONVERSION", "DOMAIN", "EXTENSION", "FUNCTION", "INDEX",
		"PROCEDURE", "ROUTINE", "SCHEMA", "SEQUENCE", "STATISTICS", "TABLE", "TYPE", "VIEW":
	default:
		return namesAt(tokens, index, objectNames)
	}
	return namesAt(tokens, index+1, shapeOf(kind))
}

// nameShape is what each dotted name of a list names.
type nameShape int

const (
	// objectNames name objects of a schema.
	objectNames nameShape = iota
	// schemaNames name schemas.
	schemaNames
	// columnNames name columns, after the relation that holds each.
	columnNames
)

// shapeOf is the shape of the names that follow the object kind kind.
func shapeOf(kind string) nameShape {
	switch kind {
	case "SCHEMA":
		return schemaNames
	case "COLUMN":
		return columnNames
	}
	return objectNames
}

// namesAt reads the comma-separated dotted names of shape that begin at
// index. A column's name loses its last part, which names the column, and
// keeps the relation's.
func namesAt(tokens []lexer.Token, index int, shape nameShape) []objectName {
	var names []objectName
	for _, parts := range nameList(tokens, index) {
		if shape == columnNames {
			parts = parts[:len(parts)-1]
		}
		names = append(names, objectName{parts: parts, schema: shape == schemaNames})
	}
	return names
}

// tableAfterOn returns the table named after the first ON from index on.
func tableAfterOn(tokens []lexer.Token, index int) []objectName {
	on := findPostgresKeyword(tokens, "ON", index)
	if on == replay.MutationTargetNotFound {
		return nil
	}
	at := on + 1
	if wordAt(tokens, at) == "ONLY" {
		at++
	}
	return namesAt(tokens, at, objectNames)
}
