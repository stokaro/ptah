package postgresreplay

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/sqlsyntax/pgsyntax"
)

// This file holds what an object the replay creates can carry past the
// guard: a write to a namespace the server owns, made through a view, a rule,
// a trigger's argument or a table a write carries on to, and a function the
// guard refuses as a call, named for the server to call later by an
// operator, an aggregate or a type, or called by a trigger's argument.
// Measured on PostgreSQL 18, each statement below is accepted on its own when
// it is read only for what it writes or calls itself:
//
//	CREATE VIEW v AS SELECT * FROM pg_authid;
//	UPDATE v SET rolcreatedb = true WHERE rolname = 'x';
//	CREATE OPERATOR public.!!! (function = pg_terminate_backend, leftarg = int, rightarg = int8);
//	SELECT 193 !!! 0::int8;
//
// The first pair sets rolcreatedb in pg_authid, the second terminates a
// backend. A view that only reads the catalog is common and harmless, so a
// view is not refused for reading one: the write through it is, judged when
// it is written over the relations as they stand then ([relationGraph]). A
// function named by an object is refused where it is named, as it would be
// where it is called ([functionReach]).

// writeTargetNamespace returns the namespace the server owns that the
// relation a write names is in, or "": the schema it is qualified with
// ([protection.qualifierNamespace]), or, written without one, the namespace
// the session's catalog holds a relation of its name in
// ([protection.unqualifiedNamespace]). What the write reaches through the
// relation is read by [protection.graphWriteNamespace].
func (p protection) writeTargetNamespace(parts []string) string {
	if namespace := p.qualifierNamespace(parts); namespace != "" {
		return namespace
	}
	if len(parts) == 1 {
		return p.unqualifiedNamespace(classRelation, parts[0])
	}
	return ""
}

// protectedNameReach returns the namespace the server owns that a dotted
// name of any class names a relation of, or "": a name in a view's query, in
// a rule's action or in a trigger's argument. There a dotted name may be a
// relation and its column, as `pg_class.relname` is, so its first part is
// read as a relation the session's catalog holds, and every later part as a
// relation of the part before it. A name qualified with a namespace the
// server owns counts only when it is a relation of that namespace, so a cast
// to pg_catalog.text, or the text search configuration pg_catalog.english,
// reaches nothing.
func (p protection) protectedNameReach(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	if namespace := p.unqualifiedNamespace(classRelation, parts[0]); namespace != "" {
		return namespace
	}
	for index := 1; index < len(parts); index++ {
		namespace := p.qualifierNamespace(parts[index-1 : index+1])
		if namespace != "" && p.protectedRelationNamed(namespace, parts[index]) {
			return namespace
		}
	}
	return ""
}

// protectedRelationNamed reports whether name, qualified with namespace, a
// name the server reserves for its own namespaces, can be a relation of a
// namespace the server owns. The session's catalog says so for the namespaces
// it read, the ones of its search path; another is taken to hold the relation
// when the server creates a namespace of that name ([unreadServerNamespace]).
// Any other qualifier, as pg_class in `pg_class.relname`, is no namespace, and
// the relation it names is read for its own name. A guard that read no session
// reads pg_catalog by [fallbackCatalogNamespace].
func (p protection) protectedRelationNamed(namespace, name string) bool {
	namespace, folded := strings.ToLower(namespace), strings.ToLower(name)
	if len(p.catalog) == 0 {
		if namespace == "pg_catalog" {
			return fallbackCatalogNamespace(p.dialect, folded) == namespace
		}
		return unreadServerNamespace(namespace)
	}
	for _, read := range p.catalog[classRelation] {
		if read == namespace {
			return p.catalog[classRelation][folded] != ""
		}
	}
	return unreadServerNamespace(namespace)
}

// unreadServerNamespace reports a namespace the server creates that is not on
// a session's search path, so the session's catalog holds none of its names:
// pg_toast, information_schema, CockroachDB's crdb_internal, and another
// session's temporary and TOAST namespaces. No user can create a schema whose
// name begins with pg_, so a qualifier that begins with it and is none of
// these names no namespace at all.
func unreadServerNamespace(namespace string) bool {
	switch namespace {
	case "pg_catalog", "pg_toast", "pg_temp", "information_schema", "crdb_internal":
		return true
	}
	return strings.HasPrefix(namespace, "pg_temp_") || strings.HasPrefix(namespace, "pg_toast_temp_")
}

// writesRows reports a statement that writes the rows of the relation it
// names: INSERT, UPDATE, DELETE, MERGE and TRUNCATE.
func writesRows(tokens []lexer.Token) bool {
	switch replay.NormalizedIdentifier(tokens[0]) {
	case "DELETE", "INSERT", "MERGE", "TRUNCATE", "UPDATE":
		return true
	}
	return false
}

// embeddedWriteNamespace returns the namespace the server owns that the
// target of a write inside tokens is in, as [protection.writeTargetNamespace]
// answers, or "": a write in a common table expression, in a rule's action,
// or in a routine's SQL-standard body.
func (p protection) embeddedWriteNamespace(tokens []lexer.Token) string {
	for index := range tokens {
		switch replay.NormalizedIdentifier(tokens[index]) {
		case "DELETE", "INSERT", "MERGE", "TRUNCATE", "UPDATE":
			for _, target := range replay.MutationTargets(tokens[index:]) {
				if namespace := p.writeTargetNamespace(target); namespace != "" {
					return namespace
				}
			}
		}
	}
	return ""
}

// graphWriteNamespace returns the namespace the server owns that a write the
// statement makes reaches through the relations the guard knows, as
// [relationGraph.writeReach] answers for each relation the statement writes
// ([writeTargets]), or "". It is asked of every statement, so a write in a
// common table expression, an INSERT ... ON CONFLICT, a MERGE, a TRUNCATE
// and a rule's action are judged as a plain write is, when they are written.
func (p protection) graphWriteNamespace(tokens []lexer.Token) string {
	for _, target := range writeTargets(tokens) {
		if namespace := p.graphReach(target); namespace != "" {
			return namespace
		}
	}
	return ""
}

// graphReach is [relationGraph.writeReach] for the relation the dotted folded
// name target names, recording the relation as what the change went through,
// and the name, when it has no schema, as one to qualify: the relation the
// server finds for a name without a schema depends on the search path, and a
// guard that read no session matches it in every schema.
func (p protection) graphReach(target []string) string {
	namespace, _ := p.graph.writeReach(p, target)
	if namespace == "" {
		return ""
	}
	p.recordThrough(fmt.Sprintf("the relation %q", target[len(target)-1]))
	if len(target) == 1 && p.unqualified != nil && *p.unqualified == "" {
		*p.unqualified = target[0]
	}
	return namespace
}

// scanning is p for a scan whose finding is not a refusal of the statement
// read: it records no unqualified name and nothing a change went through.
func (p protection) scanning() protection {
	p.unqualified = nil
	p.through = nil
	return p
}

// triggerArguments returns the arguments of the function a CREATE [OR
// REPLACE] [CONSTRAINT] TRIGGER runs, or none for another statement.
func triggerArguments(tokens []lexer.Token) [][]lexer.Token {
	if wordAt(tokens, 0) != "CREATE" {
		return nil
	}
	kindIndex := postgresSpecialDDLKindIndex(tokens)
	if !replay.TokenSequenceAt(tokens, kindIndex, "TRIGGER") && !replay.TokenSequenceAt(tokens, kindIndex, "CONSTRAINT", "TRIGGER") {
		return nil
	}
	execute := findPostgresKeyword(tokens, "EXECUTE", kindIndex+1)
	if execute == replay.MutationTargetNotFound {
		return nil
	}
	_, open := replay.QualifiedIdentifierAt(tokens, execute+2)
	return postgresCallArguments(tokens, open)
}

// triggerArgumentNamespace returns the namespace the server owns that an
// argument of the function a CREATE TRIGGER runs names a relation of, or a
// relation the guard knows that a write reaches one through, or "". A trigger
// function reads its arguments as text, and some run them as SQL: measured on
// PostgreSQL 18, `EXECUTE PROCEDURE check_foreign_key(1, 'cascade', 'id',
// 'pg_catalog.pg_description', 'objoid')`, from the refint extension, deletes
// rows of pg_description when a row of the trigger's table is deleted. The
// function is the server's or the extension's, and the guard cannot tell
// which argument it runs, so every argument is read. The text search
// configuration `'pg_catalog.english'` of tsvector_update_trigger is no
// relation and is accepted. A name the arguments hold is looked up again each
// time the trigger's table is written ([relationGraph]), so a view created
// after the trigger is judged too.
func (p protection) triggerArgumentNamespace(tokens []lexer.Token) string {
	for _, argument := range triggerArguments(tokens) {
		scanned, ok := triggerArgumentTokens(argument)
		if !ok {
			// The guard cannot say which relation an argument it cannot read
			// names, so it takes it to name one of pg_catalog.
			p.recordThrough(fmt.Sprintf("the trigger argument %s, which the guard cannot read", argumentText(argument)))
			return "pg_catalog"
		}
		scan := p.scanning()
		for _, parts := range dottedNames(scanned) {
			namespace := scan.protectedNameReach(parts)
			if namespace == "" {
				namespace = p.graph.nameReach(scan, parts)
			}
			if namespace != "" {
				p.recordThrough("the trigger argument " + argumentText(argument))
				return namespace
			}
		}
	}
	return ""
}

// triggerArgumentCall names the first function an argument of the function a
// CREATE TRIGGER runs calls that the guard refuses as a call
// ([refusedPostgresCall]), or is "". Measured on PostgreSQL 18 and
// YugabyteDB 2026.1, check_foreign_key runs `pid = pid AND
// pg_catalog.set_config('standard_conforming_strings', 'off', false) IS NOT
// NULL AND pid`, passed as its column argument, when a row of the trigger's
// table is deleted.
func triggerArgumentCall(tokens []lexer.Token, scope replay.Scope) string {
	for _, argument := range triggerArguments(tokens) {
		scanned, ok := triggerArgumentTokens(argument)
		if !ok {
			continue
		}
		if operation, name := refusedPostgresCall(scanned, scope); operation != "" {
			return fmt.Sprintf("%s %q called by the trigger argument %s", operation, name, argumentText(argument))
		}
	}
	return ""
}

// triggerArgumentTokens returns the tokens of the text a trigger argument
// passes its function: the text of a string constant, read as the server reads
// it ([pgsyntax.Value]) with the quoted identifiers in it decoded, or the
// argument's own tokens for a name or a number. It reports false for an
// argument that holds a string constant the guard cannot read as the whole
// argument.
func triggerArgumentTokens(argument []lexer.Token) ([]lexer.Token, bool) {
	constant := slices.ContainsFunc(argument, func(token lexer.Token) bool {
		return token.Type == lexer.TokenString && !quotedIdentifier(token)
	})
	if !constant {
		return argument, true
	}
	var source strings.Builder
	for index, token := range argument {
		if index > 0 && argument[index-1].End != token.Start {
			// A string continues on a later line only; the newline stands for
			// the white space the lexer dropped.
			source.WriteByte('\n')
		}
		source.WriteString(token.Value)
	}
	text := source.String()
	scanned, err := pgsyntax.Scan(text, pgsyntax.Options{})
	if err != nil {
		return nil, false
	}
	value, next, ok := pgsyntax.Value(text, scanned, 0, pgsyntax.Options{})
	if !ok || next != len(scanned) {
		return nil, false
	}
	return decodedUnicodeIdentifiers(value, replay.Tokens(value, lexerOptions()))
}

// recordThrough records what carried a change to a namespace the server owns,
// unless a reader recorded it first; see [refuseProtectedNamespace].
func (p protection) recordThrough(through string) {
	if p.through != nil && *p.through == "" {
		*p.through = through
	}
}

// argumentText is an argument as the statement writes it.
func argumentText(argument []lexer.Token) string {
	values := make([]string, 0, len(argument))
	for _, token := range argument {
		values = append(values, token.Value)
	}
	return strings.Join(values, " ")
}

// referencedFunctionReach names the first function a statement names for the
// server to call later that [functionReach] refuses, or is "". An operator,
// an aggregate, a type, a text search parser or template, an operator class,
// a conversion, a cast, a transform, a language, an access method and a
// foreign-data wrapper each name their functions without calling them, and
// the server calls each one whenever the object is used: measured on
// PostgreSQL 18 and YugabyteDB 2026.1, an operator whose FUNCTION is
// pg_terminate_backend terminates a backend when a query uses it, and an
// aggregate whose SFUNC is set_config turns standard_conforming_strings off.
// set_config is refused here whatever it sets, because the arguments the
// server passes it are not written in the statement. A function the
// statement calls, with parentheses, is read by
// [dangerousPostgresFunctionCall], as a trigger's, a cast's and an operator
// class's usually are; CockroachDB has none of these objects.
func referencedFunctionReach(tokens []lexer.Token, scope replay.Scope) string {
	first := replay.NormalizedIdentifier(tokens[0])
	if first != "CREATE" && first != "ALTER" {
		return ""
	}
	kindIndex := postgresSpecialDDLKindIndex(tokens)
	if replay.TokenSequenceAt(tokens, kindIndex, "DEFAULT", "CONVERSION") {
		kindIndex++
	}
	switch replay.NormalizedIdentifier(replay.TokenAt(tokens, kindIndex)) {
	case "OPERATOR":
		if replay.TokenSequenceAt(tokens, kindIndex, "OPERATOR", "CLASS") ||
			replay.TokenSequenceAt(tokens, kindIndex, "OPERATOR", "FAMILY") {
			return operatorClassFunctionReach(tokens, kindIndex+2, scope)
		}
		return definitionFunctionReach(tokens, kindIndex+1, scope)
	case "AGGREGATE", "TYPE":
		return definitionFunctionReach(tokens, kindIndex+1, scope)
	case "TEXT":
		if replay.TokenSequenceAt(tokens, kindIndex, "TEXT", "SEARCH") {
			return definitionFunctionReach(tokens, kindIndex+2, scope)
		}
	case "CONVERSION":
		from := findPostgresKeyword(tokens, "FROM", kindIndex+1)
		if from == replay.MutationTargetNotFound {
			return ""
		}
		return namedFunctionReach(tokens, from+1, "FROM", scope)
	}
	return clauseFunctionReach(tokens, kindIndex+1, scope)
}

// definitionFunctionReach reads every `label = value` of the parenthesized
// definition lists from start on and judges every name and string the value
// holds with [functionReach], whatever the label says: a label the guard does
// not list, an old spelling such as SFUNC1, and a label written as a quoted
// identifier, `"function"`, name a function the same way, and a string
// constant names one as well as an identifier does. An item without `=`, a
// composite type's attribute or an enum's label, names no function.
func definitionFunctionReach(tokens []lexer.Token, start int, scope replay.Scope) string {
	depth := 0
	for index := start; index+1 < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
			continue
		case tokens[index].MatchOperatorValue(")"):
			depth--
			continue
		}
		if depth == 0 || !tokens[index+1].MatchOperatorValue("=") ||
			!tokens[index-1].MatchOperatorValue("(") && !tokens[index-1].MatchOperatorValue(",") {
			continue
		}
		label := replay.NormalizedIdentifier(tokens[index])
		if label == "" {
			continue
		}
		if operation := definitionValueReach(tokens, index+2, label, scope); operation != "" {
			return operation
		}
	}
	return ""
}

// definitionValueReach judges every name and string of the value that begins
// at start and ends at the comma or the parenthesis that closes its item.
func definitionValueReach(tokens []lexer.Token, start int, label string, scope replay.Scope) string {
	depth := 0
	for index := start; index < len(tokens); index++ {
		token := tokens[index]
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			if depth == 0 {
				return ""
			}
			depth--
		case token.MatchOperatorValue(",") && depth == 0:
			return ""
		default:
			if operation := functionReach(replay.NormalizedIdentifier(token), scope); operation != "" {
				return fmt.Sprintf("%s %q named by %s", operation, replay.IdentifierValue(token), label)
			}
		}
	}
	return ""
}

// clauseFunctionReach reads the clauses that name a function without
// parentheses: `WITH FUNCTION name` of CREATE CAST and CREATE TRANSFORM, and
// `HANDLER name`, `VALIDATOR name` and `INLINE name` of CREATE LANGUAGE,
// CREATE ACCESS METHOD and CREATE FOREIGN DATA WRAPPER.
func clauseFunctionReach(tokens []lexer.Token, start int, scope replay.Scope) string {
	for index := start; index+1 < len(tokens); index++ {
		clause := wordAt(tokens, index)
		switch {
		case clause == "FUNCTION" && wordAt(tokens, index-1) == "WITH":
			clause = "WITH FUNCTION"
		case clause == "HANDLER", clause == "VALIDATOR", clause == "INLINE":
		default:
			continue
		}
		if operation := namedFunctionReach(tokens, index+1, clause, scope); operation != "" {
			return operation
		}
	}
	return ""
}

// operatorClassFunctionReach reads `FUNCTION number [(types)] function` in
// CREATE OPERATOR CLASS and ALTER OPERATOR FAMILY ... ADD, where the
// function's argument types may be left out and the function is then named
// without parentheses.
func operatorClassFunctionReach(tokens []lexer.Token, start int, scope replay.Scope) string {
	for index := start; index+2 < len(tokens); index++ {
		if !replay.TokenSequenceAt(tokens, index, "FUNCTION") || tokens[index+1].Type != lexer.TokenIdentifier {
			continue
		}
		name := index + 2
		if tokens[name].MatchOperatorValue("(") {
			name = closingParenthesis(tokens, name) + 1
		}
		if operation := namedFunctionReach(tokens, name, "FUNCTION", scope); operation != "" {
			return operation
		}
	}
	return ""
}

// namedFunctionReach is [functionReach] for the function whose dotted name
// begins at index, named by the clause clause.
func namedFunctionReach(tokens []lexer.Token, index int, clause string, scope replay.Scope) string {
	parts, next := replay.QualifiedIdentifierAt(tokens, index)
	if len(parts) == 0 {
		return ""
	}
	if operation := functionReach(replay.NormalizedIdentifier(tokens[next-1]), scope); operation != "" {
		return fmt.Sprintf("%s %q named by %s", operation, parts[len(parts)-1], clause)
	}
	return ""
}

// parentTableNamespace returns the namespace the server owns of a table a
// CREATE TABLE or an ALTER TABLE makes the parent of the table it names, or
// attaches to it as a partition, or "": `INHERITS (pg_catalog.pg_authid)`,
// `PARTITION OF`, `INHERIT` and `ATTACH PARTITION`. Each changes the
// catalog's own table, as it records the inheritance and marks the parent as
// one with children, and a write to the parent reaches the child.
func (p protection) parentTableNamespace(tokens []lexer.Token) string {
	var index int
	switch wordAt(tokens, 0) {
	case "CREATE":
		kindIndex, _ := createModifiers(tokens, postgresSpecialDDLKindIndex(tokens))
		if wordAt(tokens, kindIndex) == "FOREIGN" {
			kindIndex++
		}
		if wordAt(tokens, kindIndex) != "TABLE" {
			return ""
		}
		index = kindIndex + 1
	case "ALTER":
		kind, at := relationKind(tokens)
		if kind != "TABLE" {
			return ""
		}
		index = at
	default:
		return ""
	}
	related := inheritanceParents(tokens, index)
	for at := index; at < len(tokens); at++ {
		if replay.TokenSequenceAt(tokens, at, "ATTACH", "PARTITION") {
			if partition, _ := foldedNameAt(tokens, at+2); len(partition) > 0 {
				related = append(related, partition)
			}
		}
	}
	for _, parts := range related {
		if namespace := p.writeTargetNamespace(parts); namespace != "" {
			return namespace
		}
	}
	return ""
}
