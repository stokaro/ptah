package postgresreplay

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/sqlsyntax/pgsyntax"
)

// This file holds what an object the replay creates can carry past the
// guard: a write to a namespace the server owns, made through a view, a rule
// or a trigger's argument, and a function the guard refuses as a call, named
// for the server to call later by an operator, an aggregate or a type.
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
// view is not refused for reading one: the write through it is
// ([protection.relationReach]). A function named by an object is refused
// where it is named, as it would be where it is called ([functionReach]).

// namePosition says what a name stands for where a statement writes it.
type namePosition int

const (
	// relationPosition is a name the grammar reads as a relation: the target
	// of a write. Every name it qualifies with a namespace the server owns is
	// in that namespace.
	relationPosition namePosition = iota
	// anyPosition is a name of any class, such as one in a view's query or a
	// trigger's argument. A name qualified with a namespace the server owns
	// counts only when it is a relation of that namespace, so a cast to
	// pg_catalog.text, or the text search configuration pg_catalog.english,
	// reaches nothing.
	anyPosition
)

// relationReach is the one answer to whether writing the relation the dotted
// name parts names, standing at position, reaches a namespace the server
// owns, which it returns, or "". The name reaches one when its schema is one
// ([protection.qualifierNamespace]), when written without a schema it is a
// relation the session's catalog holds there
// ([protection.unqualifiedNamespace]), or when it is a relation the server
// writes a namespace it owns through ([reachingRelations]).
//
// A write statement asks it of its target, at relationPosition, and so does
// the action of a rule. The scan of a view's query and of a trigger's
// arguments asks it of every name they hold, at anyPosition
// ([protection.queryReach]): there a dotted name may be a relation and its
// column, as `pg_class.relname` is, so every part is read as a relation of
// the parts before it. A view whose query reaches a namespace the server owns
// becomes a relation the server writes it through, so a view over that view
// reaches it too.
func (p protection) relationReach(parts []string, position namePosition) string {
	if len(parts) == 0 {
		return ""
	}
	if position == relationPosition {
		if namespace := p.qualifierNamespace(parts); namespace != "" {
			return namespace
		}
		if len(parts) == 1 {
			if namespace := p.unqualifiedNamespace(classRelation, parts[0]); namespace != "" {
				return namespace
			}
		}
		return p.reachingRelationNamespace(parts)
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
	for index := range parts {
		if namespace := p.reachingRelationNamespace(parts[:index+1]); namespace != "" {
			return namespace
		}
	}
	return ""
}

// reachingRelationNamespace is [reachingRelations.namespace] for the relation
// parts names, recording the relation as what the change went through.
func (p protection) reachingRelationNamespace(parts []string) string {
	namespace := p.relations.namespace(parts)
	if namespace != "" {
		p.recordThrough(fmt.Sprintf("the relation %q", parts[len(parts)-1]))
	}
	return namespace
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

// embeddedWriteNamespace returns the namespace the server owns that a write
// inside tokens reaches, as [protection.relationReach] answers for its target,
// or "": a write in a common table expression, in a rule's action, or in a
// routine's SQL-standard body.
func (p protection) embeddedWriteNamespace(tokens []lexer.Token) string {
	for index := range tokens {
		switch replay.NormalizedIdentifier(tokens[index]) {
		case "DELETE", "INSERT", "MERGE", "TRUNCATE", "UPDATE":
			for _, target := range replay.MutationTargets(tokens[index:]) {
				if namespace := p.relationReach(target, relationPosition); namespace != "" {
					return namespace
				}
			}
		}
	}
	return ""
}

// queryReach returns the namespace the server owns that a name in tokens, a
// view's query or a rule's action, reaches as a relation, as
// [protection.relationReach] answers for a name of any class, or "". A name
// followed by a parenthesis is a call, not a relation: a function in FROM
// makes a view the server cannot write through, and a call the guard refuses
// is refused as a call. A string literal is a value, not a name.
//
// It reads every name rather than the ones a FROM clause holds, because the
// server reads a relation in other places as well, `TABLE pg_authid` among
// them, and a name it misses is a write it lets through. A view over a table
// of the user's whose query names a catalog relation elsewhere is read as one
// that reaches the catalog, and a write through it is refused.
func (p protection) queryReach(tokens []lexer.Token) string {
	for index := 0; index < len(tokens); index++ {
		token := tokens[index]
		if token.Type != lexer.TokenIdentifier && !quotedIdentifier(token) ||
			index > 0 && tokens[index-1].MatchOperatorValue(".") {
			continue
		}
		parts, next := replay.QualifiedIdentifierAt(tokens, index)
		if len(parts) == 0 {
			continue
		}
		index = next - 1
		if next < len(tokens) && tokens[next].MatchOperatorValue("(") {
			continue
		}
		if namespace := p.relationReach(parts, anyPosition); namespace != "" {
			return namespace
		}
	}
	return ""
}

// scanning is p for a scan whose finding is not a refusal of the statement
// read: it records no unqualified name and nothing a change went through.
func (p protection) scanning() protection {
	p.unqualified = nil
	p.through = nil
	return p
}

// triggerArgumentNamespace returns the namespace the server owns that an
// argument of the function a CREATE TRIGGER runs names a relation of, as
// [protection.queryReach] reads the argument, or "". A trigger function reads
// its arguments as text, and some run them as SQL: measured on PostgreSQL 18,
// `EXECUTE PROCEDURE check_foreign_key(1, 'cascade', 'id',
// 'pg_catalog.pg_description', 'objoid')`, from the refint extension, deletes
// rows of pg_description when a row of the trigger's table is deleted. The
// function is the server's or the extension's, and the guard cannot tell
// which argument it runs, so every argument is read. The text search
// configuration `'pg_catalog.english'` of tsvector_update_trigger is no
// relation and is accepted.
func (p protection) triggerArgumentNamespace(tokens []lexer.Token) string {
	if replay.NormalizedIdentifier(tokens[0]) != "CREATE" {
		return ""
	}
	kindIndex := postgresSpecialDDLKindIndex(tokens)
	if !replay.TokenSequenceAt(tokens, kindIndex, "TRIGGER") && !replay.TokenSequenceAt(tokens, kindIndex, "CONSTRAINT", "TRIGGER") {
		return ""
	}
	execute := findPostgresKeyword(tokens, "EXECUTE", kindIndex+1)
	if execute == replay.MutationTargetNotFound {
		return ""
	}
	_, open := replay.QualifiedIdentifierAt(tokens, execute+2)
	for _, argument := range postgresCallArguments(tokens, open) {
		scanned, ok := triggerArgumentTokens(argument)
		if !ok {
			// The guard cannot say which relation an argument it cannot read
			// names, so it takes it to name one of pg_catalog.
			p.recordThrough(fmt.Sprintf("the trigger argument %s, which the guard cannot read", argumentText(argument)))
			return "pg_catalog"
		}
		if namespace := p.scanning().queryReach(scanned); namespace != "" {
			p.recordThrough("the trigger argument " + argumentText(argument))
			return namespace
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

// supportFunctionOptions are the options of CREATE OPERATOR, CREATE AGGREGATE
// and CREATE TYPE, and of ALTER OPERATOR and ALTER TYPE ... SET, whose value
// is a function the server calls: when the operator is used, when the
// aggregate runs, when a value of the type is read, written or compared, and
// when the planner estimates a condition.
var supportFunctionOptions = []string{
	// CREATE OPERATOR and ALTER OPERATOR.
	"FUNCTION", "PROCEDURE", "RESTRICT", "JOIN",
	// CREATE AGGREGATE.
	"SFUNC", "FINALFUNC", "COMBINEFUNC", "SERIALFUNC", "DESERIALFUNC",
	"MSFUNC", "MINVFUNC", "MFINALFUNC",
	// CREATE TYPE, its range form, and ALTER TYPE ... SET.
	"INPUT", "OUTPUT", "RECEIVE", "SEND", "TYPMOD_IN", "TYPMOD_OUT",
	"ANALYZE", "SUBSCRIPT", "CANONICAL", "SUBTYPE_DIFF",
}

// referencedFunctionReach names the first function a statement names for the
// server to call later that [functionReach] refuses, or is "". An operator,
// an aggregate, a type, an operator class and a conversion each name their
// functions without calling them, and the server calls each one whenever the
// object is used: measured on PostgreSQL 18 and YugabyteDB 2026.1, an
// operator whose FUNCTION is pg_terminate_backend terminates a backend when a
// query uses it, and an aggregate whose SFUNC is set_config turns
// standard_conforming_strings off. set_config is refused here whatever it
// sets, because the arguments the server passes it are not written in the
// statement. A function the statement calls, with parentheses, is read by
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
		return supportFunctionReach(tokens, kindIndex+1, scope)
	case "AGGREGATE", "TYPE":
		return supportFunctionReach(tokens, kindIndex+1, scope)
	case "CONVERSION":
		from := findPostgresKeyword(tokens, "FROM", kindIndex+1)
		if from == replay.MutationTargetNotFound {
			return ""
		}
		return namedFunctionReach(tokens, from+1, "FROM", scope)
	}
	return ""
}

// supportFunctionReach reads every `option = function` from start on whose
// option is one of [supportFunctionOptions].
func supportFunctionReach(tokens []lexer.Token, start int, scope replay.Scope) string {
	for index := start; index+2 < len(tokens); index++ {
		option := replay.NormalizedIdentifier(tokens[index])
		if tokens[index].Type != lexer.TokenIdentifier || !tokens[index+1].MatchOperatorValue("=") ||
			!slices.Contains(supportFunctionOptions, option) {
			continue
		}
		if operation := namedFunctionReach(tokens, index+2, option, scope); operation != "" {
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

// recordReachingRelation adds to p's [reachingRelations] what an accepted
// statement makes a relation the server writes a namespace it owns through:
// a view whose query reaches one ([protection.queryReach]), a table a SELECT
// rule turns into such a view, and such a relation renamed or moved to
// another schema. A relation dropped or replaced stays recorded, so a write to
// a table later created under its name is refused too: the guard does not
// follow the objects of the dev database, and a name it forgets is a write it
// lets through.
func (p protection) recordReachingRelation(tokens []lexer.Token) {
	if p.relations == nil {
		return
	}
	scan := p.scanning()
	switch replay.NormalizedIdentifier(tokens[0]) {
	case "CREATE":
		if table, namespace := scan.ruleReach(tokens); namespace != "" {
			p.relations.add(table, namespace)
			return
		}
		kindIndex := replay.StatementObjectKindIndex(tokens)
		if kindIndex == replay.MutationTargetNotFound || !replay.TokenSequenceAt(tokens, kindIndex, "VIEW") {
			return
		}
		parts, next := replay.QualifiedIdentifierAt(tokens, replay.ObjectNameIndex(tokens, kindIndex))
		if len(parts) == 0 {
			return
		}
		if namespace := scan.queryReach(tokens[next:]); namespace != "" {
			p.relations.add(parts, namespace)
		}
	case "ALTER":
		p.relations.moved(tokens)
	}
}

// ruleReach reads `CREATE [OR REPLACE] RULE name AS ON event TO table ... DO
// action` and returns the table's dotted name and the namespace the server
// owns that a write to the table reaches through the rule, or "". A SELECT
// rule makes the table a view of its action's query, as every view's rule is,
// so the action reaches a namespace when its query does
// ([protection.queryReach]); a server older than PostgreSQL 16 turns a table
// into a view that way. Any other rule runs its action when the table is
// written, so it reaches one when the action writes one
// ([protection.embeddedWriteNamespace]); the guard refuses such a rule the
// replay creates, and [readReachingRelations] reads one the dev database
// holds.
func (p protection) ruleReach(tokens []lexer.Token) ([]string, string) {
	kindIndex := postgresSpecialDDLKindIndex(tokens)
	if replay.NormalizedIdentifier(tokens[0]) != "CREATE" || !replay.TokenSequenceAt(tokens, kindIndex, "RULE") {
		return nil, ""
	}
	on := findPostgresKeyword(tokens, "ON", kindIndex+1)
	if on == replay.MutationTargetNotFound {
		return nil, ""
	}
	to := findPostgresKeyword(tokens, "TO", on+1)
	if to == replay.MutationTargetNotFound {
		return nil, ""
	}
	table, next := replay.QualifiedIdentifierAt(tokens, to+1)
	do := findPostgresKeyword(tokens, "DO", next)
	if len(table) == 0 || do == replay.MutationTargetNotFound {
		return nil, ""
	}
	action := tokens[do+1:]
	if replay.TokenSequenceAt(tokens, on+1, "SELECT") {
		return table, p.queryReach(action)
	}
	return table, p.embeddedWriteNamespace(action)
}

// reachingRelations are the relations outside the namespaces the server owns
// that a write reaches one of those through: a view whose query reads one, a
// relation a rule makes write one, and a view over either. Each is held by its
// name, folded to lower case, and the schema it was named with, or "" for one
// named without a schema, which matches a relation of its name in every
// schema. A write to a relation named without a schema matches every schema
// too, since the guard cannot always say which schema the server finds it in.
//
// They are the ones the dev database held when [guard.ValidateSession] read
// it ([readReachingRelations]) and the ones the run's accepted statements
// created ([protection.recordReachingRelation]). The guard and every copy of
// it share one value, so a statement is read with every relation the
// statements validated before it created. The methods take nil as holding
// none.
type reachingRelations struct {
	mu     sync.Mutex
	byName map[string][]reachingRelation
}

// reachingRelation is one of [reachingRelations]: the schema it is in, or ""
// when the guard cannot say, and the namespace the server owns that a write
// to it reaches.
type reachingRelation struct {
	schema, namespace string
}

func newReachingRelations() *reachingRelations {
	return &reachingRelations{byName: make(map[string][]reachingRelation)}
}

// namespace returns the namespace the server owns that a write to the relation
// the dotted name parts names reaches, or "". The schema is the part before
// the last, which a name written without a schema leaves out.
func (r *reachingRelations) namespace(parts []string) string {
	if r == nil || len(parts) == 0 {
		return ""
	}
	schema := ""
	if len(parts) >= 2 {
		schema = parts[len(parts)-2]
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, relation := range r.byName[strings.ToLower(parts[len(parts)-1])] {
		if relation.schema == "" || schema == "" || strings.EqualFold(relation.schema, schema) {
			return relation.namespace
		}
	}
	return ""
}

// add records that a write to the relation the dotted name parts names
// reaches namespace, and reports whether the relation was not recorded
// before.
func (r *reachingRelations) add(parts []string, namespace string) bool {
	if r == nil || len(parts) == 0 {
		return false
	}
	relation := reachingRelation{namespace: namespace}
	if len(parts) >= 2 {
		relation.schema = strings.ToLower(parts[len(parts)-2])
	}
	name := strings.ToLower(parts[len(parts)-1])
	r.mu.Lock()
	defer r.mu.Unlock()
	if slices.Contains(r.byName[name], relation) {
		return false
	}
	r.byName[name] = append(r.byName[name], relation)
	return true
}

// moved records a relation of r under the name or schema an ALTER gives it:
// `ALTER TABLE|VIEW|MATERIALIZED VIEW name RENAME TO new` and `... SET SCHEMA
// schema`. The relation stays recorded under its old name as well. Renaming
// the schema the relation is in would move it too, and the guard refuses
// ALTER SCHEMA in every scope.
func (r *reachingRelations) moved(tokens []lexer.Token) {
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return
	}
	parts, next := replay.QualifiedIdentifierAt(tokens, replay.ObjectNameIndex(tokens, kindIndex))
	namespace := r.namespace(parts)
	if namespace == "" {
		return
	}
	switch {
	case replay.TokenSequenceAt(tokens, next, "RENAME", "TO"):
		renamed, _ := replay.QualifiedIdentifierAt(tokens, next+2)
		if len(renamed) == 1 {
			r.add(append(slices.Clone(parts[:len(parts)-1]), renamed[0]), namespace)
		}
	case replay.TokenSequenceAt(tokens, next, "SET", "SCHEMA"):
		schema, _ := replay.QualifiedIdentifierAt(tokens, next+2)
		if len(schema) == 1 {
			r.add([]string{schema[0], parts[len(parts)-1]}, namespace)
		}
	}
}

// readReachingRelations decodes the fifth column of [sessionCatalogQuery], the
// rules of the dev database as pg_get_ruledef writes them, and returns the
// relations a write reaches a namespace the server of dialect owns through,
// as [protection.ruleReach] reads each rule with the session's catalog. A
// rule of a relation in a namespace the server owns is left out: a write to
// that relation is refused for its namespace. The rules are read again until
// none adds a relation, so a view over a view over pg_authid is found
// whichever order the server lists them in.
func readReachingRelations(dialect string, catalog catalogNames, encoded string) (*reachingRelations, error) {
	var rows [][3]string
	if err := json.Unmarshal([]byte(encoded), &rows); err != nil {
		return nil, fmt.Errorf("read the dev session's rules: %w", err)
	}
	relations := newReachingRelations()
	policy := protection{dialect: dialect, catalog: catalog, relations: relations}
	type rule struct {
		table  []string
		tokens []lexer.Token
	}
	rules := make([]rule, 0, len(rows))
	for _, row := range rows {
		if policy.isProtectedPostgresNamespace(row[0]) {
			continue
		}
		rules = append(rules, rule{table: []string{row[0], row[1]}, tokens: replay.Tokens(row[2], lexerOptions())})
	}
	for added := true; added; {
		added = false
		for _, read := range rules {
			if _, namespace := policy.ruleReach(read.tokens); namespace != "" && relations.add(read.table, namespace) {
				added = true
			}
		}
	}
	return relations, nil
}
