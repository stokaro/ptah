package safety

import (
	"reflect"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/internal/lintscan"
)

// noSubject is what the safety report prints in the subject column of a
// statement that names no object, such as an anonymous DO block. The column
// never falls back to [StatementAssessment.NodeType]: that is a Go type name,
// and a reader of the report cannot act on it.
const noSubject = "-"

// reportSubject is the subject column of one row of the text and HTML reports.
func reportSubject(assessment StatementAssessment) string {
	if assessment.Subject == "" {
		return noSubject
	}
	return assessment.Subject
}

// nodeSubject names what a statement node acts on, the way the statement names
// it: the table an ALTER TABLE or a row-security switch changes, the index, the
// view, the policy, the role, or the object a grant is on. A schema-qualified
// sequence is named with its schema.
//
// Raw SQL and a routine kept as its SQL text are named by their words; see
// [statementSubject]. It is empty for a node that names no object: a statement
// list, a comment, an anonymous block, an owner operation that reports no
// name, and text whose name the words do not give. A rendered statement of
// such a node is named by its own words instead.
func nodeSubject(node ast.Node) string {
	if value := reflect.ValueOf(node); !value.IsValid() || value.Kind() == reflect.Pointer && value.IsNil() {
		return ""
	}
	if name, ok := namedNodeSubject(node); ok {
		return name
	}
	switch n := node.(type) {
	case *ast.AlterSequenceNode:
		return qualifiedSubject(n.Schema, n.Name)
	case *ast.CreateSequenceNode:
		return qualifiedSubject(n.Schema, n.Name)
	case *ast.DropSequenceNode:
		return qualifiedSubject(n.Schema, n.Name)
	case *ast.AlterSerialSequenceNode:
		// The sequence belongs to the column and no declaration names it, so
		// the row names the column whose values it hands out.
		return qualifiedSubject(n.Table, n.Column)
	case *ast.ExtensionStatement:
		return extensionSubject(n.Payload)
	case *ast.ExtensionAlterOperation:
		return extensionSubject(n.Payload)
	case *ast.RawSQLNode:
		return statementSubject(n.SQL, "")
	case *ast.OpaqueRoutineNode:
		return statementSubject(n.SQL, n.Dialect)
	}
	return ""
}

// namedNodeSubject is the subject of a node that names its object in one field.
// The kinds are split by what the statement does to the object, so each switch
// stays readable.
func namedNodeSubject(node ast.Node) (string, bool) {
	for _, subject := range []func(ast.Node) (string, bool){
		changedNodeSubject,
		createdNodeSubject,
		droppedNodeSubject,
		grantedNodeSubject,
	} {
		if name, ok := subject(node); ok {
			return name, true
		}
	}
	return "", false
}

// changedNodeSubject is the subject of a node that changes an object that
// exists already, its comment, or its rows.
func changedNodeSubject(node ast.Node) (string, bool) {
	switch n := node.(type) {
	case *ast.AlterTableNode:
		return n.Name, true
	case *ast.AlterIndexNode:
		return n.Name, true
	case *ast.AlterRoleNode:
		return n.Name, true
	case *ast.AlterTypeNode:
		return n.Name, true
	case *ast.AlterViewOptionsNode:
		return n.Name, true
	case *ast.AlterOwnerNode:
		return n.Name, true
	case *ast.AlterTableDisableRLSNode:
		return n.Table, true
	case *ast.AlterTableEnableRLSNode:
		return n.Table, true
	case *ast.AlterTableForceRLSNode:
		return n.Table, true
	case *ast.ObjectCommentNode:
		return n.Name, true
	case *ast.RefreshMaterializedViewNode:
		return n.Name, true
	case *ast.UpsertNode:
		return n.Table, true
	}
	return "", false
}

// createdNodeSubject is the subject of a node that creates an object.
func createdNodeSubject(node ast.Node) (string, bool) {
	switch n := node.(type) {
	case *ast.CreateDatabaseNode:
		return n.Name, true
	case *ast.CreateFunctionNode:
		return n.Name, true
	case *ast.CreateMaterializedViewNode:
		return n.Name, true
	case *ast.CreatePolicyNode:
		return n.Name, true
	case *ast.CreateRoleNode:
		return n.Name, true
	case *ast.CreateSchemaNode:
		return n.Name, true
	case *ast.CreateTableNode:
		return n.Name, true
	case *ast.CreateTriggerNode:
		return n.Name, true
	case *ast.CreateTypeNode:
		return n.Name, true
	case *ast.CreateViewNode:
		return n.Name, true
	case *ast.EnumNode:
		return n.Name, true
	case *ast.IndexNode:
		return n.Name, true
	case *ast.RoutineNode:
		return n.Name, true
	}
	return "", false
}

// grantedNodeSubject is the subject of a node that grants or revokes: the
// object a privilege is on, or the role whose membership changes.
func grantedNodeSubject(node ast.Node) (string, bool) {
	switch n := node.(type) {
	case *ast.GrantPrivilegeNode:
		return n.ObjectName, true
	case *ast.RevokePrivilegeNode:
		return n.ObjectName, true
	case *ast.GrantRoleMembershipNode:
		return n.Role, true
	case *ast.RevokeRoleMembershipNode:
		return n.Role, true
	}
	return "", false
}

// droppedNodeSubject is the subject of a node that drops an object.
func droppedNodeSubject(node ast.Node) (string, bool) {
	switch n := node.(type) {
	case *ast.DropTableNode:
		return n.Name, true
	case *ast.DropFunctionNode:
		return n.Name, true
	case *ast.DropIndexNode:
		return n.Name, true
	case *ast.DropMaterializedViewNode:
		return n.Name, true
	case *ast.DropPolicyNode:
		return n.Name, true
	case *ast.DropRoleNode:
		return n.Name, true
	case *ast.DropTriggerNode:
		return n.Name, true
	case *ast.DropTypeNode:
		return n.Name, true
	case *ast.DropViewNode:
		return n.Name, true
	}
	return "", false
}

func qualifiedSubject(qualifier, name string) string {
	if strings.TrimSpace(qualifier) == "" {
		return name
	}
	return qualifier + "." + name
}

// extensionSubject names what an owner operation acts on, from what the
// payload reports about itself after it is snapshotted: its structured
// identity, else the object its schema-change report names, else the object a
// skip line for it names. An identity's name stays a literal component rather
// than being split on dots.
func extensionSubject(payload ast.ExtensionPayload) string {
	prepared, err := ast.CloneExtensionPayload(payload)
	if err != nil {
		return ""
	}
	if source, ok := prepared.(interface{ Subject() objectidentity.ID }); ok {
		return source.Subject().Path()
	}
	if reporter, ok := prepared.(ast.ExtensionChangeReporter); ok {
		if name := reporter.SchemaChange().Name; name != "" {
			return name
		}
	}
	if reporter, ok := prepared.(interface{ OmissionSubject() (kind, name string) }); ok {
		_, name := reporter.OmissionSubject()
		return name
	}
	return ""
}

// statementSubject names the object one rendered statement acts on, read from
// its leading words: the name after `CREATE`, `ALTER` or `DROP` and the kind
// of object, after `COMMENT ON` and the kind, after `INSERT INTO`, `UPSERT
// INTO`, `REPLACE INTO`, `UPDATE`, `DELETE FROM`, `TRUNCATE`, `REFRESH
// MATERIALIZED VIEW` and `PRAGMA`. A name is written without its quotes and in
// the case the statement spells it. A statement that lists several objects,
// such as `DROP TABLE a, b`, is named by all of them, joined with ", ".
//
// It is empty for every other statement, and for one whose name it cannot
// read: an anonymous block, a lock, a procedure call, a kind of object it does
// not list, or a row limit such as SQL Server's `TOP (n)` where the name would
// be. A row is better without a subject than with a word of the statement that
// is not the object's name.
func statementSubject(statement, target string) string {
	split := lintscan.Split(statement, lintscan.ForDialect(target))
	if len(split) != 1 {
		return ""
	}
	words := subjectWords{upper: split[0].Words, source: split[0].SourceWords}
	return words.objectNames(words.subject())
}

// StatementTarget is what one statement does and the objects it does it to,
// read from the statement's leading words.
type StatementTarget struct {
	// Action is the statement's words before the name of the object it acts
	// on, uppercased, modifiers included: `CREATE TABLE`, `CREATE UNIQUE INDEX
	// CONCURRENTLY`, `DROP VIEW IF EXISTS`, `INSERT INTO`. Where the words give
	// no such name, as for `DO` or `GRANT`, it is the first word alone.
	Action string
	// Objects are the names the statement acts on, in the order it lists them,
	// each split into its parts without quotes: `app`.`notes` is {"app",
	// "notes"}, and a quoted "app.notes" is the one part "app.notes". It is
	// empty where Action is the first word alone.
	Objects [][]string
	// Clause is the head of each clause after the object's name in an ALTER
	// statement, joined with ", ": at most [clauseHeadWords] words of each,
	// stopping before the first word that is not a name or a keyword, such as
	// a literal, a parenthesis or a comma: `ADD COLUMN body TEXT`, `DROP COLUMN
	// x`, `ALTER COLUMN x SET`. A bare word is uppercased and a quoted name is
	// written without its quotes. A double-quoted word stops the head on an
	// engine that reads it as a string, as MySQL does. It is empty for every
	// other statement.
	Clause string
}

// clauseHeadWords is how many words of an ALTER's clause [StatementTarget]
// reads: enough for the operation, the name it changes and one word more, and
// few enough that the value in `ADD x text DEFAULT value` is not reached.
const clauseHeadWords = 4

// StatementTargets reads each statement of sql in order, the way the
// assessment's Subject is read; see [StatementTarget]. A statement with no
// words is skipped, so sql with no statement has none. target selects the
// lexer; an engine without a registered descriptor is read with the default
// lexer and is not refused, where [AssessSQL] returns
// [ptah.run/core/ptaherr.ErrUnsupportedDialect]. Unlike Subject, which is
// empty for text that holds several statements, each statement is read.
//
// It reads no value: a literal stops the read of a clause, and the words
// after the object's name are read only in an ALTER.
func StatementTargets(sql, target string) []StatementTarget {
	split := lintscan.Split(sql, lintscan.ForDialect(target))
	described, _ := platform.Lookup(target)
	targets := make([]StatementTarget, 0, len(split))
	for _, statement := range split {
		words := subjectWords{
			upper:               statement.Words,
			source:              statement.SourceWords,
			doubleQuotedStrings: described.Comparison.DoubleQuotedStrings,
		}
		if len(words.upper) == 0 {
			continue
		}
		name := words.subject()
		objects, next, ok := words.objectList(name)
		if !ok {
			targets = append(targets, StatementTarget{Action: words.upper[0]})
			continue
		}
		read := StatementTarget{Action: strings.Join(words.upper[:name], " "), Objects: objects}
		if words.upper[0] == "ALTER" {
			read.Clause = words.clauseHeads(next)
		}
		targets = append(targets, read)
	}
	return targets
}

// clauseHeads reads the head of each clause from index on, a clause ending at
// a comma outside parentheses; see [StatementTarget.Clause].
func (w subjectWords) clauseHeads(index int) string {
	var heads []string
	for index < len(w.upper) {
		heads = append(heads, w.clauseHead(index))
		depth := 0
		for ; index < len(w.upper) && (depth > 0 || w.upper[index] != ","); index++ {
			switch w.upper[index] {
			case "(":
				depth++
			case ")":
				depth--
			}
		}
		index++
	}
	return strings.Join(heads, ", ")
}

// clauseHead reads the head of the clause at index.
func (w subjectWords) clauseHead(index int) string {
	var head []string
	for ; index < len(w.upper) && len(head) < clauseHeadWords; index++ {
		word, source := w.upper[index], w.source[index]
		if !identifierPart(word) || w.doubleQuotedStrings && source[0] == '"' {
			break
		}
		if source[0] == '"' || source[0] == '`' {
			word = source[1 : len(source)-1]
		}
		head = append(head, word)
	}
	return strings.Join(head, " ")
}

// subject returns the index of the object's name in a statement's words, as
// [statementSubject] and [StatementTargets] read it, and -1 when the words name
// no object they read.
func (w subjectWords) subject() int {
	if len(w.upper) == 0 {
		return -1
	}
	at := 1
	switch w.upper[0] {
	case "CREATE":
		at = w.skip(at, "OR", "REPLACE")
		at = w.skip(at, "OR", "ALTER")
		return w.kind(at)
	case "ALTER", "DROP":
		return w.kind(at)
	case "COMMENT":
		if !w.at(at, "ON") {
			return -1
		}
		return w.kind(at + 1)
	case "INSERT", "UPSERT", "REPLACE":
		return w.skip(w.pastAny(at, insertModifiers), "INTO")
	case "UPDATE":
		return w.skip(w.pastAny(at, updateModifiers), "ONLY")
	case "TRUNCATE":
		return w.skip(w.skip(at, "TABLE"), "ONLY")
	case "DELETE":
		return w.skip(w.skip(w.pastAny(at, deleteModifiers), "FROM"), "ONLY")
	case "REFRESH":
		if !w.at(at, "MATERIALIZED") || !w.at(at+1, "VIEW") {
			return -1
		}
		return w.skip(at+2, "CONCURRENTLY")
	case "PRAGMA":
		return at
	}
	return -1
}

// subjectKinds are the kinds of object a CREATE, an ALTER, a DROP or a
// COMMENT ON names before the object's name.
var subjectKinds = map[string]bool{
	"COLUMN": true, "DATABASE": true, "DOMAIN": true, "EXTENSION": true, "FUNCTION": true, "INDEX": true,
	"POLICY": true, "PROCEDURE": true, "ROLE": true, "SCHEMA": true, "SEQUENCE": true, "SYNONYM": true,
	"TABLE": true, "TRIGGER": true, "TYPE": true, "USER": true, "VIEW": true,
}

// kindModifiers are the words that may stand between CREATE, ALTER or DROP
// and the kind of object.
var kindModifiers = map[string]bool{
	"CLUSTERED": true, "FULLTEXT": true, "GLOBAL": true, "LOCAL": true, "MATERIALIZED": true,
	"NONCLUSTERED": true, "RECURSIVE": true, "SPATIAL": true, "TEMP": true, "TEMPORARY": true,
	"UNIQUE": true, "UNLOGGED": true, "VIRTUAL": true,
}

// insertModifiers are the words that may stand between INSERT and INTO:
// MySQL's priority and IGNORE, and SQLite's OR and its conflict resolution.
var insertModifiers = map[string]bool{
	"ABORT": true, "DELAYED": true, "FAIL": true, "HIGH_PRIORITY": true, "IGNORE": true,
	"LOW_PRIORITY": true, "OR": true, "REPLACE": true, "ROLLBACK": true,
}

// updateModifiers are the words that may stand between UPDATE and the table:
// MySQL's LOW_PRIORITY and IGNORE, and SQLite's OR and its conflict resolution.
var updateModifiers = map[string]bool{
	"ABORT": true, "FAIL": true, "IGNORE": true, "LOW_PRIORITY": true, "OR": true, "REPLACE": true,
	"ROLLBACK": true,
}

// deleteModifiers are the words MySQL allows between DELETE and FROM.
var deleteModifiers = map[string]bool{"IGNORE": true, "LOW_PRIORITY": true, "QUICK": true}

// otherForms are the words that, read where a kind's name belongs, begin
// another form of the statement: `CREATE USER MAPPING FOR` names a role and a
// server, and `CREATE SCHEMA AUTHORIZATION` names the schema after its owner.
var otherForms = map[string]string{"SCHEMA": "AUTHORIZATION", "USER": "MAPPING"}

// nameModifiers are the words that may stand between the kind of object and
// its name, in any order.
var nameModifiers = [][]string{{"IF", "NOT", "EXISTS"}, {"IF", "EXISTS"}, {"CONCURRENTLY"}, {"ONLY"}}

// subjectWords are a statement's words as the scanner reports them, uppercased
// for matching, beside the same words as the statement spells them.
type subjectWords struct {
	upper  []string
	source []string
	// doubleQuotedStrings reports that the engine reads a double-quoted word
	// as a string rather than a name, so a clause head stops at one.
	doubleQuotedStrings bool
}

func (w subjectWords) at(index int, word string) bool {
	return index < len(w.upper) && w.upper[index] == word
}

func (w subjectWords) atAny(index int, words map[string]bool) bool {
	return index < len(w.upper) && words[w.upper[index]]
}

// pastAny returns the index past the words of set that stand at index.
func (w subjectWords) pastAny(index int, set map[string]bool) int {
	for w.atAny(index, set) {
		index++
	}
	return index
}

// skip returns the index past seq when the words at index spell it, and index
// otherwise.
func (w subjectWords) skip(index int, seq ...string) int {
	for i, word := range seq {
		if !w.at(index+i, word) {
			return index
		}
	}
	return index + len(seq)
}

// kind returns the index of the name that follows the kind of object at
// index, past the modifiers before the kind and after it, and -1 when the
// words at index name no kind this reader lists.
func (w subjectWords) kind(index int) int {
	index = w.pastAny(index, kindModifiers)
	if !w.atAny(index, subjectKinds) {
		return -1
	}
	name := w.pastNameModifiers(index + 1)
	if form, ok := otherForms[w.upper[index]]; ok && w.at(name, form) {
		return -1
	}
	return name
}

// pastNameModifiers returns the index past the modifiers at index, in
// whichever order the statement writes them.
func (w subjectWords) pastNameModifiers(index int) int {
	for _, modifier := range nameModifiers {
		if next := w.skip(index, modifier...); next != index {
			return w.pastNameModifiers(next)
		}
	}
	return index
}

// objectNames reads the comma-separated list of possibly qualified names at
// index, joined with ", ", and returns "" when the words there are not one.
// SQL Server's `TOP (n)` stands where a name would and is not read as one.
func (w subjectWords) objectNames(index int) string {
	objects, _, ok := w.objectList(index)
	if !ok {
		return ""
	}
	names := make([]string, 0, len(objects))
	for _, parts := range objects {
		names = append(names, strings.Join(parts, "."))
	}
	return strings.Join(names, ", ")
}

// objectList reads the comma-separated list of possibly qualified names at
// index, each as its parts, and the index past the list. It reports false
// when the words there are not one; see [subjectWords.objectNames].
func (w subjectWords) objectList(index int) ([][]string, int, bool) {
	if index < 0 || w.at(index, "TOP") {
		return nil, index, false
	}
	var objects [][]string
	for {
		parts, next, ok := w.objectName(index)
		if !ok {
			return nil, index, false
		}
		objects = append(objects, parts)
		if !w.at(next, ",") {
			return objects, next, true
		}
		index = next + 1
	}
}

// objectName reads the possibly qualified name at index, as its parts, and
// the index past it.
func (w subjectWords) objectName(index int) ([]string, int, bool) {
	var parts []string
	for {
		part, next, ok := w.namePart(index)
		if !ok {
			return nil, index, false
		}
		parts = append(parts, part)
		if !w.at(next, ".") {
			return parts, next, true
		}
		index = next + 1
	}
}

// namePart reads one part of a name: a quoted identifier, a bracketed one, or
// a bare word that is not a keyword ending a name.
func (w subjectWords) namePart(index int) (string, int, bool) {
	if index >= len(w.upper) {
		return "", index, false
	}
	word := w.source[index]
	if w.upper[index] == "[" {
		if index+2 >= len(w.upper) || w.upper[index+2] != "]" {
			return "", index, false
		}
		return w.source[index+1], index + 3, true
	}
	if len(word) >= 2 && (word[0] == '"' || word[0] == '`') && word[len(word)-1] == word[0] {
		quote := word[:1]
		return strings.ReplaceAll(word[1:len(word)-1], quote+quote, quote), index + 1, true
	}
	if !identifierPart(w.upper[index]) {
		return "", index, false
	}
	return word, index + 1, true
}
