package safety

import (
	"reflect"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
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
	if len(words.upper) == 0 {
		return ""
	}
	at := 1
	switch words.upper[0] {
	case "CREATE":
		at = words.skip(at, "OR", "REPLACE")
		at = words.skip(at, "OR", "ALTER")
		return words.objectNames(words.kind(at))
	case "ALTER", "DROP":
		return words.objectNames(words.kind(at))
	case "COMMENT":
		if !words.at(at, "ON") {
			return ""
		}
		return words.objectNames(words.kind(at + 1))
	case "INSERT", "UPSERT", "REPLACE":
		return words.objectNames(words.skip(words.pastAny(at, insertModifiers), "INTO"))
	case "UPDATE":
		return words.objectNames(words.skip(words.pastAny(at, updateModifiers), "ONLY"))
	case "TRUNCATE":
		return words.objectNames(words.skip(words.skip(at, "TABLE"), "ONLY"))
	case "DELETE":
		return words.objectNames(words.skip(words.skip(words.pastAny(at, deleteModifiers), "FROM"), "ONLY"))
	case "REFRESH":
		if !words.at(at, "MATERIALIZED") || !words.at(at+1, "VIEW") {
			return ""
		}
		return words.objectNames(words.skip(at+2, "CONCURRENTLY"))
	case "PRAGMA":
		return words.objectNames(at)
	}
	return ""
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
	if index < 0 || w.at(index, "TOP") {
		return ""
	}
	var names []string
	for {
		name, next, ok := w.objectName(index)
		if !ok {
			return ""
		}
		names = append(names, name)
		if !w.at(next, ",") {
			return strings.Join(names, ", ")
		}
		index = next + 1
	}
}

// objectName reads the possibly qualified name at index, and the index past
// it.
func (w subjectWords) objectName(index int) (string, int, bool) {
	var parts []string
	for {
		part, next, ok := w.namePart(index)
		if !ok {
			return "", index, false
		}
		parts = append(parts, part)
		if !w.at(next, ".") {
			return strings.Join(parts, "."), next, true
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
