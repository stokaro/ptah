package safety

import (
	"slices"
	"strings"

	"ptah.run/internal/lintscan"
)

// replaceableKinds are the objects a plan may drop and create again to change
// them, each with the drift category its change is counted under. A view and
// a trigger hold no state of their own: created again from the declaration,
// either one is the object the drop removed. On PostgreSQL a view's grants go
// with the drop, which narrows access, a warning as well. A sequence's
// position and a materialized view's rows are lost by the drop whatever
// follows it, so neither is here, and a routine is not either: its identity is
// its input signature, which the words of a DROP FUNCTION and a CREATE
// FUNCTION do not spell alike, and the drop takes the routine's grants with
// it.
var replaceableKinds = map[string]string{
	"VIEW":    "views_modified",
	"TRIGGER": "triggers_modified",
}

// objectRef is the object a DROP or a CREATE of a replaceable kind names.
type objectRef struct {
	kind string
	name string
	// table is the table a trigger hangs on, when the statement names it. A
	// drop that names none is matched by the trigger's name alone, which is
	// how MySQL and SQLite identify one.
	table string
}

// statementObject is what one statement does to an object of a replaceable
// kind.
type statementObject struct {
	ref objectRef
	// drop is true for a DROP and false for a CREATE.
	drop bool
	// cascade is true for a DROP that cascades.
	cascade bool
}

// statementRef reads the object a statement drops or creates. A statement that
// is neither, that names a kind outside [replaceableKinds], or whose name it
// cannot read exactly, reports false: it is judged by its words alone.
func statementRef(statement, target string) (statementObject, bool) {
	split := lintscan.Split(statement, lintscan.ForDialect(target))
	if len(split) != 1 || len(split[0].Words) < 3 {
		return statementObject{}, false
	}
	words := split[0].Words
	switch words[0] {
	case "DROP":
		ref, rest, read := dropRef(words[1:])
		return statementObject{ref: ref, drop: true, cascade: slices.Contains(rest, "CASCADE")}, read
	case "CREATE":
		ref, read := createRef(words[1:])
		return statementObject{ref: ref}, read
	default:
		return statementObject{}, false
	}
}

// dropRef reads `<kind> [IF EXISTS] <name> [ON <table>]`, and returns the words
// after the name for the caller to look for CASCADE in.
func dropRef(words []string) (objectRef, []string, bool) {
	kind := words[0]
	if _, replaceable := replaceableKinds[kind]; !replaceable {
		return objectRef{}, nil, false
	}
	rest := words[1:]
	if len(rest) >= 2 && rest[0] == "IF" && rest[1] == "EXISTS" {
		rest = rest[2:]
	}
	name, rest, read := qualifiedName(rest)
	if !read || (len(rest) > 0 && rest[0] == ",") {
		return objectRef{}, nil, false
	}
	ref := objectRef{kind: kind, name: name}
	if kind == "TRIGGER" && len(rest) > 0 && rest[0] == "ON" {
		table, after, readTable := qualifiedName(rest[1:])
		if !readTable {
			return objectRef{}, nil, false
		}
		ref.table, rest = table, after
	}
	return ref, rest, true
}

// createRef reads the object a CREATE names: the first replaceable kind among
// its leading words, which may carry OR REPLACE, a temporary marker or MySQL's
// view and trigger options before it, then `[IF NOT EXISTS] <name>`, and for a
// trigger the table after its first ON. A materialized view and an event
// trigger are other kinds.
func createRef(words []string) (objectRef, bool) {
	at := slices.IndexFunc(words, func(word string) bool {
		_, replaceable := replaceableKinds[word]
		return replaceable || word == "AS"
	})
	if at < 0 || words[at] == "AS" || (at > 0 && (words[at-1] == "MATERIALIZED" || words[at-1] == "EVENT")) {
		return objectRef{}, false
	}
	kind, rest := words[at], words[at+1:]
	if len(rest) >= 3 && rest[0] == "IF" && rest[1] == "NOT" && rest[2] == "EXISTS" {
		rest = rest[3:]
	}
	name, rest, read := qualifiedName(rest)
	if !read {
		return objectRef{}, false
	}
	ref := objectRef{kind: kind, name: name}
	if kind == "TRIGGER" {
		on := slices.Index(rest, "ON")
		if on < 0 {
			return objectRef{}, false
		}
		table, _, readTable := qualifiedName(rest[on+1:])
		if !readTable {
			return objectRef{}, false
		}
		ref.table = table
	}
	return ref, true
}

// qualifiedName reads `part[.part]...` and returns it with the words after it.
// A part is a quoted identifier, kept with its quotes, or a bare one, which
// the scanner upper-cases; anything else, a bracketed SQL Server name among
// them, is not read, so a name is never matched by an approximation of it.
func qualifiedName(words []string) (string, []string, bool) {
	var parts []string
	for len(words) > 0 && identifierPart(words[0]) {
		parts = append(parts, words[0])
		words = words[1:]
		if len(words) == 0 || words[0] != "." {
			return strings.Join(parts, "."), words, true
		}
		words = words[1:]
	}
	return "", nil, false
}

// identifierPart reports whether word is one part of a name as the scanner
// reports it.
func identifierPart(word string) bool {
	if len(word) >= 2 && (word[0] == '"' || word[0] == '`') && word[len(word)-1] == word[0] {
		return true
	}
	for i, r := range word {
		switch {
		case r == '_', 'A' <= r && r <= 'Z', 'a' <= r && r <= 'z':
		case i > 0 && ('0' <= r && r <= '9' || r == '$'):
		default:
			return false
		}
	}
	return word != "" && !reservedAfterName[word]
}

// reservedAfterName are the words that end a name in the statements read
// here rather than being one.
var reservedAfterName = map[string]bool{
	"ON": true, "AS": true, "CASCADE": true, "RESTRICT": true, "BEFORE": true, "AFTER": true, "INSTEAD": true, "FOR": true,
}

// replacement is the verdict on a statement that drops an object the same
// plan creates again.
type replacement struct {
	severity Severity
	reason   string
}

// replacements returns, for each statement, the verdict on a drop of an object
// a later statement of the same plan creates again with the same kind and
// name, on the same table for a trigger whose drop names one, and nil for
// every other statement.
//
// Without CASCADE the drop is half of a replacement: the plan changes the
// object, and the drop is rated as the drift report rates the change, under
// the kind's modified category. With CASCADE it stays destructive: it also
// drops every object that depends on the dropped one, and creating that one
// again brings none of them back.
func replacements(statements []string, target string) []*replacement {
	objects := make([]statementObject, len(statements))
	read := make([]bool, len(statements))
	for i, statement := range statements {
		objects[i], read[i] = statementRef(statement, target)
	}
	verdicts := make([]*replacement, len(statements))
	for i := range statements {
		if !read[i] || !objects[i].drop {
			continue
		}
		for j := i + 1; j < len(statements); j++ {
			if read[j] && !objects[j].drop && createsAgain(objects[i].ref, objects[j].ref) {
				verdicts[i] = replacementVerdict(objects[i])
				break
			}
		}
	}
	return verdicts
}

// createsAgain reports whether created is the object dropped names.
func createsAgain(dropped, created objectRef) bool {
	return dropped.kind == created.kind && dropped.name == created.name &&
		(dropped.table == "" || dropped.table == created.table)
}

// replacementVerdict is the verdict on a drop the plan follows with a create
// of the same object.
func replacementVerdict(dropped statementObject) *replacement {
	kind := dropped.ref.kind
	noun := strings.ToLower(kind)
	if dropped.cascade {
		return &replacement{
			severity: Destructive,
			reason: "DROP " + kind + " with CASCADE also drops every object that depends on the " + noun +
				", and creating the " + noun + " again does not bring them back",
		}
	}
	return &replacement{
		severity: modifiedSeverity(replaceableKinds[kind]),
		reason:   "DROP " + kind + " is followed by CREATE " + kind + " of the same " + noun + ": the plan replaces the " + noun + " rather than removing it",
	}
}

// modifiedSeverity is the severity the drift report counts category at, read
// from the declaration it counts from.
func modifiedSeverity(category string) Severity {
	for _, counted := range countedCategories {
		if counted.name == category {
			return counted.severity
		}
	}
	return Destructive
}

// applyReplacements gives each statement the plan drops and creates again the
// verdict [replacements] reaches, where the statement's words decided its
// verdict: a statement an owner judged keeps the owner's verdict.
func applyReplacements(assessments []StatementAssessment, statements []string, target string, ownWords []bool) {
	for i, verdict := range replacements(statements, target) {
		if verdict == nil || !ownWords[i] {
			continue
		}
		assessments[i].Severity, assessments[i].Reason = verdict.severity, verdict.reason
	}
}

// replacesInPlace returns the kind a CREATE OR REPLACE or CREATE OR ALTER of a
// replaceable kind names, and "" for every other statement. Such a statement
// changes the object where it exists, and is rated as the drift report rates
// the change.
func replacesInPlace(words []string) string {
	if !hasWordPrefix(words, "CREATE", "OR", "REPLACE") && !hasWordPrefix(words, "CREATE", "OR", "ALTER") {
		return ""
	}
	if len(words) < 4 {
		return ""
	}
	if _, replaceable := replaceableKinds[words[3]]; !replaceable {
		return ""
	}
	return words[3]
}

// inPlaceReplacementReason is why a CREATE OR REPLACE of kind is a change.
func inPlaceReplacementReason(kind string) string {
	return inPlaceReplacementReasons[kind]
}

// inPlaceReplacementReasons say what a replacement in place changes, per kind.
var inPlaceReplacementReasons = map[string]string{
	"VIEW":    "CREATE OR REPLACE VIEW changes the view in place, and what every query that reads it gets",
	"TRIGGER": "CREATE OR REPLACE TRIGGER changes the trigger in place, and what every write to its table does",
}
