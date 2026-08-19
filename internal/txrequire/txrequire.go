// Package txrequire answers whether SQL can run inside a transaction.
//
// There are two questions here, not one, and conflating them is what left the
// tree with two disagreeing implementations (stokaro/ptah#996):
//
//   - For a PLANNED node, "must this statement be routed away from the
//     transactional file?". A generated plan's `ALTER TYPE ... ADD VALUE`
//     always targets a type the database already has -- a diff that introduces
//     an enum emits CREATE TYPE, not ADD VALUE -- so the added value can never
//     be used in the same transaction, and the conservative answer is the
//     correct one. [NodeRequiresAutocommit] answers this.
//
//   - For an AUTHORED file, "can this file, as written, run inside one
//     transaction?". Here the author may create the type in the same file, and
//     PostgreSQL then allows the new value to be used immediately. A
//     keyword-only answer would refuse that valid workflow. [Analyze] answers
//     this, semantically.
//
// Every rule below was measured on PostgreSQL 18.4:
//
//	BEGIN; ALTER TYPE mood ADD VALUE 'great'; COMMIT;                -> accepted
//	BEGIN; ALTER TYPE mood ADD VALUE 'x'; SELECT 'x'::mood; COMMIT;  -> 55P04
//	     ERROR: unsafe use of new value "x" of enum type mood
//	     HINT:  New enum values must be committed before they can be used.
//	BEGIN; CREATE TYPE m AS ENUM ('a'); ALTER TYPE m ADD VALUE 'b';
//	       CREATE TABLE t (c m DEFAULT 'b'); COMMIT;                 -> accepted
//	BEGIN; CREATE INDEX CONCURRENTLY i ON t (c); COMMIT;             -> 25001
//
// So the enum constraint is not "ALTER TYPE ADD VALUE needs autocommit". It is
// "a value added to a type that already existed cannot be USED until the
// transaction commits", and that is why this package looks at the file rather
// than at one statement.
package txrequire

import (
	"fmt"

	"go.5x5.cz/ptah/core/platform/capability"
)

// Reason names why a statement cannot run inside a transaction.
type Reason string

const (
	// ReasonConcurrentIndex is CREATE or DROP INDEX CONCURRENTLY, which
	// PostgreSQL refuses inside a transaction block unconditionally.
	ReasonConcurrentIndex Reason = "concurrent_index"
	// ReasonEnumValueUsed is a value added to a pre-existing enum type and then
	// used before the transaction commits.
	ReasonEnumValueUsed Reason = "enum_value_used"
)

// Statement is one statement of an authored migration file.
type Statement struct {
	// Index is the zero-based position of the statement in the file.
	Index int
	// Line is the 1-based line the statement starts on, for diagnostics.
	Line int
	// SQL is the statement as written.
	SQL string
}

// Finding names one statement that keeps a file out of a transaction, and what
// to do about it.
type Finding struct {
	// Statement is the statement that cannot run transactionally. For
	// ReasonEnumValueUsed it is the statement that USES the value, because
	// that is the one PostgreSQL refuses.
	Statement Statement
	Reason    Reason
	Message   string
	Remedy    string
}

// Result is what [Analyze] concluded about one file.
type Result struct {
	// Findings is empty when the file can run inside one transaction.
	Findings []Finding
}

// RequiresAutocommit reports whether the file must run outside a transaction.
func (r Result) RequiresAutocommit() bool { return len(r.Findings) > 0 }

// Diagnostic renders the findings as one actionable sentence, naming the
// statement that forces the answer and the two ways out.
func (r Result) Diagnostic(file string) string {
	if len(r.Findings) == 0 {
		return ""
	}
	first := r.Findings[0]
	return fmt.Sprintf(
		"%s cannot run inside a transaction: line %d, %s. %s",
		file, first.Statement.Line, first.Message, first.Remedy)
}

// Analyze classifies an authored file's statements.
//
// A target that cannot host the constructs at all is never reported: the check
// is keyed on the capability that governs each rule rather than on the dialect
// name, so a preset that declines concurrent indexes is not told its file has
// one.
func Analyze(dialect string, caps capability.Capabilities, statements []Statement) Result {
	findings := make([]Finding, 0)
	created := map[string]bool{}
	pending := map[string]string{}
	for _, statement := range statements {
		words := tokenize(dialect, statement.SQL)
		sourceWords := tokenizeSource(dialect, statement.SQL)
		if typeName := createdType(words); typeName != "" {
			created[typeName] = true
		}
		if concurrentIndex(words) && concurrentIndexesAllowed(caps) {
			findings = append(findings, concurrentIndexFinding(statement))
			continue
		}
		if usesPendingValue(words, pending) {
			findings = append(findings, enumValueFinding(dialect, statement, pending))
			continue
		}
		recordAddedEnumValue(words, sourceWords, created, pending)
	}
	return Result{Findings: findings}
}

// concurrentIndexesAllowed reports whether this target can produce a concurrent
// index at all. Without either key the construct cannot appear, and reporting
// it would answer about a statement the target would refuse for its own
// reasons.
func concurrentIndexesAllowed(caps capability.Capabilities) bool {
	return caps.Has(capability.CreateIndexConcurrently) || caps.Has(capability.DropIndexConcurrently)
}

func concurrentIndexFinding(statement Statement) Finding {
	return Finding{
		Statement: statement,
		Reason:    ReasonConcurrentIndex,
		Message:   "CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block",
		Remedy: "mark the file `-- +ptah no_transaction`, or move the concurrent index " +
			"into a migration of its own",
	}
}

func enumValueFinding(dialect string, statement Statement, pending map[string]string) Finding {
	value, typeName := firstPendingUse(dialect, statement, pending)
	return Finding{
		Statement: statement,
		Reason:    ReasonEnumValueUsed,
		Message: fmt.Sprintf(
			"it uses %s, a value this file adds to the pre-existing enum type %s, "+
				"and a new enum value is not usable until the transaction that added it commits",
			value, typeName),
		Remedy: "mark the file `-- +ptah no_transaction`, or add the value in an earlier migration",
	}
}

// firstPendingUse names the added value this statement uses. It repeats the
// scan rather than threading the match out of usesPendingValue so the two
// answers cannot drift apart.
func firstPendingUse(dialect string, statement Statement, pending map[string]string) (value, typeName string) {
	for _, word := range tokenize(dialect, statement.SQL) {
		if owner, ok := pending[word]; ok {
			return word, owner
		}
	}
	return "", ""
}
