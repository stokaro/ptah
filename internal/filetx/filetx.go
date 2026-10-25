// Package filetx reads the transactions a migration file opens and closes
// itself, in PostgreSQL's spelling of transaction control.
//
// A history written for a runner that sends each file as it is, as
// golang-migrate does, has files that begin with BEGIN and end with COMMIT. The
// dev database replay and the apply of such a directory both run those files
// without a transaction of their own, and both decide which files qualify
// with [Read], so the two cannot disagree about a file.
//
// [Read] knows no dialect. The engine whose transactions it describes offers
// it: PostgreSQL's replay guard calls it, and PostgreSQL's migrator services
// carry it (internal/migratorext/postgres). CockroachDB, YugabyteDB and
// Spanner take the same words with other transaction semantics and offer
// nothing, so on them no file is read as one with transactions of its own.
package filetx

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/lexer"
)

// Blocks are the transactions one migration file opens and closes itself, as
// indexes into the file's statements. The zero value holds none.
type Blocks struct {
	// boundaries holds, in order, the index of each block's opening statement
	// followed by the index of its closing one.
	boundaries []int
}

// Empty reports whether the file opens no transaction of its own.
func (b Blocks) Empty() bool { return len(b.boundaries) == 0 }

// Boundaries returns the indexes of the statements that open and close the
// file's transactions, in order, or nil when it opens none.
func (b Blocks) Boundaries() []int { return slices.Clone(b.boundaries) }

// Inside reports whether the statement at index runs inside one of the file's
// transactions: it opens one, or comes after an opening statement and before
// the closing one. A closing statement is not inside.
func (b Blocks) Inside(index int) bool {
	_, inside := b.opening(index)
	return inside
}

// Closes reports whether the statement at index closes one of the file's
// transactions.
func (b Blocks) Closes(index int) bool {
	for i := 1; i < len(b.boundaries); i += 2 {
		if b.boundaries[i] == index {
			return true
		}
	}
	return false
}

// Opening returns the index of the statement that opened the transaction the
// statement at index runs in or closes, and false for a statement outside
// every transaction of the file.
func (b Blocks) Opening(index int) (int, bool) {
	if opened, inside := b.opening(index); inside {
		return opened, true
	}
	for i := 1; i < len(b.boundaries); i += 2 {
		if b.boundaries[i] == index {
			return b.boundaries[i-1], true
		}
	}
	return 0, false
}

// Committed returns how many of the file's leading statements stay committed
// when the statement at index fails, the statements before it having
// succeeded. A statement outside every transaction commits on its own, so
// every statement before it stays. One inside a transaction, or the statement
// that closes it, takes the whole transaction with it, so only the statements
// before the opening one stay.
func (b Blocks) Committed(index int) int {
	if opened, ok := b.Opening(index); ok {
		return opened
	}
	return index
}

func (b Blocks) opening(index int) (int, bool) {
	for i := 0; i+1 < len(b.boundaries); i += 2 {
		if index >= b.boundaries[i] && index < b.boundaries[i+1] {
			return b.boundaries[i], true
		}
	}
	return 0, false
}

// StructureError refuses a file whose transaction control [Read] cannot
// account for in full.
type StructureError struct {
	// Index is the position, in the file's statements, of the statement the
	// refusal names.
	Index int
	// Reason says what is wrong with the file's transaction control.
	Reason string
}

func (e *StructureError) Error() string {
	return fmt.Sprintf("the file controls its own transactions in a form Ptah does not run, at statement %d: %s",
		e.Index+1, e.Reason)
}

// StatementIndex returns the zero-based position of the statement the refusal
// names, for a caller that reports it against the file.
func (e *StructureError) StatementIndex() int { return e.Index }

// Read reads the statements of one migration file, in order, for the
// transactions it opens and closes itself.
//
// Accepted: a BEGIN or START TRANSACTION with isolation, access and DEFERRABLE
// modes, closed by a COMMIT or END, any number of times in a file. Everything
// else that controls a transaction is refused for the whole file with a
// [*StructureError], whether or not a matching boundary follows:
//
//   - a BEGIN inside a transaction, a COMMIT or END outside one, and a
//     transaction the file leaves open;
//   - ROLLBACK and ABORT, in any form: a file that takes its own work back
//     cannot be recorded as a history of what it did;
//   - SAVEPOINT, RELEASE and ROLLBACK TO: partial rollback has the same
//     problem, and a savepoint that outlives its statement is session state;
//   - AND CHAIN, which opens a transaction nothing in the file names, and
//     PREPARE TRANSACTION, COMMIT PREPARED and ROLLBACK PREPARED, which leave
//     a transaction on the server after the session ends.
//
// The words are PostgreSQL's; see the package documentation for which engines
// offer the reading.
func Read(statements []string) (Blocks, error) {
	var boundaries []int
	openAt := -1
	for index, statement := range statements {
		tokens := significantTokens(statement)
		if len(tokens) == 0 {
			continue
		}
		kind, reason := classifyTransactionControl(tokens)
		if reason != "" {
			return Blocks{}, &StructureError{Index: index, Reason: reason}
		}
		switch kind {
		case transactionBegin:
			if openAt >= 0 {
				return Blocks{}, &StructureError{
					Index:  index,
					Reason: fmt.Sprintf("a transaction is already open since statement %d", openAt+1),
				}
			}
			openAt = index
			boundaries = append(boundaries, index)
		case transactionEnd:
			if openAt < 0 {
				return Blocks{}, &StructureError{Index: index, Reason: "no transaction is open to close"}
			}
			openAt = -1
			boundaries = append(boundaries, index)
		case transactionNone:
		}
	}
	if openAt >= 0 {
		return Blocks{}, &StructureError{Index: openAt, Reason: "the transaction is never closed by the file"}
	}
	return Blocks{boundaries: boundaries}, nil
}

type transactionControl int

const (
	transactionNone transactionControl = iota
	transactionBegin
	transactionEnd
)

// significantTokens are the tokens of statement a reader of its first words
// looks at: everything but whitespace, comments and the terminator.
func significantTokens(statement string) []lexer.Token {
	lex := lexer.NewLexerWithOptions(statement, lexer.Options{StandardStrings: true})
	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenSemicolon:
			continue
		case lexer.TokenEOF:
			return tokens
		default:
			tokens = append(tokens, token)
		}
	}
}

// wordsAt reports whether the tokens from index on are the unquoted words of
// values, compared without regard to case.
func wordsAt(tokens []lexer.Token, index int, values ...string) bool {
	if index < 0 || index+len(values) > len(tokens) {
		return false
	}
	for offset, value := range values {
		if !tokens[index+offset].MatchIdentifierValue(value) {
			return false
		}
	}
	return true
}

// classifyTransactionControl reads the first words of a statement. It returns
// the boundary the statement is, or a reason when the statement controls a
// transaction in a form [Read] refuses.
func classifyTransactionControl(tokens []lexer.Token) (transactionControl, string) {
	if tokens[0].Type != lexer.TokenIdentifier {
		return transactionNone, ""
	}
	first := strings.ToUpper(tokens[0].Value)
	switch first {
	case "BEGIN":
		return classifyBegin(tokens, 1)
	case "START":
		if !wordsAt(tokens, 1, "TRANSACTION") {
			return transactionNone, ""
		}
		return classifyBegin(tokens, 2)
	case "COMMIT", "END":
		if wordsAt(tokens, 1, "PREPARED") {
			return transactionNone, "COMMIT PREPARED leaves a prepared transaction on the server"
		}
		return classifyEnd(tokens, first)
	case "ROLLBACK", "ABORT":
		return transactionNone, first + " takes the file's own work back, which a history of what the file did cannot record"
	case "SAVEPOINT", "RELEASE":
		return transactionNone, first + " is partial transaction state, which Ptah does not model"
	case "PREPARE":
		if wordsAt(tokens, 1, "TRANSACTION") {
			return transactionNone, "PREPARE TRANSACTION leaves a prepared transaction on the server"
		}
	}
	return transactionNone, ""
}

// classifyBegin reads the words after BEGIN or START TRANSACTION: an optional
// WORK or TRANSACTION, then transaction modes.
func classifyBegin(tokens []lexer.Token, at int) (transactionControl, string) {
	if wordsAt(tokens, at, "WORK") || wordsAt(tokens, at, "TRANSACTION") {
		at++
	}
	for at < len(tokens) {
		switch {
		case tokens[at].MatchOperatorValue(","):
			at++
		case wordsAt(tokens, at, "ISOLATION", "LEVEL", "SERIALIZABLE"):
			at += 3
		case wordsAt(tokens, at, "ISOLATION", "LEVEL", "REPEATABLE", "READ"),
			wordsAt(tokens, at, "ISOLATION", "LEVEL", "READ", "COMMITTED"),
			wordsAt(tokens, at, "ISOLATION", "LEVEL", "READ", "UNCOMMITTED"):
			at += 4
		case wordsAt(tokens, at, "READ", "WRITE"), wordsAt(tokens, at, "READ", "ONLY"),
			wordsAt(tokens, at, "NOT", "DEFERRABLE"):
			at += 2
		case wordsAt(tokens, at, "DEFERRABLE"):
			at++
		default:
			return transactionNone, "BEGIN carries " + strings.TrimSpace(tokens[at].Value) +
				", which is not a transaction mode Ptah reads"
		}
	}
	return transactionBegin, ""
}

// classifyEnd reads the words after COMMIT or END: an optional WORK or
// TRANSACTION, and nothing else. AND CHAIN would start a transaction the file
// does not name.
func classifyEnd(tokens []lexer.Token, word string) (transactionControl, string) {
	at := 1
	if wordsAt(tokens, at, "WORK") || wordsAt(tokens, at, "TRANSACTION") {
		at++
	}
	if at < len(tokens) {
		if wordsAt(tokens, at, "AND") {
			return transactionNone, word + " AND [NO] CHAIN starts a transaction the file does not name"
		}
		return transactionNone, word + " carries " + strings.TrimSpace(tokens[at].Value) + ", which Ptah does not read"
	}
	return transactionEnd, ""
}
