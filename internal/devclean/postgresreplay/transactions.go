package postgresreplay

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// TransactionBoundaries reads a migration file for the explicit transaction it
// opens and closes itself, and returns the indexes of those two statements.
//
// A history written for a runner that sends each file as it is has files that
// begin with BEGIN and end with COMMIT. The strict reading refuses both
// statements, because on their own they change state the rest of the replay
// depends on. Read across the file they do not: a transaction the file closes
// leaves the session as it found it.
//
// Only PostgreSQL is read. CockroachDB, YugabyteDB and Spanner take the same
// words with different transaction semantics, and a file that spells them is
// refused statement by statement as before.
//
// Accepted: a BEGIN or START TRANSACTION with isolation, access and
// DEFERRABLE modes, closed by a COMMIT or END, any number of times in a file.
// Everything else that controls a transaction is refused for the whole file,
// whether or not a matching boundary follows:
//
//   - a BEGIN inside a transaction, a COMMIT or END outside one, and a
//     transaction a file leaves open;
//   - ROLLBACK and ABORT, in any form: a file that takes its own work back
//     cannot be replayed as a history of what it did;
//   - SAVEPOINT, RELEASE and ROLLBACK TO: partial rollback has the same
//     problem, and a savepoint that outlives its statement is session state;
//   - AND CHAIN, which opens a transaction nothing in the file names, and
//     PREPARE TRANSACTION, COMMIT PREPARED and ROLLBACK PREPARED, which leave
//     a transaction on the server after the session ends.
//
// The statement that fails inside a transaction is not part of this reading:
// the replay rolls the session back when a file fails.
func (g guard) TransactionBoundaries(statements []string) ([]int, error) {
	if g.dialect != platform.Postgres {
		return nil, nil
	}

	var boundaries []int
	openAt := -1
	for index, statement := range statements {
		tokens := replay.Tokens(statement, lexerOptions())
		if len(tokens) == 0 {
			continue
		}
		kind, reason := classifyTransactionControl(tokens)
		if reason != "" {
			return nil, &replay.TransactionStructureError{Dialect: g.dialect, Index: index, Reason: reason}
		}
		switch kind {
		case transactionBegin:
			if openAt >= 0 {
				return nil, &replay.TransactionStructureError{
					Dialect: g.dialect, Index: index,
					Reason: fmt.Sprintf("a transaction is already open since statement %d", openAt+1),
				}
			}
			openAt = index
			boundaries = append(boundaries, index)
		case transactionEnd:
			if openAt < 0 {
				return nil, &replay.TransactionStructureError{
					Dialect: g.dialect, Index: index,
					Reason: "no transaction is open to close",
				}
			}
			openAt = -1
			boundaries = append(boundaries, index)
		}
	}
	if openAt >= 0 {
		return nil, &replay.TransactionStructureError{
			Dialect: g.dialect, Index: openAt,
			Reason: "the transaction is never closed by the file",
		}
	}
	return boundaries, nil
}

type transactionControl int

const (
	transactionNone transactionControl = iota
	transactionBegin
	transactionEnd
)

// classifyTransactionControl reads the first words of a statement. It returns
// the boundary the statement is, or a reason when the statement controls a
// transaction in a form the reading refuses.
func classifyTransactionControl(tokens []lexer.Token) (transactionControl, string) {
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "BEGIN":
		return classifyBegin(tokens, 1)
	case "START":
		if !replay.TokenSequenceAt(tokens, 1, "TRANSACTION") {
			return transactionNone, ""
		}
		return classifyBegin(tokens, 2)
	case "COMMIT", "END":
		if replay.TokenSequenceAt(tokens, 1, "PREPARED") {
			return transactionNone, "COMMIT PREPARED leaves a prepared transaction on the server"
		}
		return classifyEnd(tokens, first)
	case "ROLLBACK", "ABORT":
		return transactionNone, first + " takes the file's own work back, which a replay cannot follow"
	case "SAVEPOINT", "RELEASE":
		return transactionNone, first + " is partial transaction state, which the replay does not model"
	case "PREPARE":
		if replay.TokenSequenceAt(tokens, 1, "TRANSACTION") {
			return transactionNone, "PREPARE TRANSACTION leaves a prepared transaction on the server"
		}
	}
	return transactionNone, ""
}

// classifyBegin reads the words after BEGIN or START TRANSACTION: an optional
// WORK or TRANSACTION, then transaction modes.
func classifyBegin(tokens []lexer.Token, at int) (transactionControl, string) {
	if replay.TokenSequenceAt(tokens, at, "WORK") || replay.TokenSequenceAt(tokens, at, "TRANSACTION") {
		at++
	}
	for at < len(tokens) {
		switch {
		case tokens[at].MatchOperatorValue(","):
			at++
		case replay.TokenSequenceAt(tokens, at, "ISOLATION", "LEVEL", "SERIALIZABLE"),
			replay.TokenSequenceAt(tokens, at, "ISOLATION", "LEVEL", "REPEATABLE", "READ"),
			replay.TokenSequenceAt(tokens, at, "ISOLATION", "LEVEL", "READ", "COMMITTED"),
			replay.TokenSequenceAt(tokens, at, "ISOLATION", "LEVEL", "READ", "UNCOMMITTED"):
			at += len(isolationWords(tokens, at))
		case replay.TokenSequenceAt(tokens, at, "READ", "WRITE"), replay.TokenSequenceAt(tokens, at, "READ", "ONLY"),
			replay.TokenSequenceAt(tokens, at, "NOT", "DEFERRABLE"):
			at += 2
		case replay.TokenSequenceAt(tokens, at, "DEFERRABLE"):
			at++
		default:
			return transactionNone, "BEGIN carries " + strings.TrimSpace(tokens[at].Value) +
				", which is not a transaction mode the replay reads"
		}
	}
	return transactionBegin, ""
}

// isolationWords returns the words of the ISOLATION LEVEL clause at index.
func isolationWords(tokens []lexer.Token, index int) []lexer.Token {
	width := 3
	switch {
	case replay.TokenSequenceAt(tokens, index, "ISOLATION", "LEVEL", "REPEATABLE", "READ"),
		replay.TokenSequenceAt(tokens, index, "ISOLATION", "LEVEL", "READ", "COMMITTED"),
		replay.TokenSequenceAt(tokens, index, "ISOLATION", "LEVEL", "READ", "UNCOMMITTED"):
		width = 4
	}
	return tokens[index : index+width]
}

// classifyEnd reads the words after COMMIT or END: an optional WORK or
// TRANSACTION, and nothing else. AND CHAIN would start a transaction the file
// does not name.
func classifyEnd(tokens []lexer.Token, word string) (transactionControl, string) {
	at := 1
	if replay.TokenSequenceAt(tokens, at, "WORK") || replay.TokenSequenceAt(tokens, at, "TRANSACTION") {
		at++
	}
	if at < len(tokens) {
		if replay.TokenSequenceAt(tokens, at, "AND") {
			return transactionNone, word + " AND [NO] CHAIN starts a transaction the file does not name"
		}
		return transactionNone, word + " carries " + strings.TrimSpace(tokens[at].Value) +
			", which the replay does not read"
	}
	return transactionEnd, ""
}
