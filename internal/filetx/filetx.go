// Package filetx reads the transactions a migration file opens and closes
// itself, in PostgreSQL's spelling of transaction control.
//
// A history written for a runner that sends each file as it is, as
// golang-migrate does, has files that begin with BEGIN and end with COMMIT. The
// dev database replay runs those files without a transaction of its own, and
// decides which files qualify with [Read]. A directory of such a runner is
// read with [ReadUnwrapped], by its apply and by its replay alike, so the two
// cannot disagree about a file: it accepts the further forms that runner
// applies, such as a COMMIT outside a block.
//
// Neither reading knows a dialect. The engine whose transactions they describe
// offers them: PostgreSQL's replay guard calls both, and PostgreSQL's migrator
// services carry [ReadUnwrapped] (internal/migratorext/postgres). CockroachDB,
// YugabyteDB and Spanner take the same words with other transaction semantics
// and offer nothing, so on them no file is read as one with transactions of
// its own.
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
	// commits holds the COMMIT and END statements outside every block, which
	// [ReadUnwrapped] accepts: each ends the transaction the statements before
	// it run in when the file is sent as one query.
	commits []int
	// joined holds the statements that act on a block already open, which
	// [ReadUnwrapped] accepts: a BEGIN inside a block, and SAVEPOINT, RELEASE
	// and ROLLBACK TO.
	joined []int
}

// Empty reports whether the file controls no transaction of its own.
func (b Blocks) Empty() bool {
	return len(b.boundaries) == 0 && len(b.commits) == 0 && len(b.joined) == 0
}

// Control reports whether the statement at index is one of the file's own
// transaction control statements: a block's BEGIN or COMMIT, a COMMIT outside
// every block, a BEGIN inside one, or a savepoint statement.
func (b Blocks) Control(index int) bool {
	return slices.Contains(b.boundaries, index) || slices.Contains(b.commits, index) || slices.Contains(b.joined, index)
}

// Controls returns the indexes of the file's own transaction control
// statements, in order, or nil when it has none; see [Blocks.Control].
func (b Blocks) Controls() []int {
	controls := slices.Concat(b.boundaries, b.commits, b.joined)
	slices.Sort(controls)
	return slices.Clip(controls)
}

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

// AsOneQuery returns the transactions a file of total statements runs in when
// it is sent as one query, which is how golang-migrate sends a file by default.
// PostgreSQL then runs the statements in an implicit transaction: statements
// before a BEGIN join the transaction it opens, a COMMIT or END ends it, and
// the statements after the last one run in one more transaction that the end
// of the query commits. Measured with golang-migrate v4.20.1 on PostgreSQL
// 15.19 (stokaro/ptah#4979): for `CREATE TABLE a; BEGIN; CREATE TABLE b;
// COMMIT; CREATE TABLE c; INSERT INTO missing ...;` a and b stay, and a
// failure inside the block takes a back too.
//
// A COMMIT outside every block ends the transaction too, the implicit one the
// statements before it run in, as golang-migrate's single query ends it: for
// `ALTER TYPE color ADD VALUE 'green'; COMMIT; CREATE TABLE a (c color
// DEFAULT 'green');` the value is committed before the table uses it.
//
// Each returned transaction opens at its first statement and closes at the
// file's statement that ended it; the last one, when statements follow the
// file's last COMMIT, closes at total, past the file's last statement, where
// the end of the file commits it. A file that controls no transaction of its
// own returns the zero value.
func (b Blocks) AsOneQuery(total int) Blocks {
	if b.Empty() {
		return Blocks{}
	}
	var ends []int
	for i := 1; i < len(b.boundaries); i += 2 {
		ends = append(ends, b.boundaries[i])
	}
	ends = append(ends, b.commits...)
	slices.Sort(ends)
	var boundaries []int
	start := 0
	for _, end := range ends {
		boundaries = append(boundaries, start, end)
		start = end + 1
	}
	if start < total {
		boundaries = append(boundaries, start, total)
	}
	return Blocks{boundaries: boundaries}
}

// Closing returns the index of the statement that ends the transaction the
// statement at index runs in or ends, and false for a statement outside every
// transaction. A transaction [Blocks.AsOneQuery] closes at the end of the file
// returns the statement count it was given.
func (b Blocks) Closing(index int) (int, bool) {
	for i := 0; i+1 < len(b.boundaries); i += 2 {
		if index >= b.boundaries[i] && index <= b.boundaries[i+1] {
			return b.boundaries[i+1], true
		}
	}
	return 0, false
}

func (b Blocks) opening(index int) (int, bool) {
	for i := 0; i+1 < len(b.boundaries); i += 2 {
		if index >= b.boundaries[i] && index < b.boundaries[i+1] {
			return b.boundaries[i], true
		}
	}
	return 0, false
}

// RunsAlone reports a file of exactly one statement, comments and empty
// statements aside. A runner that opens no transaction around a file and sends
// it as it is sends that statement on its own, and PostgreSQL runs a statement
// sent on its own outside a transaction, so a statement it refuses inside one
// applies: measured with golang-migrate, a file holding only CREATE INDEX
// CONCURRENTLY applies, and the same statement beside a second one is refused
// (stokaro/ptah#4962). Every reader of such a directory that decides whether a
// file runs inside a transaction asks this: the apply, the dev database
// replay, lint and the writer of new files.
func RunsAlone(statements []string) bool {
	count := 0
	for _, statement := range statements {
		if len(significantTokens(statement)) > 0 {
			count++
		}
	}
	return count == 1
}

// Begin reports whether statement opens a transaction, as BEGIN or START
// TRANSACTION, and returns the transaction modes it names as SQL a SET
// TRANSACTION takes, or the empty string for none. PostgreSQL applies the
// modes of a BEGIN that joins a transaction already open, as one inside a
// query does, the way SET TRANSACTION applies them: an isolation level after
// the transaction's first query is refused.
func Begin(statement string) (modes string, ok bool) {
	tokens := significantTokens(statement)
	if len(tokens) == 0 || tokens[0].Type != lexer.TokenIdentifier {
		return "", false
	}
	at := 1
	switch {
	case wordsAt(tokens, 0, "BEGIN"):
	case wordsAt(tokens, 0, "START", "TRANSACTION"):
		at = 2
	default:
		return "", false
	}
	if kind, reason := classifyTransactionControl(tokens); kind != transactionBegin || reason != "" {
		return "", false
	}
	if wordsAt(tokens, at, "WORK") || wordsAt(tokens, at, "TRANSACTION") {
		at++
	}
	words := make([]string, 0, len(tokens)-at)
	for _, token := range tokens[at:] {
		words = append(words, strings.TrimSpace(token.Value))
	}
	return strings.ReplaceAll(strings.Join(words, " "), " ,", ","), true
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
//   - AND CHAIN, which opens a transaction nothing in the file names (AND NO
//     CHAIN is a plain COMMIT and is accepted), and
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

// ReadUnwrapped reads the statements of a migration file a runner sends as it
// is, with no transaction of its own around the file, as golang-migrate does,
// for the transaction control PostgreSQL runs in such a file.
//
// It accepts what [Read] accepts, and three forms [Read] refuses, each of
// which golang-migrate v4.20.1 applies through both of its PostgreSQL drivers,
// measured on PostgreSQL 15.19 (stokaro/ptah#4979):
//
//   - a COMMIT or END outside every block, which ends the transaction the
//     statements before it run in; on its own it is a no-op with a warning;
//   - a BEGIN inside a block, which joins the block with a warning;
//   - SAVEPOINT, RELEASE and ROLLBACK TO inside a block. A savepoint ends with
//     the block, so a file resumed after the block needs none of it.
//
// Refused, for the whole file, with a [*StructureError]:
//
//   - ROLLBACK and ABORT: a file that takes its own work back cannot be
//     recorded as a history of what it did;
//   - SAVEPOINT, RELEASE and ROLLBACK TO outside a block, which PostgreSQL
//     refuses there, so golang-migrate fails at them too;
//   - AND CHAIN, PREPARE TRANSACTION, COMMIT PREPARED and ROLLBACK PREPARED;
//   - a block the file leaves open, which golang-migrate's drivers settle in
//     opposite ways: lib/pq rolls it back and leaves the version dirty, pgx5
//     commits it with the version.
func ReadUnwrapped(statements []string) (Blocks, error) {
	var blocks Blocks
	openAt := -1
	for index, statement := range statements {
		tokens := significantTokens(statement)
		if len(tokens) == 0 {
			continue
		}
		if savepointStatement(tokens) {
			if openAt < 0 {
				return Blocks{}, &StructureError{Index: index, Reason: strings.ToUpper(tokens[0].Value) +
					" outside a transaction the file opened, where PostgreSQL refuses it"}
			}
			blocks.joined = append(blocks.joined, index)
			continue
		}
		kind, reason := classifyTransactionControl(tokens)
		if reason != "" {
			return Blocks{}, &StructureError{Index: index, Reason: reason}
		}
		switch kind {
		case transactionBegin:
			if openAt >= 0 {
				blocks.joined = append(blocks.joined, index)
				continue
			}
			openAt = index
			blocks.boundaries = append(blocks.boundaries, index)
		case transactionEnd:
			if openAt < 0 {
				blocks.commits = append(blocks.commits, index)
				continue
			}
			openAt = -1
			blocks.boundaries = append(blocks.boundaries, index)
		case transactionNone:
		}
	}
	if openAt >= 0 {
		return Blocks{}, &StructureError{Index: openAt, Reason: "the transaction is never closed by the file"}
	}
	return blocks, nil
}

// savepointStatement reports SAVEPOINT, RELEASE [SAVEPOINT] and ROLLBACK
// [WORK | TRANSACTION] TO [SAVEPOINT].
func savepointStatement(tokens []lexer.Token) bool {
	switch {
	case wordsAt(tokens, 0, "SAVEPOINT"), wordsAt(tokens, 0, "RELEASE"):
		return true
	case wordsAt(tokens, 0, "ROLLBACK"):
		at := 1
		if wordsAt(tokens, at, "WORK") || wordsAt(tokens, at, "TRANSACTION") {
			at++
		}
		return wordsAt(tokens, at, "TO")
	}
	return false
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
// TRANSACTION, and AND NO CHAIN, which is a plain COMMIT, and nothing else.
// AND CHAIN would start a transaction the file does not name.
func classifyEnd(tokens []lexer.Token, word string) (transactionControl, string) {
	at := 1
	if wordsAt(tokens, at, "WORK") || wordsAt(tokens, at, "TRANSACTION") {
		at++
	}
	if wordsAt(tokens, at, "AND", "NO", "CHAIN") {
		at += 3
	}
	if at < len(tokens) {
		if wordsAt(tokens, at, "AND", "CHAIN") {
			return transactionNone, word + " AND CHAIN starts a transaction the file does not name"
		}
		return transactionNone, word + " carries " + strings.TrimSpace(tokens[at].Value) + ", which Ptah does not read"
	}
	return transactionEnd, ""
}
