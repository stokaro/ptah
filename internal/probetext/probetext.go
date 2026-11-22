// Package probetext decides whether declared SQL text may be written into a
// probe statement, and raises the warning for a declaration whose text may
// not.
//
// A probe puts a declaration through the server inside a transaction it rolls
// back, so that a comparison reads the declaration the way the server stores
// it: a CHECK expression goes into `CREATE TABLE ... CHECK (<text>)`. The text
// is the author's, and a plan-only command runs the probe. Text that closes
// the parentheses the statement gave it, or that ends the statement, is SQL of
// its own to the server, and a COMMIT in it outlives the rollback: measured on
// SQL Server 2025 and PostgreSQL 18, `ptah migrations plan` created a table
// that way and reported no executable statement.
//
// Both servers parse a whole batch before they run any of it, so text that
// stays inside its parentheses and holds no statement separator cannot end the
// statement around it. That is what this package checks, with the dialect's
// lexer, so that a parenthesis or a semicolon inside a string, a quoted name
// or a comment counts for nothing, as it does on the server. A keyword list
// could not do it: T-SQL needs no semicolon between two statements.
//
// The check is only as good as the lexer's agreement with the server. Where a
// session setting decides how the server reads text, the probe transaction
// pins it to the reading checked here: on the PostgreSQL family
// standard_conforming_strings is on, so a backslash in an ordinary string is a
// character, and a probe that turns it off fails the transaction check in
// ptah.run/internal/probetx. Where servers of one dialect read text two ways,
// the text is read both: CockroachDB ends a line comment at a line feed only,
// where PostgreSQL ends one at a carriage return too. Text is refused when any
// reading lets it out.
package probetext

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"unicode/utf8"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/rundiag"
)

// ErrUncontained is the error every refusal of this package wraps: the text
// could reach past the place a probe statement writes it in.
var ErrUncontained = errors.New("declared text could reach past its place in a probe statement")

// uncontained is one refusal. Its message completes the sentence "the probe
// does not run, because ...".
type uncontained struct{ reason string }

func (e uncontained) Error() string { return e.reason }

func (uncontained) Unwrap() error { return ErrUncontained }

var (
	errUnterminated = uncontained{"its text leaves a string, quoted name or comment open"}
	errCloses       = uncontained{"its text closes a parenthesis it does not open"}
	errLeavesOpen   = uncontained{"its text leaves a parenthesis open"}
	errSeparator    = uncontained{"its text holds a statement separator"}
	errForeignQuote = uncontained{"its text holds a quote the server does not read as one"}
	errUnreadable   = uncontained{"its text holds a character the lexer cannot place"}
	errEncoding     = uncontained{"its text is not valid UTF-8 or holds a NUL character"}
)

// sentinel follows the text the lexer reads. A string, quoted name or comment
// the text leaves open runs into it, which is how an open one is told from one
// that ends where the text does. It starts with a line break because every
// probe statement writes one after the declared text, so a line comment that
// ends the text, as in `a > 0 -- positive only`, ends there and leaves the
// statement's closing parenthesis to the statement.
const sentinel = "\n )"

// separators says where a semicolon may stand in the checked text.
type separators int

const (
	// noSeparator refuses every semicolon.
	noSeparator separators = iota
	// trailingSeparator accepts one semicolon with nothing but white space
	// after it, the end of a whole statement.
	trailingSeparator
	// anySeparator accepts every semicolon, for a statement whose body
	// separates its own statements; the server decides where it ends.
	anySeparator
)

// Fragment reports why text, declared for dialect, could reach past the
// clause a probe statement writes it in, or nil when it cannot. It is for text
// written inside a statement Ptah spells around it: an expression between
// parentheses, a view's body after AS, a type or a default. The statement
// writes a line break after the text, so a line comment the text ends with
// ends there.
//
// Refused is text that leaves a string, quoted name or comment open; that
// closes a parenthesis it does not open, or leaves one open; that holds a
// semicolon outside a string, quoted name or comment; or that holds a quote
// the dialect's server does not read as one. Each error wraps
// [ErrUncontained].
func Fragment(dialect, text string) error {
	return check(dialect, text, noSeparator)
}

// Fragments is [Fragment] for each text a probe writes into one statement. It
// reports the first refusal, so a warning names one cause.
func Fragments(dialect string, texts ...string) error {
	for _, text := range texts {
		if refusal := Fragment(dialect, text); refusal != nil {
			return refusal
		}
	}
	return nil
}

// Statement is [Fragment] for a whole statement a probe rendered from a
// declaration, such as a CREATE FUNCTION carrying declared parameters and a
// declared body. One semicolon may end it.
//
// It is for the PostgreSQL family, whose server needs a semicolon between two
// statements. A T-SQL batch needs none, so a whole statement says nothing
// there; check each declared piece with [Fragment] instead.
func Statement(dialect, statement string) error {
	return check(dialect, statement, trailingSeparator)
}

// StatementWithBody is [Statement] for a statement whose SQL-standard body,
// `BEGIN ATOMIC ... END`, separates its own statements with semicolons. The
// lexer cannot tell where such a body ends: `END` closes a CASE too, and
// `SELECT 1 AS end` names a column, so text can make a lexer and the server
// disagree. Semicolons are accepted here, and the caller has the server parse
// the statement as one before it runs it, which PostgreSQL refuses with
// `cannot insert multiple commands into a prepared statement` for text that
// holds two.
func StatementWithBody(dialect, statement string) error {
	return check(dialect, statement, anySeparator)
}

// Exec runs statement, a probe statement that carries declared text, inside
// tx on a PostgreSQL-family server, as one command. It prepares the statement
// and runs the prepared statement, so the server parses the text on the
// extended query protocol, where one statement holds one command: text that
// adds a second is refused when it is prepared, before any of it runs. The
// three servers answer as [ParseAlone] says.
//
// tx.ExecContext is not that. pgx sends a statement with no arguments on the
// simple query protocol, which runs every command the text holds, so text that
// slips past [Fragment] or [Statement] would run as SQL of its own, and a
// function it calls, such as dblink_exec, can commit on another connection
// without ending the transaction. A refused statement leaves tx aborted for
// the caller to roll back to its savepoint. A statement the server reads as
// more than one is answered with an error that wraps both [ErrUncontained]
// and the server's error, so the caller warns about the declaration as it
// does for text the check refuses; any other refusal is the server's error.
// Measured on PostgreSQL 18,
// CockroachDB v26.4.1 and YugabyteDB 2026.1.2, the prepared statement is
// released after a refusal too.
func Exec(ctx context.Context, tx *sql.Tx, statement string) error {
	err := execPrepared(ctx, tx, statement)
	if severalStatements(err) {
		return splitByServer{server: err}
	}
	return err
}

// splitByServer is the refusal of [Exec] for a statement the server read as
// more than one. It reads as the refusal of [ParseAlone] and keeps the
// server's error, with its SQLSTATE, in its chain.
type splitByServer struct{ server error }

func (e splitByServer) Error() string { return errSeveralStatements.reason }

func (e splitByServer) Unwrap() []error { return []error{ErrUncontained, e.server} }

// execPrepared prepares statement in tx, runs it and releases it.
func execPrepared(ctx context.Context, tx *sql.Tx, statement string) error {
	prepared, err := tx.PrepareContext(ctx, statement)
	if err != nil {
		return err
	}
	_, err = prepared.ExecContext(ctx)
	if closeErr := prepared.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

// errSeveralStatements is the refusal of [ParseAlone].
var errSeveralStatements = uncontained{"the server reads more than one statement in its text"}

// parseSavepoint is the savepoint [ParseAlone] returns to after the server
// refuses to parse a statement, which aborts the transaction.
const parseSavepoint = "ptah_parse_alone"

// ParseAlone has a PostgreSQL-family server parse statement, inside tx,
// without running it, so that the caller runs the statement only where the
// server reads it as one. It prepares the statement and releases the prepared
// statement at once; the caller then runs the same text, which the server
// parses the same way.
//
// refusal wraps [ErrUncontained] when the server reports more than one
// statement in the text, and is the server's error when it refuses to parse
// the text for another reason; the caller runs the statement only when it is
// nil. Measured on PostgreSQL 18, YugabyteDB 2026.1.2 and CockroachDB v26.4.1,
// each refuses to prepare two statements, PostgreSQL and YugabyteDB with
// `cannot insert multiple commands into a prepared statement` and CockroachDB
// with `prepared statement had 2 statements, expected 1`. A refusal is
// answered after a rollback to a savepoint, so tx goes on; err is a failure of
// tx itself.
//
// It is the server's half of [StatementWithBody], and it costs three round
// trips more than running the statement does.
func ParseAlone(ctx context.Context, tx *sql.Tx, statement string) (refusal, err error) {
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+parseSavepoint); err != nil {
		return nil, fmt.Errorf("parse a probe statement: savepoint: %w", err)
	}
	prepared, refused := tx.PrepareContext(ctx, statement)
	if refused != nil {
		if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+parseSavepoint); err != nil {
			return nil, fmt.Errorf("parse a probe statement: roll back to savepoint: %w", err)
		}
		if severalStatements(refused) {
			return errSeveralStatements, nil
		}
		return refused, nil
	}
	if err := prepared.Close(); err != nil {
		return nil, fmt.Errorf("parse a probe statement: release the prepared statement: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+parseSavepoint); err != nil {
		return nil, fmt.Errorf("parse a probe statement: release savepoint: %w", err)
	}
	return nil, nil
}

// severalStatements reports whether err is a server's refusal to prepare text
// holding more than one statement. CockroachDB answers SQLSTATE 42P14 for it.
// PostgreSQL answers 42601, the code of every syntax error, so the message
// decides there; a server that translates its messages is then refused to
// prepare for another reason, which keeps the statement from running all the
// same and raises no warning.
func severalStatements(err error) bool {
	var coded interface{ SQLState() string }
	if !errors.As(err, &coded) {
		return false
	}
	switch coded.SQLState() {
	case "42P14":
		return true
	case "42601":
		return strings.Contains(err.Error(), "multiple commands")
	default:
		return false
	}
}

// check reads text every way the dialect's server can and refuses it when any
// reading lets it out.
func check(dialect, text string, allowed separators) error {
	if !utf8.ValidString(text) || strings.ContainsRune(text, 0) {
		// The lexer ends its input at a NUL, so whatever follows one is
		// text it never reads.
		return errEncoding
	}
	for _, reading := range readings(dialect) {
		if err := read(reading.apply(text), reading, allowed); err != nil {
			return err
		}
	}
	return nil
}

// reading is one way a server of the dialect can read text.
type reading struct {
	options lexer.Options
	// dollarQuotes reports that the server reads $tag$ ... $tag$ as one
	// string. The shared lexer reads one in every dialect, so where the
	// server does not, the inside of one is SQL to it.
	dollarQuotes bool
	// lineFeedEndsComments reports that only a line feed ends a line
	// comment, as on CockroachDB, where the lexer ends one at a carriage
	// return too.
	lineFeedEndsComments bool
}

// apply is text as this reading's lexer has to see it. A carriage return
// becomes a space, which keeps every character in its place and keeps a line
// comment open across it.
func (r reading) apply(text string) string {
	if !r.lineFeedEndsComments {
		return text
	}
	return strings.ReplaceAll(text, "\r", " ")
}

// readings is every way a server of dialect can read text.
//
// Outside the PostgreSQL family it is the descriptor's rules alone. SQL Server
// is the one engine there whose probes write declared text, and measured on
// SQL Server 2025 through go-mssqldb it ends a line comment at a carriage
// return, as the shared lexer does.
//
// The descriptor's EscapeStrings, the E'...' strings of the PostgreSQL family,
// stands in for facts no descriptor field states, measured on PostgreSQL 18,
// CockroachDB v26.4.1 and YugabyteDB 2026.1.2. Their servers read dollar
// quotes; they read every character outside ASCII as a name character, so
// `x€$a$` is one name and opens no dollar quote; and they read `$1$a$` as the
// parameter `$1` and a dollar quote. The family's servers also differ among
// themselves, and the text is read every way they do: CockroachDB reads
// b'...' as a byte string whose backslash escapes, where PostgreSQL's B'...'
// is a bit string; CockroachDB continues a string across a line break but not
// across a comment, where its descriptor states no continued strings; and
// CockroachDB ends a line comment at a line feed only.
//
// A backslash in an ordinary string is read as the descriptor states, because
// the probe transaction pins standard_conforming_strings on; reading it as an
// escape as well would refuse every live string that ends in one, such as
// `'C:\'`, and plan the declaration again on every run.
func readings(dialect string) []reading {
	options := dialectlexer.Options(dialect)
	described, _ := platform.Lookup(dialect)
	if !described.Lexical.EscapeStrings {
		return []reading{{options: options}}
	}
	options.NonASCIINames = true
	options.DollarParameters = true
	var set []reading
	for _, continued := range []bool{false, true} {
		for _, byteEscapes := range []bool{false, true} {
			for _, lineFeedOnly := range []bool{false, true} {
				read := options
				read.PostgreSQLStringConstants = continued
				read.ByteStringEscapes = byteEscapes
				set = append(set, reading{options: read, dollarQuotes: true, lineFeedEndsComments: lineFeedOnly})
			}
		}
	}
	return set
}

// read checks text under one reading.
func read(text string, reading reading, allowed separators) error {
	end := len(text)
	scanner := lexer.NewLexerWithOptions(text+sentinel, reading.options)
	state := readState{allowed: allowed, dollarQuotes: reading.dollarQuotes}
	for {
		token := scanner.NextToken()
		if token.Type == lexer.TokenEOF || token.Start >= end {
			break
		}
		if token.Type == lexer.TokenWhitespace {
			continue
		}
		if token.End > end {
			return errUnterminated
		}
		if err := state.take(token); err != nil {
			return err
		}
	}
	if state.depth != 0 {
		return errLeavesOpen
	}
	return nil
}

// readState is what [read] has seen of the text so far.
type readState struct {
	allowed      separators
	dollarQuotes bool
	// depth counts the parentheses open.
	depth int
	// separated reports that a semicolon ended the statement.
	separated bool
}

// take reads one token that ends inside the text, and reports why the text
// may not be written into a probe.
func (s *readState) take(token lexer.Token) error {
	if s.separated {
		return errSeparator
	}
	switch token.Type {
	case lexer.TokenSemicolon:
		switch s.allowed {
		case noSeparator:
			return errSeparator
		case trailingSeparator:
			s.separated = true
		case anySeparator:
		}
	case lexer.TokenOperator:
		switch token.Value {
		case "(":
			s.depth++
		case ")":
			s.depth--
			if s.depth < 0 {
				return errCloses
			}
		}
	case lexer.TokenIdentifier:
		// No server a probe runs on quotes a name in backticks, so the
		// text between two is SQL to it.
		if strings.HasPrefix(token.Value, "`") {
			return errForeignQuote
		}
	case lexer.TokenString:
		if !s.dollarQuotes && strings.HasPrefix(token.Value, "$") {
			return errForeignQuote
		}
	case lexer.TokenUnknown:
		return errUnreadable
	case lexer.TokenComment, lexer.TokenWhitespace, lexer.TokenEOF:
	}
	return nil
}

// WarnRefused is [Warn] for refused, the error of a probe statement [Exec]
// ran: it warns when the server read more than one statement in the text, and
// says nothing for any other refusal, which leaves the declaration compared
// as written without a warning.
func WarnRefused(ctx context.Context, key, object string, refused error) {
	if errors.Is(refused, ErrUncontained) {
		Warn(ctx, key, object, refused)
	}
}

// Warn raises, for the run ctx carries, the warning that object is compared as
// written because refusal, an error of [Fragment], [Statement] or
// [StatementWithBody], kept its probe from running, and writes it at once:
// the comparison it changes is what the command prints next. key identifies
// the declaration, so a run that probes it twice warns once, and object names
// it the way a reader finds it, such as `CHECK ck_price on items`. Without a
// run it writes the warning nowhere, as [rundiag.Warn] does for a caller with
// no stream.
func Warn(ctx context.Context, key, object string, refusal error) {
	_ = rundiag.Warn(ctx, rundiag.Stream(ctx), rundiag.UnprobedDeclarations, rundiag.Record{
		Key: key,
		Line: fmt.Sprintf("%s is compared as written, not in the form the server stores: "+
			"the probe that spells it does not run, because %v", object, refusal),
		Attrs: []slog.Attr{slog.String("object", object), slog.String("cause", refusal.Error())},
	})
	rundiag.Settle(ctx, rundiag.UnprobedDeclarations)
}
