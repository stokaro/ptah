// Package serverdiag reads what a database server said about a failure,
// beyond its code, out of a driver's error: the PostgreSQL protocol's DETAIL
// and HINT, the error position, the context line, and the names of the schema,
// table, column, data type and constraint involved.
//
// It is kept apart from internal/servercode on purpose. servercode reads the
// codes, which a run document may publish because a code cannot quote data.
// The fields read here can: a server writes row values and statement text
// into them. So nothing reads this package for a default output. A caller
// reaches it for a local diagnostic its user asked for, such as
// `ptah migrations up --server-details`.
//
// The reading is from the driver's error type, through any wrapping, and never
// from a message. A field the server did not send stays empty, and an error no
// server answered, such as a lost connection or a client-side timeout, has no
// answer at all.
package serverdiag

import (
	"cmp"
	"errors"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgconn"
)

// MaxFieldBytes bounds each text field of an [Answer]. A longer field is cut
// at a character boundary at or below the bound, and [Answer.Truncated]
// reports that one was. A DETAIL quotes the key a constraint refused, and a
// key can be as long as the index allows.
const MaxFieldBytes = 1024

// Answer is a server's own account of one failure. Every field is what the
// server sent, after redaction and the [MaxFieldBytes] bound; a field it did
// not send is empty, and a position it did not send is zero.
type Answer struct {
	// Severity is the severity the server reported, such as ERROR, in its
	// unlocalized spelling when the server sent one.
	Severity string `json:"severity,omitempty"`
	// SQLState is the SQLSTATE code. It is the same code a run document's
	// failure record carries.
	SQLState string `json:"sqlstate,omitempty"`
	// Message is the primary message.
	Message string `json:"message,omitempty"`
	// Detail is the DETAIL field: for a constraint, the key it refused.
	Detail string `json:"detail,omitempty"`
	// Hint is the HINT field.
	Hint string `json:"hint,omitempty"`
	// Position is the 1-based character position of the error in the
	// statement as the server received it.
	Position int `json:"position,omitempty"`
	// Where is the context the server reported, such as the function and
	// line a PL/pgSQL error came from.
	Where string `json:"where,omitempty"`
	// Schema, Table, Column, DataType and Constraint name the objects the
	// server associated with the failure.
	Schema     string `json:"schema,omitempty"`
	Table      string `json:"table,omitempty"`
	Column     string `json:"column,omitempty"`
	DataType   string `json:"data_type,omitempty"`
	Constraint string `json:"constraint,omitempty"`
	// Truncated reports that at least one text field was cut at
	// [MaxFieldBytes].
	Truncated bool `json:"truncated,omitempty"`
}

// Redactor is an error that reports a statement its writer changed before it
// sent it, by writing in a value the reader of the error must not see, such as
// a role's password. RedactServerText takes those values out of text the
// server answered with, as the error's own message already has them taken out.
//
// [Read] applies every Redactor in an error's chain to every field, because the
// server's fields are read past the redacting error, from the driver error it
// wraps. [Rewritten] reports that the chain holds one.
type Redactor interface {
	RedactServerText(text string) string
}

// Read returns the server's account of the failure err reports, and nil when
// no server answered it.
//
// Only the PostgreSQL protocol, as the pgx driver reports it, carries these
// fields, so an error from any other engine's driver is nil too. The answer is
// found through any wrapping, a join included.
//
// Each text field is redacted before it is bounded, so a secret the bound cuts
// in two is still taken out whole: first by every [Redactor] in err's chain,
// then by redact when it is not nil.
func Read(err error, redact func(string) string) *Answer {
	serverErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		return nil
	}
	redactors := chainRedactors(err)
	clean := func(text string) string {
		for _, redactor := range redactors {
			text = redactor.RedactServerText(text)
		}
		if redact != nil {
			text = redact(text)
		}
		return text
	}
	answer := &Answer{Position: int(serverErr.Position)}
	fields := []struct {
		target *string
		value  string
	}{
		{&answer.Severity, cmp.Or(serverErr.SeverityUnlocalized, serverErr.Severity)},
		{&answer.SQLState, serverErr.Code},
		{&answer.Message, serverErr.Message},
		{&answer.Detail, serverErr.Detail},
		{&answer.Hint, serverErr.Hint},
		{&answer.Where, serverErr.Where},
		{&answer.Schema, serverErr.SchemaName},
		{&answer.Table, serverErr.TableName},
		{&answer.Column, serverErr.ColumnName},
		{&answer.DataType, serverErr.DataTypeName},
		{&answer.Constraint, serverErr.ConstraintName},
	}
	for _, field := range fields {
		if field.value == "" {
			continue
		}
		bounded, cut := bound(clean(field.value))
		*field.target = bounded
		answer.Truncated = answer.Truncated || cut
	}
	return answer
}

// Rewritten reports whether a writer changed the statement err reports before
// it sent it, which a [Redactor] in err's chain says. A position the server
// gives then counts characters of the text the server received, which is not
// the statement as the migrator reports it.
func Rewritten(err error) bool {
	return len(chainRedactors(err)) > 0
}

// chainRedactors is every [Redactor] in err's chain, in the order a depth-first
// walk meets them.
func chainRedactors(err error) []Redactor {
	var found []Redactor
	walk(err, func(link error) {
		if redactor, ok := link.(Redactor); ok {
			found = append(found, redactor)
		}
	})
	return found
}

// walk visits err and everything it wraps, through both forms of Unwrap.
func walk(err error, visit func(error)) {
	if err == nil {
		return
	}
	visit(err)
	switch wrapper := err.(type) {
	case interface{ Unwrap() error }:
		walk(wrapper.Unwrap(), visit)
	case interface{ Unwrap() []error }:
		for _, wrapped := range wrapper.Unwrap() {
			walk(wrapped, visit)
		}
	}
}

// bound cuts text to at most [MaxFieldBytes] bytes, at the start of a
// character, and reports whether it cut.
func bound(text string) (string, bool) {
	if len(text) <= MaxFieldBytes {
		return text, false
	}
	end := MaxFieldBytes
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end], true
}
