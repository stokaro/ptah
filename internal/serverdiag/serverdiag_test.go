package serverdiag_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/internal/serverdiag"
)

// redactingError stands for a writer that wrote a value into the statement it
// sent and keeps the value out of its own message.
type redactingError struct {
	err    error
	secret string
}

func (e redactingError) Error() string {
	return strings.ReplaceAll(e.err.Error(), e.secret, "[secret]")
}
func (e redactingError) Unwrap() error { return e.err }
func (e redactingError) RedactServerText(text string) string {
	return strings.ReplaceAll(text, e.secret, "[secret]")
}

// everyField is a PostgreSQL error that fills every field the package reads.
func everyField() *pgconn.PgError {
	return &pgconn.PgError{
		Severity:            "FEHLER",
		SeverityUnlocalized: "ERROR",
		Code:                "23503",
		Message:             `insert or update on table "orders" violates foreign key constraint "orders_customer_fkey"`,
		Detail:              `Key (customer)=(42) is not present in table "customers".`,
		Hint:                "Insert the customer first.",
		Position:            17,
		Where:               "SQL statement \"INSERT INTO orders VALUES (1, 42)\"\nPL/pgSQL function load() line 3 at SQL statement",
		SchemaName:          "shop",
		TableName:           "orders",
		ColumnName:          "customer",
		DataTypeName:        "integer",
		ConstraintName:      "orders_customer_fkey",
	}
}

func TestRead_HappyPath(t *testing.T) {
	everything := serverdiag.Answer{
		Severity:   "ERROR",
		SQLState:   "23503",
		Message:    `insert or update on table "orders" violates foreign key constraint "orders_customer_fkey"`,
		Detail:     `Key (customer)=(42) is not present in table "customers".`,
		Hint:       "Insert the customer first.",
		Position:   17,
		Where:      "SQL statement \"INSERT INTO orders VALUES (1, 42)\"\nPL/pgSQL function load() line 3 at SQL statement",
		Schema:     "shop",
		Table:      "orders",
		Column:     "customer",
		DataType:   "integer",
		Constraint: "orders_customer_fkey",
	}
	overBound := "a" + strings.Repeat("é", serverdiag.MaxFieldBytes)
	tests := []struct {
		name   string
		err    error
		redact func(string) string
		want   serverdiag.Answer
	}{
		{
			name: "every field is read through the wrapping a writer and the migrator add",
			err:  fmt.Errorf("failed to execute migration SQL: %w", fmt.Errorf("SQL execution failed: %w\nSQL: INSERT", everyField())),
			want: everything,
		},
		{
			name: "an answer is found through a join",
			err:  errors.Join(errors.New("rollback failed"), fmt.Errorf("commit: %w", everyField())),
			want: everything,
		},
		{
			name: "a field the server did not send stays absent",
			err:  &pgconn.PgError{Severity: "ERROR", Code: "42601", Message: "syntax error at end of input", Position: 9},
			want: serverdiag.Answer{Severity: "ERROR", SQLState: "42601", Message: "syntax error at end of input", Position: 9},
		},
		{
			name: "the localized severity stands when the server sent no other",
			err:  &pgconn.PgError{Severity: "FEHLER", Code: "42P01", Message: "Relation existiert nicht"},
			want: serverdiag.Answer{Severity: "FEHLER", SQLState: "42P01", Message: "Relation existiert nicht"},
		},
		{
			name: "a redactor in the chain takes its value out of every field the server wrote it into",
			err: redactingError{secret: "pw-SENTINEL", err: &pgconn.PgError{
				Code: "42601", Message: `syntax error at or near "pw-SENTINEL"`, Detail: "near pw-SENTINEL", Hint: "quote pw-SENTINEL",
			}},
			want: serverdiag.Answer{
				SQLState: "42601", Message: `syntax error at or near "[secret]"`, Detail: "near [secret]", Hint: "quote [secret]",
			},
		},
		{
			name:   "the caller's redaction runs on every field",
			err:    &pgconn.PgError{Code: "08001", Message: "could not connect with password=hunter2", Detail: "hunter2 refused"},
			redact: func(text string) string { return strings.ReplaceAll(text, "hunter2", "***") },
			want:   serverdiag.Answer{SQLState: "08001", Message: "could not connect with password=***", Detail: "*** refused"},
		},
		{
			name: "a field over the bound is cut at the start of a character",
			err:  &pgconn.PgError{Code: "23505", Detail: overBound},
			want: serverdiag.Answer{SQLState: "23505", Detail: overBound[:serverdiag.MaxFieldBytes-1], Truncated: true},
		},
		{
			name:   "a secret the bound would cut in two is taken out before the cut",
			err:    &pgconn.PgError{Code: "23505", Detail: strings.Repeat("x", serverdiag.MaxFieldBytes-4) + "hunter2"},
			redact: func(text string) string { return strings.ReplaceAll(text, "hunter2", "***") },
			want:   serverdiag.Answer{SQLState: "23505", Detail: strings.Repeat("x", serverdiag.MaxFieldBytes-4) + "***"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := serverdiag.Read(test.err, test.redact)
			c.Assert(got, qt.DeepEquals, &test.want)
		})
	}
}

// No server answered any of these, so nothing is invented for them: not a
// message, and not a code read out of one.
func TestRead_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "no error"},
		{name: "a lost connection", err: fmt.Errorf("failed to execute migration SQL: %w", errors.New("unexpected EOF; ERROR: 23503"))},
		{name: "a client-side timeout", err: fmt.Errorf("SQL execution failed: %w", context.DeadlineExceeded)},
		{name: "another engine's driver", err: &mysqldriver.MySQLError{Number: 1452, Message: "Cannot add or update a child row"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(serverdiag.Read(test.err, nil), qt.IsNil)
		})
	}
}

func TestRewritten(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "a writer that wrote a value into the statement says so",
			err:  fmt.Errorf("failed: %w", redactingError{secret: "pw", err: &pgconn.PgError{Code: "42601"}}),
			want: true,
		},
		{
			name: "a writer's redaction is found through a join",
			err:  errors.Join(errors.New("rollback failed"), redactingError{secret: "pw", err: &pgconn.PgError{Code: "42601"}}),
			want: true,
		},
		{name: "a statement sent as written", err: fmt.Errorf("failed: %w", &pgconn.PgError{Code: "42601"})},
		{name: "no error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(serverdiag.Rewritten(test.err), qt.Equals, test.want)
		})
	}
}
