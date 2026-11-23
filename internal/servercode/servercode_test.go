package servercode_test

import (
	"errors"
	"fmt"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	mssql "github.com/microsoft/go-mssqldb"

	"ptah.run/internal/servercode"
)

// The messages hold what the document must not repeat, so a row that read a
// code out of one would fail on its expected value.
func TestRead_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantState  string
		wantNumber int
	}{
		{
			name:      "the PostgreSQL protocol gives a SQLSTATE and no number",
			err:       &pgconn.PgError{Code: "23505", Message: "duplicate key value"},
			wantState: "23505",
		},
		{
			name:       "the MySQL protocol gives both",
			err:        &mysqldriver.MySQLError{Number: 1062, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "Duplicate entry 'x'"},
			wantState:  "23000",
			wantNumber: 1062,
		},
		{
			name:       "the MySQL protocol may leave the SQLSTATE out",
			err:        &mysqldriver.MySQLError{Number: 1105, Message: "unknown"},
			wantNumber: 1105,
		},
		{
			name:       "SQL Server gives a number and no SQLSTATE",
			err:        mssql.Error{Number: 2627, Message: "The duplicate key value is (x)."},
			wantNumber: 2627,
		},
		{
			name:       "ClickHouse gives a number and no SQLSTATE",
			err:        &clickhousedriver.Exception{Code: 60, Message: "Unknown table"},
			wantNumber: 60,
		},
		{
			name:       "an answer is found through the wrapping a writer adds",
			err:        fmt.Errorf("failed to execute: %w", fmt.Errorf("SQL execution failed: %w\nSQL: INSERT", mssql.Error{Number: 208})),
			wantNumber: 208,
		},
		{
			name:      "an answer is found through a join",
			err:       errors.Join(errors.New("rollback failed"), fmt.Errorf("commit: %w", &pgconn.PgError{Code: "23503"})),
			wantState: "23503",
		},
		{name: "an error no server wrote", err: errors.New("connection lost; 23505")},
		{name: "no error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			state, number := servercode.Read(test.err)
			c.Assert(state, qt.Equals, test.wantState)
			c.Assert(number, qt.Equals, test.wantNumber)
		})
	}
}

// Only the refusal of a duplicate key is a unique violation. A deadlock, a
// refused privilege and a lost connection keep their own meaning, and a
// number means what the engine that sent it says: ClickHouse's code 1 is not
// Oracle's ORA-00001.
func TestUniqueViolation(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "PostgreSQL unique_violation", err: &pgconn.PgError{Code: "23505"}, want: true},
		{name: "PostgreSQL serialization failure", err: &pgconn.PgError{Code: "40001"}},
		{name: "PostgreSQL insufficient privilege", err: &pgconn.PgError{Code: "42501"}},
		{name: "PostgreSQL foreign_key_violation", err: &pgconn.PgError{Code: "23503"}},
		{name: "PostgreSQL not_null_violation", err: &pgconn.PgError{Code: "23502"}},
		{name: "MySQL duplicate entry", err: &mysqldriver.MySQLError{Number: 1062}, want: true},
		{name: "MySQL deadlock", err: &mysqldriver.MySQLError{Number: 1213}},
		{name: "MySQL command denied", err: &mysqldriver.MySQLError{Number: 1142}},
		{name: "MySQL foreign key violation", err: &mysqldriver.MySQLError{Number: 1452}},
		{name: "SQL Server primary key violation", err: mssql.Error{Number: 2627}, want: true},
		{name: "SQL Server unique index violation", err: mssql.Error{Number: 2601}, want: true},
		{name: "SQL Server deadlock victim", err: mssql.Error{Number: 1205}},
		{name: "ClickHouse code 1", err: &clickhousedriver.Exception{Code: 1}},
		{name: "a wrapped duplicate key", err: fmt.Errorf("record: %w", &pgconn.PgError{Code: "23505"}), want: true},
		{name: "an error no server wrote", err: errors.New("duplicate key value violates unique constraint")},
		{name: "no error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(servercode.UniqueViolation(test.err), qt.Equals, test.want)
		})
	}
}

// TestRefusedInATransaction reads the three SQLSTATEs a PostgreSQL-protocol
// server refuses a statement with for running inside a transaction block, and
// nothing else: a MySQL-protocol error carrying 25001 is from an engine whose
// rollback does not undo a schema change.
func TestRefusedInATransaction(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "PostgreSQL active_sql_transaction", err: &pgconn.PgError{Code: "25001"}, want: true},
		{name: "PostgreSQL invalid_transaction_termination", err: &pgconn.PgError{Code: "2D000"}, want: true},
		{name: "PostgreSQL unsafe_new_enum_value_usage", err: &pgconn.PgError{Code: "55P04"}, want: true},
		{name: "a wrapped refusal", err: fmt.Errorf("failed to apply migration: %w", &pgconn.PgError{Code: "25001"}), want: true},
		{name: "PostgreSQL in_failed_sql_transaction", err: &pgconn.PgError{Code: "25P02"}},
		{name: "PostgreSQL no_active_sql_transaction", err: &pgconn.PgError{Code: "25P01"}},
		{name: "PostgreSQL syntax error", err: &pgconn.PgError{Code: "42601"}},
		{name: "MySQL with the SQLSTATE 25001", err: &mysqldriver.MySQLError{Number: 1568, SQLState: [5]byte{'2', '5', '0', '0', '1'}}},
		{name: "an error no server wrote", err: errors.New("CREATE INDEX CONCURRENTLY cannot run inside a transaction block")},
		{name: "no error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(servercode.RefusedInATransaction(test.err), qt.Equals, test.want)
		})
	}
}
