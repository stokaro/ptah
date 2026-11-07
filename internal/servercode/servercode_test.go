package servercode_test

import (
	"errors"
	"fmt"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	mssql "github.com/microsoft/go-mssqldb"

	qt "github.com/frankban/quicktest"

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
