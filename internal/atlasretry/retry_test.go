package atlasretry_test

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	mssql "github.com/microsoft/go-mssqldb"

	"go.5x5.cz/ptah/internal/atlasretry"
)

func TestIsRetryable(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "PostgreSQL serialization failure",
			err:  &pgconn.PgError{Code: "40001"},
			want: true,
		},
		{
			name: "PostgreSQL deadlock",
			err:  &pgconn.PgError{Code: "40P01"},
			want: true,
		},
		{
			name: "MySQL lock wait timeout",
			err:  &mysqldriver.MySQLError{Number: 1205},
			want: true,
		},
		{
			name: "MySQL deadlock",
			err:  &mysqldriver.MySQLError{Number: 1213},
			want: true,
		},
		{
			name: "SQL Server deadlock",
			err:  mssql.Error{Number: 1205},
			want: true,
		},
		{
			name: "SQLite busy",
			err:  sqliteCodeError{code: 5},
			want: true,
		},
		{
			name: "SQLite locked extended code",
			err:  sqliteCodeError{code: 6 | 1<<8},
			want: true,
		},
		{
			name: "wrapped retryable error",
			err:  fmt.Errorf("commit transaction: %w", &pgconn.PgError{Code: "40001"}),
			want: true,
		},
		{
			name: "PostgreSQL unique violation",
			err:  &pgconn.PgError{Code: "23505"},
			want: false,
		},
		{
			name: "MySQL duplicate entry",
			err:  &mysqldriver.MySQLError{Number: 1062},
			want: false,
		},
		{
			name: "SQL Server unique violation",
			err:  mssql.Error{Number: 2627},
			want: false,
		},
		{
			name: "SQLite constraint",
			err:  sqliteCodeError{code: 19},
			want: false,
		},
		{
			name: "ordinary error",
			err:  errors.New("permanent failure"),
			want: false,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(atlasretry.IsRetryable(tt.err), qt.Equals, tt.want)
		})
	}
}

type sqliteCodeError struct {
	code int
}

func (e sqliteCodeError) Error() string {
	return "SQLite error"
}

func (e sqliteCodeError) Code() int {
	return e.code
}
