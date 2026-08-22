package integrationharness

// White-box testing required: applyScenarioOutcome and isMySQLPrivilegeRefusal
// are unexported, and their only exported caller is the runner, which needs a
// live server and an account without CREATE USER to reach either. What is under
// test is the classification of a driver error and the outcome it produces, so
// both are driven directly rather than through a database.

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestApplyScenarioOutcomePreconditionSkips pins that a scenario which could not
// set up its precondition is reported as skipped rather than failed.
//
// This is the whole of stokaro/ptah#1901: permission_restrictions needs an
// account it is allowed to create, the suite deliberately connects as an
// ordinary application account, and the resulting refusal was reported as a
// failure every night. A red cell nobody can act on is worse than no cell.
//
// The plain-error row is the control that keeps the skip narrow. A scenario that
// genuinely failed must still fail, or this fix would silence the gate instead
// of making it readable.
func TestApplyScenarioOutcomePreconditionSkips(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantSkipped bool
		wantSuccess bool
		wantReason  string
		wantError   string
	}{
		{
			name:        "precondition unavailable is skipped",
			err:         fmt.Errorf("%w: needs CREATE USER", ErrPreconditionUnavailable),
			wantSkipped: true,
			wantSuccess: false,
			wantReason:  "scenario precondition unavailable: needs CREATE USER",
			wantError:   "",
		},
		{
			name:        "an ordinary failure still fails",
			err:         errors.New("restricted connection unexpectedly read mysql.user"),
			wantSkipped: false,
			wantSuccess: false,
			wantReason:  "",
			wantError:   "restricted connection unexpectedly read mysql.user",
		},
		{
			name:        "no error passes",
			err:         nil,
			wantSkipped: false,
			wantSuccess: true,
			wantReason:  "",
			wantError:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			var result TestResult
			applyScenarioOutcome(&result, tt.err)

			c.Assert(result.Skipped, qt.Equals, tt.wantSkipped)
			c.Assert(result.Success, qt.Equals, tt.wantSuccess)
			c.Assert(result.SkipReason, qt.Equals, tt.wantReason)
			c.Assert(result.Error, qt.Equals, tt.wantError)
		})
	}
}

// TestIsMySQLPrivilegeRefusal pins which server answers count as a refusal.
//
// The three accepted numbers are the ones the two setup statements can draw:
// 1227 for CREATE USER without the global privilege, and 1044 and 1142 for the
// GRANT without GRANT OPTION. Matching is by number because the message names
// the privilege, and MySQL and MariaDB word it differently.
//
// Error 1064 and the non-driver error are the controls. A syntax error and an
// arbitrary failure are defects the suite must keep reporting, so widening this
// to "anything the server refused" would turn real failures into quiet skips.
func TestIsMySQLPrivilegeRefusal(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "create user without the privilege",
			err:  &mysqldriver.MySQLError{Number: 1227, Message: "Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation"},
			want: true,
		},
		{
			name: "grant refused at the database",
			err:  &mysqldriver.MySQLError{Number: 1044, Message: "Access denied for user 'ptah_user'@'%' to database 'ptah_test'"},
			want: true,
		},
		{
			name: "grant refused at the table",
			err:  &mysqldriver.MySQLError{Number: 1142, Message: "GRANT command denied to user 'ptah_user'@'%'"},
			want: true,
		},
		{
			name: "wrapped refusal is still a refusal",
			err:  fmt.Errorf("create restricted account: %w", &mysqldriver.MySQLError{Number: 1227}),
			want: true,
		},
		{
			name: "a syntax error is a defect",
			err:  &mysqldriver.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"},
			want: false,
		},
		{
			name: "a non-driver error is a defect",
			err:  errors.New("connection reset by peer"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(isMySQLPrivilegeRefusal(tt.err), qt.Equals, tt.want)
		})
	}
}
