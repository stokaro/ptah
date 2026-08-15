package mysql

// White-box testing required: describeRoutinePrivilegeRefusal is unexported and
// its only exported callers are the two ExecuteSQL methods, which need a live
// server refusing a real CREATE FUNCTION to reach it. The behavior under test is
// the classification of a driver error and the sentence it produces, so it is
// driven directly rather than through a database.

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestDescribeRoutinePrivilegeRefusal pins which error is rewritten and what the
// replacement says.
//
// Error 1419 is a deployment gate, not a statement defect. Measured on MySQL
// 26.7.0 with the pinned image's defaults and a user holding ALL PRIVILEGES on
// its own database but only USAGE globally, a function declared DETERMINISTIC
// and one declared READS SQL DATA are both refused with it, while the same
// user's CREATE TABLE succeeds. No rendering answers it, so the operator has to
// be told what does.
//
// The row for 1418 is the control that keeps this narrow: the characteristic
// gate IS answerable by rendering, and the renderer already answers it, so
// dressing it up in a privilege message would hide a rendering regression
// behind friendly text. Anything that is not 1419 must fall through untouched.
func TestDescribeRoutinePrivilegeRefusal(t *testing.T) {
	const statement = "CREATE FUNCTION `f1`() RETURNS int DETERMINISTIC RETURN 1"

	tests := []struct {
		name    string
		err     error
		rewrite bool
	}{
		{
			name:    "1419 privilege gate is rewritten",
			err:     &mysqldriver.MySQLError{Number: 1419, Message: "You do not have the SUPER privilege and binary logging is enabled"},
			rewrite: true,
		},
		{
			name:    "1418 characteristic gate is left alone",
			err:     &mysqldriver.MySQLError{Number: 1418, Message: "This function has none of DETERMINISTIC, NO SQL, or READS SQL DATA"},
			rewrite: false,
		},
		{
			name:    "an unrelated MySQL error is left alone",
			err:     &mysqldriver.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"},
			rewrite: false,
		},
		{
			name:    "a non-driver error is left alone",
			err:     errors.New("dial tcp: connection refused"),
			rewrite: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := describeRoutinePrivilegeRefusal(test.err, statement)

			c.Check(got != nil, qt.Equals, test.rewrite)
		})
	}

	t.Run("the rewritten message is actionable and keeps the cause", func(t *testing.T) {
		c := qt.New(t)
		got := describeRoutinePrivilegeRefusal(
			&mysqldriver.MySQLError{Number: 1419, Message: "You do not have the SUPER privilege and binary logging is enabled"},
			statement,
		)

		c.Assert(got, qt.IsNotNil)
		// It says what Ptah cannot do here, rather than naming a MySQL internal.
		c.Check(got.Error(), qt.Contains, "does not permit Ptah to manage stored functions")
		// It names the privilege and a remedy the operator controls.
		c.Check(got.Error(), qt.Contains, "SUPER privilege")
		c.Check(got.Error(), qt.Contains, "Grant SUPER to the migrating user")
		// It does not recommend the variable MySQL itself calls less safe.
		c.Check(got.Error(), qt.Contains, "Ptah does not suggest it")
		// The driver error is still wrapped, so errors.As keeps working.
		c.Check(got.Error(), qt.Contains, statement)
		var mysqlErr *mysqldriver.MySQLError
		c.Check(got, qt.ErrorAs, &mysqlErr)
	})
}
