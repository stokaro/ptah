package mysqlconnect_test

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/internal/connector/mysqlconnect"
)

// Each message is one a server wrote, measured on MySQL 8.4 and MariaDB 11.8
// under a strict session, for a change a loose session makes to the rows. The
// column names hold a quote and a backtick, which neither server escapes.
func TestStoredValueRefusal_NamesTheColumn(t *testing.T) {
	tests := []struct {
		name    string
		number  uint16
		message string
		want    string
	}{
		{name: "a removed ENUM member", number: 1265, message: "Data truncated for column 'n' at row 1", want: "n"},
		{name: "a value out of range", number: 1264, message: "Out of range value for column 'n' at row 1", want: "n"},
		{name: "a string too long", number: 1406, message: "Data too long for column 'n' at row 1", want: "n"},
		{name: "a string that is no integer", number: 1366, message: "Incorrect integer value: 'abc' for column 'n' at row 1", want: "n"},
		{name: "a name with quotes on MySQL", number: 1292, message: "Incorrect date value: 'nope' for column 'a'b`c' at row 1", want: "a'b`c"},
		{name: "a qualified name on MariaDB", number: 1292, message: "Incorrect datetime value: 'nope' for column `app`.`w`.`n` at row 1", want: "n"},
		{name: "a qualified name with quotes on MariaDB", number: 1292, message: "Incorrect datetime value: 'nope' for column `app`.`w`.`a'b`c` at row 1", want: "a'b`c"},
		{name: "a value that names another column", number: 1366, message: "Incorrect integer value: 'x for column y at row 2' for column 'n' at row 1", want: "n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := fmt.Errorf("execute statement: %w", &mysqldriver.MySQLError{Number: test.number, Message: test.message})

			column, refused := mysqlconnect.Service{}.StoredValueRefusal(err)

			c.Assert(refused, qt.IsTrue)
			c.Assert(column, qt.Equals, test.want)
		})
	}
}

// A refusal whose message names no column is still a refusal: MySQL refuses a
// NULL in a column that turns NOT NULL, and MariaDB a string that is no
// integer, without naming one. A message whose column the reader cannot
// find names none either.
func TestStoredValueRefusal_NamesNoColumn(t *testing.T) {
	tests := []struct {
		name    string
		number  uint16
		message string
	}{
		{name: "a NULL in a NOT NULL column on MySQL", number: 1138, message: "Invalid use of NULL value"},
		{name: "a string that is no integer on MariaDB", number: 1292, message: "Truncated incorrect INTEGER value: 'abc'"},
		{name: "a column too short to quote", number: 1265, message: "Data truncated for column ' at row 1"},
		{name: "a column with no row after it", number: 1265, message: "Data truncated for column 'n'"},
		{name: "a column with no quotes", number: 1265, message: "Data truncated for column n at row 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			column, refused := mysqlconnect.Service{}.StoredValueRefusal(&mysqldriver.MySQLError{Number: test.number, Message: test.message})

			c.Assert(refused, qt.IsTrue)
			c.Assert(column, qt.Equals, "")
		})
	}
}

// The control: an error the server gives for any other reason, and an error
// that is not the server's, is no refusal to rewrite stored values.
func TestStoredValueRefusal_OtherErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "a duplicate key", err: &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry '1' for key 'c.PRIMARY'"}},
		{name: "a client error", err: errors.New("Data truncated for column 'n' at row 1")},
		{name: "no error", err: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			column, refused := mysqlconnect.Service{}.StoredValueRefusal(test.err)

			c.Assert(refused, qt.IsFalse)
			c.Assert(column, qt.Equals, "")
		})
	}
}
