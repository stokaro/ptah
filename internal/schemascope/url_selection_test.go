package schemascope_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemascope"
)

func TestRefuseOutsideURLDatabase_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		address string
		schemas []string
	}{
		{name: "no selection", address: "mysql://u@localhost/m1"},
		{name: "matching database", address: "mysql://u@localhost/m1", schemas: []string{"m1"}},
		{name: "surrounding URL whitespace", address: " mysql://u@localhost/m1 ", schemas: []string{"m1"}},
		{name: "repeated database", address: "mysql://u@localhost/m1", schemas: []string{"m1", "m1"}},
		{name: "MariaDB alias", address: "maria://u@localhost/m1", schemas: []string{"m1"}},
		{name: "driver TCP", address: "mysql://u@tcp(localhost:3306)/m1", schemas: []string{"m1"}},
		{name: "driver socket", address: "mariadb://u@unix(/tmp/mysql.sock)/m1", schemas: []string{"m1"}},
		{name: "socket query database", address: "mysql+unix://u@/tmp/mysql.sock?database=m1", schemas: []string{"m1"}},
		{name: "escaped database", address: "mysql://u@localhost/a%20b", schemas: []string{"a b"}},
		{name: "comma is identifier data", address: "maria+unix://u@/tmp/mysql.sock?database=m1%2Cm2", schemas: []string{"m1,m2"}},
		{name: "whole MySQL server", address: "mysql://u@localhost/", schemas: []string{"m1", "m2"}},
		{name: "whole MariaDB server", address: "mariadb://u@localhost", schemas: []string{"m1", "m2"}},
		{name: "whole socket server", address: "mariadb+unix://u@/tmp/mysql.sock", schemas: []string{"m1", "m2"}},
		{name: "PostgreSQL schemas", address: "postgres://u@localhost/db", schemas: []string{"app", "crm"}},
		{name: "SQL Server schemas", address: "sqlserver://u@localhost?database=db", schemas: []string{"app", "crm"}},
		{name: "local schema file", address: "file://schema.sql", schemas: []string{"m1", "m2"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemascope.RefuseOutsideURLDatabase(test.address, test.schemas)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestRefuseOutsideURLDatabase_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		address string
		schemas []string
		outside string
	}{
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "additional database", address: "mysql://u:secret@localhost/m1", schemas: []string{"m1", "m2"}, outside: "m2"},
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "surrounding URL whitespace", address: " mysql://u:secret@localhost/m1 ", schemas: []string{"m1", "m2"}, outside: "m2"},
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "only another database", address: "mysql://u:secret@localhost/m1", schemas: []string{"m2"}, outside: "m2"},
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "MariaDB", address: "mariadb://u:secret@localhost/m1", schemas: []string{"m1", "m2"}, outside: "m2"},
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "MariaDB alias", address: "maria://u:secret@localhost/m1", schemas: []string{"m1", "m2"}, outside: "m2"},
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "driver TCP", address: "mysql://u:secret@tcp(localhost:3306)/m1", schemas: []string{"m1", "m2"}, outside: "m2"},
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "driver socket", address: "mariadb://u:secret@unix(/tmp/mysql.sock)/m1", schemas: []string{"m1", "m2"}, outside: "m2"},
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "socket database", address: "mysql+unix://u:secret@/tmp/mysql.sock?database=m1", schemas: []string{"m1", "m2"}, outside: "m2"},
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "MariaDB socket alias", address: "maria+unix://u:secret@/tmp/mysql.sock?database=m1", schemas: []string{"m2"}, outside: "m2"},
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "case is unknown before connecting", address: "mysql://u:secret@localhost/m1", schemas: []string{"M1"}, outside: "M1"},
		// #nosec G101 -- synthetic password verifies redaction; no connection is opened.
		{name: "space belongs to the name", address: "mysql://u:secret@localhost/m1", schemas: []string{" m1"}, outside: " m1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemascope.RefuseOutsideURLDatabase(test.address, test.schemas)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`schema selection names database "`+test.outside+
				`", but the URL is limited to database "m1"; remove the database from the URL for a whole-server command, or select only "m1" in the URL's exact spelling`))
			c.Assert(err.Error(), qt.Not(qt.Contains), "secret")
		})
	}
}
