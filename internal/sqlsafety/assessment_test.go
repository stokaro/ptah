package sqlsafety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/sqlsafety"
)

func TestSQLForAssessment_ExpandsExecutableComments(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		sql     string
		want    string
	}{
		{name: "mysql", dialect: "mysql", sql: "/*! DROP TABLE users */", want: "  DROP TABLE users  "},
		{name: "mysql version guard", dialect: "mysql", sql: "/*!50700 DROP TABLE users */", want: "  DROP TABLE users  "},
		{name: "mariadb", dialect: "mariadb", sql: "/*M! DROP TABLE users */", want: "  DROP TABLE users  "},
		{name: "mariadb version guard", dialect: "mariadb", sql: "/*M!100100 DROP TABLE users */", want: "  DROP TABLE users  "},
		{name: "mysql ignores mariadb comment", dialect: "mysql", sql: "/*M! DROP TABLE users */ SELECT 1", want: "  SELECT 1"},
		{name: "mariadb ignores mysql 50700 range", dialect: "mariadb", sql: "/*!50700 DROP TABLE users */ SELECT 1", want: "  SELECT 1"},
		{name: "ordinary comment", dialect: "mysql", sql: "/* DROP TABLE users */ SELECT 1", want: "  SELECT 1"},
		{name: "ordinary comment separates tokens", dialect: "mysql", sql: "DROP/* reason */TABLE users", want: "DROP TABLE users"},
		{name: "other dialect", dialect: "postgres", sql: "/*! DROP TABLE users */ SELECT 1", want: "  SELECT 1"},
		{name: "string literal", dialect: "mysql", sql: "SELECT '/*! DROP TABLE users */'", want: "SELECT '/*! DROP TABLE users */'"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got := sqlsafety.SQLForAssessment(test.sql, test.dialect)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
