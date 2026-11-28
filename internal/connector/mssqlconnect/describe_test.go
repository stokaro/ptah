package mssqlconnect_test

import (
	"database/sql/driver"
	"errors"
	"net/url"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/connector"
	"ptah.run/internal/connector/mssqlconnect"
	"ptah.run/internal/dbschema/dbtest"
)

// describeAnswers is a server whose connection's user has defaultSchema as
// its default schema, nil answering NULL, and which answers namedIsDefault
// when asked whether the schema the URL names is that one. A non-nil
// schemaErr is what the server answers when asked for them instead.
func describeAnswers(defaultSchema, namedIsDefault driver.Value, schemaErr error) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "@@VERSION"):
			return dbtest.QueryResult{Columns: []string{"version"}, Rows: [][]driver.Value{{"Microsoft SQL Server 2025"}}}, nil
		case strings.Contains(query, "CATALOG_DEFAULT"):
			return dbtest.QueryResult{Columns: []string{"collation"}, Rows: [][]driver.Value{{"SQL_Latin1_General_CP1_CI_AS"}}}, nil
		case strings.Contains(query, "SCHEMA_NAME()"):
			return dbtest.QueryResult{Columns: []string{"schema", "named_is_default"}, Rows: [][]driver.Value{{defaultSchema, namedIsDefault}}}, schemaErr
		}
		return dbtest.QueryResult{}, errors.New("unexpected query: " + query)
	}
}

// TestDescribe_Schema_HappyPath pins the schema a SQL Server connection works
// in: the one the URL's schema parameter names, or else the default schema of
// the connection's user, where the server creates a name written without a
// schema. A user the server gives no default schema is in dbo.
//
// It pins the metadata schema beside it, where Ptah keeps its own tables: the
// schema the URL names, or else dbo, whatever the user's default schema is.
// A database migrated as sa keeps its history in dbo, and a user whose default
// schema is app must read that history rather than an empty one in app.
//
// And it pins the schema the server creates an unqualified name in, where a
// URL names another one than the user's default schema, as the server
// compares the two: empty where there is no parameter or it names that
// schema.
func TestDescribe_Schema_HappyPath(t *testing.T) {
	tests := []struct {
		name            string
		url             string
		defaultSchema   driver.Value
		namedIsDefault  bool
		want            string
		wantMetadata    string
		wantUnqualified string
	}{
		{"the user's default schema", "sqlserver://app@db:1433?database=shop", "app", false, "app", "dbo", ""},
		{"dbo for a database owner", "sqlserver://sa@db:1433?database=shop", "dbo", false, "dbo", "dbo", ""},
		{"dbo where the server answers none", "sqlserver://sa@db:1433?database=shop", nil, false, "dbo", "dbo", ""},
		{"the schema the URL names", "sqlserver://app@db:1433?database=shop&schema=sales", "app", false, "sales", "sales", "app"},
		{"dbo the URL names for a user with none", "sqlserver://sa@db:1433?database=shop&schema=sales", nil, false, "sales", "sales", "dbo"},
		{"the user's default schema the URL names", "sqlserver://app@db:1433?database=shop&schema=APP", "app", true, "APP", "APP", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, describeAnswers(test.defaultSchema, test.namedIsDefault, nil))
			parsed, err := url.Parse(test.url)
			c.Assert(err, qt.IsNil)

			info, err := mssqlconnect.Service{}.Describe(c.Context(), &connector.Pool{DB: db.SQL}, connector.Request{URL: test.url, Parsed: parsed}, catalog.ServerInfo{})

			c.Assert(err, qt.IsNil)
			c.Assert(info.Schema, qt.Equals, test.want)
			c.Assert(info.IdentifierSemantics.DefaultSchema, qt.Equals, test.want)
			c.Assert(info.MetadataSchema, qt.Equals, test.wantMetadata)
			c.Assert(info.UnqualifiedSchema, qt.Equals, test.wantUnqualified)
		})
	}
}

// TestDescribe_Schema_FailurePath pins that a default schema the server does
// not answer fails the connection rather than falling back to dbo, where the
// plan would be compared in a schema its names do not land in.
func TestDescribe_Schema_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, describeAnswers("app", false, errors.New("permission denied")))
	parsed, err := url.Parse("sqlserver://app@db:1433?database=shop")
	c.Assert(err, qt.IsNil)

	info, err := mssqlconnect.Service{}.Describe(c.Context(), &connector.Pool{DB: db.SQL}, connector.Request{Parsed: parsed}, catalog.ServerInfo{})

	c.Assert(err, qt.ErrorMatches, `failed to get the SQL Server default schema: .*permission denied`)
	c.Assert(info.Schema, qt.Equals, "")
}
