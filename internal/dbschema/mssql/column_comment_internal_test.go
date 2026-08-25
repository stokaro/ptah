package mssql

// White-box testing required: the column read is package-local and its scan is
// not reachable through an exported API.

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// A column's MS_Description reaches the schema -- stokaro/ptah#2168.
//
// The query has always asked for it. There was nowhere to put the value until
// types.DBColumn gained a Comment, so the scan read it and threw it away:
//
//	if comment.Valid {
//	    _ = comment.String
//	}
//
// Discarding it meant every column comment read as absent, so the comparison
// saw one being added where one already existed, and SQL Server answers
// `Property cannot be added. Property already exists` to that.
func TestReadColumnsByTable_CarriesTheColumnComment(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringColumnComments)
	reader := NewSQLServerReader(db.SQL, "dbo")

	columns, err := reader.readColumnsByTable()

	c.Assert(err, qt.IsNil)
	byTable := columns[catalogTableKey{schema: "dbo", table: "users"}]
	c.Assert(byTable, qt.HasLen, 2)
	c.Assert(byTable[0].Comment, qt.Equals, "")
	c.Assert(byTable[1].Comment, qt.Equals, "login address")
}

// answeringColumnComments scripts the column read, and refuses a projection
// that does not ask for what it returns.
//
// A fake that answers a fixed set of columns whatever the query selects cannot
// tell a reader that stopped asking for MS_Description from one that still
// does.
func answeringColumnComments(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "MS_Description") {
		return dbtest.QueryResult{}, errUnaskedDescription
	}
	return dbtest.QueryResult{
		Columns: []string{
			"schema_name", "table_name", "column_name", "type_name",
			"max_length", "precision", "scale", "is_nullable",
			"is_identity", "ident_seed", "ident_incr", "column_id",
			"default_definition", "generated_expression", "is_persisted", "comment",
		},
		Rows: [][]driver.Value{
			{"dbo", "users", "id", "int",
				int64(4), int64(10), int64(0), false,
				int64(1), int64(1), int64(1), int64(1),
				nil, nil, nil, nil},
			{"dbo", "users", "email", "nvarchar",
				int64(510), int64(0), int64(0), false,
				int64(0), nil, nil, int64(2),
				nil, nil, nil, "login address"},
		},
	}, nil
}

var errUnaskedDescription = errors.New("column query does not ask for MS_Description")
