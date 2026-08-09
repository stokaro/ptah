package mysql

// White-box testing required: what is under test is how readIndexes assembles a
// key out of the information_schema.STATISTICS rows that describe it, and both
// halves of that -- the projection and the assembly -- are unexported. Reaching
// them through ReadSchema would mean scripting every other catalog query to
// observe this one, and the rows this scripts are the catalog's own, so the
// result is still measured against what MySQL reports rather than against an
// internal shape.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// statisticsColumns is the projection readIndexes selects, one row per key
// part.
var statisticsColumns = []string{
	"INDEX_NAME",
	"TABLE_NAME",
	"COLUMN_NAME",
	"NON_UNIQUE",
	"INDEX_TYPE",
}

// wideKeyColumnNames is a 16-part key of 64-character column names: 1039 bytes
// once joined with commas, past the 1024-byte group_concat_max_len MySQL 9.7
// defaults to. Read through GROUP_CONCAT the last name arrived truncated to
// `abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvw`, and the plan built a
// CREATE INDEX over a column the table does not have.
func wideKeyColumnNames() []string {
	names := make([]string, 0, 16)
	for part := 1; part <= 16; part++ {
		names = append(names, fmt.Sprintf(
			"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghij%02d",
			part,
		))
	}
	return names
}

func wideKeyRows() [][]driver.Value {
	rows := make([][]driver.Value, 0, 16)
	for _, name := range wideKeyColumnNames() {
		rows = append(rows, []driver.Value{"idx_wide", "wide", name, int64(1), "BTREE"})
	}
	return rows
}

// TestReadIndexes_AssemblesKeysFromTheirParts is stokaro/ptah#1245's second
// half: the key columns MySQL and MariaDB are compared on have to survive the
// read. Measured on MySQL 9.7.1 and MariaDB 11.8.8, replaying a database's own
// `schema inspect` output for the first two rows below ended in
// `Error 1072 (42000): Key column 'a' doesn't exist in table` where the pinned
// community binary v1.3.0 reported "Schema is synced".
func TestReadIndexes_AssemblesKeysFromTheirParts(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		rows   [][]driver.Value
		assert func(c *qt.C, indexes []types.DBIndex)
	}{
		{
			name: "column name containing a comma",
			rows: [][]driver.Value{
				{"idx_weird", "t2", "a,b", int64(1), "BTREE"},
			},
			assert: func(c *qt.C, indexes []types.DBIndex) {
				c.Assert(indexes, qt.HasLen, 1)
				c.Assert(indexes[0].Columns, qt.DeepEquals, []string{"a,b"})
				c.Assert(indexes[0].KeyPartsIncomplete, qt.IsFalse)
			},
		},
		{
			name: "sixteen part key past group_concat_max_len",
			rows: wideKeyRows(),
			assert: func(c *qt.C, indexes []types.DBIndex) {
				c.Assert(indexes, qt.HasLen, 1)
				c.Assert(indexes[0].Columns, qt.DeepEquals, wideKeyColumnNames())
				c.Assert(indexes[0].KeyPartsIncomplete, qt.IsFalse)
			},
		},
		{
			name: "key order follows the rows",
			rows: [][]driver.Value{
				{"idx_pair", "t", "b", int64(1), "BTREE"},
				{"idx_pair", "t", "a", int64(1), "BTREE"},
			},
			assert: func(c *qt.C, indexes []types.DBIndex) {
				c.Assert(indexes, qt.HasLen, 1)
				c.Assert(indexes[0].Columns, qt.DeepEquals, []string{"b", "a"})
			},
		},
		{
			name: "one index name per owning table",
			rows: [][]driver.Value{
				{"idx_name", "orders", "reference", int64(1), "BTREE"},
				{"idx_name", "users", "email", int64(0), "BTREE"},
			},
			assert: func(c *qt.C, indexes []types.DBIndex) {
				c.Assert(indexes, qt.HasLen, 2)
				c.Assert(indexes[0].TableName, qt.Equals, "orders")
				c.Assert(indexes[0].Columns, qt.DeepEquals, []string{"reference"})
				c.Assert(indexes[0].IsUnique, qt.IsFalse)
				c.Assert(indexes[1].TableName, qt.Equals, "users")
				c.Assert(indexes[1].Columns, qt.DeepEquals, []string{"email"})
				c.Assert(indexes[1].IsUnique, qt.IsTrue)
			},
		},
		{
			name: "unique key and its definition",
			rows: [][]driver.Value{
				{"uq_users_email", "users", "email", int64(0), "BTREE"},
			},
			assert: func(c *qt.C, indexes []types.DBIndex) {
				c.Assert(indexes, qt.HasLen, 1)
				c.Assert(indexes[0].IsUnique, qt.IsTrue)
				c.Assert(indexes[0].IsPrimary, qt.IsFalse)
				c.Assert(indexes[0].Definition, qt.Equals, "BTREE INDEX uq_users_email ON users (email)")
			},
		},
		{
			name: "primary key",
			rows: [][]driver.Value{
				{"PRIMARY", "users", "id", int64(0), "BTREE"},
			},
			assert: func(c *qt.C, indexes []types.DBIndex) {
				c.Assert(indexes, qt.HasLen, 1)
				c.Assert(indexes[0].IsPrimary, qt.IsTrue)
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			reader := NewMySQLReader(statisticsDB(c, test.rows).SQL, "app")

			indexes, err := reader.readIndexes("app")

			c.Assert(err, qt.IsNil)
			test.assert(c, indexes)
		})
	}
}

// TestReadIndexes_ReportsAKeyPartItCannotName covers a functional key part,
// whose STATISTICS row carries the expression and a NULL COLUMN_NAME. The read
// used to fail outright -- `converting NULL to string is unsupported`, exit 1
// on a MySQL 9.7.1 database the pinned community binary v1.3.0 reports synced
// -- because GROUP_CONCAT collapsed the whole row. The part is now reported as
// missing from Columns so a comparison can decline to read a partial key as a
// whole one.
func TestReadIndexes_ReportsAKeyPartItCannotName(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		rows   [][]driver.Value
		assert func(c *qt.C, indexes []types.DBIndex)
	}{
		{
			name: "whole key is an expression",
			rows: [][]driver.Value{
				{"idx_expr", "t3", nil, int64(1), "BTREE"},
			},
			assert: func(c *qt.C, indexes []types.DBIndex) {
				c.Assert(indexes, qt.HasLen, 1)
				c.Assert(indexes[0].Columns, qt.HasLen, 0)
				c.Assert(indexes[0].KeyPartsIncomplete, qt.IsTrue)
			},
		},
		{
			name: "column beside an expression",
			rows: [][]driver.Value{
				{"idx_mixed", "t4", "b", int64(1), "BTREE"},
				{"idx_mixed", "t4", nil, int64(1), "BTREE"},
			},
			assert: func(c *qt.C, indexes []types.DBIndex) {
				c.Assert(indexes, qt.HasLen, 1)
				c.Assert(indexes[0].Columns, qt.DeepEquals, []string{"b"})
				c.Assert(indexes[0].KeyPartsIncomplete, qt.IsTrue)
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			reader := NewMySQLReader(statisticsDB(c, test.rows).SQL, "app")

			indexes, err := reader.readIndexes("app")

			c.Assert(err, qt.IsNil)
			test.assert(c, indexes)
		})
	}
}

// statisticsDB answers the index query with rows and refuses every other query,
// so a projection change that stops selecting key parts fails here rather than
// silently reading something else.
func statisticsDB(c *qt.C, rows [][]driver.Value) *dbtest.DB {
	return dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		queried := strings.Contains(query, "FROM information_schema.STATISTICS") &&
			strings.Contains(query, "s.SEQ_IN_INDEX")
		c.Assert(queried, qt.IsTrue, qt.Commentf("query: %s", query))
		return dbtest.QueryResult{Columns: statisticsColumns, Rows: rows}, nil
	})
}
