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
	tests := []struct {
		name string
		rows [][]driver.Value
		// want is the whole read rather than the one field a row is about.
		// Assembly decides every field of every index it returns, so a row that
		// named only its own leaves the rest free to change unobserved.
		want []types.DBIndex
	}{
		{
			name: "column name containing a comma",
			rows: [][]driver.Value{
				{"idx_weird", "t2", "a,b", int64(1), "BTREE"},
			},
			want: []types.DBIndex{{
				Name:       "idx_weird",
				TableName:  "t2",
				Columns:    []string{"a,b"},
				Definition: "BTREE INDEX idx_weird ON t2 (a,b)",
			}},
		},
		{
			name: "sixteen part key past group_concat_max_len",
			rows: wideKeyRows(),
			want: []types.DBIndex{{
				Name:       "idx_wide",
				TableName:  "wide",
				Columns:    wideKeyColumnNames(),
				Definition: "BTREE INDEX idx_wide ON wide (" + strings.Join(wideKeyColumnNames(), ",") + ")",
			}},
		},
		{
			name: "key order follows the rows",
			rows: [][]driver.Value{
				{"idx_pair", "t", "b", int64(1), "BTREE"},
				{"idx_pair", "t", "a", int64(1), "BTREE"},
			},
			want: []types.DBIndex{{
				Name:       "idx_pair",
				TableName:  "t",
				Columns:    []string{"b", "a"},
				Definition: "BTREE INDEX idx_pair ON t (b,a)",
			}},
		},
		{
			name: "one index name per owning table",
			rows: [][]driver.Value{
				{"idx_name", "orders", "reference", int64(1), "BTREE"},
				{"idx_name", "users", "email", int64(0), "BTREE"},
			},
			want: []types.DBIndex{
				{
					Name:       "idx_name",
					TableName:  "orders",
					Columns:    []string{"reference"},
					Definition: "BTREE INDEX idx_name ON orders (reference)",
				},
				{
					Name:       "idx_name",
					TableName:  "users",
					Columns:    []string{"email"},
					IsUnique:   true,
					Definition: "BTREE INDEX idx_name ON users (email)",
				},
			},
		},
		{
			name: "unique key and its definition",
			rows: [][]driver.Value{
				{"uq_users_email", "users", "email", int64(0), "BTREE"},
			},
			want: []types.DBIndex{{
				Name:       "uq_users_email",
				TableName:  "users",
				Columns:    []string{"email"},
				IsUnique:   true,
				Definition: "BTREE INDEX uq_users_email ON users (email)",
			}},
		},
		{
			name: "primary key",
			rows: [][]driver.Value{
				{"PRIMARY", "users", "id", int64(0), "BTREE"},
			},
			want: []types.DBIndex{{
				Name:       "PRIMARY",
				TableName:  "users",
				Columns:    []string{"id"},
				IsUnique:   true,
				IsPrimary:  true,
				Definition: "BTREE INDEX PRIMARY ON users (id)",
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewMySQLReader(statisticsDB(c, test.rows).SQL, "app")

			indexes, err := reader.readIndexes("app")

			c.Assert(err, qt.IsNil)
			c.Assert(indexes, qt.DeepEquals, test.want)
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
	tests := []struct {
		name string
		rows [][]driver.Value
		want []types.DBIndex
	}{
		{
			name: "whole key is an expression",
			rows: [][]driver.Value{
				{"idx_expr", "t3", nil, int64(1), "BTREE"},
			},
			want: []types.DBIndex{{
				Name:               "idx_expr",
				TableName:          "t3",
				Definition:         "BTREE INDEX idx_expr ON t3 ()",
				KeyPartsIncomplete: true,
			}},
		},
		{
			name: "column beside an expression",
			rows: [][]driver.Value{
				{"idx_mixed", "t4", "b", int64(1), "BTREE"},
				{"idx_mixed", "t4", nil, int64(1), "BTREE"},
			},
			want: []types.DBIndex{{
				Name:               "idx_mixed",
				TableName:          "t4",
				Columns:            []string{"b"},
				Definition:         "BTREE INDEX idx_mixed ON t4 (b)",
				KeyPartsIncomplete: true,
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewMySQLReader(statisticsDB(c, test.rows).SQL, "app")

			indexes, err := reader.readIndexes("app")

			c.Assert(err, qt.IsNil)
			c.Assert(indexes, qt.DeepEquals, test.want)
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

// TestReadFunctionsCarriesReplacementOwnershipFacts pins the two catalog facts
// the comparator needs before it may replace a SQL SECURITY DEFINER routine.
// DEFINER names the existing routine owner; CURRENT_USER() names the account
// whose privileges a replacement CREATE would use.
func TestReadFunctionsCarriesReplacementOwnershipFacts(t *testing.T) {
	c := qt.New(t)
	db := functionOwnershipDB(c)
	reader := NewMySQLReader(db.SQL, "app")

	functions, err := reader.readFunctions("app")

	c.Assert(err, qt.IsNil)
	c.Assert(functions, qt.HasLen, 1)
	c.Assert(functions[0].Definer, qt.Equals, "owner_a@%")
	c.Assert(functions[0].CurrentAccount, qt.Equals, "migrator_a@%")
}

func functionOwnershipDB(c *qt.C) *dbtest.DB {
	c.Helper()
	return dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "FROM information_schema.PARAMETERS"):
			return dbtest.QueryResult{
				Columns: []string{"ROUTINE_TYPE", "SPECIFIC_NAME", "PARAMETER_NAME", "DTD_IDENTIFIER"},
			}, nil
		case strings.Contains(query, "FROM information_schema.ROUTINES"):
			c.Assert(query, qt.Contains, "DEFINER")
			c.Assert(query, qt.Contains, "CURRENT_USER()")
			return dbtest.QueryResult{
				Columns: []string{
					"ROUTINE_NAME", "DTD_IDENTIFIER", "IS_DETERMINISTIC",
					"SQL_DATA_ACCESS", "SECURITY_TYPE", "DEFINER", "CURRENT_USER()",
					"ROUTINE_DEFINITION", "ROUTINE_COMMENT", "ROUTINE_TYPE",
				},
				Rows: [][]driver.Value{{
					"f", "int", "YES", "NO SQL", "DEFINER", "owner_a@%",
					"migrator_a@%", "RETURN 1", "owned function", "FUNCTION",
				}},
			}, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
		}
	})
}

// enumColumnsColumns is the projection readEnums selects, one row per
// enum-typed column.
var enumColumnsColumns = []string{
	"TABLE_NAME",
	"COLUMN_NAME",
	"COLUMN_TYPE",
}

// TestReadEnums_NamesEachOneAfterItsColumn covers stokaro/ptah#1716's second
// half.
//
// MySQL has no enum type in its catalog: an enum is a COLUMN whose type carries
// a value list. The read used to name each one after those values --
// `enum_active_inactive` -- which made the identity a function of the thing most
// likely to change. Adding one value renamed the declaration `schema inspect`
// prints and, through `introspect`, the generated Go type and every constant:
// EnumActiveInactive became EnumActiveInactiveArchived and EnumActiveInactiveActive
// became EnumActiveInactiveArchivedActive, so an author who had committed those
// models got a rename across their code for adding a value.
//
// The rows below are what the two behaviors disagree about. The same value list
// on two columns is TWO enums here, because the engine has no shared type for
// them to be; naming by values collapsed them into one and asserted a
// relationship the database does not record.
func TestReadEnums_NamesEachOneAfterItsColumn(t *testing.T) {
	tests := []struct {
		name string
		rows [][]driver.Value
		want []types.DBEnum
	}{
		{
			name: "the name is the column, not the values",
			rows: [][]driver.Value{
				{"users", "state", "enum('active','inactive')"},
			},
			want: []types.DBEnum{{Name: "users_state", Values: []string{"active", "inactive"}}},
		},
		{
			name: "adding a value leaves the name alone",
			rows: [][]driver.Value{
				{"users", "state", "enum('active','inactive','archived')"},
			},
			want: []types.DBEnum{{Name: "users_state", Values: []string{"active", "inactive", "archived"}}},
		},
		{
			name: "one value list on two columns is two enums",
			rows: [][]driver.Value{
				{"orders", "state", "enum('open','closed')"},
				{"tickets", "state", "enum('open','closed')"},
			},
			want: []types.DBEnum{
				{Name: "orders_state", Values: []string{"open", "closed"}},
				{Name: "tickets_state", Values: []string{"open", "closed"}},
			},
		},
		{
			name: "two columns of one table stay apart",
			rows: [][]driver.Value{
				{"t", "kind", "enum('a','b')"},
				{"t", "level", "enum('low','high')"},
			},
			want: []types.DBEnum{
				{Name: "t_kind", Values: []string{"a", "b"}},
				{Name: "t_level", Values: []string{"low", "high"}},
			},
		},
		{
			name: "a type carrying no values is skipped rather than named",
			rows: [][]driver.Value{
				{"t", "broken", "enum()"},
				{"t", "kind", "enum('a')"},
			},
			want: []types.DBEnum{{Name: "t_kind", Values: []string{"a"}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewMySQLReader(enumColumnsDB(c, test.rows).SQL, "app")

			enums, err := reader.readEnums("app")

			c.Assert(err, qt.IsNil)
			c.Assert(enums, qt.DeepEquals, test.want)
		})
	}
}

// enumColumnsDB scripts the information_schema.COLUMNS read readEnums makes,
// asserting the projection carries the column identity rather than the type
// alone -- without TABLE_NAME and COLUMN_NAME there is nothing to name an enum
// after, so a read that stopped selecting them would fall back to the values.
func enumColumnsDB(c *qt.C, rows [][]driver.Value) *dbtest.DB {
	return dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		queried := strings.Contains(query, "FROM information_schema.COLUMNS") &&
			strings.Contains(query, "TABLE_NAME") &&
			strings.Contains(query, "COLUMN_NAME") &&
			strings.Contains(query, "DATA_TYPE = 'enum'")
		c.Assert(queried, qt.IsTrue, qt.Commentf("query: %s", query))
		return dbtest.QueryResult{Columns: enumColumnsColumns, Rows: rows}, nil
	})
}
