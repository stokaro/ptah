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

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
)

// statisticsColumns is the projection readIndexes selects, one row per key
// part.
var statisticsColumns = []string{
	"INDEX_NAME",
	"TABLE_NAME",
	"COLUMN_NAME",
	"NON_UNIQUE",
	"INDEX_TYPE",
	"SUB_PART",
	"COLLATION",
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
		rows = append(rows, []driver.Value{"idx_wide", "wide", name, int64(1), "BTREE", nil, "A"})
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
		want []catalog.Index
	}{
		{
			// The access method the server chose, in the structured field
			// rather than only inside the definition string. A comparison
			// reading the string cannot tell SPATIAL from BTREE without
			// parsing DDL, and one that read neither reported a table with a
			// BTREE index as satisfying a desired SPATIAL one
			// (stokaro/ptah#2721).
			name: "a spatial index keeps its access method",
			rows: [][]driver.Value{
				{"sx_geo_location", "geo", "location", int64(1), "SPATIAL", nil, "A"},
			},
			want: []catalog.Index{{
				Name:       "sx_geo_location",
				TableName:  "geo",
				Method:     "SPATIAL",
				Columns:    []string{"location"},
				Definition: "SPATIAL INDEX sx_geo_location ON geo (location)",
			}},
		},
		{
			name: "column name containing a comma",
			rows: [][]driver.Value{
				{"idx_weird", "t2", "a,b", int64(1), "BTREE"},
			},
			want: []catalog.Index{{
				Name:       "idx_weird",
				TableName:  "t2",
				Method:     "BTREE",
				Columns:    []string{"a,b"},
				Definition: "BTREE INDEX idx_weird ON t2 (a,b)",
			}},
		},
		{
			name: "sixteen part key past group_concat_max_len",
			rows: wideKeyRows(),
			want: []catalog.Index{{
				Name:       "idx_wide",
				TableName:  "wide",
				Method:     "BTREE",
				Columns:    wideKeyColumnNames(),
				Definition: "BTREE INDEX idx_wide ON wide (" + strings.Join(wideKeyColumnNames(), ",") + ")",
			}},
		},
		{
			name: "key order follows the rows",
			rows: [][]driver.Value{
				{"idx_pair", "t", "b", int64(1), "BTREE", nil, "A"},
				{"idx_pair", "t", "a", int64(1), "BTREE", nil, "A"},
			},
			want: []catalog.Index{{
				Name:       "idx_pair",
				TableName:  "t",
				Method:     "BTREE",
				Columns:    []string{"b", "a"},
				Definition: "BTREE INDEX idx_pair ON t (b,a)",
			}},
		},
		{
			// MySQL requires a length for an index on a BLOB or TEXT column,
			// so a key that loses it produces a description the server refuses
			// with `used in key specification without a key length`
			// (stokaro/ptah#2112). SUB_PART is where the catalog keeps it.
			name: "a prefix key carries its length",
			rows: [][]driver.Value{
				{"idx_notes", "orders", "notes", int64(1), "BTREE", int64(20), "A"},
			},
			want: []catalog.Index{{
				Name:       "idx_notes",
				TableName:  "orders",
				Method:     "BTREE",
				Columns:    []string{"notes"},
				Parts:      []catalog.IndexPart{{Name: "notes", Prefix: "20"}},
				Definition: "BTREE INDEX idx_notes ON orders (notes)",
			}},
		},
		{
			// The control. A whole-column key says nothing an `on` block would
			// carry, so Parts stays empty and the compact `columns = [...]`
			// spelling is what the document gets.
			name: "a whole-column key carries no parts",
			rows: [][]driver.Value{
				{"idx_plain", "orders", "customer_id", int64(1), "BTREE", nil, "A"},
			},
			want: []catalog.Index{{
				Name:       "idx_plain",
				TableName:  "orders",
				Method:     "BTREE",
				Columns:    []string{"customer_id"},
				Definition: "BTREE INDEX idx_plain ON orders (customer_id)",
			}},
		},
		{
			// A mixed key keeps every part, because dropping the unprefixed
			// ones would render the key over one column.
			name: "a mixed key keeps both kinds in order",
			rows: [][]driver.Value{
				{"idx_mixed", "orders", "customer_id", int64(1), "BTREE", nil, "A"},
				{"idx_mixed", "orders", "notes", int64(1), "BTREE", int64(20), "A"},
			},
			want: []catalog.Index{{
				Name:      "idx_mixed",
				TableName: "orders",
				Method:    "BTREE",
				Columns:   []string{"customer_id", "notes"},
				Parts: []catalog.IndexPart{
					{Name: "customer_id"},
					{Name: "notes", Prefix: "20"},
				},
				Definition: "BTREE INDEX idx_mixed ON orders (customer_id,notes)",
			}},
		},
		{
			name: "one index name per owning table",
			rows: [][]driver.Value{
				{"idx_name", "orders", "reference", int64(1), "BTREE", nil, "A"},
				{"idx_name", "users", "email", int64(0), "BTREE", nil, "A"},
			},
			want: []catalog.Index{
				{
					Name:       "idx_name",
					TableName:  "orders",
					Method:     "BTREE",
					Columns:    []string{"reference"},
					Definition: "BTREE INDEX idx_name ON orders (reference)",
				},
				{
					Name:       "idx_name",
					TableName:  "users",
					Method:     "BTREE",
					Columns:    []string{"email"},
					IsUnique:   true,
					Definition: "BTREE INDEX idx_name ON users (email)",
				},
			},
		},
		{
			name: "unique key and its definition",
			rows: [][]driver.Value{
				{"uq_users_email", "users", "email", int64(0), "BTREE", nil, "A"},
			},
			want: []catalog.Index{{
				Name:       "uq_users_email",
				TableName:  "users",
				Method:     "BTREE",
				Columns:    []string{"email"},
				IsUnique:   true,
				Definition: "BTREE INDEX uq_users_email ON users (email)",
			}},
		},
		{
			name: "primary key",
			rows: [][]driver.Value{
				{"PRIMARY", "users", "id", int64(0), "BTREE", nil, "A"},
			},
			want: []catalog.Index{{
				Name:       "PRIMARY",
				TableName:  "users",
				Method:     "BTREE",
				Columns:    []string{"id"},
				IsUnique:   true,
				IsPrimary:  true,
				Definition: "BTREE INDEX PRIMARY ON users (id)",
			}},
		},
		{
			// COLLATION is the catalog's answer, 'A' or 'D'. KEY (a DESC) and
			// KEY (a) are different indexes on both engines, so a read that
			// discarded this could not tell a declaration that changed
			// direction from one that did not (stokaro/ptah#2816).
			name: "a descending key part carries its direction",
			rows: [][]driver.Value{
				{"idx_desc", "t", "a", int64(1), "BTREE", nil, "D"},
			},
			want: []catalog.Index{{
				Name:       "idx_desc",
				TableName:  "t",
				Method:     "BTREE",
				Columns:    []string{"a"},
				Parts:      []catalog.IndexPart{{Name: "a", Desc: true}},
				Definition: "BTREE INDEX idx_desc ON t (a)",
			}},
		},
		{
			// The control. An ascending key says nothing its column names do
			// not, so its parts are dropped -- and an assertion that only
			// checked the descending row would pass just as well if every part
			// were marked descending.
			name: "an ascending key part is not reported descending",
			rows: [][]driver.Value{
				{"idx_asc", "t", "a", int64(1), "BTREE", nil, "A"},
			},
			want: []catalog.Index{{
				Name:       "idx_asc",
				TableName:  "t",
				Method:     "BTREE",
				Columns:    []string{"a"},
				Definition: "BTREE INDEX idx_asc ON t (a)",
			}},
		},
		{
			// A key with no order to report at all: both engines leave
			// COLLATION null for a FULLTEXT key.
			name: "a key the catalog gives no direction is ascending",
			rows: [][]driver.Value{
				{"ft", "t", "bio", int64(1), "FULLTEXT", nil, nil},
			},
			want: []catalog.Index{{
				Name:       "ft",
				TableName:  "t",
				Method:     "FULLTEXT",
				Columns:    []string{"bio"},
				Definition: "FULLTEXT INDEX ft ON t (bio)",
			}},
		},
		{
			// Direction travels per part, beside the prefix length, so a key
			// mixing them keeps both facts in order.
			name: "direction and prefix travel together, per part",
			rows: [][]driver.Value{
				{"idx_mix", "t", "a", int64(1), "BTREE", nil, "D"},
				{"idx_mix", "t", "b", int64(1), "BTREE", int64(7), "A"},
			},
			want: []catalog.Index{{
				Name:      "idx_mix",
				TableName: "t",
				Method:    "BTREE",
				Columns:   []string{"a", "b"},
				Parts: []catalog.IndexPart{
					{Name: "a", Desc: true},
					{Name: "b", Prefix: "7"},
				},
				Definition: "BTREE INDEX idx_mix ON t (a,b)",
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewMySQLReader(statisticsDB(c, test.rows).SQL, "app")

			indexes, err := reader.readIndexes(t.Context(), "app")

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
		want []catalog.Index
	}{
		{
			name: "whole key is an expression",
			rows: [][]driver.Value{
				{"idx_expr", "t3", nil, int64(1), "BTREE", nil, "A"},
			},
			want: []catalog.Index{{
				Name:               "idx_expr",
				TableName:          "t3",
				Method:             "BTREE",
				Definition:         "BTREE INDEX idx_expr ON t3 ()",
				KeyPartsIncomplete: true,
			}},
		},
		{
			name: "column beside an expression",
			rows: [][]driver.Value{
				{"idx_mixed", "t4", "b", int64(1), "BTREE", nil, "A"},
				{"idx_mixed", "t4", nil, int64(1), "BTREE", nil, "A"},
			},
			want: []catalog.Index{{
				Name:               "idx_mixed",
				TableName:          "t4",
				Method:             "BTREE",
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

			indexes, err := reader.readIndexes(t.Context(), "app")

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

	functions, err := reader.readFunctions(t.Context(), "app")

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
		want []catalog.Enum
	}{
		{
			name: "the name is the column, not the values",
			rows: [][]driver.Value{
				{"users", "state", "enum('active','inactive')"},
			},
			want: []catalog.Enum{{Name: "users_state", Values: []string{"active", "inactive"}}},
		},
		{
			name: "adding a value leaves the name alone",
			rows: [][]driver.Value{
				{"users", "state", "enum('active','inactive','archived')"},
			},
			want: []catalog.Enum{{Name: "users_state", Values: []string{"active", "inactive", "archived"}}},
		},
		{
			name: "one value list on two columns is two enums",
			rows: [][]driver.Value{
				{"orders", "state", "enum('open','closed')"},
				{"tickets", "state", "enum('open','closed')"},
			},
			want: []catalog.Enum{
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
			want: []catalog.Enum{
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
			want: []catalog.Enum{{Name: "t_kind", Values: []string{"a"}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewMySQLReader(enumColumnsDB(c, test.rows).SQL, "app")

			enums, err := reader.readEnums(t.Context(), "app")

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
