package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

// The same hint must survive each SQL entry point and a render/read round trip.
func TestRead_IndexBlockSize_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range []struct {
			name, sql string
			size      uint64
		}{
			{"inline", "CREATE TABLE t(id int PRIMARY KEY, a int, KEY k(a) KEY_BLOCK_SIZE=8)", 8},
			{"unique", "CREATE TABLE t(id int PRIMARY KEY, a int, UNIQUE KEY k(a) KEY_BLOCK_SIZE 4 COMMENT 'hint')", 4},
			{"standalone", "CREATE TABLE t(id int PRIMARY KEY,a int); CREATE INDEX k ON t(a) KEY_BLOCK_SIZE=8", 8},
			{"alter", "CREATE TABLE t(id int PRIMARY KEY,a int); ALTER TABLE t ADD KEY k(a) KEY_BLOCK_SIZE=4", 4},
			{"last option wins", "CREATE TABLE t(id int PRIMARY KEY,a int, KEY k(a) KEY_BLOCK_SIZE=4 COMMENT 'x' KEY_BLOCK_SIZE=8)", 8},
			{"default", "CREATE TABLE t(id int PRIMARY KEY,a int, UNIQUE KEY k(a) KEY_BLOCK_SIZE=0)", 0},
		} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				db, _, err := sqlschema.Read([]byte(test.sql), dialect)
				c.Assert(err, qt.IsNil)
				c.Assert(db.Indexes, qt.HasLen, 1)
				c.Assert(db.Indexes[0].KeyBlockSize, qt.Equals, test.size)
				statements, err := builtin.GetOrderedCreateStatements(&db, dialect)
				c.Assert(err, qt.IsNil)
				again, _, err := sqlschema.Read([]byte(strings.Join(statements, "\n")), dialect)
				c.Assert(err, qt.IsNil)
				c.Assert(again.Indexes, qt.HasLen, 1)
				c.Assert(again.Indexes[0].KeyBlockSize, qt.Equals, test.size)
			})
		}
	}
}

func TestRead_IndexBlockSize_FailurePath(t *testing.T) {
	for _, value := range []string{"-1", "'8'", "1.5", "18446744073709551616", ""} {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			_, _, err := sqlschema.Read([]byte("CREATE TABLE t(a int, KEY k(a) KEY_BLOCK_SIZE="+value+")"), "mysql")
			c.Assert(err, qt.ErrorMatches, `(?s).*KEY_BLOCK_SIZE.*expected a non-negative integer.*`)
		})
	}
}
