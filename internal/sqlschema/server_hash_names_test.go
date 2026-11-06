package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestRead_AnUnnamedUniqueOnAServerThatHashesNames_HappyPath is a UNIQUE or a CHECK on the
// table with no name, on a server that would name it with a hash that differs by database.
// It takes a name here, past any name the schema holds, and a name the author wrote stays.
func TestRead_AnUnnamedUniqueOnAServerThatHashesNames_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "created over two columns",
			sql:  "CREATE TABLE t (id int PRIMARY KEY, v int, w int, UNIQUE (v, w));",
			want: []string{"t_v_w_key"},
		},
		{
			name: "named by its author",
			sql:  "CREATE TABLE t (id int PRIMARY KEY, v int, CONSTRAINT mine UNIQUE (v));",
			want: []string{"mine"},
		},
		{
			name: "after a constraint in the same statement that holds the name",
			sql:  "CREATE TABLE t (id int, v int, w int, UNIQUE (v, w), CONSTRAINT t_v_w_key UNIQUE (w));",
			want: []string{"t_v_w_key1", "t_v_w_key"},
		},
		{
			name: "added to a table",
			sql:  "CREATE TABLE t (id int, v int, w int);\nALTER TABLE t ADD UNIQUE (v, w);",
			want: []string{"t_v_w_key"},
		},
		{
			name: "a check on the table",
			sql:  "CREATE TABLE t (id int, w int, CHECK (w > 0));",
			want: []string{"t_w_check"},
		},
		{
			name: "a check beside a constraint that holds the name",
			sql:  "CREATE TABLE t (id int, w int, CHECK (w > 0), CONSTRAINT t_w_check UNIQUE (w));",
			want: []string{"t_w_check1", "t_w_check"},
		},
		{
			name: "a check added to a table",
			sql:  "CREATE TABLE t (id int, w int);\nALTER TABLE t ADD CHECK (w > 0);",
			want: []string{"t_w_check"},
		},
		{
			name: "added beside a constraint that holds the name",
			sql:  "CREATE TABLE t (id int, v int, w int, CONSTRAINT t_v_w_key UNIQUE (w));\nALTER TABLE t ADD UNIQUE (v, w);",
			want: []string{"t_v_w_key", "t_v_w_key1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := sqlschema.Read([]byte(test.sql), platform.SQLServer)

			c.Assert(err, qt.IsNil)
			names := make([]string, 0, len(db.Constraints))
			for _, constraint := range db.Constraints {
				names = append(names, constraint.Name)
			}
			c.Assert(names, qt.DeepEquals, test.want)
		})
	}
}

// TestRead_AnUnnamedUniqueOnAServerThatHashesNames_FailurePath is what the rule leaves
// alone: a server whose own rule is read elsewhere keeps it.
func TestRead_AnUnnamedUniqueOnAServerThatHashesNames_FailurePath(t *testing.T) {
	c := qt.New(t)

	db, _, err := sqlschema.Read([]byte("CREATE TABLE t (id int, v int, w int, UNIQUE (v, w));"), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Constraints, qt.HasLen, 1)
	c.Assert(db.Constraints[0].Name, qt.Not(qt.Equals), "t_v_w_key")
}
