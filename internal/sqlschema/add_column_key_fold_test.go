package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestRead_AddedColumnKeyFoldsIntoItsPrimaryKey pins the UNIQUE of an added
// column that is also the table's primary key: PostgreSQL builds one key for
// the two, in either order, so the column is read as its primary key alone, as
// the same column of a CREATE TABLE is. A column with UNIQUE and no primary
// key keeps it, and a read with no dialect folds nothing.
func TestRead_AddedColumnKeyFoldsIntoItsPrimaryKey(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		column      string
		wantPrimary bool
		wantUnique  bool
	}{
		{name: "primary key then unique", dialect: platform.Postgres, column: "id int PRIMARY KEY UNIQUE", wantPrimary: true, wantUnique: false},
		{name: "unique then primary key", dialect: platform.Postgres, column: "id int UNIQUE PRIMARY KEY", wantPrimary: true, wantUnique: false},
		{name: "unique alone", dialect: platform.Postgres, column: "id int UNIQUE", wantPrimary: false, wantUnique: true},
		{name: "a read with no dialect", dialect: "", column: "id int PRIMARY KEY UNIQUE", wantPrimary: true, wantUnique: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, _, err := sqlschema.Read([]byte("CREATE TABLE t (a int);\nALTER TABLE t ADD COLUMN "+test.column+";\n"), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 2)
			c.Assert([]bool{db.Fields[1].Primary, db.Fields[1].Unique}, qt.DeepEquals, []bool{test.wantPrimary, test.wantUnique})
			c.Assert(db.Constraints, qt.HasLen, 0)
		})
	}
}
