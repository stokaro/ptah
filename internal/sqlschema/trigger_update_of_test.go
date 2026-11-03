package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestRead_TriggerUpdateOfColumns_CutAsTheServerCutsThem pins the column list
// of a trigger's UPDATE OF: a name past the 63 bytes PostgreSQL keeps is cut as
// the column's own declaration is, quoted or not, and a name within the limit,
// or one a read with no dialect keeps, stays as written.
func TestRead_TriggerUpdateOfColumns_CutAsTheServerCutsThem(t *testing.T) {
	long := strings.Repeat("c", 70)
	tests := []struct {
		name    string
		dialect string
		column  string
		want    string
	}{
		{name: "a long unquoted name on PostgreSQL", dialect: platform.Postgres, column: long, want: "UPDATE OF " + long[:63]},
		{name: "a long quoted name on PostgreSQL", dialect: platform.Postgres, column: `"` + strings.ToUpper(long) + `"`,
			want: `UPDATE OF "` + strings.ToUpper(long)[:63] + `"`},
		{name: "a name within the limit", dialect: platform.Postgres, column: "b", want: "UPDATE OF b"},
		{name: "a read with no dialect", dialect: "", column: long, want: "UPDATE OF " + long},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql := "CREATE TABLE t (id int PRIMARY KEY, " + test.column + " int);\n" +
				"CREATE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;\n" +
				"CREATE TRIGGER t_upd BEFORE UPDATE OF " + test.column + " ON t FOR EACH ROW EXECUTE FUNCTION trg();\n"

			db, _, err := sqlschema.Read([]byte(sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(db.Triggers, qt.HasLen, 1)
			c.Assert(db.Triggers[0].Event, qt.Equals, test.want)
		})
	}
}
