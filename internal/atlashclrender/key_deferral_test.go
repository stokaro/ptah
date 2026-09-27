package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// deferrableKeyDatabase is a table whose primary key or UNIQUE defers its
// check, as named.
func deferrableKeyDatabase(primaryKey, unique bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Slot", Name: "slots", PrimaryKey: []string{"id"}, PrimaryKeyDeferrable: primaryKey,
		}},
		Fields: []schemamodel.Field{
			{StructName: "Slot", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Slot", Name: "pos", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Slot", Table: "slots", Name: "slots_pos_key", Type: "UNIQUE",
			Columns: []string{"pos"}, Deferrable: unique,
		}},
	}
}

// TestRender_DeferrableKey_FailurePath refuses a key that defers its check:
// Atlas HCL writes a UNIQUE as a unique index, which cannot defer, and has no
// deferral on a primary key. Written plain, the key would reject at once the
// rows its author arranged to fix before commit.
func TestRender_DeferrableKey_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		primaryKey bool
		unique     bool
		wantErr    string
	}{
		{name: "a primary key", primaryKey: true, wantErr: `the primary key of table "slots" defers its check, which Atlas HCL cannot represent`},
		{name: "a UNIQUE", unique: true, wantErr: `UNIQUE "slots_pos_key" defers its check, which Atlas HCL cannot represent`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(deferrableKeyDatabase(test.primaryKey, test.unique), "postgres", "public")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, atlashclrender.Result{})
		})
	}
}

// TestRender_DeferrableKey_HappyPath is the control: the same keys that do not
// defer are written.
func TestRender_DeferrableKey_HappyPath(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspected(deferrableKeyDatabase(false, false), "postgres", "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, `table "slots"`)
}

// TestRender_DeferrableForeignKey_HappyPath is the other control: a foreign
// key that defers is not a key, and HCL writes its deferral.
func TestRender_DeferrableForeignKey_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := deferrableKeyDatabase(false, false)
	database.Constraints = append(database.Constraints, schemamodel.Constraint{
		StructName: "Slot", Table: "slots", Name: "slots_next_fk", Type: "FOREIGN KEY",
		Columns: []string{"pos"}, ForeignTable: "slots", ForeignColumns: []string{"id"}, Deferrable: true,
	})

	result, err := atlashclrender.RenderInspected(database, "postgres", "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, "deferrable")
}
