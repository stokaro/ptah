package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
)

// deferredKeys is posts with two foreign keys to users, one on its column and
// one in the constraint list, each with the deferral given.
func deferredKeys(deferrable bool, initially string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Post", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "Post", Name: "user_id", Type: "INTEGER", Foreign: "users(id)",
				ForeignKeyName: "posts_user_id_fkey", Deferrable: deferrable, Initially: initially,
			},
			{StructName: "Post", Name: "editor_id", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Post", Table: "posts", Name: "posts_editor_id_fkey", Type: "FOREIGN KEY",
			Columns: []string{"editor_id"}, ForeignTable: "users", ForeignColumns: []string{"id"},
			Deferrable: deferrable, Initially: initially,
		}},
	}
}

// TestToDBSchema_ForeignKeyDeferralStaysIdempotent compares a document with
// itself. Without the deferral on the catalog side, each deferrable key is
// dropped and added again, and `schema diff` of a SQL file with itself plans
// that for every deferrable key (stokaro/ptah#3818).
func TestToDBSchema_ForeignKeyDeferralStaysIdempotent(t *testing.T) {
	tests := []struct {
		name       string
		deferrable bool
		initially  string
	}{
		{name: "deferrable", deferrable: true},
		{name: "initially deferred", deferrable: true, initially: "deferred"},
		{name: "initially immediate", deferrable: true, initially: "immediate"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := deferredKeys(test.deferrable, test.initially)

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), db, must.Must(goschematodb.ToDBSchema(t.Context(), db, platform.Postgres, must.Must(builtin.New()))), platform.Postgres, must.Must(builtin.New())))

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%#v", diff))
		})
	}
}

// TestToDBSchema_ForeignKeyDeferralIsCompared is the control: a document whose
// keys are not deferrable, compared with the catalog of one whose keys are,
// has changes.
func TestToDBSchema_ForeignKeyDeferralIsCompared(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithDialect(
		t.Context(), deferredKeys(false, ""), must.Must(goschematodb.ToDBSchema(t.Context(), deferredKeys(true, "deferred"), platform.Postgres, must.Must(builtin.New()))), platform.Postgres, must.Must(builtin.New()),
	))

	c.Assert(diff.HasChanges(), qt.IsTrue)
}
