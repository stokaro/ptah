package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestIndexDeclarationsOf_ResolvesTheOwner is the owner-resolution table that
// lived beside the index resolver until the resolver went.
//
// Which relation an index belongs to is not written on the index: a declaration
// may name the table, may name none and belong to the struct it was written on,
// and a materialized view is an owner too. The answer is derived once, here,
// and travels on the addition (stokaro/ptah#2315).
func TestIndexDeclarationsOf_ResolvesTheOwner(t *testing.T) {
	tests := []struct {
		name      string
		tables    []schemamodel.Table
		index     schemamodel.Index
		wantOwner string
	}{
		{
			name:      "struct association",
			tables:    []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
			index:     schemamodel.Index{StructName: "User", Name: "idx_email"},
			wantOwner: "app.users",
		},
		{
			name:      "explicit struct table association",
			tables:    []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
			index:     schemamodel.Index{StructName: "User", Name: "idx_email", TableName: "users"},
			wantOwner: "app.users",
		},
		{
			name:      "explicit unqualified struct table association",
			tables:    []schemamodel.Table{{StructName: "User", Name: "users"}},
			index:     schemamodel.Index{StructName: "User", Name: "idx_email", TableName: "users"},
			wantOwner: "users",
		},
		{
			name:      "unique plain table association",
			tables:    []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
			index:     schemamodel.Index{Name: "idx_email", TableName: "users"},
			wantOwner: "app.users",
		},
		{
			name:      "explicit table without table metadata",
			index:     schemamodel.Index{Name: "idx_email", TableName: "users"},
			wantOwner: "users",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables:  test.tables,
				Indexes: []schemamodel.Index{test.index},
			}

			declared := difftypes.IndexDeclarationsOf(desired)

			c.Assert(declared, qt.HasLen, 1)
			c.Assert(declared[0].TableName, qt.Equals, test.wantOwner)
			c.Assert(declared[0].Index, qt.DeepEquals, test.index)
		})
	}
}

// TestIndexDeclarationsOf_ResolvesAMaterializedViewOwner is the other relation
// kind an index can belong to. PostgreSQL indexes a materialized view, and a
// UNIQUE index on one is what REFRESH MATERIALIZED VIEW CONCURRENTLY requires;
// resolving against tables alone left the owner empty (stokaro/ptah#1725).
func TestIndexDeclarationsOf_ResolvesAMaterializedViewOwner(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		MaterializedViews: []schemamodel.MaterializedView{{Name: "app.user_counts"}},
		Indexes: []schemamodel.Index{
			{Name: "idx_user_counts", TableName: "app.user_counts", Fields: []string{"c"}},
		},
	}

	declared := difftypes.IndexDeclarationsOf(desired)

	c.Assert(declared, qt.HasLen, 1)
	c.Assert(declared[0].TableName, qt.Equals, "app.user_counts")
}
