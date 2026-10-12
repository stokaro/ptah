package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// The PostgreSQL family keeps an index's, a schema's and a role's comment as
// an object comment, which COMMENT ON changes in place. A changed one is
// compared where the target stores the comment and reads it back, and planned
// as one statement rather than as a rebuild.

// commentedSchemaAndRole declares the schema app and the role r, each with
// comment.
func commentedSchemaAndRole(comment string) *schemamodel.Database {
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "app", Comment: comment}},
		Roles:   []schemamodel.Role{{Name: "r", Comment: comment, Inherit: true}},
	}
}

// reportedSchemaAndRole is commentedSchemaAndRole as a reader reports it.
func reportedSchemaAndRole(comment string) *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "app", Comment: comment}},
		Roles:   []catalog.Role{{Name: "r", Comment: comment, Inherit: true}},
	}
}

// TestCompare_IndexComment_HappyPath changes the comment of an index the plan
// keeps.
func TestCompare_IndexComment_HappyPath(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(dialect, optionedIndexTable("new", false), liveOptionedIndexTable("old", false))

			c.Assert(diff.IndexCommentsChanged, qt.DeepEquals, []difftypes.IndexCommentChange{{
				TableName: "t", Name: "k_a", Current: "old", Desired: "new",
			}})
			c.Assert(diff.IndexesAdded, qt.HasLen, 0)
			c.Assert(diff.IndexesRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompare_IndexComment_NotInPlace records no comment change where none
// is written in place: Spanner stores no index comment, and an index the plan
// rebuilds takes its comment from the statement that creates it.
func TestCompare_IndexComment_NotInPlace(t *testing.T) {
	unique := optionedIndexTable("new", false)
	unique.Indexes[0].Unique = true
	tests := []struct {
		name    string
		dialect string
		desired *schemamodel.Database
		added   int
	}{
		{name: "Spanner", dialect: platform.Spanner, desired: optionedIndexTable("new", false)},
		{name: "a rebuilt index", dialect: platform.Postgres, desired: unique, added: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(test.dialect, test.desired, liveOptionedIndexTable("old", false))

			c.Assert(diff.IndexCommentsChanged, qt.HasLen, 0)
			c.Assert(diff.IndexesAdded, qt.HasLen, test.added)
		})
	}
}

// TestCompare_SchemaAndRoleComments_HappyPath changes the comment of a schema
// and of a role both sides hold, where the target stores each.
func TestCompare_SchemaAndRoleComments_HappyPath(t *testing.T) {
	schemaChange := difftypes.ObjectCommentChange{Kind: difftypes.CommentedSchema, Name: "app", Current: "old", Desired: "new"}
	roleChange := difftypes.ObjectCommentChange{Kind: difftypes.CommentedRole, Name: "r", Current: "old", Desired: "new"}
	outOfScope := reportedSchemaAndRole("old")
	outOfScope.RolesOutOfScope, outOfScope.Roles = outOfScope.Roles, nil
	tests := []struct {
		name    string
		dialect string
		current *catalog.Database
		want    []difftypes.ObjectCommentChange
	}{
		{name: "PostgreSQL", dialect: platform.Postgres, current: reportedSchemaAndRole("old"),
			want: []difftypes.ObjectCommentChange{roleChange, schemaChange}},
		{name: "YugabyteDB", dialect: platform.YugabyteDB, current: reportedSchemaAndRole("old"),
			want: []difftypes.ObjectCommentChange{roleChange, schemaChange}},
		{
			// CockroachDB has no COMMENT ON ROLE, so only the schema's is
			// compared.
			name: "CockroachDB", dialect: platform.CockroachDB, current: reportedSchemaAndRole("old"),
			want: []difftypes.ObjectCommentChange{schemaChange},
		},
		{
			// A role the read did not describe is still a role the server
			// has, as the role comparison reads it.
			name: "a role outside the described scope", dialect: platform.Postgres, current: outOfScope,
			want: []difftypes.ObjectCommentChange{roleChange, schemaChange},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(test.dialect, commentedSchemaAndRole("new"), test.current)

			c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, test.want)
		})
	}
}

// TestCompare_SchemaAndRoleComments_Synced compares equal comments, and
// compares none on Spanner, which stores neither.
func TestCompare_SchemaAndRoleComments_Synced(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		current *catalog.Database
	}{
		{name: "equal comments", dialect: platform.Postgres, current: reportedSchemaAndRole("new")},
		{name: "Spanner", dialect: platform.Spanner, current: reportedSchemaAndRole("old")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(test.dialect, commentedSchemaAndRole("new"), test.current)

			c.Assert(diff.ObjectCommentsChanged, qt.HasLen, 0)
		})
	}
}

// TestCompare_SchemaComment_LeftOutIsNotCompared leaves a schema's comment
// alone when the declaration states none. An Atlas HCL document declares
// `schema "public" {}` before any table may name the schema, and PostgreSQL
// creates every database with `public` commented, so the block is not a
// request to remove the comment. A role's comment the declaration leaves out
// is still removed: CREATE ROLE gives a role none.
func TestCompare_SchemaComment_LeftOutIsNotCompared(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Roles:   []schemamodel.Role{{Name: "r", Inherit: true}},
	}
	current := &catalog.Database{
		Schemas: []catalog.Schema{{Name: "public", Comment: "standard public schema"}},
		Roles:   []catalog.Role{{Name: "r", Comment: "old", Inherit: true}},
	}

	diff := compareForDialect(platform.Postgres, desired, current)

	c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, []difftypes.ObjectCommentChange{
		{Kind: difftypes.CommentedRole, Name: "r", Current: "old", Desired: ""},
	})
}
