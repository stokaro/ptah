package atlasfilter_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// The two fixtures hold the same three default privileges, one per side of a
// comparison. A default privilege has no name of its own, so each row is told
// apart by its object type, and the three differ in the axes a selector can
// reach: the schema they apply in, the role they apply to, and the role they
// grant to.
func defaultPrivilegeDatabaseFixture() *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "public"}, {Name: "app"}},
		Tables: []catalog.Table{
			{Name: "users", Columns: []catalog.Column{{Name: "id"}}},
		},
		Roles: []catalog.Role{
			{Name: "defaults_owner"},
			{Name: "defaults_reader"},
			{Name: "other_owner"},
			{Name: "other_reader"},
		},
		DefaultPrivileges: []catalog.DefaultPrivilege{
			{
				Grantor: "defaults_owner", ObjectType: "TABLES",
				Grantee: "defaults_reader", Privilege: "SELECT",
			},
			{
				Grantor: "defaults_owner", Schema: "app", ObjectType: "SEQUENCES",
				Grantee: "defaults_reader", Privilege: "USAGE",
			},
			{
				Grantor: "other_owner", Schema: "app", ObjectType: "FUNCTIONS",
				Grantee: "other_reader", Privilege: "EXECUTE",
			},
		},
	}
}

func defaultPrivilegeGeneratedFixture() *schemamodel.Database {
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}, {Name: "app"}},
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "BIGINT"},
		},
		Roles: []schemamodel.Role{
			{Name: "defaults_owner"},
			{Name: "defaults_reader"},
			{Name: "other_owner"},
			{Name: "other_reader"},
		},
		DefaultPrivileges: []schemamodel.DefaultPrivilege{
			{
				StructName: "PublicDefaults", Grantor: "defaults_owner",
				ObjectType: "TABLES", Grantee: "defaults_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
			},
			{
				StructName: "AppSequenceDefaults", Grantor: "defaults_owner", Schema: "app",
				ObjectType: "SEQUENCES", Grantee: "defaults_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
			},
			{
				StructName: "AppFunctionDefaults", Grantor: "other_owner", Schema: "app",
				ObjectType: "FUNCTIONS", Grantee: "other_reader",
				Privileges: []schemamodel.PrivilegeGrant{{Privilege: "EXECUTE"}},
			},
		},
	}
}

func databaseDefaultPrivilegeTypes(privileges []catalog.DefaultPrivilege) []string {
	types := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		types = append(types, privilege.ObjectType)
	}
	return types
}

func generatedDefaultPrivilegeTypes(privileges []schemamodel.DefaultPrivilege) []string {
	types := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		types = append(types, privilege.ObjectType)
	}
	return types
}

// TestExcludeSubtractsDefaultPrivilegesOnBothSidesTheSameWay drives both
// exclusions from one row, because a selector that removed a default privilege
// from the live side alone turns it into something to grant, and from the
// desired side alone into something to revoke.
//
// The first row is the control that the family survives at all: the database
// side is cloned field by field, which is the shape that drops a new collection
// in silence, and a projection that lost the field would satisfy every row
// below that expects something gone.
//
// The grantor rows and the grantee rows are separate because they are separate
// reads. A filter that asked the grantee alone keeps the two defaults_owner
// rows, which then render an ALTER DEFAULT PRIVILEGES FOR ROLE naming a role
// the same description no longer defines.
func TestExcludeSubtractsDefaultPrivilegesOnBothSidesTheSameWay(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    []string
	}{
		{
			name:    "a selector naming none of them leaves every one",
			pattern: "nosuch_object",
			want:    []string{"TABLES", "SEQUENCES", "FUNCTIONS"},
		},
		{
			name:    "an excluded schema takes its defaults with it",
			pattern: "app",
			want:    []string{"TABLES"},
		},
		{
			name:    "an excluded grantee takes the defaults granted to it",
			pattern: "defaults_reader[type=role]",
			want:    []string{"FUNCTIONS"},
		},
		{
			name:    "an excluded grantor takes the defaults that apply to it",
			pattern: "defaults_owner[type=role]",
			want:    []string{"FUNCTIONS"},
		},
		{
			name:    "excluding one end of one row leaves the others",
			pattern: "other_reader[type=role]",
			want:    []string{"TABLES", "SEQUENCES"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			gotDatabase, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
				defaultPrivilegeDatabaseFixture(), []string{test.pattern}, "public")
			c.Assert(err, qt.IsNil)

			gotGenerated, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(
				defaultPrivilegeGeneratedFixture(), []string{test.pattern}, "public")
			c.Assert(err, qt.IsNil)

			c.Assert(databaseDefaultPrivilegeTypes(gotDatabase.DefaultPrivileges),
				qt.DeepEquals, test.want)
			c.Assert(generatedDefaultPrivilegeTypes(gotGenerated.DefaultPrivileges),
				qt.DeepEquals, test.want)
		})
	}
}

// TestExcludeGeneratedLeavesTheCallersDefaultPrivilegesAlone pins that the
// desired-side exclusion reads its argument and writes a projection.
//
// The generated clone is a shallow struct copy plus a clone per slice field, so
// a field left out of it shares its backing array with the caller's state. The
// exclusion then hands that array to [schemamodel.Finalize], which canonicalizes
// every default privilege in place -- the caller's own rows, not a copy of them.
// The lowercase privilege in the fixture is what makes that visible.
func TestExcludeGeneratedLeavesTheCallersDefaultPrivilegesAlone(t *testing.T) {
	c := qt.New(t)
	schema := defaultPrivilegeGeneratedFixture()
	schema.DefaultPrivileges[0].Privileges = []schemamodel.PrivilegeGrant{{Privilege: "select"}}
	before := slices.Clone(schema.DefaultPrivileges)

	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(
		schema, []string{"nosuch_object"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(got.DefaultPrivileges, qt.HasLen, 3)
	c.Assert(schema.DefaultPrivileges, qt.DeepEquals, before)
	c.Assert(schema.DefaultPrivileges[0].Privileges[0].Privilege, qt.Equals, "select")
}

// TestScopeNarrowsDefaultPrivilegesToTheSchemaUniverse is the positive
// projection's half of the agreement above. A schema universe is the operator
// naming what the comparison covers, and a default privilege belongs to its
// schema and to nothing else.
func TestScopeNarrowsDefaultPrivilegesToTheSchemaUniverse(t *testing.T) {
	tests := []struct {
		name    string
		schemas []string
		want    []string
	}{
		{
			name:    "the connected schema",
			schemas: []string{"public"},
			want:    []string{"TABLES"},
		},
		{
			name:    "a named schema",
			schemas: []string{"app"},
			want:    []string{"SEQUENCES", "FUNCTIONS"},
		},
		{
			name:    "both",
			schemas: []string{"public", "app"},
			want:    []string{"TABLES", "SEQUENCES", "FUNCTIONS"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{Schemas: test.schemas, DefaultSchema: "public"}

			gotDatabase, err := atlasfilter.ScopeDatabase(defaultPrivilegeDatabaseFixture(), scope)
			c.Assert(err, qt.IsNil)

			gotGenerated, err := atlasfilter.ScopeGenerated(defaultPrivilegeGeneratedFixture(), scope)
			c.Assert(err, qt.IsNil)

			c.Assert(databaseDefaultPrivilegeTypes(gotDatabase.DefaultPrivileges),
				qt.DeepEquals, test.want)
			c.Assert(generatedDefaultPrivilegeTypes(gotGenerated.DefaultPrivileges),
				qt.DeepEquals, test.want)
		})
	}
}

// TestScopeKeepsTheRolesADefaultPrivilegeNames is the ordering the projection
// depends on: the roles are kept for the statements that survived, so they are
// decided after the default privileges rather than before.
//
// The selection here names a table and no role, and no grant survives it, so
// the kept default privilege is the only statement naming defaults_owner and
// defaults_reader. Without the retention both roles leave and the projection
// describes an ALTER DEFAULT PRIVILEGES pointing at roles it says are absent;
// with the projection ordered the other way round the retention reads an empty
// list and does the same thing.
//
// The two roles the dropped rows named stay out, which is what keeps this a
// retention rule rather than a rule that keeps every role.
func TestScopeKeepsTheRolesADefaultPrivilegeNames(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{
		Schemas:       []string{"public"},
		Include:       []string{"users"},
		DefaultSchema: "public",
	}

	gotDatabase, err := atlasfilter.ScopeDatabase(defaultPrivilegeDatabaseFixture(), scope)
	c.Assert(err, qt.IsNil)

	gotGenerated, err := atlasfilter.ScopeGenerated(defaultPrivilegeGeneratedFixture(), scope)
	c.Assert(err, qt.IsNil)

	c.Assert(databaseDefaultPrivilegeTypes(gotDatabase.DefaultPrivileges),
		qt.DeepEquals, []string{"TABLES"})
	c.Assert(databaseRoleNames(gotDatabase.Roles),
		qt.DeepEquals, []string{"defaults_owner", "defaults_reader"})

	c.Assert(generatedDefaultPrivilegeTypes(gotGenerated.DefaultPrivileges),
		qt.DeepEquals, []string{"TABLES"})
	c.Assert(generatedRoleNames(gotGenerated.Roles),
		qt.DeepEquals, []string{"defaults_owner", "defaults_reader"})
}
