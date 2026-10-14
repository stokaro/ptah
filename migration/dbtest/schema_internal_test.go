package dbtest

// White-box testing required: preserveUnmanagedObjects is an internal safety
// boundary that filters a precomputed live diff. SQLite introspection cannot
// reliably synthesize every case-equivalent replacement needed to exercise
// this policy through the public runner API.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestPreserveUnmanagedObjects_UsesIdentifierSemantics(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("sqlite")
	diff := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		IndexesAdded:        difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx_users_name", Fields: []string{"name"}}, TableName: "users"}},
		IndexesRemoved: []difftypes.IndexRef{{
			Name:      "IDX_USERS_NAME",
			TableName: "USERS",
		}},
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
			Name:      "uq_users_name",
			TableName: "users",
		}},
		ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{{
			Name:      "UQ_USERS_NAME",
			TableName: "USERS",
		}},
		EnumsModified: []difftypes.EnumDiff{
			{EnumName: "status", ValuesAdded: []string{"published"}, ValuesRemoved: []string{"legacy"}},
			{EnumName: "priority", ValuesRemoved: []string{"obsolete"}},
		},
	}

	c.Assert(preserveUnmanagedObjects(t.Context(), diff, "sqlite", must.Must(builtin.New()).Selections()), qt.IsNil)

	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{{
		Name:      "idx_users_name",
		TableName: "users",
	}})
	c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, []string{"uq_users_name"})
	c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, difftypes.ConstraintRemovals{{
		Name:      "uq_users_name",
		TableName: "users",
	}})
	c.Assert(diff.EnumsModified, qt.DeepEquals, []difftypes.EnumDiff{{
		EnumName:    "status",
		ValuesAdded: []string{"published"},
	}})
}

// Additive schema tests retain grants and grant-option additions, and leave
// revocations out of the plan that runs against shared security state.
func TestPreserveUnmanagedObjects_KeepsTheDefaultPrivilegesTheDatabaseHolds(t *testing.T) {
	c := qt.New(t)
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader"}, identifier.ForDialect("postgres")))
	granted := schemaext.ChangeRecord{Subject: ref, Value: &pgdefaultprivilege.Change{Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}}
	option := schemaext.ChangeRecord{Subject: ref, Value: &pgdefaultprivilege.Change{GrantOptionsAdded: []string{"UPDATE"}}}
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
		granted, {Subject: ref, Value: &pgdefaultprivilege.Change{Revoked: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}}}},
		option, {Subject: ref, Value: &pgdefaultprivilege.Change{GrantOptionsRevoked: []string{"DELETE"}}},
	}}
	c.Assert(preserveUnmanagedObjects(t.Context(), diff, "postgres", must.Must(builtin.New()).Selections()), qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{granted, option})
}

func TestPreserveUnmanagedObjects_NormalizesReplacementForPlanner(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("sqlite")
	diff := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
			Name:            "check_users_name",
			TableName:       "users",
			Type:            "CHECK",
			CheckExpression: "name <> ''",
		}},
		ConstraintsRemoved: []difftypes.ConstraintRemovalInfo{{
			Name:      "CHECK_USERS_NAME",
			TableName: "USERS",
			Type:      "CHECK",
		}},
	}

	c.Assert(preserveUnmanagedObjects(t.Context(), diff, "postgres", must.Must(builtin.New()).Selections()), qt.IsNil)
	sql, err := planner.GenerateSchemaDiffSQL(
		context.Background(), must.Must(builtin.New()),
		diff, "postgres",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Matches, `(?s).*DROP CONSTRAINT IF EXISTS "check_users_name".*ADD CONSTRAINT "check_users_name".*`)
}

// opaqueChange is a feature change whose owner does not say whether it takes
// state away.
type opaqueChange struct{}

func (*opaqueChange) Kind() schemaext.Kind               { return "example.org/opaque" }
func (*opaqueChange) CloneChange() schemaext.ChangeValue { return &opaqueChange{} }

// TestPreserveUnmanagedObjects_KeepsFeatureChangesThatAddState pins the
// additive apply over owned feature changes: a change whose owner says it
// takes state away is left out, at the top and on a table; one whose owner
// says it does not is applied even beside no structural change; and one whose
// owner does not say is applied only beside a structural change, as before.
func TestPreserveUnmanagedObjects_KeepsFeatureChangesThatAddState(t *testing.T) {
	c := qt.New(t)
	policy := func(name string, before *pgpolicy.ObservedPolicy, after *pgpolicy.DesiredPolicy) schemaext.ChangeRecord {
		return schemaext.ChangeRecord{Subject: pgpolicy.PolicyRef("", "orders", name),
			Value: &pgpolicy.PolicyChange{Before: before, After: after, Access: pgpolicy.PolicyAccess(before, after, pgpolicy.ExpressionsSame)}}
	}
	switches := func(table string, before, after bool) schemaext.ChangeRecord {
		return schemaext.ChangeRecord{Subject: pgpolicy.Table(pgpolicy.PolicyRef("", table, "x")), Value: &pgpolicy.TableStateChange{
			Before: &pgpolicy.ObservedTableState{Enabled: before}, After: &pgpolicy.DesiredTableState{Enabled: after},
			Access: pgpolicy.TableStateAccess(&pgpolicy.ObservedTableState{Enabled: before}, &pgpolicy.DesiredTableState{Enabled: after})}}
	}
	held := &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}, Composition: pgpolicy.Permissive}
	created, enabled := policy("created", nil, &pgpolicy.DesiredPolicy{}), switches("orders", false, true)
	opaque := schemaext.ChangeRecord{Subject: pgpolicy.Table(pgpolicy.PolicyRef("", "audit", "x")), Value: &opaqueChange{}}
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{policy("dropped_at_the_top", held, nil)},
		TablesModified: []difftypes.TableDiff{
			{TableName: "orders", FeatureChanges: []schemaext.ChangeRecord{created, policy("dropped", held, nil), enabled}},
			{TableName: "invoices", FeatureChanges: []schemaext.ChangeRecord{switches("invoices", true, false)}},
			{TableName: "audit", FeatureChanges: []schemaext.ChangeRecord{opaque}},
		},
	}

	c.Assert(preserveUnmanagedObjects(t.Context(), diff, "postgres", must.Must(builtin.New()).Selections()), qt.IsNil)

	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].TableName, qt.Equals, "orders")
	c.Assert(diff.TablesModified[0].FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{created, enabled})
}

func TestPreserveUnmanagedObjects_KeepsAdditionsFromMixedDefaultPrivileges(t *testing.T) {
	c := qt.New(t)
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, identifier.ForDialect("postgres")))
	mixed := &pgdefaultprivilege.Change{
		Before:  &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}, {Privilege: "UPDATE"}, {Privilege: "DELETE", WithOption: true}}},
		Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}},
		GrantOptionsAdded: []string{"UPDATE"}, GrantOptionsRevoked: []string{"DELETE"},
	}
	snapshot := mixed.Copy()
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ref, Value: mixed}}}
	c.Assert(preserveUnmanagedObjects(t.Context(), diff, "postgres", must.Must(builtin.New()).Selections()), qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{{Subject: ref, Value: &pgdefaultprivilege.Change{
		Before: snapshot.Before, Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, GrantOptionsAdded: []string{"UPDATE"},
	}}})
	c.Assert(mixed, qt.DeepEquals, snapshot)
}
