package privilegecompare_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegecompare"
)

// Global observations record changes from PostgreSQL's built-in ACL. Missing
// rows retain that ACL; scoped observations start without any rights.
func TestCompareObjects_GlobalDefaults(t *testing.T) {
	tests := []struct {
		name     string
		identity pgdefaultprivilege.Identity
		desired  *pgdefaultprivilege.Desired
		current  *pgdefaultprivilege.Observed
		roles    []string
		want     []*pgdefaultprivilege.Change
	}{
		{name: "revoke owner all", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"},
			desired: &pgdefaultprivilege.Desired{Revoked: []string{"ALL"}},
			want:    []*pgdefaultprivilege.Change{{Revoked: grants("ALL")}}},
		{name: "revoke owner all after partial revoke", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "SEQUENCES", Grantee: "owner"},
			desired: &pgdefaultprivilege.Desired{Revoked: []string{"ALL"}}, current: &pgdefaultprivilege.Observed{Revoked: []string{"UPDATE"}},
			want: []*pgdefaultprivilege.Change{{Revoked: grants("ALL")}}},
		{name: "explicit complete list matches revoked all", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "SEQUENCES", Grantee: "owner"},
			desired: &pgdefaultprivilege.Desired{Revoked: []string{"USAGE", "SELECT", "UPDATE"}}, current: &pgdefaultprivilege.Observed{Revoked: []string{"ALL"}}},
		{name: "explicit complete list revokes all", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "SEQUENCES", Grantee: "owner"},
			desired: &pgdefaultprivilege.Desired{Revoked: []string{"USAGE", "SELECT", "UPDATE"}},
			want:    []*pgdefaultprivilege.Change{{Revoked: grants("ALL")}}},
		{name: "portable table privileges without maintain", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"},
			desired: &pgdefaultprivilege.Desired{Revoked: []string{"ALL"}}, current: &pgdefaultprivilege.Observed{Revoked: []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}}},
		{name: "all restores missing owner privilege", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"},
			desired: &pgdefaultprivilege.Desired{Privileges: grants("ALL")}, current: &pgdefaultprivilege.Observed{Revoked: []string{"DELETE"}},
			want: []*pgdefaultprivilege.Change{{Granted: grants("ALL")}}},
		{name: "revoke all matches revoked all", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "owner"},
			desired: &pgdefaultprivilege.Desired{Revoked: []string{"ALL"}}, current: &pgdefaultprivilege.Observed{Revoked: []string{"ALL"}}},
		{name: "grant outside built-in default", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"},
			desired: &pgdefaultprivilege.Desired{Privileges: grants("SELECT")}, want: []*pgdefaultprivilege.Change{{Granted: grants("SELECT")}}},
		{name: "trim managed global schema grant", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "SCHEMAS", Grantee: "reader"}, roles: []string{"owner"},
			current: &pgdefaultprivilege.Observed{Privileges: grants("USAGE")}, want: []*pgdefaultprivilege.Change{{Revoked: grants("USAGE")}}},
		{name: "scoped revoke does not change built-in default", identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"},
			desired: &pgdefaultprivilege.Desired{Revoked: []string{"EXECUTE"}}},
		{name: "built-in privilege gains grant option", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TYPES", Grantee: "PUBLIC"},
			desired: &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE", WithOption: true}}},
			want:    []*pgdefaultprivilege.Change{{GrantOptionsAdded: []string{"USAGE"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := globalComparisonValues(test.desired, test.current)
			got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), comparison(c, test.identity, desired, current, test.roles))
			c.Assert(err, qt.IsNil)
			c.Assert(got.Complete, qt.IsTrue)
			c.Assert(got.Undecided, qt.HasLen, 0)
			c.Assert(changeDeltas(c, got.Changes), qt.CmpEquals(cmpopts.EquateEmpty()), test.want)
		})
	}
}

func grants(names ...string) []pgdefaultprivilege.Grant {
	var result []pgdefaultprivilege.Grant
	for _, name := range names {
		result = append(result, pgdefaultprivilege.Grant{Privilege: name})
	}
	return result
}

// The before snapshot has separate reversal tests. Every delta component stays
// in this projection, so an extra grant or option change fails these cases.
func changeDeltas(c *qt.C, records []schemaext.ChangeRecord) []*pgdefaultprivilege.Change {
	var result []*pgdefaultprivilege.Change
	for _, record := range records {
		change, ok := record.Value.(*pgdefaultprivilege.Change)
		c.Assert(ok, qt.IsTrue)
		copied := change.Copy()
		copied.Before = nil
		result = append(result, copied)
	}
	return result
}

func TestCompareObjects_ExplicitRevokesPreserveOtherRights(t *testing.T) {
	tests := []struct {
		name    string
		revoked []string
		current []pgdefaultprivilege.Grant
		want    []*pgdefaultprivilege.Change
	}{
		{name: "held subset", revoked: []string{"INSERT"}, current: grants("SELECT", "INSERT"), want: []*pgdefaultprivilege.Change{{Revoked: grants("INSERT")}}},
		{name: "absent right", revoked: []string{"DELETE"}, current: grants("SELECT")},
		{name: "all", revoked: []string{"ALL"}, current: grants("SELECT", "INSERT"), want: []*pgdefaultprivilege.Change{{Revoked: grants("INSERT", "SELECT")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"},
				[]schemaext.Value{&pgdefaultprivilege.Desired{Revoked: test.revoked}}, []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: test.current}}, nil)
			got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Undecided, qt.HasLen, 0)
			c.Assert(changeDeltas(c, got.Changes), qt.CmpEquals(cmpopts.EquateEmpty()), test.want)
		})
	}
}

func TestCompareObjects_TableAllMatchesServerRights(t *testing.T) {
	portable := grants("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")
	tests := []struct {
		name    string
		current []pgdefaultprivilege.Grant
		want    []*pgdefaultprivilege.Change
	}{
		{name: "postgres 16", current: portable},
		{name: "postgres 17", current: append(slices.Clone(portable), pgdefaultprivilege.Grant{Privilege: "MAINTAIN"})},
		{name: "file all", current: grants("ALL")},
		{name: "missing rights", current: grants("SELECT", "INSERT"), want: []*pgdefaultprivilege.Change{{Granted: grants("ALL")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"},
				[]schemaext.Value{&pgdefaultprivilege.Desired{Privileges: grants("ALL")}}, []schemaext.Value{&pgdefaultprivilege.Observed{Privileges: test.current}}, nil)
			got, err := (privilegecompare.Service{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Undecided, qt.HasLen, 0)
			c.Assert(changeDeltas(c, got.Changes), qt.CmpEquals(cmpopts.EquateEmpty()), test.want)
		})
	}
}

// globalComparisonValues distinguishes absent values from explicit empty ACLs.
func globalComparisonValues(declaration *pgdefaultprivilege.Desired, observation *pgdefaultprivilege.Observed) (desired, current []schemaext.Value) {
	if declaration != nil {
		desired = append(desired, declaration)
	}
	if observation != nil {
		current = append(current, observation)
	}
	return desired, current
}
