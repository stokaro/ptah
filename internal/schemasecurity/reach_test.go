package schemasecurity_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemasecurity"
)

// relations are tables of the default schema, named by names.
func relations(names ...string) []schemasecurity.ReachRelation {
	out := make([]schemasecurity.ReachRelation, 0, len(names))
	for _, name := range names {
		out = append(out, schemasecurity.ReachRelation{Name: name})
	}
	return out
}

// granted gives role one privilege on each of the default schema's relations
// named by names.
func granted(role string, read, write bool, names ...string) []schemasecurity.ReachGrant {
	out := make([]schemasecurity.ReachGrant, 0, len(names))
	for _, name := range names {
		out = append(out, schemasecurity.ReachGrant{
			Role: role, Relation: schemasecurity.ReachRelation{Name: name}, Read: read, Write: write,
		})
	}
	return out
}

// principals are roles that are not superusers.
func principals(names ...string) []schemasecurity.ReachPrincipal {
	out := make([]schemasecurity.ReachPrincipal, 0, len(names))
	for _, name := range names {
		out = append(out, schemasecurity.ReachPrincipal{Name: name})
	}
	return out
}

// reachLines runs the analysis over reach and states each reach finding on
// one line, in the report's order.
func reachLines(reach *schemasecurity.Reach) []string {
	report := schemasecurity.Analyze(&schemamodel.Database{}, schemasecurity.Options{Reach: reach})
	findings := slices.DeleteFunc(report.Findings, func(finding schemasecurity.Finding) bool {
		return !strings.HasPrefix(finding.Code, "roles/")
	})
	lines := make([]string, 0, len(findings))
	for _, finding := range findings {
		detail := finding.Detail.Reach
		lines = append(lines, fmt.Sprintf("%s %s %s %d/%d over %d%% through=%v sources=%v",
			finding.Code, finding.Severity, finding.Subject.Name, detail.Reached, detail.Relations,
			detail.ThresholdPercent, detail.Through, detail.Sources))
	}
	return lines
}

// defaultThresholds sets the thresholds a caller that states none uses.
func defaultThresholds(reach schemasecurity.Reach) *schemasecurity.Reach {
	reach.WarnPercent = schemasecurity.DefaultReachWarnPercent
	reach.ErrorPercent = schemasecurity.DefaultReachErrorPercent
	return &reach
}

func TestAnalyzeReach_MeasuresReadAndWriteApart(t *testing.T) {
	c := qt.New(t)
	reach := defaultThresholds(schemasecurity.Reach{
		Relations:  relations("a", "b", "c", "d"),
		Grants:     append(granted("reporting", true, false, "a", "b", "c"), granted("reporting", false, true, "a")...),
		Principals: principals("reporting"),
	})

	c.Assert(reachLines(reach), qt.DeepEquals, []string{
		"roles/read-reach warning reporting 3/4 over 50% through=[] sources=[]",
	})
}

func TestAnalyzeReach_FollowsMembershipThroughEveryLevel(t *testing.T) {
	c := qt.New(t)
	reach := defaultThresholds(schemasecurity.Reach{
		Relations:  relations("a", "b", "c", "d"),
		Grants:     granted("reader", true, false, "a", "b", "c", "d"),
		Principals: principals("app", "mid", "reader"),
		Memberships: []schemasecurity.ReachMembership{
			{Role: "reader", Member: "mid"},
			{Role: "mid", Member: "app"},
		},
	})

	c.Assert(reachLines(reach), qt.DeepEquals, []string{
		"roles/read-reach error app 4/4 over 75% through=[mid reader] sources=[]",
		"roles/read-reach error mid 4/4 over 75% through=[reader] sources=[]",
		"roles/read-reach error reader 4/4 over 75% through=[] sources=[]",
	})
}

func TestAnalyzeReach_CountsAccessNoGrantStates(t *testing.T) {
	c := qt.New(t)
	reach := defaultThresholds(schemasecurity.Reach{
		Relations: relations("a", "b", "c", "d"),
		Principals: append(principals("auditor", "ops", "writer"),
			schemasecurity.ReachPrincipal{Name: "admin", Superuser: true}),
		Memberships: []schemasecurity.ReachMembership{{Role: "admin", Member: "ops"}},
		WideAccess: []catalog.WideAccess{
			{Role: "auditor", Read: true, Source: "pg_read_all_data"},
			{Role: "writer", Write: true, Source: "global privileges"},
		},
	})

	c.Assert(reachLines(reach), qt.DeepEquals, []string{
		"roles/read-reach error admin 4/4 over 75% through=[] sources=[superuser held by admin]",
		"roles/read-reach error auditor 4/4 over 75% through=[] sources=[pg_read_all_data held by auditor]",
		"roles/read-reach error ops 4/4 over 75% through=[admin] sources=[superuser held by admin]",
		"roles/write-reach error admin 4/4 over 75% through=[] sources=[superuser held by admin]",
		"roles/write-reach error ops 4/4 over 75% through=[admin] sources=[superuser held by admin]",
		"roles/write-reach error writer 4/4 over 75% through=[] sources=[global privileges held by writer]",
	})
}

func TestAnalyzeReach_SchemaGrantReachesOnlyItsSchema(t *testing.T) {
	c := qt.New(t)
	reach := defaultThresholds(schemasecurity.Reach{
		Relations: append(relations("a", "b", "c"), schemasecurity.ReachRelation{Schema: "other", Name: "x"}),
		Grants: []schemasecurity.ReachGrant{
			{Role: "app", OnSchema: true, Read: true},
			{Role: "side", OnSchema: true, Relation: schemasecurity.ReachRelation{Schema: "other"}, Read: true},
		},
		Principals: principals("app", "side"),
	})

	c.Assert(reachLines(reach), qt.DeepEquals, []string{
		"roles/read-reach warning app 3/4 over 50% through=[] sources=[]",
	})
}

func TestAnalyzeReach_ReportsWhatPublicHoldsOnce(t *testing.T) {
	c := qt.New(t)
	reach := defaultThresholds(schemasecurity.Reach{
		Relations:  relations("a", "b", "c", "d"),
		Grants:     append(granted("PUBLIC", true, false, "a", "b", "c", "d"), granted("writer", false, true, "a", "b", "c")...),
		Principals: principals("app", "writer"),
	})

	c.Assert(reachLines(reach), qt.DeepEquals, []string{
		"roles/read-reach error PUBLIC 4/4 over 75% through=[] sources=[]",
		"roles/write-reach warning writer 3/4 over 50% through=[] sources=[]",
	})
}

func TestAnalyzeReach_ThresholdIsExceededNotMet(t *testing.T) {
	tests := []struct {
		name         string
		warn, failAt int
		reached      []string
		want         []string
	}{
		{name: "half is not above half", warn: 50, failAt: 75, reached: []string{"a", "b"}, want: make([]string, 0)},
		{name: "three quarters is a warning", warn: 50, failAt: 75, reached: []string{"a", "b", "c"},
			want: []string{"roles/read-reach warning app 3/4 over 50% through=[] sources=[]"}},
		{name: "every relation under a 100 error threshold", warn: 50, failAt: 100, reached: []string{"a", "b", "c", "d"},
			want: []string{"roles/read-reach warning app 4/4 over 50% through=[] sources=[]"}},
		{name: "zero thresholds report one relation", warn: 0, failAt: 0, reached: []string{"a"},
			want: []string{"roles/read-reach error app 1/4 over 0% through=[] sources=[]"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reach := &schemasecurity.Reach{
				Relations:    relations("a", "b", "c", "d"),
				Grants:       granted("app", true, false, test.reached...),
				Principals:   principals("app"),
				WarnPercent:  test.warn,
				ErrorPercent: test.failAt,
			}

			c.Assert(reachLines(reach), qt.DeepEquals, test.want)
		})
	}
}

func TestAnalyzeReach_NoRelationsNoFinding(t *testing.T) {
	c := qt.New(t)
	reach := defaultThresholds(schemasecurity.Reach{
		Principals: []schemasecurity.ReachPrincipal{{Name: "admin", Superuser: true}},
	})

	report := schemasecurity.Analyze(&schemamodel.Database{}, schemasecurity.Options{Reach: reach})

	c.Assert(reachLines(reach), qt.DeepEquals, make([]string, 0))
	c.Assert(skippedReachCodes(report), qt.DeepEquals, make([]string, 0))
}

func TestAnalyzeReach_SkippedWithoutTheAccessNoGrantStates(t *testing.T) {
	c := qt.New(t)

	report := schemasecurity.Analyze(&schemamodel.Database{}, schemasecurity.Options{})

	c.Assert(reachLines(nil), qt.DeepEquals, make([]string, 0))
	c.Assert(skippedReachCodes(report), qt.DeepEquals, []string{schemasecurity.ReadReachRule, schemasecurity.WriteReachRule})
}

// skippedReachCodes are the reach rules the report names as skipped.
func skippedReachCodes(report schemasecurity.Report) []string {
	codes := make([]string, 0)
	for _, skipped := range report.SkippedRules {
		codes = append(codes, skipped.Code)
	}
	return slices.DeleteFunc(codes, func(code string) bool { return !strings.HasPrefix(code, "roles/") })
}

// readCatalog is a catalog read that established memberships and the access
// no grant states, found none of either, and holds the tables named by names
// in the default schema.
func readCatalog(names ...string) *catalog.Database {
	live := &catalog.Database{RoleMemberships: make([]catalog.RoleMembership, 0), WideAccess: make([]catalog.WideAccess, 0)}
	for _, name := range names {
		live.Tables = append(live.Tables, catalog.Table{Name: name})
	}
	return live
}

func TestReachFromCatalog_NotEstablished(t *testing.T) {
	tests := []struct {
		name string
		live *catalog.Database
	}{
		{name: "no catalog", live: nil},
		{name: "the access no grant states was not read", live: &catalog.Database{RoleMemberships: make([]catalog.RoleMembership, 0)}},
		{name: "memberships were not read", live: &catalog.Database{WideAccess: make([]catalog.WideAccess, 0)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemasecurity.ReachFromCatalog(test.live, "", 50, 75), qt.IsNil)
		})
	}
}

// TestReachFromCatalog_NamesTheDefaultSchemaOnce is the MySQL shape: a table
// of the connection's database is read with no schema, while its grants and
// its views name the database.
func TestReachFromCatalog_NamesTheDefaultSchemaOnce(t *testing.T) {
	c := qt.New(t)
	live := readCatalog("a", "b", "c")
	live.Views = []catalog.View{{Schema: "appdb", Name: "v"}}
	live.Roles = []catalog.Role{{Name: "reader"}, {Name: "schemar"}}
	for _, name := range []string{"a", "b", "c", "v"} {
		live.Grants = append(live.Grants, catalog.Grant{
			Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "appdb", ObjectName: name,
		})
	}
	live.Grants = append(live.Grants, catalog.Grant{Role: "schemar", Privilege: "INSERT", ObjectType: "SCHEMA", ObjectName: "appdb"})

	c.Assert(reachLines(schemasecurity.ReachFromCatalog(live, "appdb", 50, 75)), qt.DeepEquals, []string{
		"roles/read-reach error reader 4/4 over 75% through=[] sources=[]",
		"roles/write-reach error schemar 4/4 over 75% through=[] sources=[]",
	})
}

func TestReachFromCatalog_FollowsAMembershipThatCanBeUsed(t *testing.T) {
	c := qt.New(t)
	live := readCatalog("a", "b")
	live.Roles = []catalog.Role{{Name: "reader"}, {Name: "switcher"}, {Name: "neither"}}
	live.Grants = []catalog.Grant{
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "a"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "b"},
	}
	live.RoleMemberships = []catalog.RoleMembership{
		{Role: "reader", Member: "switcher", InheritOption: new(false), SetOption: new(true)},
		{Role: "reader", Member: "neither", InheritOption: new(false), SetOption: new(false)},
	}

	c.Assert(reachLines(schemasecurity.ReachFromCatalog(live, "", 50, 75)), qt.DeepEquals, []string{
		"roles/read-reach error reader 2/2 over 75% through=[] sources=[]",
		"roles/read-reach error switcher 2/2 over 75% through=[reader] sources=[]",
	})
}

// TestReachFromCatalog_CountsOnlyPrivilegesOverRows reads an owner that kept
// its implicit privileges on one table and revoked them on the other, and
// privileges that read or change no rows.
func TestReachFromCatalog_CountsOnlyPrivilegesOverRows(t *testing.T) {
	c := qt.New(t)
	live := readCatalog("a", "b")
	live.RolesOutOfScope = []catalog.Role{{Name: "owner"}, {Name: "user"}}
	live.Grants = []catalog.Grant{
		{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "a", Implicit: true},
		{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "b", Implicit: true, IsPartialRevoke: true},
		{Role: "user", Privilege: "REFERENCES", ObjectType: "TABLE", ObjectName: "a"},
		{Role: "user", Privilege: "TRIGGER", ObjectType: "TABLE", ObjectName: "b"},
		{Role: "user", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "public"},
	}

	c.Assert(reachLines(schemasecurity.ReachFromCatalog(live, "", 40, 75)), qt.DeepEquals, []string{
		"roles/read-reach warning owner 1/2 over 40% through=[] sources=[]",
	})
}

// TestAnalyzeReach_SuggestsTheSourceBeforeTheGrants reads a superuser and a
// role reaching as much through grants: a revoke narrows the second, and
// nothing on a relation narrows the first.
func TestAnalyzeReach_SuggestsTheSourceBeforeTheGrants(t *testing.T) {
	c := qt.New(t)
	reach := defaultThresholds(schemasecurity.Reach{
		Relations:  relations("a", "b"),
		Grants:     granted("reader", true, false, "a", "b"),
		Principals: append(principals("reader"), schemasecurity.ReachPrincipal{Name: "admin", Superuser: true}),
	})

	report := schemasecurity.Analyze(&schemamodel.Database{}, schemasecurity.Options{Reach: reach})
	suggestions := make([]string, 0)
	for _, finding := range report.Findings {
		suggestions = append(suggestions, finding.Code+" "+finding.Subject.Name+": "+finding.Suggestion)
	}

	c.Assert(suggestions, qt.DeepEquals, []string{
		"roles/read-reach admin: first take away what detail.reach.sources names -- superuser, a role that reads or " +
			"writes all data, a privilege on every database -- since no revoke on a relation narrows it; then grant " +
			"the role only the relations it needs",
		"roles/read-reach reader: grant the role only the relations it needs, or split it into narrower roles; " +
			"detail.reach.through names the roles it reaches them through",
		"roles/write-reach admin: first take away what detail.reach.sources names -- superuser, a role that reads or " +
			"writes all data, a privilege on every database -- since no revoke on a relation narrows it; then grant " +
			"the role only the relations it needs",
	})
}

// TestReachFromCatalog_SuperuserNeedsASwitch reads two members of a superuser
// role: one may SET ROLE to it, and one only inherits from it, which on
// PostgreSQL passes on its grants but not its superuser bit.
func TestReachFromCatalog_SuperuserNeedsASwitch(t *testing.T) {
	c := qt.New(t)
	live := readCatalog("a", "b", "c")
	live.Roles = []catalog.Role{{Name: "admin", Superuser: true}, {Name: "switcher"}, {Name: "heir"}}
	live.Grants = []catalog.Grant{
		{Role: "admin", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "a"},
		{Role: "admin", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "b"},
	}
	live.RoleMemberships = []catalog.RoleMembership{
		{Role: "admin", Member: "switcher", InheritOption: new(false), SetOption: new(true)},
		{Role: "admin", Member: "heir", InheritOption: new(true), SetOption: new(false)},
	}

	c.Assert(reachLines(schemasecurity.ReachFromCatalog(live, "", 50, 75)), qt.DeepEquals, []string{
		"roles/read-reach error admin 3/3 over 75% through=[] sources=[superuser held by admin]",
		"roles/read-reach warning heir 2/3 over 50% through=[admin] sources=[]",
		"roles/read-reach error switcher 3/3 over 75% through=[admin] sources=[superuser held by admin]",
		"roles/write-reach error admin 3/3 over 75% through=[] sources=[superuser held by admin]",
		"roles/write-reach error switcher 3/3 over 75% through=[admin] sources=[superuser held by admin]",
	})
}
