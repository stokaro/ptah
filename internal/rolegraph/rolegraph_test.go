package rolegraph_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/rolegraph"
)

// edge is a membership with no admin option, which is what nearly every row
// compares.
func edge(role, member string) catalog.RoleMembership {
	return catalog.RoleMembership{Role: role, Member: member}
}

// TestForPlanRoles keeps a server-wide role list from replacing an unrelated
// plan while retaining attributes for a role the plan names or reaches.
func TestForPlanRoles(t *testing.T) {
	database := &catalog.Database{
		ConnectedRole: "runner",
		Roles: []catalog.Role{
			{Name: "runner"}, {Name: "group"}, {Name: "recipient"}, {Name: "outside"},
		},
		RoleMemberships: []catalog.RoleMembership{edge("group", "runner")},
	}
	for _, test := range []struct {
		name       string
		statements []string
		capability capability.Capabilities
		want       []catalog.Role
	}{
		{name: "unrelated server role",
			want: database.Roles[:2]},
		{name: "statement recipient",
			statements: []string{`GRANT SELECT ON t TO recipient`},
			want:       database.Roles[:3]},
		{name: "membership capability does not include unrelated roles",
			capability: capability.Of(map[capability.Capability]bool{capability.RoleMembership: true}),
			want:       database.Roles[:2]},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rolegraph.ForPlanRoles(database, test.statements, test.capability), qt.DeepEquals, test.want)
		})
	}
}

// TestForPlanMembershipOptions distinguishes a grant that conveys access from
// a creator's ADMIN-only membership. A statement naming the created role still
// binds the plan to that membership.
func TestForPlanMembershipOptions(t *testing.T) {
	no, yes := false, true
	adminOnly := catalog.RoleMembership{Role: "created", Member: "runner", AdminOption: true,
		InheritOption: &no, SetOption: &no}
	reader := catalog.RoleMembership{Role: "reader", Member: "runner",
		InheritOption: &yes, SetOption: &no}
	database := &catalog.Database{ConnectedRole: "runner", RoleMemberships: []catalog.RoleMembership{adminOnly, reader}}
	for _, test := range []struct {
		name       string
		statements []string
		want       []catalog.RoleMembership
	}{
		{name: "unrelated creator membership", want: []catalog.RoleMembership{reader}},
		{name: "plan names the created role", statements: []string{`DROP ROLE created`},
			want: []catalog.RoleMembership{adminOnly, reader}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rolegraph.ForPlan(database, test.statements, capability.Of(nil)), qt.DeepEquals, test.want)
		})
	}
}

// graph is the shape the rows walk. `app` belongs to `group`, which belongs to
// `top`. `reader` and `writer` hold `app` through `app_users`. `peer` is another
// member of `group`, and `apart` and `apart_group` have no path to `app`.
var graph = []catalog.RoleMembership{
	edge("apart_group", "apart"),
	edge("app", "app_users"),
	edge("app_users", "reader"),
	edge("app_users", "writer"),
	edge("group", "app"),
	edge("group", "peer"),
	edge("top", "group"),
}

func TestRelated_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		edges []catalog.RoleMembership
		roles []string
		want  []catalog.RoleMembership
	}{
		{
			name:  "both directions from one role, transitively",
			edges: graph,
			roles: []string{"app"},
			want: []catalog.RoleMembership{
				edge("app", "app_users"),
				edge("app_users", "reader"),
				edge("app_users", "writer"),
				edge("group", "app"),
				edge("top", "group"),
			},
		},
		{
			name:  "a group's other members are its holders",
			edges: graph,
			roles: []string{"group"},
			want: []catalog.RoleMembership{
				edge("app", "app_users"),
				edge("app_users", "reader"),
				edge("app_users", "writer"),
				edge("group", "app"),
				edge("group", "peer"),
				edge("top", "group"),
			},
		},
		{
			name:  "a member holds its groups and nothing beside them",
			edges: graph,
			roles: []string{"peer"},
			want:  []catalog.RoleMembership{edge("group", "peer"), edge("top", "group")},
		},
		{
			name:  "two roles take the union",
			edges: graph,
			roles: []string{"apart", "peer"},
			want: []catalog.RoleMembership{
				edge("apart_group", "apart"),
				edge("group", "peer"),
				edge("top", "group"),
			},
		},
		{
			name:  "a role with no edge selects nothing",
			edges: graph,
			roles: []string{"lonely"},
			want:  nil,
		},
		{
			name:  "no roles select nothing",
			edges: graph,
			roles: nil,
			want:  nil,
		},
		{
			name:  "no edges select nothing",
			edges: nil,
			roles: []string{"app"},
			want:  nil,
		},
		{
			name:  "a cycle ends the walk",
			edges: []catalog.RoleMembership{edge("a", "b"), edge("b", "a"), edge("c", "d")},
			roles: []string{"a"},
			want:  []catalog.RoleMembership{edge("a", "b"), edge("b", "a")},
		},
		{
			name: "the admin option stays on the edge",
			edges: []catalog.RoleMembership{
				{Role: "group", Member: "app", AdminOption: true},
				edge("apart_group", "apart"),
			},
			roles: []string{"app"},
			want:  []catalog.RoleMembership{{Role: "group", Member: "app", AdminOption: true}},
		},
		{
			name: "two rows of one edge read as one, ordered whatever order they came in",
			edges: []catalog.RoleMembership{
				edge("z", "app"),
				{Role: "a", Member: "app", AdminOption: true},
				edge("a", "app"),
				edge("z", "app"),
			},
			roles: []string{"app"},
			want: []catalog.RoleMembership{
				edge("a", "app"),
				{Role: "a", Member: "app", AdminOption: true},
				edge("z", "app"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rolegraph.Related(test.edges, test.roles), qt.DeepEquals, test.want)
		})
	}
}

func TestNamed_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		edges      []catalog.RoleMembership
		statements []string
		want       []string
	}{
		{
			name:       "a quoted name",
			edges:      []catalog.RoleMembership{edge("group", "app")},
			statements: []string{`GRANT SELECT ON "t" TO "app"`},
			want:       []string{"app"},
		},
		{
			name:       "an unquoted name in another case",
			edges:      []catalog.RoleMembership{edge("group", "app")},
			statements: []string{`GRANT SELECT ON t TO APP`},
			want:       []string{"app"},
		},
		{
			name:       "the name on either end of an edge, read once",
			edges:      []catalog.RoleMembership{edge("group", "app"), edge("top", "group")},
			statements: []string{`CREATE POLICY p ON t TO "group"`},
			want:       []string{"group"},
		},
		{
			name:       "backticks and a string literal",
			edges:      []catalog.RoleMembership{edge("group", "app"), edge("other", "peer")},
			statements: []string{"GRANT SELECT ON `t` TO `app`", `SELECT pg_has_role('other', 'member')`},
			want:       []string{"app", "other"},
		},
		{
			name:       "a name holding characters an identifier does not",
			edges:      []catalog.RoleMembership{edge("group", "v-token-1"), edge("group", "ops@example.com")},
			statements: []string{`GRANT SELECT ON t TO "v-token-1"`, `GRANT SELECT ON t TO "ops@example.com"`},
			want:       []string{"ops@example.com", "v-token-1"},
		},
		{
			name:       "a routine body",
			edges:      []catalog.RoleMembership{edge("group", "app")},
			statements: []string{`CREATE FUNCTION f() RETURNS void AS $$ BEGIN SET ROLE app; END $$ LANGUAGE plpgsql`},
			want:       []string{"app"},
		},
		{
			name:       "a table with the role's name counts",
			edges:      []catalog.RoleMembership{edge("group", "app")},
			statements: []string{`CREATE TABLE "app" (id integer)`},
			want:       []string{"app"},
		},
		{
			name:       "a longer identifier holding the name does not",
			edges:      []catalog.RoleMembership{edge("group", "app")},
			statements: []string{`CREATE TABLE app_events (id integer); CREATE TABLE myapp (id integer); CREATE TABLE app2 (id integer)`},
			want:       nil,
		},
		{
			name:       "a later occurrence after one inside a longer identifier",
			edges:      []catalog.RoleMembership{edge("group", "app")},
			statements: []string{`GRANT SELECT ON app_events TO app`},
			want:       []string{"app"},
		},
		{
			name:       "a name with no statement naming it",
			edges:      []catalog.RoleMembership{edge("group", "app")},
			statements: []string{`CREATE TABLE t (id integer)`},
			want:       nil,
		},
		{
			name:       "no statements",
			edges:      []catalog.RoleMembership{edge("group", "app")},
			statements: nil,
			want:       nil,
		},
		{
			name:       "an edge with an empty end names nothing",
			edges:      []catalog.RoleMembership{edge("", "")},
			statements: []string{`SELECT 1`},
			want:       nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rolegraph.Named(test.edges, test.statements), qt.DeepEquals, test.want)
		})
	}
}

// described builds a read holding graph and the fields each row sets.
func described(change func(*catalog.Database)) *catalog.Database {
	database := &catalog.Database{ConnectedRole: "runner", RoleMemberships: graph}
	change(database)
	return database
}

// TestForPlan_HappyPath has one row per source of a role the plan depends on.
// Each names `peer` or `apart` through that source alone, so a source that stops
// being read drops its row's edge. `runner` holds nothing in the graph.
func TestForPlan_HappyPath(t *testing.T) {
	peerEdges := []catalog.RoleMembership{edge("group", "peer"), edge("top", "group")}
	tests := []struct {
		name       string
		database   *catalog.Database
		statements []string
		want       []catalog.RoleMembership
	}{
		{
			name:     "nothing ties the plan to the graph",
			database: described(func(*catalog.Database) {}),
			want:     nil,
		},
		{
			name: "the role the read ran as",
			database: described(func(database *catalog.Database) {
				database.ConnectedRole = "peer"
			}),
			want: peerEdges,
		},
		{
			name: "a listed role is no tie to the plan",
			database: described(func(database *catalog.Database) {
				database.Roles = []catalog.Role{{Name: "peer"}}
			}),
			want: nil,
		},
		{
			name: "a privilege holder where the read lists every role",
			database: described(func(database *catalog.Database) {
				database.Grants = []catalog.Grant{{Role: "peer"}}
			}),
			want: peerEdges,
		},
		{
			name: "a grantor",
			database: described(func(database *catalog.Database) {
				database.Grants = []catalog.Grant{{Role: "PUBLIC", GrantedBy: "peer"}}
			}),
			want: peerEdges,
		},
		{
			name: "an object owner",
			database: described(func(database *catalog.Database) {
				database.ObjectOwners = []catalog.ObjectOwner{{Kind: "table", Name: "t", Owner: "peer"}}
			}),
			want: peerEdges,
		},
		{
			name: "a routine definer, in its account spelling",
			database: described(func(database *catalog.Database) {
				database.Functions = []catalog.Function{{Name: "f", Definer: "peer@%"}}
			}),
			want: peerEdges,
		},
		{
			name:       "a role a statement names",
			database:   described(func(*catalog.Database) {}),
			statements: []string{`GRANT SELECT ON t TO "peer"`},
			want:       peerEdges,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := rolegraph.ForPlan(test.database, test.statements, capability.Of(nil))
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestForPlan_KeepsEveryMembershipItCannotNarrow holds the two reads no scoping
// applies to. The edges come back in the read's order, which is why they are
// compared to the graph as it was read rather than to a sorted copy.
func TestForPlan_KeepsEveryMembershipItCannotNarrow(t *testing.T) {
	unsorted := []catalog.RoleMembership{edge("z", "y"), edge("a", "b")}
	tests := []struct {
		name         string
		database     *catalog.Database
		capabilities capability.Capabilities
	}{
		{
			name:         "a read that does not say which role it ran as",
			database:     &catalog.Database{RoleMemberships: unsorted},
			capabilities: capability.Of(nil),
		},
		{
			name:         "a target that compares memberships",
			database:     &catalog.Database{ConnectedRole: "runner", RoleMemberships: unsorted},
			capabilities: capability.Of(map[capability.Capability]bool{capability.RoleMembership: true}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := rolegraph.ForPlan(test.database, nil, test.capabilities)
			c.Assert(got, qt.DeepEquals, unsorted)
		})
	}
}
