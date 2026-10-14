package oracle

// White-box testing required: the order DropAllTables drops domains in is
// decided by dependentsFirst over two catalog reads, and the exported cleanup
// shows only whether the last statement succeeded, not which order produced
// it.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestDependentsFirst_DropsEachDomainAfterTheDomainsReferencingIt pins the
// order. A flexible domain references the domains it chooses from, and Oracle
// answers ORA-11502 to dropping one of those first, measured on 23.26.3.0.0.
func TestDependentsFirst_DropsEachDomainAfterTheDomainsReferencingIt(t *testing.T) {
	tests := []struct {
		name       string
		domains    []string
		references map[string][]string
		want       []string
	}{
		{name: "no references keeps the listed order", domains: []string{"A", "B", "C"},
			want: []string{"A", "B", "C"}},
		{name: "a flexible domain before its members", domains: []string{"M1", "M2", "Z_FLEX"},
			references: map[string][]string{"Z_FLEX": {"M1", "M2"}},
			want:       []string{"Z_FLEX", "M1", "M2"}},
		{name: "a chain", domains: []string{"A", "B", "C"},
			references: map[string][]string{"B": {"A"}, "C": {"B"}},
			want:       []string{"C", "B", "A"}},
		{name: "a reference from a domain the cleanup does not drop", domains: []string{"A", "B"},
			references: map[string][]string{"ELSEWHERE": {"A"}},
			want:       []string{"A", "B"}},
		{name: "a cycle keeps the rest rather than losing it", domains: []string{"A", "B", "C"},
			references: map[string][]string{"A": {"B"}, "B": {"A"}},
			want:       []string{"C", "A", "B"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			domains := make([]cleanupObject, 0, len(test.domains))
			for _, name := range test.domains {
				domains = append(domains, cleanupObject{kind: "DOMAIN", name: name})
			}

			ordered := dependentsFirst(domains, test.references)

			names := make([]string, 0, len(ordered))
			for _, domain := range ordered {
				names = append(names, domain.name)
			}
			c.Assert(names, qt.DeepEquals, test.want)
		})
	}
}

// TestCleanupObjectQuery_ListsDomains pins that the cleanup lists domains at
// all, and asks which domains reference which.
func TestCleanupObjectQuery_ListsDomains(t *testing.T) {
	c := qt.New(t)
	c.Assert(cleanupObjectQuery, qt.Contains, "'TYPE', 'DOMAIN')")
	c.Assert(domainDependencyQuery, qt.Contains, "d.referenced_type = 'DOMAIN'")
}

// TestCleanupObjectQuery_LeavesAMaterializedViewsTableToTheView pins the
// exclusion that keeps the cleanup from dropping a table that DROP
// MATERIALIZED VIEW already took: measured on 23.26.3.0.0, that DROP TABLE
// answered ORA-00942 and stopped the cleanup.
func TestCleanupObjectQuery_LeavesAMaterializedViewsTableToTheView(t *testing.T) {
	c := qt.New(t)
	c.Assert(cleanupObjectQuery, qt.Contains, "SELECT 1 FROM user_mviews m WHERE m.mview_name = o.object_name")
}
