package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/atlasfilter"
)

// PostgreSQL scopes a policy's name to its table, so two tables may each hold
// a policy named p. These tests hold an exclusion to naming one policy on one
// table, as it names one index on one table: `notes.p[type=policy]` or
// `public.notes.p[type=policy]`. The fixture is the two tables of the public
// schema, notes holding the policies p and notes_owner, and other holding a
// policy p of its own, on both sides of a comparison.
//
// integration/policy_selector_e2e_test.go runs the same selectors through the
// shipped binaries against PostgreSQL.

// selectorPolicies are the policies the fixture holds on each side, as
// table and policy name. schema is the schema the source writes, which is
// empty where the source leaves the table unqualified.
var selectorPolicies = [][2]string{{"notes", "p"}, {"notes", "notes_owner"}, {"other", "p"}}

func selectorPolicyRefs(schema string) []objectidentity.ID {
	refs := make([]objectidentity.ID, 0, len(selectorPolicies))
	for _, policy := range selectorPolicies {
		refs = append(refs, pgpolicy.PolicyRef(schema, policy[0], policy[1]))
	}
	return refs
}

func observedSelectorPolicies(c *qt.C, schema string) *catalog.Database {
	c.Helper()
	objects := make([]schemaext.Object, 0, len(selectorPolicies))
	for _, ref := range selectorPolicyRefs(schema) {
		objects = append(objects, must.Must(pgpolicy.ObservedPolicyObject(ref, pgpolicy.ObservedPolicy{
			Command: pgpolicy.CommandSelect, Composition: pgpolicy.Permissive, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}},
		})))
	}
	return &catalog.Database{
		Tables:          []catalog.Table{{Schema: schema, Name: "notes"}, {Schema: schema, Name: "other"}},
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
	}
}

func desiredSelectorPolicies(c *qt.C, schema string) *schemamodel.Database {
	c.Helper()
	objects := make([]schemaext.Object, 0, len(selectorPolicies))
	for _, ref := range selectorPolicyRefs(schema) {
		objects = append(objects, must.Must(pgpolicy.DesiredPolicyObject(ref, pgpolicy.DesiredPolicy{Command: pgpolicy.CommandSelect})))
	}
	return &schemamodel.Database{
		Tables:         []schemamodel.Table{{Schema: schema, Name: "notes", StructName: "Notes"}, {Schema: schema, Name: "other", StructName: "Other"}},
		FeatureObjects: must.Must(schemaext.NewObjects(objects...)),
	}
}

// policyPaths names each policy as schema.table.policy, and is nil for no
// policy.
func policyPaths(refs []objectidentity.ID) []string {
	var paths []string
	for _, ref := range refs {
		paths = append(paths, ref.Path())
	}
	return paths
}

// realmScope is the scope of a run over every schema of a PostgreSQL
// database, the scope `ptah schema compare` and a ptah-compat URL naming no
// schema give a pattern.
func realmScope(exclude ...string) atlasfilter.Scope {
	return atlasfilter.Scope{Selections: ownerSelections(), Exclude: exclude, DefaultSchema: "public", RealmRelativePatterns: true}
}

func TestExcludeNamesAPolicyThroughItsTable_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		// schema is the schema both sources write the tables and policies in.
		schema string
		want   []string
	}{
		{
			name:    "table and policy",
			exclude: []string{"notes.p[type=policy]"},
			schema:  "public",
			want:    []string{"public.notes.notes_owner", "public.other.p"},
		},
		{
			name:    "schema, table and policy",
			exclude: []string{"public.notes.p[type=policy]"},
			schema:  "public",
			want:    []string{"public.notes.notes_owner", "public.other.p"},
		},
		{
			name:    "the same name on the other table",
			exclude: []string{"public.other.p[type=policy]"},
			schema:  "public",
			want:    []string{"public.notes.notes_owner", "public.notes.p"},
		},
		{
			name:    "table and policy, on unqualified sources",
			exclude: []string{"notes.p[type=policy]"},
			want:    []string{"public.notes.notes_owner", "public.other.p"},
		},
		{
			name:    "schema, table and policy, on unqualified sources",
			exclude: []string{"public.notes.p[type=policy]"},
			want:    []string{"public.notes.notes_owner", "public.other.p"},
		},
		{
			name:    "every policy of one table",
			exclude: []string{"notes.*[type=policy]"},
			schema:  "public",
			want:    []string{"public.other.p"},
		},
		{
			name:    "one name on every table",
			exclude: []string{"*.p[type=policy]"},
			schema:  "public",
			want:    []string{"public.notes.notes_owner"},
		},
		{
			name:    "every policy of the schema",
			exclude: []string{"public.*[type=policy]"},
			schema:  "public",
		},
		{
			name:    "every policy of every schema",
			exclude: []string{"*.*[type=policy]"},
			schema:  "public",
		},
		{
			name:    "every policy",
			exclude: []string{"*[type=policy]"},
			schema:  "public",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			observed := observedSelectorPolicies(c, test.schema)
			desired := desiredSelectorPolicies(c, test.schema)

			current, currentReport, currentErr := atlasfilter.ExcludeDatabaseScopeReport(observed, realmScope(test.exclude...))
			declared, declaredReport, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(desired, realmScope(test.exclude...))

			c.Assert(currentErr, qt.IsNil)
			c.Assert(declaredErr, qt.IsNil)
			c.Assert(policyPaths(current.FeatureObjects.Refs()), qt.DeepEquals, test.want)
			c.Assert(policyPaths(declared.FeatureObjects.Refs()), qt.DeepEquals, test.want)
			c.Assert(currentReport.Unmatched, qt.IsNil)
			c.Assert(declaredReport.Unmatched, qt.IsNil)
			c.Assert(policyPaths(observed.FeatureObjects.Refs()), qt.HasLen, len(selectorPolicies))
		})
	}
}

// TestExcludeNamesAPolicyOfASchemaBoundRun is the scope of a run bound to one
// schema, where a pattern's first part is a table: `notes.p` names the
// policy, and the schema is the connection's.
func TestExcludeNamesAPolicyOfASchemaBoundRun(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{"notes.p[type=policy]"}, DefaultSchema: "public"}

	current, currentReport, currentErr := atlasfilter.ExcludeDatabaseScopeReport(observedSelectorPolicies(c, ""), scope)
	declared, declaredReport, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(desiredSelectorPolicies(c, ""), scope)

	c.Assert(currentErr, qt.IsNil)
	c.Assert(declaredErr, qt.IsNil)
	c.Assert(policyPaths(current.FeatureObjects.Refs()), qt.DeepEquals, []string{"public.notes.notes_owner", "public.other.p"})
	c.Assert(policyPaths(declared.FeatureObjects.Refs()), qt.DeepEquals, []string{"public.notes.notes_owner", "public.other.p"})
	c.Assert(currentReport.Unmatched, qt.IsNil)
	c.Assert(declaredReport.Unmatched, qt.IsNil)
}

// TestExcludeReportsAPolicySelectorThatNamesNothing keeps every policy and
// reports the selector, which `schema apply` refuses and the comparison verbs
// print as a warning. A policy named with its schema and no table is one of
// them: it names no policy on one table, and would name the policies of
// every table that use the name.
func TestExcludeReportsAPolicySelectorThatNamesNothing(t *testing.T) {
	tests := []struct {
		name    string
		exclude string
	}{
		{name: "schema and policy", exclude: "public.p[type=policy]"},
		{name: "policy alone", exclude: "p[type=policy]"},
		{name: "a table that does not exist", exclude: "public.missing.p[type=policy]"},
		{name: "a policy the table does not hold", exclude: "notes.missing[type=policy]"},
		{name: "a policy of another schema", exclude: "app.notes.p[type=policy]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			observed := observedSelectorPolicies(c, "public")
			desired := desiredSelectorPolicies(c, "public")

			current, currentReport, currentErr := atlasfilter.ExcludeDatabaseScopeReport(observed, realmScope(test.exclude))
			declared, declaredReport, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(desired, realmScope(test.exclude))

			c.Assert(currentErr, qt.IsNil)
			c.Assert(declaredErr, qt.IsNil)
			c.Assert(currentReport.Unmatched, qt.DeepEquals, []string{test.exclude})
			c.Assert(declaredReport.Unmatched, qt.DeepEquals, []string{test.exclude})
			c.Assert(policyPaths(current.FeatureObjects.Refs()), qt.DeepEquals, policyPaths(observed.FeatureObjects.Refs()))
			c.Assert(policyPaths(declared.FeatureObjects.Refs()), qt.DeepEquals, policyPaths(desired.FeatureObjects.Refs()))
		})
	}
}

// TestExcludeOfATableStillAsksItsPolicySelector excludes notes and one of its
// policies. The policy leaves with its table before the policy selectors are
// asked, and the second selector still named a policy, so neither side
// reports it.
func TestExcludeOfATableStillAsksItsPolicySelector(t *testing.T) {
	c := qt.New(t)
	scope := realmScope("public.notes", "notes.p[type=policy]")

	current, currentReport, currentErr := atlasfilter.ExcludeDatabaseScopeReport(observedSelectorPolicies(c, "public"), scope)
	declared, declaredReport, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(desiredSelectorPolicies(c, "public"), scope)

	c.Assert(currentErr, qt.IsNil)
	c.Assert(declaredErr, qt.IsNil)
	c.Assert(policyPaths(current.FeatureObjects.Refs()), qt.DeepEquals, []string{"public.other.p"})
	c.Assert(policyPaths(declared.FeatureObjects.Refs()), qt.DeepEquals, []string{"public.other.p"})
	c.Assert(currentReport.Unmatched, qt.IsNil)
	c.Assert(declaredReport.Unmatched, qt.IsNil)
}

// TestExcludeOfATableReportsAPolicySelectorItDoesNotHold is the control for
// the test above: a policy notes does not hold is still reported.
func TestExcludeOfATableReportsAPolicySelectorItDoesNotHold(t *testing.T) {
	c := qt.New(t)
	scope := realmScope("public.notes", "notes.missing[type=policy]")

	_, currentReport, currentErr := atlasfilter.ExcludeDatabaseScopeReport(observedSelectorPolicies(c, "public"), scope)
	_, declaredReport, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(desiredSelectorPolicies(c, "public"), scope)

	c.Assert(currentErr, qt.IsNil)
	c.Assert(declaredErr, qt.IsNil)
	c.Assert(currentReport.Unmatched, qt.DeepEquals, []string{"notes.missing[type=policy]"})
	c.Assert(declaredReport.Unmatched, qt.DeepEquals, []string{"notes.missing[type=policy]"})
}

// TestIncludeKeepsThePoliciesWithTheirTable holds the include path to its
// contract: an include of notes keeps both of its policies and not the
// policy p on other.
func TestIncludeKeepsThePoliciesWithTheirTable(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"public.notes"}, DefaultSchema: "public"}

	current, currentErr := atlasfilter.ScopeDatabase(observedSelectorPolicies(c, "public"), scope)
	declared, declaredErr := atlasfilter.ScopeGenerated(desiredSelectorPolicies(c, "public"), scope)

	c.Assert(currentErr, qt.IsNil)
	c.Assert(declaredErr, qt.IsNil)
	c.Assert(policyPaths(current.FeatureObjects.Refs()), qt.DeepEquals, []string{"public.notes.notes_owner", "public.notes.p"})
	c.Assert(policyPaths(declared.FeatureObjects.Refs()), qt.DeepEquals, []string{"public.notes.notes_owner", "public.notes.p"})
}

// TestIncludeOfAPolicy_FailurePath names a policy in an include, through its
// table and on its own. A policy rides along with its table, so both are
// refused, as on every other spelling.
func TestIncludeOfAPolicy_FailurePath(t *testing.T) {
	tests := []struct {
		name, include, wantErr string
	}{
		{
			name:    "through its table",
			include: "public.notes.p[type=policy]",
			wantErr: `unsupported Atlas include selector "public\.notes\.p\[type=policy\]": policy resources ride along with their parent and cannot be included on their own`,
		},
		{
			name:    "on its own",
			include: "p[type=policy]",
			wantErr: `unsupported Atlas include selector "p\[type=policy\]": policy resources ride along with their parent and cannot be included on their own`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{Selections: ownerSelections(), Include: []string{test.include}, DefaultSchema: "public"}

			current, currentErr := atlasfilter.ScopeDatabase(observedSelectorPolicies(c, "public"), scope)
			declared, declaredErr := atlasfilter.ScopeGenerated(desiredSelectorPolicies(c, "public"), scope)

			c.Assert(currentErr, qt.ErrorMatches, test.wantErr)
			c.Assert(declaredErr, qt.ErrorMatches, test.wantErr)
			c.Assert(current, qt.IsNil)
			c.Assert(declared, qt.IsNil)
		})
	}
}
