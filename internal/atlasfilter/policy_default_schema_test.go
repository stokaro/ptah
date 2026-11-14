package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/atlasfilter"
)

// A reader and a source build a policy's identity before any connection is
// known, so a schema they did not write holds PostgreSQL's offline default,
// public. These tests hold a scope to the schema the identity's source wrote:
// the policy a read found on its default schema `reporting` stays with its
// table, and an exclusion takes a policy with the table it names however the
// two are qualified.

// observedPolicyOnTheDefaultSchema is the read of `reporting.audit` under
// `search_path=reporting,public` with `--schemas public,reporting`: the
// reader writes the default schema as empty, on the table and on the policy.
func observedPolicyOnTheDefaultSchema(c *qt.C) *catalog.Database {
	c.Helper()
	return &catalog.Database{
		Tables: []catalog.Table{{Schema: "public", Name: "orders"}, {Name: "audit"}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.ObservedPolicyObject(
			pgpolicy.PolicyRef("", "audit", "audit_visible"),
			pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}, Composition: pgpolicy.Permissive},
		)))),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
	}
}

func TestScopeDatabaseKeepsAPolicyOnTheSessionsDefaultSchema(t *testing.T) {
	c := qt.New(t)
	source := observedPolicyOnTheDefaultSchema(c)

	got, err := atlasfilter.ScopeDatabase(source, atlasfilter.Scope{
		Selections: ownerSelections(), Schemas: []string{"public", "reporting"}, DefaultSchema: "reporting",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.FeatureObjects.Refs(), qt.DeepEquals, source.FeatureObjects.Refs())
}

// TestScopeDatabaseLeavesOutAPolicyWithItsTable is the control for the test
// above: the same policy goes when the scope leaves its table out.
func TestScopeDatabaseLeavesOutAPolicyWithItsTable(t *testing.T) {
	c := qt.New(t)
	source := observedPolicyOnTheDefaultSchema(c)

	got, err := atlasfilter.ScopeDatabase(source, atlasfilter.Scope{
		Selections: ownerSelections(), Schemas: []string{"public"}, DefaultSchema: "reporting",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.FeatureObjects.Refs(), qt.HasLen, 0)
}

func TestScopeGeneratedKeepsAPolicyDeclaredWithoutASchema(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "audit", StructName: "Audit"}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(
			pgpolicy.PolicyRef("", "audit", "audit_visible"), pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll},
		)))),
	}

	got, err := atlasfilter.ScopeGenerated(source, atlasfilter.Scope{
		Selections: ownerSelections(), Schemas: []string{"reporting"}, DefaultSchema: "reporting",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.FeatureObjects.Refs(), qt.DeepEquals, source.FeatureObjects.Refs())
}

// TestExcludeGeneratedTakesAPolicyWithTheTableItNames excludes a table the
// source wrote as `public.audit` and whose policy names it `audit`: under
// the default schema public they are one table, so the policy goes with it.
func TestExcludeGeneratedTakesAPolicyWithTheTableItNames(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Schema: "public", Name: "audit", StructName: "Audit"}, {Schema: "public", Name: "orders", StructName: "Orders"}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(
			pgpolicy.PolicyRef("", "audit", "audit_visible"), pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll},
		)))),
	}

	got, _, err := atlasfilter.ExcludeGeneratedScopeReport(source, atlasfilter.Scope{
		Selections: ownerSelections(), Exclude: []string{"audit"}, DefaultSchema: "public",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Name, qt.Equals, "orders")
	c.Assert(got.FeatureObjects.Refs(), qt.HasLen, 0)
}

func TestExcludePoliciesLeavesRowSecurityUnderTheCaller(t *testing.T) {
	c := qt.New(t)
	observed := &catalog.Database{
		Tables: []catalog.Table{{Name: "tasks", RLSEnabled: true, Facets: must.Must(schemaext.NewFacets(&pgpolicy.ObservedTableState{Enabled: true}))}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.ObservedPolicyObject(
			pgpolicy.PolicyRef("", "tasks", "admin_read"), pgpolicy.ObservedPolicy{Command: pgpolicy.CommandSelect, Composition: pgpolicy.Permissive, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}},
		)))),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
	}
	desired := &schemamodel.Database{
		Tables:           []schemamodel.Table{{Name: "tasks", StructName: "Tasks", Facets: must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: true}))}},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{StructName: "Tasks", Table: "tasks"}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(
			pgpolicy.PolicyRef("", "tasks", "admin_read"), pgpolicy.DesiredPolicy{Command: pgpolicy.CommandSelect},
		)))),
	}
	patterns := []string{"*[type=policy]"}
	current, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(ownerSelections(), observed, patterns, "public")
	c.Assert(err, qt.IsNil)
	declared, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(ownerSelections(), desired, patterns, "public")
	c.Assert(err, qt.IsNil)
	c.Assert(current.FeatureObjects.Refs(), qt.HasLen, 0)
	c.Assert(declared.FeatureObjects.Refs(), qt.HasLen, 0)
	c.Assert(current.Tables[0].RLSEnabled, qt.IsFalse)
	c.Assert(current.Tables[0].Facets.Kinds(), qt.HasLen, 0)
	c.Assert(declared.Tables[0].Facets.Kinds(), qt.HasLen, 0)
	c.Assert(declared.RLSEnabledTables, qt.HasLen, 0)
	c.Assert(observed.Tables[0].RLSEnabled, qt.IsTrue)
	c.Assert(observed.Tables[0].Facets.Kinds(), qt.HasLen, 1)
	c.Assert(desired.Tables[0].Facets.Kinds(), qt.HasLen, 1)
}
