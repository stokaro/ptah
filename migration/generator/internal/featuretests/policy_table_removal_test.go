package featuretests_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// policyTableRemoval plans the removal of a table that holds a row-security
// policy, from a database read with the coverage a PostgreSQL read records:
// complete knowledge of policies and of default privileges, which belong to a
// schema or the whole database and never to a table.
func policyTableRemoval(c *qt.C, runtime generator.Runtime) generator.BidirectionalSchemaPlanOptions {
	c.Helper()
	coverage := must.Must(pgpolicy.CompleteCoverage(schemaext.Observed))
	coverage = must.Must(coverage.Combine(must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Observed))))
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "docs", Type: "BASE TABLE",
			Facets: must.Must(schemaext.NewFacets(&pgpolicy.ObservedTableState{Enabled: true, Forced: true})),
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "member_id", DataType: "bigint", IsNullable: "YES", OrdinalPosition: 2},
			}}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.ObservedPolicyObject(
			pgpolicy.PolicyRef("", "docs", "p_docs"),
			pgpolicy.ObservedPolicy{Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{{Name: "doc_reader"}},
				Using: new("(member_id > 0)"), Composition: pgpolicy.Restrictive})))),
		FeatureCoverage: coverage,
	}
	desired := &schemamodel.Database{}
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, []string{"docs"})
	return generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
		Dialect: platform.Postgres, Capabilities: capability.ForDialect(platform.Postgres)}
}

// TestReverseTableRemovalRestoresARowSecurityPolicy_HappyPath plans the
// rollback of dropping a table that holds a policy. The rollback creates the
// table, switches row security on and forces it, and creates the policy with
// its composition, command, role and expression. The default privileges the
// read knows of describe no table, so the recreated table asks no owner of
// theirs to account for it.
func TestReverseTableRemovalRestoresARowSecurityPolicy_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	opts := policyTableRemoval(c, runtime)

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesAdded, qt.HasLen, 1)
	sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, opts.Capabilities, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Matches, `(?s).*CREATE TABLE "docs" \(.*`+
		`ALTER TABLE "docs" ENABLE ROW LEVEL SECURITY;\s+`+
		`ALTER TABLE "docs" FORCE ROW LEVEL SECURITY;\s+`+
		`CREATE POLICY "p_docs" ON "docs" AS RESTRICTIVE FOR SELECT TO "doc_reader"\s+USING \(\(member_id > 0\)\)\s*;\s*`)
}

// vocabularylessRuntime hides the runtime's attached kinds, as a runtime
// composed without them would.
type vocabularylessRuntime struct{ generator.Runtime }

// TestReverseTableRemovalWithoutAttachedKinds_FailurePath plans the same
// rollback on a runtime that cannot name its attached kinds. The restored table
// then keeps a claim of every captured kind, default privileges included, and
// the rollback is refused rather than planned without an owner for them.
func TestReverseTableRemovalWithoutAttachedKinds_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime := vocabularylessRuntime{must.Must(builtin.New())}
	opts := policyTableRemoval(c, runtime)

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `error planning reverse migration: .*no parent planning service for "postgres"/"ptah\.run/pgdefaultprivilege/default-privilege" on table public\.docs`)
	c.Assert(plan, qt.IsNil)
}
