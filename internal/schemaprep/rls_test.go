package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaprep"
)

func TestQualifyRLSPolicyForTarget(t *testing.T) {
	c := qt.New(t)
	t.Parallel()
	policy := schemamodel.RLSPolicy{Name: "tenant_policy", Table: "accounts"}

	qualified := schemaprep.QualifyRLSPolicyForTarget(policy, "billing", platform.SQLServer)
	c.Assert(qualified.Name, qt.Equals, "billing.tenant_policy")
	c.Assert(qualified.Table, qt.Equals, "billing.accounts")
	c.Assert(schemaprep.QualifyRLSPolicyForTarget(policy, "billing", platform.Postgres), qt.DeepEquals, policy)
	c.Assert(schemaprep.QualifyRLSPolicyForTarget(policy, "", platform.SQLServer), qt.DeepEquals, policy)
}
