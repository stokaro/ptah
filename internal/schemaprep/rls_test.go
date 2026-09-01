package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaprep"
)

func TestQualifyRLSPolicyForTarget(t *testing.T) {
	t.Parallel()
	policy := schemamodel.RLSPolicy{Name: "tenant_policy", Table: "accounts"}

	qualified := schemaprep.QualifyRLSPolicyForTarget(policy, "billing", platform.SQLServer)
	qt.Assert(t, qualified.Name, qt.Equals, "billing.tenant_policy")
	qt.Assert(t, qualified.Table, qt.Equals, "billing.accounts")
	qt.Assert(t, schemaprep.QualifyRLSPolicyForTarget(policy, "billing", platform.Postgres), qt.DeepEquals, policy)
	qt.Assert(t, schemaprep.QualifyRLSPolicyForTarget(policy, "", platform.SQLServer), qt.DeepEquals, policy)
}
