package schemaprep

import (
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// QualifyRLSPolicyForTarget puts a SQL Server policy and its target table into
// the schema in which the target table was declared. Other targets retain the
// declaration exactly as written.
func QualifyRLSPolicyForTarget(
	policy schemamodel.RLSPolicy,
	tableSchema, targetPlatform string,
) schemamodel.RLSPolicy {
	if targetPlatform != platform.SQLServer || tableSchema == "" {
		return policy
	}
	policy.Table = tableSchema + "." + policy.Table
	policy.Name = tableSchema + "." + policy.Name
	return policy
}
