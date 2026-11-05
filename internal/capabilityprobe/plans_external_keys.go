package capabilityprobe

import (
	"ptah.run/core/platform/capability"
)

// The statements the external object experiments send. The location names a
// host under the reserved .invalid domain: YDB contacts a data source only
// when a query reads through it, and the probe reads nothing through one.
const (
	externalSourceStatement = "CREATE EXTERNAL DATA SOURCE sk_eds WITH (SOURCE_TYPE = 'ObjectStorage', " +
		"LOCATION = 'https://probe.invalid/bucket/', AUTH_METHOD = 'NONE')"
	externalReplaceStatement = "CREATE OR REPLACE EXTERNAL DATA SOURCE sk_eds_replaced WITH (SOURCE_TYPE = " +
		"'ObjectStorage', LOCATION = 'https://probe.invalid/other/', AUTH_METHOD = 'NONE')"
)

// withExternalKeys adds the questions whether the line creates an external
// data source, whether it replaces one with CREATE OR REPLACE, and whether a
// data source names a secret by its path. External objects Ptah models are
// YDB's, which the YDB provider measures on its own lines, so every engine
// here is asked the statements and its refusal is the measurement, as with a
// secret.
func withExternalKeys(p plan) plan {
	p.experiments = append(p.experiments,
		proven(capability.ExternalDataSources, schemaChange{change: []string{externalSourceStatement}}),
		proven(capability.ExternalObjectReplace, schemaChange{change: []string{externalReplaceStatement}}),
		proven(capability.ExternalDataSourceSecretPaths, schemaChange{change: []string{
			"CREATE EXTERNAL DATA SOURCE sk_eds_pg WITH (SOURCE_TYPE = 'PostgreSQL', LOCATION = " +
				"'probe.invalid:5432', DATABASE_NAME = 'd', AUTH_METHOD = 'BASIC', LOGIN = 'u', " +
				"PASSWORD_SECRET_PATH = 'sk_secret')",
		}}),
	)
	return p
}
