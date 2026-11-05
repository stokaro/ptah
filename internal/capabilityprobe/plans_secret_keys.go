package capabilityprobe

import (
	"ptah.run/core/platform/capability"
)

// secretStatement creates the probe's secret. The value is a literal: the
// probe sends its statement as written, and a secret in its own namespace
// protects nothing.
const secretStatement = "CREATE SECRET sk_secret WITH (value = 'probe')"

// withSecretKeys adds the question whether the line creates a secret. A
// secret Ptah models is YDB's, which the YDB provider measures on its own
// lines, so every engine here is asked the statement and its refusal is the
// measurement, as with a changefeed.
func withSecretKeys(p plan) plan {
	p.experiments = append(p.experiments, proven(capability.Secrets, schemaChange{
		change: []string{secretStatement},
	}))
	return p
}
