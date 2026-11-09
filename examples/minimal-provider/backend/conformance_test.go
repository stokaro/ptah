package backend_test

import (
	"testing"

	"example.com/ptah-tutorial-provider/backend"

	"ptah.run/provider-sdk/conformance"
	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// The offline part of the SDK's conformance suite, with the server in the
// test process: no executable to build and no database file to open.
func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Config{
		Target:           conformance.InProcess(backend.Server()),
		ProviderID:       backend.ProviderID,
		RequiredFeatures: hostRequires,
		Calls: []conformance.Call{{
			Name:    "analyze",
			Request: &protocol.AnalyzeSQLRequest{Engine: backend.Engine, Sources: []string{`DROP TABLE "notes";`}},
		}},
	})
}
