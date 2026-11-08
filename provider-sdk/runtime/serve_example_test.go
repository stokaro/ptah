package runtime_test

import (
	"fmt"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

// notesProvider stands for the type that implements a provider's services. A
// real provider writes these methods. This one embeds the interfaces so the
// example stays short, and nothing calls them.
type notesProvider struct {
	provider.Sessions
	provider.Inspector
	provider.Renderer
	provider.Executor
	provider.SQLAnalyzer
}

// ExampleServe is the main function of a provider executable. The description
// offers the services Ptah requires of every provider, and the server joins it
// with the type that implements them. Serve refuses a server where the two
// differ.
//
// Under a host, Serve blocks until the host shuts the process down. Started by
// hand it has no authenticated transport and returns the error the example
// prints. A real main function writes that error to stderr and exits with
// status 1, because Serve reserves stdout for the bootstrap.
func ExampleServe() {
	owner := &notesProvider{}
	err := runtime.Serve(runtime.Server{
		Description: &protocol.Description{
			Revision:         provider.Revision,
			ProviderId:       "example.org/notes",
			Version:          "0.1.0",
			SemanticIdentity: "notes-1",
			Features: []string{
				provider.InspectFeature,
				provider.RenderFeature,
				provider.ExecuteFeature,
				provider.AnalyzeSQLFeature,
			},
			Engines: []*protocol.Engine{{
				Id:            "notes",
				DefaultSchema: "main",
				Features:      []string{provider.CommonTableIdentityFeature},
				Identifiers: &protocol.IdentifierRules{
					TableNames:     protocol.IdentifierRules_EXACT,
					ColumnNames:    protocol.IdentifierRules_EXACT,
					IndexNames:     protocol.IdentifierRules_EXACT,
					IndexNamespace: protocol.IdentifierRules_SCHEMA,
				},
			}},
		},
		Sessions:    owner,
		Inspector:   owner,
		Renderer:    owner,
		Executor:    owner,
		SQLAnalyzer: owner,
	})
	if err != nil {
		fmt.Println(err)
	}
	// Output: provider bootstrap requires authenticated transport
}
