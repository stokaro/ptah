// Package backend serves the tutorial engine to Ptah through the provider SDK.
// A tutorial database is a SQLite file that holds tables and columns of a
// narrow grammar. The provider reads them, renders changes to them as SQL,
// runs that SQL and splits it into statements. It refuses whatever it cannot
// describe completely, so a read never reports less than the file holds.
package backend

import (
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// version is the release the provider reports. A release build sets it:
// go build -ldflags "-X example.com/ptah-tutorial-provider/backend.version=0.1.0".
var version = "0.0.0-dev"

const (
	// ProviderID identifies the provider. A manifest entry names the same
	// value, and Ptah refuses an executable that describes another one.
	ProviderID = "example.com/ptah-tutorial-provider"
	// Engine is the engine the provider serves. It is also the scheme of the
	// addresses Open accepts: tutorial:///absolute/path/to/file.db.
	Engine = "tutorial"
	// defaultSchema is the one schema a tutorial database has.
	defaultSchema = "main"
)

// Description is what the provider tells Ptah when Ptah starts it: who it
// is, the engine it serves and the services it offers. Ptah starts a
// provider only when it offers inspection, rendering, execution and SQL
// analysis.
func Description() *protocol.Description {
	return &protocol.Description{
		Revision:         provider.Revision,
		ProviderId:       ProviderID,
		Version:          version,
		SemanticIdentity: "tutorial-1",
		Engines: []*protocol.Engine{{
			Id:            Engine,
			DefaultSchema: defaultSchema,
			Identifiers:   identifierRules(),
			// Ptah plans the tables and columns of this engine with its
			// common planner and asks the provider to render each change.
			Features: []string{provider.CommonTableIdentityFeature, provider.LocksFeature},
		}},
		Features: []string{
			provider.InspectFeature,
			provider.RenderFeature,
			provider.ExecuteFeature,
			provider.AnalyzeSQLFeature,
			// Ptah takes a lock before it applies a plan.
			provider.LocksFeature,
			// A failed call carries a reason the provider wrote.
			provider.ErrorReasonsFeature,
		},
	}
}

// identifierRules says how the engine compares names: SQLite folds ASCII
// letters in table, column and index names, and an index name is unique in
// its schema.
func identifierRules() *protocol.IdentifierRules {
	return &protocol.IdentifierRules{
		TableNames:     protocol.IdentifierRules_ASCII_CASE_INSENSITIVE,
		ColumnNames:    protocol.IdentifierRules_ASCII_CASE_INSENSITIVE,
		IndexNames:     protocol.IdentifierRules_ASCII_CASE_INSENSITIVE,
		IndexNamespace: protocol.IdentifierRules_SCHEMA,
	}
}
