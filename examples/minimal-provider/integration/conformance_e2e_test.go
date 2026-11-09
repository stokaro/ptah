//go:build integration

package integration_test

import (
	"testing"

	"example.com/ptah-tutorial-provider/backend"
	qt "github.com/frankban/quicktest"

	"ptah.run/provider-sdk/conformance"
	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// dropNotes is the change that removes the table createNotes creates.
func dropNotes(id string) *protocol.Change {
	return &protocol.Change{Id: id, Object: &protocol.Reference{Kind: "table", Name: "notes"}, Operation: &protocol.Change_DropTable{DropTable: &protocol.Empty{}}}
}

// The SDK's conformance suite checks the executable as Ptah starts it: the
// handshake, the description and its negotiation, every service, the
// offline calls below, and on a database file sessions, reads, locks, a
// change applied and reverted, and a statement the database refuses.
func TestConformance(t *testing.T) {
	c := qt.New(t)
	executable, digest := build(c)
	_, location := address(c)
	conformance.Run(t, conformance.Config{
		Target:            conformance.Executable(executable, digest),
		ProviderID:        backend.ProviderID,
		RequiredFeatures:  hostRequires,
		CredentialAddress: "tutorial://user:" + conformance.Canary + "@localhost/app.db",
		Calls: []conformance.Call{
			{Name: "render-create", Request: &protocol.RenderRequest{Engine: backend.Engine, Changes: []*protocol.Change{createNotes("create")}}},
			{Name: "render-unsupported-type", Request: &protocol.RenderRequest{Engine: backend.Engine, Changes: []*protocol.Change{unsupportedColumn}}, Refused: true},
			{Name: "analyze", Request: &protocol.AnalyzeSQLRequest{Engine: backend.Engine, Sources: []string{`CREATE TABLE "notes" ("id" INTEGER NOT NULL PRIMARY KEY);`}}},
		},
		Live: &conformance.Live{
			Address: location,
			Changes: []conformance.Change{{
				Name:   "notes",
				Apply:  conformance.Step{Render: &protocol.RenderRequest{Changes: []*protocol.Change{createNotes("create")}}},
				Revert: conformance.Step{Render: &protocol.RenderRequest{Changes: []*protocol.Change{dropNotes("drop")}}},
			}},
			Refused: []*protocol.Unit{{ChangeId: "missing", Body: &protocol.Unit_Sql{Sql: `DROP TABLE "missing";`}}},
		},
	})
}
