//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

func TestIndependentProviderOwnsSQLFraming(t *testing.T) {
	c := qt.New(t)
	binary := buildFixture(c, "provider")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	client := start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.AnalyzeSQLFeature}})
	source := "\nCREATE TABLE \"semi;colon\" (\"i\" INTEGER NOT NULL);\nDROP TABLE \"other\";"
	result, err := client.AnalyzeSQL(c.Context(), &protocol.AnalyzeSQLRequest{Engine: "fixture", Sources: []string{source, " \n"}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 2)
	c.Assert(result.Files[0].Statements, qt.HasLen, 2)
	first := result.Files[0].Statements[0]
	c.Assert(source[first.Start:first.End], qt.Equals, "CREATE TABLE \"semi;colon\" (\"i\" INTEGER NOT NULL);")
	c.Assert(first.Line, qt.Equals, uint32(2))
	c.Assert(result.Files[0].Statements[1].Line, qt.Equals, uint32(3))
	c.Assert(result.Files[1].Statements, qt.HasLen, 0)
	for _, text := range []string{"CREATE TABLE \"x\" (\"i\" INTEGER CHECK (1));", "DROP SECRET x", "-- unsupported comment\nDROP TABLE \"x\";", "CREATE TABLE \"unterminated"} {
		rejected, err := client.AnalyzeSQL(c.Context(), &protocol.AnalyzeSQLRequest{Engine: "fixture", Sources: []string{source, text}})
		c.Assert(err, qt.IsNotNil)
		c.Assert(rejected, qt.IsNil)
	}
	session := open(c, client, "")
	_, err = client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.Id, Schemas: []string{"main"}})
	c.Assert(err, qt.IsNil)
	_, err = client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.Id, Schemas: []string{"other"}})
	c.Assert(err, qt.IsNotNil)
}
