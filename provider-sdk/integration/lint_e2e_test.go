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
	"google.golang.org/protobuf/proto"
)

func TestIndependentProviderLintsWithoutSession(t *testing.T) {
	c := qt.New(t)
	binary := buildFixture(c, "lint")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	client := start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.LintFeature}})
	c.Assert(client.Description().GetEngines()[0].GetLintRules()[0].GetCode(), qt.Equals, "FX101")
	sources := []*protocol.LintSource{{Name: "1.sql", Sql: "CREATE TABLE a (id INTEGER);\nDROP TABLE a;", Up: true}}

	split, err := client.SplitLintStatements(c.Context(), &protocol.SplitLintRequest{Engine: "fixture", Sources: sources})
	c.Assert(err, qt.IsNil)
	c.Assert(split.Files, qt.HasLen, 1)
	c.Assert(split.Files[0].Statements, qt.HasLen, 2)
	c.Assert(split.Files[0].Statements[1].Line, qt.Equals, uint32(2))
	c.Assert(split.Files[0].Statements[1].Effects, qt.HasLen, 1)
	c.Assert(split.Files[0].Statements[0].Effects, qt.HasLen, 0)

	files := []*protocol.LintFile{{Name: "1.sql", Up: true, Statements: split.Files[0].Statements}}
	annotated, err := client.AnnotateLintStatements(c.Context(), &protocol.AnnotateLintRequest{Engine: "fixture",
		Facts: []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEFUL_DROP}, Sources: sources, Files: files})
	c.Assert(err, qt.IsNil)
	c.Assert(annotated.Files[0].Statements[1].Effects[0].Fact, qt.Equals, protocol.LintFact_LINT_FACT_STATEFUL_DROP)

	request := &protocol.CheckLintRequest{Engine: "fixture", Codes: []string{"FX101"}, Files: files}
	original := proto.CloneOf(request)
	checked, err := client.CheckLintMigrations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(original, request), qt.IsTrue)
	c.Assert(checked.Complete, qt.IsTrue)
	c.Assert(checked.Findings, qt.HasLen, 1)
	c.Assert(checked.Findings[0].Statement, qt.Equals, int32(1))

	request.Codes = []string{"ZZ999"}
	checked, err = client.CheckLintMigrations(c.Context(), request)
	c.Assert(err, qt.IsNotNil)
	c.Assert(checked, qt.IsNil)
}
