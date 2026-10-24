//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

// documentClient starts the fixture command that reads its engine's SQL
// documents whole.
func documentClient(c *qt.C) *transport.Client {
	c.Helper()
	binary := buildFixture(c, "sql-document")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.ReadSQLSchemaFeature}})
}

// documentRequest is a load of the fixture engine, one document per text.
func documentRequest(client *transport.Client, texts ...string) *protocol.ReadSQLSchemaRequest {
	engine := client.Description().GetEngines()[0]
	request := &protocol.ReadSQLSchemaRequest{Engine: engine.GetId(), TargetFeatures: engine.GetFeatures(),
		Identifiers: &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(engine.GetIdentifiers()), DefaultSchema: engine.GetDefaultSchema()}}
	for i, text := range texts {
		request.Documents = append(request.Documents, &protocol.SQLDocument{Id: []string{"one", "two"}[i], Path: []string{"one.sql", "two.sql"}[i], Text: text})
	}
	return request
}

func TestIndependentProviderReadsSQLDocumentsWhole(t *testing.T) {
	c := qt.New(t)
	client := documentClient(c)
	declaration := client.Description().GetEngines()[0].GetSqlSchema()
	c.Assert(declaration.GetMode(), qt.Equals, protocol.SQLSchemaDeclaration_DOCUMENT)
	c.Assert(declaration.GetHeaderKinds(), qt.DeepEquals, []string{"retention"})
	request := documentRequest(client,
		"-- ptah:not-described retention \"kept\"\nCREATE TABLE items (id INTEGER PRIMARY KEY);\nCREATE TABLE IF NOT EXISTS audit (id INTEGER);\nCREATE RETENTION hourly SECONDS 3600;\n",
		"ALTER TABLE items ADD label TEXT NOT NULL;\nDROP TABLE audit;\nCREATE TABLE audit (id INTEGER, at TEXT);\n",
	)
	request.Limits = []*protocol.SchemaCoverageLimit{{Kind: "retention", Name: "kept"}}
	original := proto.CloneOf(request)

	result, err := client.ReadSQLSchema(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
	c.Assert(result.GetComplete(), qt.IsTrue)
	c.Assert(result.GetTables(), qt.HasLen, 2)
	items, audit := result.GetTables()[0], result.GetTables()[1]
	c.Assert(items.GetName(), qt.Equals, "items")
	c.Assert(items.GetPrimaryKey(), qt.DeepEquals, []string{"id"})
	c.Assert(items.GetColumns(), qt.HasLen, 2)
	c.Assert(items.GetColumns()[1].GetName(), qt.Equals, "label")
	c.Assert(items.GetColumns()[1].GetNullable(), qt.IsFalse)
	c.Assert(audit.GetName(), qt.Equals, "audit")
	c.Assert(audit.GetColumns(), qt.HasLen, 2)
	c.Assert(result.GetNativeObjects(), qt.HasLen, 1)
	c.Assert(result.GetNativeObjects()[0].GetOwner().GetName(), qt.Equals, "hourly")
	c.Assert(string(result.GetNativeObjects()[0].GetPayload()), qt.Equals, `{"seconds":3600}`)
	c.Assert(result.GetDeclarations(), qt.HasLen, 2)
	first, second := result.GetDeclarations()[0], result.GetDeclarations()[1]
	c.Assert(first.GetDocumentId(), qt.Equals, "one")
	c.Assert(first.GetGuards(), qt.HasLen, 1)
	c.Assert(first.GetGuards()[0].GetName(), qt.Equals, "audit")
	c.Assert(first.GetTables()[1].GetGuarded(), qt.IsTrue)
	c.Assert(second.GetDocumentId(), qt.Equals, "two")
	c.Assert(second.GetTables()[0].GetDrop(), qt.IsTrue)
	c.Assert(second.GetObjects(), qt.HasLen, 1)
	c.Assert(second.GetObjects()[0].GetName(), qt.Equals, "audit")
	c.Assert(result.GetTraces(), qt.HasLen, 3)
	c.Assert(result.GetTraces()[1].GetSource().GetDocumentId(), qt.Equals, "two")
	c.Assert(result.GetTraces()[1].GetSource().GetSpan().GetStart().GetLine(), qt.Equals, uint32(3))
	c.Assert(result.GetCoverage().GetKinds()[0].GetKnowledge().GetState(), qt.Equals, protocol.ModelKnowledge_COMPLETE)
	c.Assert(result.GetCoverage().GetSubjects(), qt.HasLen, 1)
	c.Assert(result.GetCoverage().GetSubjects()[0].GetSubject().GetName(), qt.Equals, "kept")
	c.Assert(result.GetCoverage().GetSubjects()[0].GetKnowledge().GetState(), qt.Equals, protocol.ModelKnowledge_UNINSPECTED)
	again, err := client.ReadSQLSchema(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, again), qt.IsTrue)
}

func TestIndependentProviderReadsSQLDocumentsInLoadOrder(t *testing.T) {
	c := qt.New(t)
	client := documentClient(c)
	alter, create := "ALTER TABLE items ADD label TEXT;\n", "CREATE TABLE items (id INTEGER);\n"
	inOrder, err := client.ReadSQLSchema(c.Context(), documentRequest(client, create, alter))
	c.Assert(err, qt.IsNil)
	c.Assert(inOrder.GetComplete(), qt.IsTrue)
	c.Assert(inOrder.GetTables()[0].GetColumns(), qt.HasLen, 2)

	reversed, err := client.ReadSQLSchema(c.Context(), documentRequest(client, alter, create))

	c.Assert(err, qt.IsNil)
	c.Assert(reversed.GetComplete(), qt.IsFalse)
	c.Assert(reversed.GetDiagnostics(), qt.HasLen, 1)
	c.Assert(reversed.GetDiagnostics()[0].GetDocumentId(), qt.Equals, "one")
	c.Assert(reversed.GetDiagnostics()[0].GetCode(), qt.Equals, protocol.SQLDiagnostic_INVALID)
}

func TestIndependentProviderRefusesSQLDocumentsWithPositions(t *testing.T) {
	c := qt.New(t)
	client := documentClient(c)

	result, err := client.ReadSQLSchema(c.Context(), documentRequest(client, "CREATE TABLE a (id INTEGER);\n", "CREATE TABLE b (id INTEGER);\nGRANT x TO y;\n"))

	c.Assert(err, qt.IsNil)
	c.Assert(result.GetComplete(), qt.IsFalse)
	c.Assert(result.GetTables(), qt.HasLen, 0)
	c.Assert(result.GetDeclarations(), qt.HasLen, 0)
	c.Assert(result.GetTraces(), qt.HasLen, 0)
	c.Assert(result.GetCoverage(), qt.IsNil)
	c.Assert(result.GetDiagnostics(), qt.HasLen, 1)
	diagnostic := result.GetDiagnostics()[0]
	c.Assert(diagnostic.GetCode(), qt.Equals, protocol.SQLDiagnostic_UNSUPPORTED)
	c.Assert(diagnostic.GetSeverity(), qt.Equals, protocol.SQLDiagnostic_ERROR)
	c.Assert(diagnostic.GetDocumentId(), qt.Equals, "two")
	c.Assert(diagnostic.GetMessage(), qt.Equals, "unsupported statement GRANT")
	c.Assert(proto.Equal(diagnostic.GetSpan(), &protocol.SourceSpan{
		Start: &protocol.SourcePosition{ByteOffset: 29, Line: 2, Column: 1},
		End:   &protocol.SourcePosition{ByteOffset: 34, Line: 2, Column: 6},
	}), qt.IsTrue)
}

func TestIndependentProviderSQLReadingCancellationExposesNothing(t *testing.T) {
	c := qt.New(t)
	client := documentClient(c)
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	result, err := client.ReadSQLSchema(canceled, documentRequest(client, "CREATE TABLE a (id INTEGER);\n"))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)

	during, stop := context.WithTimeout(c.Context(), 300*time.Millisecond)
	defer stop()
	result, err = client.ReadSQLSchema(during, documentRequest(client, "CREATE TABLE stalled (id INTEGER);\n"))
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(result, qt.IsNil)

	recovered, err := client.ReadSQLSchema(c.Context(), documentRequest(client, "CREATE TABLE a (id INTEGER);\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(recovered.GetComplete(), qt.IsTrue)
}
