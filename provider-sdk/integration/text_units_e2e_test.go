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

const tableOperationKind = "example.org/fixture/table-operation"

// textUnitsClient starts the operation-models program in its native-text
// mode, with a host that lists hostFeatures.
func textUnitsClient(c *qt.C, hostFeatures ...string) *transport.Client {
	c.Helper()
	binary := buildFixture(c, "operation-models")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.RenderFeature, provider.ExecuteFeature}, Environment: []string{"FIXTURE_RENDER_MODE=native-text"}, HostFeatures: hostFeatures})
}

// nativeTableChange is a captured native operation on the table items, as a
// planner hands it to Render: normalized by the provider, in the role and
// with the parent its action takes.
func nativeTableChange(c *qt.C, client *transport.Client, payload string, role protocol.NativeOperation_Role) *protocol.Change {
	c.Helper()
	var definition string
	for _, model := range client.Description().GetEngines()[0].GetValueModels() {
		if model.GetKind() == tableOperationKind {
			definition = model.GetDefinitionSha256()
		}
	}
	table := &protocol.Reference{Engine: "fixture", Kind: "table", Name: "items"}
	normalized, err := client.NormalizeValues(c.Context(), &protocol.NormalizeValuesRequest{Engine: "fixture", Values: []*protocol.ValueInput{{
		Id: "0", Representation: protocol.ValueInput_OPERATION, Value: &protocol.Envelope{ProviderId: "example.org/fixture", Kind: tableOperationKind,
			SchemaVersion: 1, Encoding: "json", DefinitionSha256: definition, Owner: table, Payload: []byte(payload)},
	}}})
	c.Assert(err, qt.IsNil)
	native := &protocol.NativeOperation{Value: normalized.GetValues()[0], Role: role}
	if role == protocol.NativeOperation_ALTER_TABLE {
		native.Parent = table
	}
	return &protocol.Change{Id: "0", Object: table, Operation: &protocol.Change_Native{Native: native}}
}

func textRender(change *protocol.Change, form protocol.RenderRequest_Form) *protocol.RenderRequest {
	return &protocol.RenderRequest{Engine: "fixture", Changes: []*protocol.Change{change}, Capabilities: map[string]bool{"transactional_ddl": false}, Form: form}
}

// TestTextUnits_HappyPath renders a native operation as SQL text when the host
// asks for text, and the provider's Execute runs that text as a file would
// hold it: the column it adds is there when the database is read back. The
// same change in the form that takes native operations stays native.
func TestTextUnits_HappyPath(t *testing.T) {
	c := qt.New(t)
	client := textUnitsClient(c, provider.TextUnitsFeature)
	session := open(c, client, "")
	created, err := client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: session.GetId(), RequestId: "create", Units: []*protocol.Unit{
		{ChangeId: "create", Body: &protocol.Unit_Sql{Sql: `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY);`}},
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(created.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	change := nativeTableChange(c, client, `{"action":"add-column","column":{"name":"added","type":"REAL","nullable":true}}`, protocol.NativeOperation_ALTER_TABLE)

	native, err := client.Render(c.Context(), textRender(change, protocol.RenderRequest_ANY))
	c.Assert(err, qt.IsNil)
	c.Assert(native.GetUnits()[0].GetNative(), qt.IsNotNil)
	text, err := client.Render(c.Context(), textRender(change, protocol.RenderRequest_TEXT))
	c.Assert(err, qt.IsNil)
	c.Assert(text.GetUnits(), qt.HasLen, 1)
	c.Assert(text.GetUnits()[0].GetSql(), qt.Equals, `ALTER TABLE "items" ADD COLUMN "added" REAL;`)

	applied, err := client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: session.GetId(), RequestId: "text", Units: []*protocol.Unit{
		{ChangeId: "0", Body: &protocol.Unit_Sql{Sql: text.GetUnits()[0].GetSql()}},
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(applied.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	schema, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.GetId()})
	c.Assert(err, qt.IsNil)
	columns := schema.GetTables()[0].GetColumns()
	c.Assert(columns, qt.HasLen, 2)
	c.Assert(columns[1].GetName(), qt.Equals, "added")
}

// TestTextUnits_FailurePath refuses, with no unit, a change the provider runs
// only as a native operation when the host asks for text, and refuses to send
// the text form to a provider a host did not negotiate text units with.
func TestTextUnits_FailurePath(t *testing.T) {
	t.Run("a change without a text form", func(t *testing.T) {
		c := qt.New(t)
		client := textUnitsClient(c, provider.TextUnitsFeature)
		change := nativeTableChange(c, client, `{"action":"rename","to":"next"}`, protocol.NativeOperation_ALTER_TABLE)

		result, err := client.Render(c.Context(), textRender(change, protocol.RenderRequest_TEXT))

		c.Assert(err, qt.IsNil)
		c.Assert(result.GetComplete(), qt.IsFalse)
		c.Assert(result.GetUnits(), qt.HasLen, 0)
		c.Assert(result.GetRefusal().GetReason(), qt.Equals, protocol.RenderRefusal_NO_TEXT_FORM)
		c.Assert(result.GetRefusal().GetChangeId(), qt.Equals, "0")
		c.Assert(result.GetRefusal().GetMessage(), qt.Equals, "the fixture runs a table rename only as a native operation")
	})
	t.Run("a host that did not negotiate text units", func(t *testing.T) {
		c := qt.New(t)
		client := textUnitsClient(c)
		change := nativeTableChange(c, client, `{"action":"add-column","column":{"name":"added","type":"REAL","nullable":true}}`, protocol.NativeOperation_ALTER_TABLE)

		result, err := client.Render(c.Context(), textRender(change, protocol.RenderRequest_TEXT))

		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(result, qt.IsNil)
	})
}
