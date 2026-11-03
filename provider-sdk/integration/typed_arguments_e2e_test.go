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

// typedArguments binds an Int32, a null of an optional Uint64 and a list of
// two Utf8 elements.
func typedArguments() *protocol.RunStatementRequest {
	return &protocol.RunStatementRequest{SessionId: "session", RequestId: "typed", Sql: "SELECT ?, ?, ?", Access: protocol.RunStatementRequest_READ_WRITE,
		MaxRows: 10, TypedArguments: []*protocol.StatementArgument{
			{Type: &protocol.ArgumentType{Scalar: protocol.ArgumentType_INT32}, Value: &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: -5}}},
			{Type: &protocol.ArgumentType{Scalar: protocol.ArgumentType_UINT64, Optional: true}, Value: &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}},
			{Type: &protocol.ArgumentType{Scalar: protocol.ArgumentType_STRING, List: true}, Elements: []*protocol.DataValue{
				{Value: &protocol.DataValue_StringValue{StringValue: "CZ"}}, {Value: &protocol.DataValue_StringValue{StringValue: "SK"}}}},
		}}
}

// An independent provider receives each argument with its type from a host
// that lists the feature. A host that does not list it cannot send typed
// arguments, and nothing reaches the provider.
func TestIndependentProviderReadsTypedArgumentsOnlyWhenNegotiated(t *testing.T) {
	c := qt.New(t)
	binary := buildSDKFixture(c, "sdk-typed-arguments")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	options := transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.StatementsFeature}, HostFeatures: []string{provider.TypedArgumentsFeature}}

	typed := start(c, options)
	c.Assert(typed.Description().Features, qt.Contains, provider.TypedArgumentsFeature)
	result, err := typed.RunStatement(c.Context(), typedArguments())
	c.Assert(err, qt.IsNil)
	types := make([][]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		types = append(types, []any{row.Values[0].GetStringValue(), row.Values[1].GetBoolValue(), row.Values[2].GetBoolValue()})
	}
	c.Assert(types, qt.DeepEquals, [][]any{{"INT32", false, false}, {"UINT64", true, false}, {"STRING", false, true}})
	c.Assert(result.Rows[0].Values[3].GetInt64Value(), qt.Equals, int64(-5))
	c.Assert(result.Rows[1].Values[3].GetNull(), qt.IsNotNil)
	c.Assert(result.Rows[2].Values[3].GetInt64Value(), qt.Equals, int64(2))

	options.HostFeatures = nil
	plain := start(c, options)
	c.Assert(plain.Description().Features, qt.Not(qt.Contains), provider.TypedArgumentsFeature)
	result, err = plain.RunStatement(c.Context(), typedArguments())
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*typed statement arguments were not negotiated`)
	c.Assert(result, qt.IsNil)
}
