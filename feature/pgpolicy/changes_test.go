package pgpolicy_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgpolicy"
)

func allCodecs(c *qt.C) schemaext.Registry {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: pgpolicy.Owner,
		Codecs: append(append(pgpolicy.Codecs(), pgpolicy.ChangeCodecs()...), pgpolicy.OperationCodecs()...)})
	c.Assert(err, qt.IsNil)
	return runtime.Codecs()
}

var (
	permissive  = &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: &tenant, Composition: pgpolicy.Permissive}
	restrictive = &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: &tenant, Composition: pgpolicy.Restrictive}
	unchanged   = schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "only the policy's comment changes"}
)

func declared(command pgpolicy.Command, composition pgpolicy.Composition, roles ...pgpolicy.RoleSelector) *pgpolicy.DesiredPolicy {
	return &pgpolicy.DesiredPolicy{Command: command, Composition: composition, Roles: roles, Using: &tenant}
}

// TestPolicyAccess pins what each policy change can do to access, whether or
// not the table enforces row security: a permissive policy reaching more can
// grant rows and a restrictive one reaching more can only hide them, a
// changed expression is unknown, and a role keyword the server resolves at
// creation cannot be compared here.
func TestPolicyAccess(t *testing.T) {
	reader, writer := pgpolicy.RoleSelector{Name: "reader"}, pgpolicy.RoleSelector{Name: "writer"}
	public := pgpolicy.RoleSelector{Keyword: pgpolicy.Public}
	tests := []struct {
		name        string
		before      *pgpolicy.ObservedPolicy
		after       *pgpolicy.DesiredPolicy
		expressions pgpolicy.ExpressionFinding
		want        schemaext.Access
		reason      string
	}{
		{name: "a permissive policy created", after: declared("", ""), want: schemaext.AccessWidens,
			reason: "a permissive policy can admit rows to the roles it names"},
		{name: "a restrictive policy created", after: declared("", pgpolicy.Restrictive), want: schemaext.AccessNarrows,
			reason: "a restrictive policy can hide rows from the roles it names"},
		{name: "a permissive policy dropped", before: permissive, want: schemaext.AccessNarrows,
			reason: "removing a permissive policy can hide the rows it admitted"},
		{name: "a restrictive policy dropped", before: restrictive, want: schemaext.AccessWidens,
			reason: "removing a restrictive policy can reveal the rows it hid"},
		{name: "made restrictive", before: permissive, after: declared("", pgpolicy.Restrictive, reader), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessNarrows, reason: "a permissive policy made restrictive can hide rows it admitted"},
		{name: "made permissive", before: restrictive, after: declared("", "", reader), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessWidens, reason: "a restrictive policy made permissive can admit rows it hid"},
		{name: "a permissive policy narrowed to SELECT", before: permissive, after: declared(pgpolicy.CommandSelect, "", reader), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessNarrows, reason: "the policy no longer applies to commands it applied to"},
		{name: "a restrictive policy narrowed to SELECT", before: restrictive, after: declared(pgpolicy.CommandSelect, pgpolicy.Restrictive, reader), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessWidens, reason: "the policy no longer applies to commands it applied to"},
		{name: "a permissive policy given a role", before: permissive, after: declared("", "", reader, writer), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessWidens, reason: "the policy applies to roles it did not apply to"},
		{name: "a permissive policy given to PUBLIC", before: permissive, after: declared("", "", public), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessWidens, reason: "the policy applies to roles it did not apply to"},
		{name: "a permissive policy taken from PUBLIC to one role", before: &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll,
			Roles: []pgpolicy.RoleSelector{public}, Using: &tenant, Composition: pgpolicy.Permissive}, after: declared("", "", reader), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessNarrows, reason: "the policy no longer applies to roles it applied to"},
		{name: "a restrictive policy given a role", before: restrictive, after: declared("", pgpolicy.Restrictive, reader, writer), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessNarrows, reason: "the policy applies to roles it did not apply to"},
		{name: "a role exchanged", before: permissive, after: declared("", "", writer), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessWidens, reason: "the policy applies to roles it did not apply to"},
		{name: "a role keyword", before: permissive, after: declared("", "", pgpolicy.RoleSelector{Keyword: pgpolicy.CurrentUser}), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessUnknown, reason: "a role keyword the server resolves when the policy is created makes its roles unknown here"},
		{name: "an expression changed", before: permissive, after: declared("", "", reader),
			want: schemaext.AccessUnknown, reason: "a USING or WITH CHECK expression changed, and which rows it admits is not established"},
		{name: "an expression changed and a role added", before: permissive, after: declared("", "", reader, writer),
			want: schemaext.AccessWidens, reason: "the policy applies to roles it did not apply to"},
		{name: "nothing an access rule reads", before: permissive, after: declared("", "", reader), expressions: pgpolicy.ExpressionsSame,
			want: schemaext.AccessUnchanged, reason: "only the policy's comment changes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgpolicy.PolicyAccess(test.before, test.after, test.expressions), qt.Equals,
				schemaext.AccessEffect{Access: test.want, Reason: test.reason})
		})
	}
}

// TestTableStateAccess pins what a change of the switches can do. FORCE
// matters only while row security is enabled.
func TestTableStateAccess(t *testing.T) {
	tests := []struct {
		name   string
		before pgpolicy.ObservedTableState
		after  pgpolicy.DesiredTableState
		want   schemaext.Access
		reason string
	}{
		{name: "enabled", after: pgpolicy.DesiredTableState{Enabled: true}, want: schemaext.AccessNarrows,
			reason: "enabling row security hides every row no policy admits"},
		{name: "disabled", before: pgpolicy.ObservedTableState{Enabled: true}, want: schemaext.AccessWidens,
			reason: "disabling row security reveals every row its policies hid"},
		{name: "forced while enabled", before: pgpolicy.ObservedTableState{Enabled: true}, after: pgpolicy.DesiredTableState{Enabled: true, Forced: true},
			want: schemaext.AccessNarrows, reason: "FORCE ROW LEVEL SECURITY subjects the table's owner to its policies"},
		{name: "unforced while enabled", before: pgpolicy.ObservedTableState{Enabled: true, Forced: true}, after: pgpolicy.DesiredTableState{Enabled: true},
			want: schemaext.AccessWidens, reason: "NO FORCE ROW LEVEL SECURITY exempts the table's owner from its policies"},
		{name: "enabled and unforced", before: pgpolicy.ObservedTableState{Forced: true}, after: pgpolicy.DesiredTableState{Enabled: true},
			want: schemaext.AccessNarrows, reason: "enabling row security hides every row no policy admits"},
		{name: "forced while disabled", after: pgpolicy.DesiredTableState{Forced: true}, want: schemaext.AccessUnchanged,
			reason: "the table enforces row security neither before nor after the change"},
		{name: "disabled and unforced", before: pgpolicy.ObservedTableState{Enabled: true, Forced: true}, want: schemaext.AccessWidens,
			reason: "disabling row security reveals every row its policies hid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgpolicy.TableStateAccess(&test.before, &test.after), qt.Equals, schemaext.AccessEffect{Access: test.want, Reason: test.reason})
		})
	}
}

// TestChanges_RoundTripThroughTheCodecs pins that each change survives its
// codec with its assessment and comment-only finding.
func TestChanges_RoundTripThroughTheCodecs(t *testing.T) {
	c := qt.New(t)
	codecs := allCodecs(c)
	changes := []schemaext.Payload{
		&pgpolicy.PolicyChange{After: declared("", ""), Access: pgpolicy.PolicyAccess(nil, declared("", ""), pgpolicy.ExpressionsSame)},
		&pgpolicy.PolicyChange{Before: permissive, Access: pgpolicy.PolicyAccess(permissive, nil, pgpolicy.ExpressionsSame)},
		&pgpolicy.PolicyChange{Before: permissive, After: &pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: &tenant, Comment: "new"},
			CommentOnly: true, Access: unchanged},
		&pgpolicy.TableStateChange{Before: &pgpolicy.ObservedTableState{}, After: &pgpolicy.DesiredTableState{Enabled: true},
			Access: pgpolicy.TableStateAccess(&pgpolicy.ObservedTableState{}, &pgpolicy.DesiredTableState{Enabled: true})},
	}

	data, err := codecs.Marshal(t.Context(), schemaext.Change, changes)
	c.Assert(err, qt.IsNil)
	decoded, err := codecs.Unmarshal(t.Context(), data)

	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, changes)
}

// The wires the refusal tables vary, one key at a time.
// TestCodecs_AcceptTheWiresTheRefusalsVary pins that each is accepted, so a
// refusal row is refused for the key it changes.
const (
	accessWire       = `{"access":"widens","reason":"r"}`
	policyChangeWire = `{"before":null,"after":{},"comment_only":false,"access":` + accessWire + `}`
	tableChangeWire  = `{"before":{"enabled":false,"forced":false},"after":{"enabled":true,"forced":false},"access":` + accessWire + `}`
)

// TestCodecs_AcceptTheWiresTheRefusalsVary is the acceptance control of the
// refusal tables below.
func TestCodecs_AcceptTheWiresTheRefusalsVary(t *testing.T) {
	operations := pgpolicy.OperationCodecs()
	tests := []struct {
		name  string
		codec schemaext.Codec
		input string
	}{
		{name: "a policy change", codec: pgpolicy.PolicyChangeCodec(), input: policyChangeWire},
		{name: "a table change", codec: pgpolicy.TableStateChangeCodec(), input: tableChangeWire},
		{name: "a policy operation", codec: operations[0], input: `{"schema":"","table":"t","name":"p","change":` + policyChangeWire + `}`},
		{name: "a policy comment operation", codec: operations[1], input: `{"schema":"","table":"t","name":"p","comment":""}`},
		{name: "a table operation", codec: operations[2], input: `{"schema":"","table":"t","change":` + tableChangeWire + `}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, err := test.codec.Decode(json.RawMessage(test.input))

			c.Assert(err, qt.IsNil)
			c.Assert(value, qt.IsNotNil)
		})
	}
}

// TestChanges_RefuseWhatTheyCannotHold pins the change invariants and wire,
// each operand's included: the model's codec checks it, so a key in another
// letter case or an omitted value spelled out is refused inside a change too.
func TestChanges_RefuseWhatTheyCannotHold(t *testing.T) {
	policy, table := pgpolicy.PolicyChangeCodec(), pgpolicy.TableStateChangeCodec()
	access := accessWire
	tests := []struct {
		name  string
		codec schemaext.Codec
		input string
	}{
		{name: "a policy change with neither operand", codec: policy, input: `{"before":null,"after":null,"comment_only":false,"access":` + access + `}`},
		{name: "a policy change without its assessment", codec: policy, input: `{"before":null,"after":{},"comment_only":false}`},
		{name: "a policy change with a null assessment", codec: policy, input: `{"before":null,"after":{},"comment_only":false,"access":null}`},
		{name: "a policy change with an invalid assessment", codec: policy, input: `{"before":null,"after":{},"comment_only":false,"access":{"access":"safe","reason":"r"}}`},
		{name: "a comment-only creation", codec: policy, input: `{"before":null,"after":{},"comment_only":true,"access":` + access + `}`},
		{name: "a policy change with an unknown key", codec: policy, input: `{"before":null,"after":{},"comment_only":false,"access":` + access + `,"extra":1}`},
		{name: "a table change without a before", codec: table, input: `{"before":null,"after":{"enabled":true,"forced":false},"access":` + access + `}`},
		{name: "a table change that moves no switch", codec: table,
			input: `{"before":{"enabled":true,"forced":false},"after":{"enabled":true,"forced":false},"access":` + access + `}`},
		{name: "a policy change with a null comment-only finding", codec: policy, input: `{"before":null,"after":{},"comment_only":null,"access":` + access + `}`},
		{name: "a policy change whose declaration has a key in another letter case", codec: policy,
			input: `{"before":null,"after":{"Comment":"c"},"comment_only":false,"access":` + access + `}`},
		{name: "a policy change whose declaration spells out an empty comment", codec: policy,
			input: `{"before":null,"after":{"comment":""},"comment_only":false,"access":` + access + `}`},
		{name: "a policy change whose observation has a key in another letter case", codec: policy,
			input: `{"before":{"Command":"ALL","roles":[{"name":"r"}],"composition":"permissive"},"after":null,"comment_only":false,"access":` + access + `}`},
		{name: "a table change whose observation has a key in another letter case", codec: table,
			input: `{"before":{"Enabled":true,"forced":false},"after":{"enabled":false,"forced":false},"access":` + access + `}`},
		{name: "a table change whose declaration has a key in another letter case", codec: table,
			input: `{"before":{"enabled":false,"forced":false},"after":{"Enabled":true,"forced":false},"access":` + access + `}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, err := test.codec.Decode(json.RawMessage(test.input))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestChanges_Effects pins the lifecycle effect each change records apart from
// its access assessment.
func TestChanges_Effects(t *testing.T) {
	tests := []struct {
		name   string
		change interface{ Effect() schemaext.Effect }
		want   schemaext.Effect
	}{
		{name: "a creation", change: &pgpolicy.PolicyChange{After: declared("", ""), Access: unchanged},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: "creates a row-security policy"}},
		{name: "a removal", change: &pgpolicy.PolicyChange{Before: permissive, Access: unchanged},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "drops a row-security policy; a rollback recreates it from the captured definition"}},
		{name: "a comment", change: &pgpolicy.PolicyChange{Before: permissive, After: declared("", ""), CommentOnly: true, Access: unchanged},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: "changes only the comment of a row-security policy"}},
		{name: "a change", change: &pgpolicy.PolicyChange{Before: permissive, After: declared("", ""), Access: unchanged},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes a row-security policy"}},
		{name: "an invalid change", change: &pgpolicy.PolicyChange{Access: unchanged}, want: schemaext.Effect{}},
		{name: "switches", change: &pgpolicy.TableStateChange{Before: &pgpolicy.ObservedTableState{}, After: &pgpolicy.DesiredTableState{Enabled: true}, Access: unchanged},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes whether the table's policies apply"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.Effect(), qt.Equals, test.want)
		})
	}
}

// TestOperations_RoundTripThroughTheCodecs pins that each operation survives
// its codec, and that a comment operation reports no access change.
func TestOperations_RoundTripThroughTheCodecs(t *testing.T) {
	c := qt.New(t)
	codecs := allCodecs(c)
	operations := []schemaext.Payload{
		&pgpolicy.PolicyOperation{Schema: "app", Table: "orders", Name: "tenant",
			Change: pgpolicy.PolicyChange{Before: permissive, After: declared(pgpolicy.CommandSelect, ""), Access: pgpolicy.PolicyAccess(permissive, declared(pgpolicy.CommandSelect, ""), pgpolicy.ExpressionsSame)}},
		&pgpolicy.PolicyCommentOperation{Table: "orders", Name: "tenant", Comment: ""},
		&pgpolicy.TableStateOperation{Table: "orders", Change: pgpolicy.TableStateChange{Before: &pgpolicy.ObservedTableState{Enabled: true},
			After: &pgpolicy.DesiredTableState{}, Access: pgpolicy.TableStateAccess(&pgpolicy.ObservedTableState{Enabled: true}, &pgpolicy.DesiredTableState{})}},
	}

	data, err := codecs.Marshal(t.Context(), schemaext.Operation, operations)
	c.Assert(err, qt.IsNil)
	decoded, err := codecs.Unmarshal(t.Context(), data)

	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, operations)
	c.Assert(operations[1].(*pgpolicy.PolicyCommentOperation).Subject(), qt.Equals, pgpolicy.PolicyRef("", "orders", "tenant"))
	c.Assert(operations[1].(*pgpolicy.PolicyCommentOperation).AccessEffect().Access, qt.Equals, schemaext.AccessUnchanged)
}

// TestChanges_EncodeOperandsAsTheirModelsDo pins that an operand encodes to
// the same bytes inside a change as on its own, and a change to the same bytes
// inside an operation: each role list in its canonical order, the server's
// spelling of the roles included, and the value left alone.
func TestChanges_EncodeOperandsAsTheirModelsDo(t *testing.T) {
	c := qt.New(t)
	models := pgpolicy.PolicyCodecs()
	after := &pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Name: "writer"}, {Name: "app"}},
		Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: "writer"}, {Name: "app"}}}}
	before := &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Name: "writer"}, {Name: "app"}},
		Composition: pgpolicy.Permissive}
	change := &pgpolicy.PolicyChange{Before: before, After: after, Access: unchanged}

	encoded, err := pgpolicy.PolicyChangeCodec().Encode(change)
	c.Assert(err, qt.IsNil)
	operation, err := pgpolicy.OperationCodecs()[0].Encode(&pgpolicy.PolicyOperation{Table: "orders", Name: "tenant", Change: *change})
	c.Assert(err, qt.IsNil)

	var changeFields, operationFields map[string]json.RawMessage
	c.Assert(json.Unmarshal(encoded, &changeFields), qt.IsNil)
	c.Assert(json.Unmarshal(operation, &operationFields), qt.IsNil)
	c.Assert(string(changeFields["after"]), qt.Equals, `{"roles":[{"name":"app"},{"name":"writer"}],"normalized":{"roles":[{"name":"app"},{"name":"writer"}]}}`)
	c.Assert(string(changeFields["after"]), qt.Equals, string(must.Must(models[0].Encode(after))))
	c.Assert(string(changeFields["before"]), qt.Equals, string(must.Must(models[1].Encode(before))))
	c.Assert(string(operationFields["change"]), qt.Equals, string(encoded))
	c.Assert(after.Roles[0], qt.Equals, pgpolicy.RoleSelector{Name: "writer"}, qt.Commentf("encoding leaves the value alone"))
	c.Assert(after.Normalized.Roles[0], qt.Equals, pgpolicy.RoleSelector{Name: "writer"}, qt.Commentf("encoding leaves the value alone"))
	c.Assert(before.Roles[0], qt.Equals, pgpolicy.RoleSelector{Name: "writer"}, qt.Commentf("encoding leaves the value alone"))
}

// TestPayloads_SnapshotSharesNothing pins that a snapshot of each change and
// operation is independent of the value it was taken from.
func TestPayloads_SnapshotSharesNothing(t *testing.T) {
	c := qt.New(t)
	codecs := allCodecs(c)
	policyChange := func() *pgpolicy.PolicyChange {
		return &pgpolicy.PolicyChange{
			Before: &pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: new(tenant), Composition: pgpolicy.Permissive},
			After:  &pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: new(tenant), Comment: "c"}, CommentOnly: true, Access: unchanged}
	}
	tableChange := func() *pgpolicy.TableStateChange {
		return &pgpolicy.TableStateChange{Before: &pgpolicy.ObservedTableState{}, After: &pgpolicy.DesiredTableState{Enabled: true}, Access: unchanged}
	}
	policy, table := policyChange(), tableChange()
	policyOperation := &pgpolicy.PolicyOperation{Table: "orders", Name: "tenant", Change: *policyChange()}
	comment := &pgpolicy.PolicyCommentOperation{Table: "orders", Name: "tenant", Comment: "c"}
	tableOperation := &pgpolicy.TableStateOperation{Table: "orders", Change: *tableChange()}

	changes, err := codecs.SnapshotPayloads(t.Context(), schemaext.Change, []schemaext.Payload{policy, table})
	c.Assert(err, qt.IsNil)
	operations, err := codecs.SnapshotPayloads(t.Context(), schemaext.Operation, []schemaext.Payload{policyOperation, comment, tableOperation})
	c.Assert(err, qt.IsNil)
	policy.Before.Roles[0].Name, *policy.After.Using = "writer", "false"
	table.After.Forced = true
	policyOperation.Change.After.Roles[0].Name, *policyOperation.Change.Before.Using = "writer", "false"
	comment.Comment = "other"
	tableOperation.Change.Before.Forced = true

	c.Assert(changes, qt.DeepEquals, []schemaext.Payload{policyChange(), tableChange()})
	c.Assert(operations, qt.DeepEquals, []schemaext.Payload{
		&pgpolicy.PolicyOperation{Table: "orders", Name: "tenant", Change: *policyChange()},
		&pgpolicy.PolicyCommentOperation{Table: "orders", Name: "tenant", Comment: "c"},
		&pgpolicy.TableStateOperation{Table: "orders", Change: *tableChange()},
	})
}

// TestOperations_RefuseWhatTheyCannotHold pins the operation invariants and
// wire, the change's included: its codec checks it, operands and all.
func TestOperations_RefuseWhatTheyCannotHold(t *testing.T) {
	codecs := pgpolicy.OperationCodecs()
	policy, comment, table := codecs[0], codecs[1], codecs[2]
	tests := []struct {
		name  string
		codec schemaext.Codec
		input string
	}{
		{name: "a policy operation with a key in another letter case", codec: policy,
			input: `{"schema":"","Table":"t","name":"p","change":` + policyChangeWire + `}`},
		{name: "a policy operation with a null schema", codec: policy, input: `{"schema":null,"table":"t","name":"p","change":` + policyChangeWire + `}`},
		{name: "a policy operation on no table", codec: policy, input: `{"schema":"","table":"","name":"p","change":` + policyChangeWire + `}`},
		{name: "a policy operation whose declaration has a key in another letter case", codec: policy,
			input: `{"schema":"","table":"t","name":"p","change":{"before":null,"after":{"Comment":"c"},"comment_only":false,"access":` + accessWire + `}}`},
		{name: "a comment operation with a key in another letter case", codec: comment, input: `{"schema":"","table":"t","Name":"p","comment":""}`},
		{name: "a comment operation without its comment", codec: comment, input: `{"schema":"","table":"t","name":"p"}`},
		{name: "a comment operation on no policy", codec: comment, input: `{"schema":"","table":"t","name":"","comment":""}`},
		{name: "a table operation with a key in another letter case", codec: table, input: `{"schema":"","Table":"t","change":` + tableChangeWire + `}`},
		{name: "a table operation on no table", codec: table, input: `{"schema":"","table":" ","change":` + tableChangeWire + `}`},
		{name: "a table operation whose observation has a key in another letter case", codec: table,
			input: `{"schema":"","table":"t","change":{"before":{"Enabled":false,"forced":false},"after":{"enabled":true,"forced":false},"access":` + accessWire + `}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, err := test.codec.Decode(json.RawMessage(test.input))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

func TestOperations_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		operation interface{ Validate() error }
	}{
		{name: "a policy on no table", operation: &pgpolicy.PolicyOperation{Name: "p", Change: pgpolicy.PolicyChange{After: declared("", ""), Access: unchanged}}},
		{name: "a policy with no name", operation: &pgpolicy.PolicyOperation{Table: "t", Change: pgpolicy.PolicyChange{After: declared("", ""), Access: unchanged}}},
		{name: "a policy with an invalid change", operation: &pgpolicy.PolicyOperation{Table: "t", Name: "p"}},
		{name: "a comment on no policy", operation: &pgpolicy.PolicyCommentOperation{Table: "t"}},
		{name: "switches of no table", operation: &pgpolicy.TableStateOperation{Change: pgpolicy.TableStateChange{
			Before: &pgpolicy.ObservedTableState{}, After: &pgpolicy.DesiredTableState{Enabled: true}, Access: unchanged}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.operation.Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}
