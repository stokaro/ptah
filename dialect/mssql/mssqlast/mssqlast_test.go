package mssqlast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlast"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
)

func operation(change mssqldiff.SecurityPolicy) *mssqlast.SecurityPolicy {
	return &mssqlast.SecurityPolicy{Schema: "rls", Name: "ten]ancy", Change: change}
}

var (
	created = mssqldiff.SecurityPolicy{After: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{{Type: mssqlschema.Filter,
		Function: mssqlschema.ObjectName{Schema: "rls", Name: "fn"}, Arguments: []string{"tenant_id"}, Table: mssqlschema.ObjectName{Schema: "app", Name: "t"}}}},
		Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "starts filtering"}}
	dropped = mssqldiff.SecurityPolicy{Before: &mssqlschema.ObservedSecurityPolicy{Enabled: true, SchemaBinding: true},
		Access: schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "no predicate"}}
)

// TestCodecs_RoundTripAnOperation pins that an operation survives its codec
// through a registered runtime.
func TestCodecs_RoundTripAnOperation(t *testing.T) {
	c := qt.New(t)
	codecs := must.Must(builtin.New()).Codecs()

	data, err := codecs.Marshal(t.Context(), schemaext.Operation, []schemaext.Payload{operation(created)})
	c.Assert(err, qt.IsNil)
	decoded, err := codecs.Unmarshal(t.Context(), data)

	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{operation(created)})
}

// TestCodecs_FailurePath pins the operation codec's refusals, each a typed
// model refusal: a blank name, a change its own codec refuses, and a missing
// key.
func TestCodecs_FailurePath(t *testing.T) {
	const change = `{"before":null,"after":{"predicates":[]},"access":{"access":"unchanged","reason":"nothing"}}`
	tests := []struct {
		name  string
		input string
	}{
		{name: "a blank name", input: `{"schema":"rls","name":" ","change":` + change + `}`},
		{name: "a change without operands", input: `{"schema":"rls","name":"p","change":{"before":null,"after":null,"access":{"access":"unchanged","reason":"x"}}}`},
		{name: "no change", input: `{"schema":"rls","name":"p"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := mssqlast.Codecs()[0].Decode([]byte(test.input))
			var refusal *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestSecurityPolicy_DescribesItself pins what reports read off an operation:
// the action, the escaped name, and the subject.
func TestSecurityPolicy_DescribesItself(t *testing.T) {
	c := qt.New(t)
	kind, name := operation(created).OmissionSubject()

	c.Assert(operation(created).SchemaChange(), qt.DeepEquals, ast.ExtensionChange{Action: ast.ExtensionAdd, Name: "[rls].[ten]]ancy]"})
	c.Assert(operation(dropped).SchemaChange(), qt.DeepEquals, ast.ExtensionChange{Action: ast.ExtensionDrop, Name: "[rls].[ten]]ancy]"})
	c.Assert([]string{kind, name}, qt.DeepEquals, []string{"security policy", "[rls].[ten]]ancy]"})
	c.Assert(operation(created).Subject(), qt.DeepEquals, mssqlschema.SecurityPolicyRef("rls", "ten]ancy"))
	c.Assert(operation(created).AccessEffect(), qt.Equals, created.Access)
}
