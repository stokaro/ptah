package mssqlschema_test

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine"
)

func registry(c *qt.C) schemaext.Registry {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: mssqlschema.Owner, Codecs: mssqlschema.Codecs()})
	c.Assert(err, qt.IsNil)
	return runtime.Codecs()
}

var (
	tenantFunction = mssqlschema.ObjectName{Schema: "rls", Name: "fn_tenant"}
	orders         = mssqlschema.ObjectName{Schema: "app", Name: "orders"}
	invoices       = mssqlschema.ObjectName{Schema: "billing", Name: "invoices"}
	off            = false
	on             = true
)

// filter and block build the bindings the tests use.
func filter(table mssqlschema.ObjectName, arguments ...string) mssqlschema.Predicate {
	return mssqlschema.Predicate{Type: mssqlschema.Filter, Function: tenantFunction, Arguments: arguments, Table: table}
}

func block(table mssqlschema.ObjectName, operation mssqlschema.BlockOperation, arguments ...string) mssqlschema.Predicate {
	return mssqlschema.Predicate{Type: mssqlschema.Block, Function: tenantFunction, Arguments: arguments, Table: table, Operation: operation}
}

// TestCodecs_RoundTripEveryValue pins that each representation survives its
// codec unchanged. The values a policy's meaning hangs on are each present: a
// disabled policy binding two tables of two schemas, BEFORE UPDATE beside AFTER
// UPDATE on one table, a block predicate for every operation, an expression
// argument, a function taking none, schema binding off and replication left
// out.
func TestCodecs_RoundTripEveryValue(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "a disabled declaration binding two tables", representation: schemaext.Desired, value: &mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{
				filter(orders, "tenant_id"),
				block(orders, mssqlschema.AfterUpdate, "tenant_id"),
				block(orders, mssqlschema.BeforeUpdate, "tenant_id"),
				filter(invoices, "CAST(tenant AS int) + 0"),
				block(invoices, ""),
			},
			Enabled: &off, SchemaBinding: &off, NotForReplication: true, StructName: "Tenancy",
		}},
		{name: "a declaration taking every default", representation: schemaext.Desired, value: &mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")},
		}},
		{name: "a declaration naming the defaults", representation: schemaext.Desired, value: &mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.AfterInsert, "tenant_id"), block(orders, mssqlschema.BeforeDelete, "tenant_id")},
			Enabled:    &on, SchemaBinding: &on,
		}},
		{name: "a disabled observation", representation: schemaext.Observed, value: &mssqlschema.ObservedSecurityPolicy{
			Predicates:    []mssqlschema.Predicate{filter(orders, "[tenant_id]"), block(invoices, mssqlschema.AfterUpdate, "[tenant]")},
			SchemaBinding: true, NotForReplication: true,
		}},
		{name: "an observation holding no predicate", representation: schemaext.Observed, value: &mssqlschema.ObservedSecurityPolicy{
			Enabled: true, SchemaBinding: true,
		}},
		{name: "a declaration holding no predicate", representation: schemaext.Desired, value: &mssqlschema.DesiredSecurityPolicy{
			Enabled: &off,
		}},
		{name: "a declaration the server spelled", representation: schemaext.Desired, value: &mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id"), block(orders, mssqlschema.AfterInsert, "CAST(tenant AS int) + 0")},
			Normalized: []mssqlschema.Predicate{filter(orders, "[tenant_id]"), block(orders, mssqlschema.AfterInsert, "CONVERT([int],[tenant])+(0)")},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codecs := registry(c)

			data, err := codecs.Marshal(t.Context(), test.representation, []schemaext.Payload{test.value})
			c.Assert(err, qt.IsNil)
			decoded, err := codecs.Unmarshal(t.Context(), data)

			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.HasLen, 1)
			c.Assert(test.value.Equal(decoded[0].(schemaext.Value)), qt.IsTrue, qt.Commentf("%s", data))
		})
	}
}

// TestCodecs_EncodePredicatesAsASet pins the canonical encoding: predicates
// have no meaningful order, so two policies listing the same bindings
// differently encode to the same bytes and are equal, ordered by table, type
// and operation. An omitted state, schema binding and argument list stay
// omitted.
func TestCodecs_EncodePredicatesAsASet(t *testing.T) {
	c := qt.New(t)
	codecs := registry(c)
	first := &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		block(orders, mssqlschema.BeforeUpdate, "tenant_id"), filter(invoices), filter(orders, "tenant_id"),
	}}
	second := &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		filter(orders, "tenant_id"), filter(invoices), block(orders, mssqlschema.BeforeUpdate, "tenant_id"),
	}}

	encoded := must.Must(codecs.Encode(t.Context(), schemaext.Desired, []schemaext.Payload{first, second}))

	c.Assert(string(encoded[0].Payload), qt.Equals, string(encoded[1].Payload))
	c.Assert(string(encoded[0].Payload), qt.Equals, `{"predicates":[`+
		`{"arguments":["tenant_id"],"function":{"name":"fn_tenant","schema":"rls"},"operation":"BEFORE UPDATE","table":{"name":"orders","schema":"app"},"type":"BLOCK"},`+
		`{"arguments":["tenant_id"],"function":{"name":"fn_tenant","schema":"rls"},"table":{"name":"orders","schema":"app"},"type":"FILTER"},`+
		`{"function":{"name":"fn_tenant","schema":"rls"},"table":{"name":"invoices","schema":"billing"},"type":"FILTER"}]}`)
	c.Assert(first.Equal(second), qt.IsTrue)
	c.Assert(first.Predicates[0], qt.DeepEquals, block(orders, mssqlschema.BeforeUpdate, "tenant_id"), qt.Commentf("encoding leaves the value alone"))
}

// TestCodecs_EncodeAnEmptyPolicyAsAList pins that a policy holding no
// predicate names an empty list, which both definitions require, rather than
// null. SQL Server creates such a policy, and keeps one whose last predicate
// was dropped.
func TestCodecs_EncodeAnEmptyPolicyAsAList(t *testing.T) {
	c := qt.New(t)
	codecs := registry(c)

	observed := must.Must(codecs.Encode(t.Context(), schemaext.Observed, []schemaext.Payload{&mssqlschema.ObservedSecurityPolicy{Enabled: true}}))
	desired := must.Must(codecs.Encode(t.Context(), schemaext.Desired, []schemaext.Payload{&mssqlschema.DesiredSecurityPolicy{}}))

	c.Assert(string(observed[0].Payload), qt.Equals, `{"enabled":true,"not_for_replication":false,"predicates":[],"schema_binding":false}`)
	c.Assert(string(desired[0].Payload), qt.Equals, `{"predicates":[]}`)
}

// TestEqual_TellsBindingsApart pins what equality reads: a policy differs from
// another by an operation, a predicate type, a table, a function, an argument
// or a flag, and an omitted flag is not the default it requests.
func TestEqual_TellsBindingsApart(t *testing.T) {
	base := mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.AfterUpdate, "tenant_id")}}
	tests := []struct {
		name  string
		other mssqlschema.DesiredSecurityPolicy
	}{
		{name: "BEFORE UPDATE for AFTER UPDATE", other: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.BeforeUpdate, "tenant_id")}}},
		{name: "every operation for one", other: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, "", "tenant_id")}}},
		{name: "another table", other: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(invoices, mssqlschema.AfterUpdate, "tenant_id")}}},
		{name: "another function", other: mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{{
			Type: mssqlschema.Block, Function: mssqlschema.ObjectName{Schema: "rls", Name: "fn_other"}, Arguments: []string{"tenant_id"},
			Table: orders, Operation: mssqlschema.AfterUpdate}}}},
		{name: "another argument", other: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.AfterUpdate, "owner_id")}}},
		{name: "a state named", other: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.AfterUpdate, "tenant_id")}, Enabled: &on}},
		{name: "a schema binding named", other: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.AfterUpdate, "tenant_id")}, SchemaBinding: &on}},
		{name: "replication left out", other: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.AfterUpdate, "tenant_id")}, NotForReplication: true}},
		{name: "a struct name", other: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.AfterUpdate, "tenant_id")}, StructName: "Tenancy"}},
		{name: "the server's spelling", other: mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.AfterUpdate, "tenant_id")},
			Normalized: []mssqlschema.Predicate{block(orders, mssqlschema.AfterUpdate, "[tenant_id]")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(base.Equal(&test.other), qt.IsFalse)
		})
	}
}

// TestEqual_TellsObservationsApart pins what observed equality reads: the
// state, the schema binding, replication, and the predicate type alone. A
// predicate list in another order is the same policy.
func TestEqual_TellsObservationsApart(t *testing.T) {
	base := mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{block(orders, "", "tenant_id"), filter(invoices)},
		Enabled: true, SchemaBinding: true}
	tests := []struct {
		name  string
		other mssqlschema.ObservedSecurityPolicy
		equal bool
	}{
		{name: "the predicates in another order", equal: true, other: mssqlschema.ObservedSecurityPolicy{
			Predicates: []mssqlschema.Predicate{filter(invoices), block(orders, "", "tenant_id")}, Enabled: true, SchemaBinding: true}},
		{name: "disabled", other: mssqlschema.ObservedSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, "", "tenant_id"), filter(invoices)}, SchemaBinding: true}},
		{name: "without schema binding", other: mssqlschema.ObservedSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, "", "tenant_id"), filter(invoices)}, Enabled: true}},
		{name: "replication left out", other: mssqlschema.ObservedSecurityPolicy{
			Predicates: []mssqlschema.Predicate{block(orders, "", "tenant_id"), filter(invoices)}, Enabled: true, SchemaBinding: true,
			NotForReplication: true}},
		{name: "a filter where the block was", other: mssqlschema.ObservedSecurityPolicy{
			Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id"), filter(invoices)}, Enabled: true, SchemaBinding: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(base.Equal(&test.other), qt.Equals, test.equal)
		})
	}
}

// TestClone_SharesNothing pins that a clone of either representation is
// independent: changing its predicates, their arguments or a flag leaves the
// original alone.
func TestClone_SharesNothing(t *testing.T) {
	c := qt.New(t)
	enabled := true
	original := &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}, Enabled: &enabled}

	cloned := original.Clone().(*mssqlschema.DesiredSecurityPolicy)
	cloned.Predicates[0].Arguments[0] = "owner_id"
	cloned.Predicates[0].Table = invoices
	*cloned.Enabled = false

	observed := &mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}}
	observedClone := observed.Clone().(*mssqlschema.ObservedSecurityPolicy)
	observedClone.Predicates[0].Arguments[0] = "owner_id"
	observedClone.Predicates[0].Table = invoices

	c.Assert(original.Predicates[0], qt.DeepEquals, filter(orders, "tenant_id"))
	c.Assert(*original.Enabled, qt.IsTrue)
	c.Assert(observed.Predicates[0], qt.DeepEquals, filter(orders, "tenant_id"))
	c.Assert((*mssqlschema.DesiredSecurityPolicy)(nil).Clone(), qt.DeepEquals, schemaext.Value((*mssqlschema.DesiredSecurityPolicy)(nil)))
	c.Assert((*mssqlschema.ObservedSecurityPolicy)(nil).Clone(), qt.DeepEquals, schemaext.Value((*mssqlschema.ObservedSecurityPolicy)(nil)))
}

// TestCodecs_RefuseWhatTheModelCannotHold pins the wire rules and the
// invariants the validators keep: unknown, null and case-variant keys, empty
// values, unqualified names, an operation on a filter predicate, an unknown
// type or operation, two predicates for one operation on one table, a block
// predicate for every operation beside another.
func TestCodecs_RefuseWhatTheModelCannotHold(t *testing.T) {
	desired, observed := mssqlschema.Codecs()[0], mssqlschema.Codecs()[1]
	const fn = `"function":{"schema":"rls","name":"fn_tenant"}`
	const table = `"table":{"schema":"app","name":"orders"}`
	tests := []struct {
		name  string
		codec schemaext.Codec
		input string
	}{
		{name: "null", codec: desired, input: `null`},
		{name: "no predicate list", codec: desired, input: `{"enabled":true}`},
		{name: "an unknown key", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,` + table + `}],"state":"ON"}`},
		{name: "a null key", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,` + table + `}],"enabled":null}`},
		{name: "a key in another case", codec: desired, input: `{"Predicates":[{"type":"FILTER",` + fn + `,` + table + `}]}`},
		{name: "an unknown predicate key", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,` + table + `,"timing":"AFTER"}]}`},
		{name: "an empty predicate type", codec: desired, input: `{"predicates":[{"type":"",` + fn + `,` + table + `}]}`},
		{name: "an unknown predicate type", codec: desired, input: `{"predicates":[{"type":"AUDIT",` + fn + `,` + table + `}]}`},
		{name: "a predicate without a table", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `}]}`},
		{name: "a table without a schema", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,"table":{"name":"orders"}}]}`},
		{name: "a function with an empty name", codec: desired, input: `{"predicates":[{"type":"FILTER","function":{"schema":"rls","name":""},` + table + `}]}`},
		{name: "an empty argument", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,"arguments":[" "],` + table + `}]}`},
		{name: "a filter with an operation", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,` + table + `,"operation":"AFTER INSERT"}]}`},
		{name: "an unknown operation", codec: desired, input: `{"predicates":[{"type":"BLOCK",` + fn + `,` + table + `,"operation":"AFTER DELETE"}]}`},
		{name: "two filters on one table", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,` + table + `},{"type":"FILTER",` + fn + `,` + table + `}]}`},
		{name: "two blocks for one operation", codec: observed, input: `{"predicates":[` +
			`{"type":"BLOCK",` + fn + `,` + table + `,"operation":"BEFORE UPDATE"},{"type":"BLOCK",` + fn + `,` + table + `,"operation":"BEFORE UPDATE"}],` +
			`"enabled":true,"schema_binding":true,"not_for_replication":false}`},
		{name: "a block for every operation beside another", codec: desired, input: `{"predicates":[` +
			`{"type":"BLOCK",` + fn + `,` + table + `},{"type":"BLOCK",` + fn + `,` + table + `,"operation":"AFTER INSERT"}]}`},
		{name: "an observation without its state", codec: observed, input: `{"predicates":[],"schema_binding":true,"not_for_replication":false}`},
		{name: "an observation with a struct name", codec: observed, input: `{"predicates":[],"enabled":true,"schema_binding":true,` +
			`"not_for_replication":false,"struct_name":"T"}`},
		{name: "a NUL in a name", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,"table":{"schema":"app","name":"or\u0000ders"}}]}`},
		{name: "a blank table name", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,"table":{"schema":"app","name":"  "}}]}`},
		{name: "a block with an empty operation", codec: desired, input: `{"predicates":[{"type":"BLOCK",` + fn + `,` + table + `,"operation":""}]}`},
		{name: "an empty argument list", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,"arguments":[],` + table + `}]}`},
		{name: "an empty struct name", codec: desired, input: `{"predicates":[],"struct_name":""}`},
		{name: "replication spelled out as false", codec: desired, input: `{"predicates":[],"not_for_replication":false}`},
		{name: "the predicates not a list", codec: desired, input: `{"predicates":{}}`},
		{name: "a state that is not a boolean", codec: observed, input: `{"predicates":[],"enabled":"ON","schema_binding":true,"not_for_replication":false}`},
		{name: "two filters on one table in two cases", codec: desired, input: `{"predicates":[{"type":"FILTER",` + fn + `,` + table + `},` +
			`{"type":"FILTER",` + fn + `,"table":{"schema":"APP","name":"Orders"}}]}`},
		{name: "two filters on one table, one name with trailing spaces", codec: observed, input: `{"predicates":[{"type":"FILTER",` + fn + `,` + table + `},` +
			`{"type":"FILTER",` + fn + `,"table":{"schema":"app","name":"orders  "}}],"enabled":true,"schema_binding":true,"not_for_replication":false}`},
		{name: "a block for every operation beside another in another case", codec: desired, input: `{"predicates":[` +
			`{"type":"BLOCK",` + fn + `,` + table + `},{"type":"BLOCK",` + fn + `,"table":{"schema":"app","name":"ORDERS"},"operation":"AFTER INSERT"}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := test.codec.Decode([]byte(test.input))
			var refusal *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Kind, qt.Equals, mssqlschema.SecurityPolicyKind)
			c.Assert(refusal.Representation, qt.Equals, test.codec.Representation)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestCodecs_DecodeTheEncodersSpelling pins the controls of the refusals
// above: a declared state or schema binding written as false is a value, not
// an omission, and a spelled-out operation, argument list and struct name
// decode.
func TestCodecs_DecodeTheEncodersSpelling(t *testing.T) {
	c := qt.New(t)

	decoded, err := mssqlschema.Codecs()[0].Decode([]byte(`{"predicates":[{"type":"BLOCK","function":{"schema":"rls","name":"fn_tenant"},` +
		`"arguments":["tenant_id"],"table":{"schema":"app","name":"orders"},"operation":"AFTER INSERT"}],` +
		`"enabled":false,"schema_binding":false,"not_for_replication":true,"struct_name":"Tenancy"}`))

	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, schemaext.Payload(&mssqlschema.DesiredSecurityPolicy{
		Predicates: []mssqlschema.Predicate{block(orders, mssqlschema.AfterInsert, "tenant_id")},
		Enabled:    &off, SchemaBinding: &off, NotForReplication: true, StructName: "Tenancy",
	}))
}

// TestCodecs_DefinitionsNameTheEncodedKeys pins each published definition to
// what the encoder writes: its properties are the keys of a value with every
// field set, and its required keys are those of a value with none set.
func TestCodecs_DefinitionsNameTheEncodedKeys(t *testing.T) {
	full := []mssqlschema.Predicate{block(orders, mssqlschema.AfterInsert, "tenant_id")}
	bare := []mssqlschema.Predicate{{Type: mssqlschema.Filter, Function: tenantFunction, Table: orders}}
	tests := []struct {
		name       string
		codec      schemaext.Codec
		full, bare schemaext.Payload
	}{
		{name: "desired", codec: mssqlschema.Codecs()[0],
			full: &mssqlschema.DesiredSecurityPolicy{Predicates: full, Enabled: &on, SchemaBinding: &on, NotForReplication: true, StructName: "T",
				Normalized: full},
			bare: &mssqlschema.DesiredSecurityPolicy{Predicates: bare}},
		{name: "observed", codec: mssqlschema.Codecs()[1],
			full: &mssqlschema.ObservedSecurityPolicy{Predicates: full, Enabled: true},
			bare: &mssqlschema.ObservedSecurityPolicy{Predicates: bare}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var definition jsonDefinition
			c.Assert(json.Unmarshal(test.codec.Definition, &definition), qt.IsNil)
			predicate := definition.Properties["predicates"].Items
			name := predicate.Properties["table"]
			fullKeys, fullPredicate := encodedKeys(c, test.codec, test.full)
			bareKeys, barePredicate := encodedKeys(c, test.codec, test.bare)

			c.Assert(sortedKeys(definition.Properties), qt.DeepEquals, fullKeys)
			c.Assert(slices.Sorted(slices.Values(definition.Required)), qt.DeepEquals, bareKeys)
			c.Assert(sortedKeys(predicate.Properties), qt.DeepEquals, fullPredicate)
			c.Assert(slices.Sorted(slices.Values(predicate.Required)), qt.DeepEquals, barePredicate)
			c.Assert(sortedKeys(name.Properties), qt.DeepEquals, []string{"name", "schema"})
			c.Assert(slices.Sorted(slices.Values(name.Required)), qt.DeepEquals, []string{"name", "schema"})
		})
	}
}

// jsonDefinition reads the parts of a JSON Schema definition the agreement
// test compares; encoding/json skips the other keywords.
type jsonDefinition struct {
	Required   []string                   `json:"required"`
	Properties map[string]*jsonDefinition `json:"properties"`
	Items      *jsonDefinition            `json:"items"`
}

// encodedKeys returns the keys an encoded policy holds and those of its first
// predicate, each sorted.
func encodedKeys(c *qt.C, codec schemaext.Codec, value schemaext.Payload) (policy, predicate []string) {
	c.Helper()
	encoded, err := codec.Encode(value)
	c.Assert(err, qt.IsNil)
	var keys map[string]json.RawMessage
	var predicates struct {
		Predicates []map[string]json.RawMessage `json:"predicates"`
	}
	c.Assert(json.Unmarshal(encoded, &keys), qt.IsNil)
	c.Assert(json.Unmarshal(encoded, &predicates), qt.IsNil)
	c.Assert(predicates.Predicates, qt.HasLen, 1)
	return sortedKeys(keys), sortedKeys(predicates.Predicates[0])
}

func sortedKeys[V any](values map[string]V) []string {
	return slices.Sorted(maps.Keys(values))
}

// TestCodecs_RefuseToEncodeAnInvalidValue pins that encoding validates first,
// so an invalid declaration never reaches the wire.
func TestCodecs_RefuseToEncodeAnInvalidValue(t *testing.T) {
	c := qt.New(t)

	encoded, err := mssqlschema.Codecs()[0].Encode(&mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		{Type: mssqlschema.Filter, Function: tenantFunction, Table: orders, Operation: mssqlschema.AfterInsert}}})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*a FILTER predicate takes no operation, got "AFTER INSERT"`)
	c.Assert(encoded, qt.IsNil)
}

// TestSecurityPolicyRef_IsSchemaScoped pins the identity: a schema and a name,
// no table parent, each part kept as written, the SQL Server default schema
// for an omitted one, and case folded only for names the catalog resolved to
// one class.
func TestSecurityPolicyRef_IsSchemaScoped(t *testing.T) {
	c := qt.New(t)

	ref := mssqlschema.SecurityPolicyRef("rls", " Tenancy")
	defaulted := mssqlschema.SecurityPolicyRef("", "tenancy")

	c.Assert(mssqlschema.ValidateSecurityPolicyRef(ref), qt.IsNil)
	c.Assert(ref.Kind, qt.Equals, objectidentity.Kind(mssqlschema.SecurityPolicyKind))
	c.Assert(ref.Parent.Empty(), qt.IsTrue)
	c.Assert(ref.Schema.Source, qt.Equals, "rls")
	c.Assert(ref.Name.Source, qt.Equals, " Tenancy")
	c.Assert(defaulted.Schema.Source, qt.Equals, "dbo")
	c.Assert(defaulted.Schema.Defaulted, qt.IsTrue)
	c.Assert(mssqlschema.SecurityPolicyRef("RLS", "TENANCY").Key(), qt.Not(qt.Equals), mssqlschema.SecurityPolicyRef("rls", "tenancy").Key(),
		qt.Commentf("offline, the collation is unknown, so case is kept"))
	resolved := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").WithResolvedNames([]identifier.ResolvedName{
		{Name: "RLS", Key: "rls"}, {Name: "rls", Key: "rls"}, {Name: "TENANCY", Key: "tenancy"}, {Name: "tenancy", Key: "tenancy"},
	})
	c.Assert(mssqlschema.SecurityPolicyRefWith(resolved, "RLS", "TENANCY").Key(), qt.Equals,
		mssqlschema.SecurityPolicyRefWith(resolved, "rls", "tenancy").Key(), qt.Commentf("names the catalog resolved to one class share a key"))
}

// TestValidateSecurityPolicyRef_FailurePath refuses an identity of another
// kind, one under a table, one without a name or with a blank part, and one
// whose parts are not valid text.
func TestValidateSecurityPolicyRef_FailurePath(t *testing.T) {
	underTable := mssqlschema.SecurityPolicyRef("rls", "tenancy")
	underTable.Parent = objectidentity.Part{Source: "orders", Normalized: "orders"}
	otherKind := mssqlschema.SecurityPolicyRef("rls", "tenancy")
	otherKind.Kind = objectidentity.KindTable
	tests := []struct {
		name string
		ref  objectidentity.ID
	}{
		{name: "under a table", ref: underTable},
		{name: "another kind", ref: otherKind},
		{name: "no name", ref: mssqlschema.SecurityPolicyRef("rls", "")},
		{name: "a blank name", ref: mssqlschema.SecurityPolicyRef("rls", " ")},
		{name: "a blank schema", ref: mssqlschema.SecurityPolicyRef("  ", "tenancy")},
		{name: "a NUL in the name", ref: mssqlschema.SecurityPolicyRef("rls", "ten\x00ancy")},
		{name: "a schema that is not UTF-8", ref: mssqlschema.SecurityPolicyRef("r\xffls", "tenancy")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mssqlschema.ValidateSecurityPolicyRef(test.ref), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

// TestObjects_HappyPath pins that an object holds a copy of the policy it was
// given, under the ref it was given.
func TestObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	ref := mssqlschema.SecurityPolicyRef("rls", "tenancy")
	declared := mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}}
	observed := mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}, Enabled: true}

	desiredObject, desiredErr := mssqlschema.DesiredSecurityPolicyObject(ref, declared)
	observedObject, observedErr := mssqlschema.ObservedSecurityPolicyObject(ref, observed)
	declared.Predicates[0].Arguments[0] = "owner_id"
	observed.Predicates[0].Arguments[0] = "owner_id"

	c.Assert(desiredErr, qt.IsNil)
	c.Assert(observedErr, qt.IsNil)
	c.Assert(desiredObject.Ref, qt.DeepEquals, ref)
	c.Assert(observedObject.Ref, qt.DeepEquals, ref)
	c.Assert(desiredObject.Value, qt.DeepEquals, schemaext.Value(&mssqlschema.DesiredSecurityPolicy{
		Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}}))
	c.Assert(observedObject.Value, qt.DeepEquals, schemaext.Value(&mssqlschema.ObservedSecurityPolicy{
		Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")}, Enabled: true}))
}

// TestObjects_FailurePath pins that both constructors refuse what their
// validators refuse, and an invalid ref, as the typed model error of their
// representation, and return no object.
func TestObjects_FailurePath(t *testing.T) {
	ref := mssqlschema.SecurityPolicyRef("rls", "tenancy")
	tests := []struct {
		name           string
		build          func() (schemaext.Object, error)
		representation schemaext.Representation
		wantErr        string
	}{
		{name: "a declaration with two blocks for every write", representation: schemaext.Desired,
			wantErr: `.*has a BLOCK predicate for every operation beside another BLOCK predicate`,
			build: func() (schemaext.Object, error) {
				return mssqlschema.DesiredSecurityPolicyObject(ref, mssqlschema.DesiredSecurityPolicy{
					Predicates: []mssqlschema.Predicate{block(orders, ""), block(orders, mssqlschema.BeforeDelete)}})
			}},
		{name: "a declaration under a blank name", representation: schemaext.Desired,
			wantErr: `.*a policy needs its schema and its name`,
			build: func() (schemaext.Object, error) {
				return mssqlschema.DesiredSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", " "), mssqlschema.DesiredSecurityPolicy{})
			}},
		{name: "an observation with two filters on one table", representation: schemaext.Observed,
			wantErr: `.*table \[app\]\.\[orders\] has two FILTER predicates`,
			build: func() (schemaext.Object, error) {
				return mssqlschema.ObservedSecurityPolicyObject(ref, mssqlschema.ObservedSecurityPolicy{
					Predicates: []mssqlschema.Predicate{filter(orders), filter(orders)}})
			}},
		{name: "an observation without a name", representation: schemaext.Observed,
			wantErr: `.*a security policy requires a schema and a name, and no table parent`,
			build: func() (schemaext.Object, error) {
				return mssqlschema.ObservedSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", ""), mssqlschema.ObservedSecurityPolicy{})
			}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			object, err := test.build()
			var refusal *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Kind, qt.Equals, mssqlschema.SecurityPolicyKind)
			c.Assert(refusal.Representation, qt.Equals, test.representation)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(object, qt.DeepEquals, schemaext.Object{})
		})
	}
}

// TestCoverage_RecordsTheClaimItIsGiven pins coverage: the namespace-wide
// claim and a policy's own knowledge over it.
func TestCoverage_RecordsTheClaimItIsGiven(t *testing.T) {
	c := qt.New(t)
	tenancy := mssqlschema.SecurityPolicyRef("rls", "tenancy")
	other := mssqlschema.SecurityPolicyRef("rls", "other")

	coverage, err := mssqlschema.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: mssqlschema.SecurityPolicyKind, Subject: tenancy,
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "an unreadable predicate"}}})

	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(mssqlschema.SecurityPolicyKind, tenancy).State, qt.Equals, schemaext.Unrepresentable)
	c.Assert(coverage.Lookup(mssqlschema.SecurityPolicyKind, other).State, qt.Equals, schemaext.Complete)
}

// TestValidateDesiredSecurityPolicy_FailurePath pins the validator on values
// built in Go, which no codec shape checks first: an unqualified or blank
// table or function, an argument that is empty or carries a NUL byte, a
// struct name that is not valid text, and a nil declaration.
func TestValidateDesiredSecurityPolicy_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		policy  *mssqlschema.DesiredSecurityPolicy
		wantErr string
	}{
		{name: "a table without its schema", wantErr: `.*a predicate table needs its schema and its name`,
			policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
				{Type: mssqlschema.Filter, Function: tenantFunction, Table: mssqlschema.ObjectName{Name: "orders"}}}}},
		{name: "a function without its schema", wantErr: `.*a predicate function needs its schema and its name`,
			policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
				{Type: mssqlschema.Filter, Function: mssqlschema.ObjectName{Name: "fn_tenant"}, Table: orders}}}},
		{name: "a blank function name", wantErr: `.*a predicate function needs its schema and its name`,
			policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
				{Type: mssqlschema.Filter, Function: mssqlschema.ObjectName{Schema: "rls", Name: "\t "}, Table: orders}}}},
		{name: "an empty argument", wantErr: `.*a predicate argument cannot be empty`,
			policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id", "")}}},
		{name: "a NUL in an argument", wantErr: `.*security policy predicate argument contains a NUL byte`,
			policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant\x00id")}}},
		{name: "a NUL in the struct name", wantErr: `.*security policy struct name contains a NUL byte`,
			policy: &mssqlschema.DesiredSecurityPolicy{StructName: "Ten\x00ancy"}},
		{name: "a struct name that is not UTF-8", wantErr: `.*security policy struct name is not valid UTF-8`,
			policy: &mssqlschema.DesiredSecurityPolicy{StructName: "Ten\xffancy"}},
		{name: "the server's spelling of another slot", wantErr: `.*a normalized security policy holds one predicate for each declared one, in its slot`,
			policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")},
				Normalized: []mssqlschema.Predicate{filter(invoices, "[tenant_id]")}}},
		{name: "the server's spelling of fewer predicates", wantErr: `.*a normalized security policy holds one predicate for each declared one, in its slot`,
			policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id"), filter(invoices, "tenant_id")},
				Normalized: []mssqlschema.Predicate{filter(orders, "[tenant_id]")}}},
		{name: "an invalid predicate in the server's spelling", wantErr: `.*a predicate argument cannot be empty`,
			policy: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{filter(orders, "tenant_id")},
				Normalized: []mssqlschema.Predicate{filter(orders, "")}}},
		{name: "nil", wantErr: `.*nil security policy declaration`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := mssqlschema.ValidateDesiredSecurityPolicy(test.policy)
			var refusal *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Representation, qt.Equals, schemaext.Desired)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestObjectName_StringQuotesEachPart pins the T-SQL spelling messages use: a
// closing bracket inside a part is doubled, so the rendering names one object.
func TestObjectName_StringQuotesEachPart(t *testing.T) {
	c := qt.New(t)
	c.Assert(mssqlschema.ObjectName{Schema: "app", Name: "a]b"}.String(), qt.Equals, "[app].[a]]b]")
	c.Assert(mssqlschema.ObjectName{Schema: "a.b", Name: "[c"}.String(), qt.Equals, "[a.b].[[c]")
}

// TestFingerprint_FollowsTheBindings pins the durable identity a registry
// gives a policy: listing the bindings in another order keeps it, and moving a
// block predicate from AFTER UPDATE to BEFORE UPDATE or disabling the policy
// changes it.
func TestFingerprint_FollowsTheBindings(t *testing.T) {
	c := qt.New(t)
	codecs := registry(c)
	fingerprint := func(policy *mssqlschema.DesiredSecurityPolicy) string {
		c.Helper()
		return must.Must(codecs.Fingerprint(t.Context(), schemaext.Desired, []schemaext.Payload{policy}))
	}
	base := fingerprint(&mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		filter(invoices, "tenant_id"), block(orders, mssqlschema.AfterUpdate, "tenant_id")}})

	c.Assert(fingerprint(&mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		block(orders, mssqlschema.AfterUpdate, "tenant_id"), filter(invoices, "tenant_id")}}), qt.Equals, base)
	c.Assert(fingerprint(&mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		filter(invoices, "tenant_id"), block(orders, mssqlschema.BeforeUpdate, "tenant_id")}}), qt.Not(qt.Equals), base)
	c.Assert(fingerprint(&mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
		filter(invoices, "tenant_id"), block(orders, mssqlschema.AfterUpdate, "tenant_id")}, Enabled: &off}), qt.Not(qt.Equals), base)
}
