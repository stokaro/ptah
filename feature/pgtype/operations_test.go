package pgtype_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// operationCodec returns the operation codec of kind.
func operationCodec(c *qt.C, kind schemaext.Kind) schemaext.Codec {
	c.Helper()
	for _, candidate := range pgtype.OperationCodecs() {
		if candidate.Prototype.Kind() == kind {
			return candidate
		}
	}
	c.Fatalf("no operation codec of kind %s", kind)
	return schemaext.Codec{}
}

func target(kind schemaext.Kind, schema, name string) pgtype.Target {
	return pgtype.Target{Kind: kind, Schema: schema, Name: name}
}

func TestOperationCodecs_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		operation ast.ExtensionPayload
		wire      string
	}{
		{name: "a domain creation", operation: &pgtype.CreateDomain{Target: target(pgtype.DomainKind, "app", "qty"), BaseType: "int", NotNull: true},
			wire: `{"target":{"kind":"ptah.run/pgtype/domain","schema":"app","name":"qty"},"base_type":"int","not_null":true,` +
				`"default":"","default_expr":"","check":""}`},
		{name: "a domain alteration, its empty list written null",
			operation: &pgtype.AlterDomain{Target: target(pgtype.DomainKind, "", "qty"), DropChecks: make([]string, 0), AddCheck: "VALUE = 1"},
			wire: `{"target":{"kind":"ptah.run/pgtype/domain","schema":"","name":"qty"},"drop_checks":null,"add_check":"VALUE = 1",` +
				`"set_default":"","not_null":null}`},
		{name: "a composite type alteration",
			operation: &pgtype.AlterComposite{Target: target(pgtype.CompositeKind, "", "addr"), Drop: []string{"zip"},
				Add: []pgtype.Attribute{{Name: "postcode", Type: "text"}}},
			wire: `{"target":{"kind":"ptah.run/pgtype/composite","schema":"","name":"addr"},"drop":["zip"],` +
				`"add":[{"name":"postcode","type":"text"}]}`},
		{name: "a range type creation", operation: &pgtype.CreateRange{Target: target(pgtype.RangeKind, "", "fr"), Subtype: "float8"},
			wire: `{"target":{"kind":"ptah.run/pgtype/range","schema":"","name":"fr"},"subtype":"float8","subtype_opclass":"",` +
				`"collation":"","canonical":"","subtype_diff":""}`},
		{name: "a drop", operation: &pgtype.Drop{Target: target(pgtype.CompositeKind, "", "addr"), Cascade: true},
			wire: `{"target":{"kind":"ptah.run/pgtype/composite","schema":"","name":"addr"},"cascade":true}`},
		{name: "a comment", operation: &pgtype.Comment{Target: target(pgtype.DomainKind, "", "qty")},
			wire: `{"target":{"kind":"ptah.run/pgtype/domain","schema":"","name":"qty"},"comment":""}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			model := operationCodec(c, test.operation.Kind())

			encoded := must.Must(model.Encode(test.operation))
			decoded := must.Must(model.Decode(encoded))

			c.Assert(string(encoded), qt.Equals, test.wire)
			c.Assert(must.Must(model.Encode(decoded)), qt.DeepEquals, encoded)
		})
	}
}

func TestOperations_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		operation interface{ Validate() error }
		want      string
	}{
		{name: "a domain creation about a range", operation: &pgtype.CreateDomain{Target: target(pgtype.RangeKind, "", "x"), BaseType: "int"},
			want: `.*the operation is about a domain, not "ptah.run/pgtype/range"`},
		{name: "a domain creation without a base type", operation: &pgtype.CreateDomain{Target: target(pgtype.DomainKind, "", "x")},
			want: `.*a domain creation names a base type`},
		{name: "a target without a name", operation: &pgtype.Drop{Target: target(pgtype.DomainKind, "app", " ")},
			want: `.*a domain requires a schema-scoped identity with a name`},
		{name: "a target of another owner", operation: &pgtype.Drop{Target: target("example.org/type", "", "x")},
			want: `.*"example.org/type" is not a PostgreSQL user type kind`},
		{name: "a domain alteration that changes nothing", operation: &pgtype.AlterDomain{Target: target(pgtype.DomainKind, "", "x")},
			want: `.*a domain alteration changes something`},
		{name: "a constraint named twice", operation: &pgtype.AlterDomain{Target: target(pgtype.DomainKind, "", "x"), DropChecks: []string{"c", "c"}},
			want: `.*domain check "c" is named twice`},
		{name: "a composite type without attributes", operation: &pgtype.CreateComposite{Target: target(pgtype.CompositeKind, "", "x")},
			want: `.*a composite type requires at least one attribute`},
		{name: "a composite type alteration that changes nothing", operation: &pgtype.AlterComposite{Target: target(pgtype.CompositeKind, "", "x")},
			want: `.*a composite type alteration changes something`},
		{name: "a range type creation without a subtype", operation: &pgtype.CreateRange{Target: target(pgtype.RangeKind, "", "x")},
			want: `.*a range type creation names a subtype`},
		{name: "a comment that is no text", operation: &pgtype.Comment{Target: target(pgtype.DomainKind, "", "x"), Comment: "a\x00b"},
			want: `.*type comment contains a NUL byte`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.operation.Validate()

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestOperationCodecs_FailurePath pins wire refusals the struct decoder
// would not make: an empty list spelled out, a missing key and a target of
// a kind this owner does not have.
func TestOperationCodecs_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		kind schemaext.Kind
		wire string
	}{
		{name: "an empty constraint list", kind: pgtype.AlterDomainKind,
			wire: `{"target":{"kind":"ptah.run/pgtype/domain","schema":"","name":"x"},"drop_checks":[],"add_check":"VALUE = 1",` +
				`"set_default":"","not_null":null}`},
		{name: "a missing key", kind: pgtype.DropKind, wire: `{"target":{"kind":"ptah.run/pgtype/domain","schema":"","name":"x"}}`},
		{name: "a target without its schema", kind: pgtype.DropKind,
			wire: `{"target":{"kind":"ptah.run/pgtype/domain","name":"x"},"cascade":false}`},
		{name: "a target of another kind", kind: pgtype.DropKind,
			wire: `{"target":{"kind":"ptah.run/pgtype/enum","schema":"","name":"x"},"cascade":false}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := operationCodec(c, test.kind).Decode(json.RawMessage(test.wire))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// gated is an operation a target needs a capability for.
type gated interface {
	RequiredCapability() capability.Capability
	OmissionSubject() (kind, name string)
}

// TestOperations_NameTheirCapabilityAndObject pins what a renderer's skip
// line reads: the key each statement needs and the object it names, which is
// the key and the kind the PostgreSQL-family renderer used for the common
// user types.
func TestOperations_NameTheirCapabilityAndObject(t *testing.T) {
	tests := []struct {
		name      string
		operation gated
		key       capability.Capability
		kind      string
		object    string
	}{
		{name: "a domain", operation: &pgtype.CreateDomain{Target: target(pgtype.DomainKind, "app", "qty")},
			key: capability.DomainTypes, kind: "domain", object: "app.qty"},
		{name: "a domain alteration", operation: &pgtype.AlterDomain{Target: target(pgtype.DomainKind, "", "qty")},
			key: capability.DomainTypes, kind: "domain", object: "qty"},
		{name: "a composite type", operation: &pgtype.CreateComposite{Target: target(pgtype.CompositeKind, "", "addr")},
			key: capability.CompositeTypes, kind: "composite type", object: "addr"},
		{name: "a composite type alteration", operation: &pgtype.AlterComposite{Target: target(pgtype.CompositeKind, "", "addr")},
			key: capability.CompositeTypes, kind: "composite type", object: "addr"},
		{name: "a range type", operation: &pgtype.CreateRange{Target: target(pgtype.RangeKind, "", "fr")},
			key: capability.RangeTypes, kind: "range type", object: "fr"},
		{name: "a domain's comment", operation: &pgtype.Comment{Target: target(pgtype.DomainKind, "", "qty")},
			key: capability.DomainComments, kind: "domain comment", object: "qty"},
		{name: "a range type's comment", operation: &pgtype.Comment{Target: target(pgtype.RangeKind, "app", "fr")},
			key: capability.TypeComments, kind: "type comment", object: "app.fr"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			kind, object := test.operation.OmissionSubject()

			c.Assert(test.operation.RequiredCapability(), qt.Equals, test.key)
			c.Assert(kind, qt.Equals, test.kind)
			c.Assert(object, qt.Equals, test.object)
		})
	}
}

// TestOperations_ReportTheirEffectAndChange pins what a safety check and a
// schema-change report read: a drop and a dropped attribute destroy, a
// creation adds, and each names the type it is about.
func TestOperations_ReportTheirEffectAndChange(t *testing.T) {
	tests := []struct {
		name      string
		operation interface {
			Effect() schemaext.Effect
			SchemaChange() ast.ExtensionChange
		}
		impact schemaext.Impact
		action ast.ExtensionChangeAction
	}{
		{name: "a domain creation", operation: &pgtype.CreateDomain{Target: target(pgtype.DomainKind, "", "qty"), BaseType: "int"},
			impact: schemaext.Additive, action: ast.ExtensionAdd},
		{name: "a domain alteration", operation: &pgtype.AlterDomain{Target: target(pgtype.DomainKind, "", "qty"), AddCheck: "VALUE = 1"},
			impact: schemaext.Behavioral, action: ast.ExtensionModify},
		{name: "an added attribute", operation: &pgtype.AlterComposite{Target: target(pgtype.CompositeKind, "", "qty"),
			Add: []pgtype.Attribute{{Name: "a", Type: "int"}}}, impact: schemaext.Additive, action: ast.ExtensionModify},
		{name: "a dropped attribute", operation: &pgtype.AlterComposite{Target: target(pgtype.CompositeKind, "", "qty"),
			Drop: []string{"a"}}, impact: schemaext.Destructive, action: ast.ExtensionModify},
		{name: "a drop", operation: &pgtype.Drop{Target: target(pgtype.RangeKind, "", "qty")},
			impact: schemaext.Destructive, action: ast.ExtensionDrop},
		{name: "a comment", operation: &pgtype.Comment{Target: target(pgtype.RangeKind, "", "qty")},
			impact: schemaext.Additive, action: ast.ExtensionModify},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.operation.Effect().Impact, qt.Equals, test.impact)
			c.Assert(test.operation.SchemaChange(), qt.Equals, ast.ExtensionChange{Action: test.action, Name: "qty"})
		})
	}
}
