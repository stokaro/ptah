package difftypes_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestRangeChanges_TheWireShapeIsUnchanged pins the promise the type makes.
//
// `RangesAdded` and `RangesRemoved` now carry the range type instead of its
// name, the first family to do so under stokaro/ptah#2315. `ptah schema diff
// --format json` serializes the comparator's model as it stands, so the change
// would have altered a document stamped `format_version: 1` -- and 33 of these
// families remain, which is 33 format changes for one architectural move.
//
// So the encoding stays what it was. This test is what makes that a promise
// rather than an intention: it fails the moment the operands reach the wire,
// which is the moment a version bump becomes due.
func TestRangeChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.RangeChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run, which every field of this type distinguishes from []",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.RangeChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the operands do not reach the wire",
			changes: difftypes.RangeChanges{
				{Name: "r", Subtype: "integer", SubtypeOpClass: "int4_ops", Canonical: "r_canonical"},
			},
			want: `["r"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
		{
			name: "a schema-qualified range keeps its qualified spelling",
			changes: difftypes.RangeChanges{
				{Name: "r", Schema: "app", Subtype: "integer"},
			},
			want: `["app.r"]`,
			why:  "the name lists carried qualified names, and the identity a consumer keys on is unchanged",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestRangeChanges_TheDefinitionSurvivesInMemory is the other half: the wire
// staying flat must not mean the operands were dropped on the way in.
//
// Without this, the marshaller above would pass against a type that had thrown
// the definition away, which is exactly the state this change exists to leave.
func TestRangeChanges_TheDefinitionSurvivesInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.RangeChanges{
		{Name: "r", Schema: "app", Subtype: "integer", SubtypeOpClass: "int4_ops"},
	}

	c.Assert(changes[0].Subtype, qt.Equals, "integer")
	c.Assert(changes[0].SubtypeOpClass, qt.Equals, "int4_ops")
	c.Assert(changes.Names(), qt.DeepEquals, []string{"app.r"})
	c.Assert(schemamodel.Range(changes[0]).QualifiedName(), qt.Equals, "app.r")
}

// TestCompositeTypeChanges_TheWireShapeIsUnchanged is the same promise for the
// second family off `[]string`.
//
// One test per family rather than one shared table: the promise is per FIELD,
// and a table that iterated over an interface would pass for a family whose
// marshaller had never been written.
func TestCompositeTypeChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.CompositeTypeChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.CompositeTypeChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the fields do not reach the wire",
			changes: difftypes.CompositeTypeChanges{
				{Name: "addr", Fields: []schemamodel.CompositeField{{Name: "street", Type: "text"}}},
			},
			want: `["addr"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
		{
			name: "a schema-qualified composite keeps its qualified spelling",
			changes: difftypes.CompositeTypeChanges{
				{Name: "addr", Schema: "app"},
			},
			want: `["app.addr"]`,
			why:  "the identity a consumer keys on is unchanged",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestCompositeTypeChanges_TheFieldsSurviveInMemory is the other half: a flat
// wire must not mean the operands were dropped on the way in.
func TestCompositeTypeChanges_TheFieldsSurviveInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.CompositeTypeChanges{
		{Name: "addr", Schema: "app", Fields: []schemamodel.CompositeField{
			{Name: "street", Type: "text"},
			{Name: "city", Type: "text"},
		}},
	}

	c.Assert(changes[0].Fields, qt.HasLen, 2)
	c.Assert(changes[0].Fields[1].Type, qt.Equals, "text")
	c.Assert(changes.Names(), qt.DeepEquals, []string{"app.addr"})
}

// TestSequenceChanges_TheWireShapeIsUnchanged is the same promise for the third
// family off `[]string`.
func TestSequenceChanges_TheWireShapeIsUnchanged(t *testing.T) {
	increment := int64(5)
	tests := []struct {
		name    string
		changes difftypes.SequenceChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.SequenceChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the definition does not reach the wire",
			changes: difftypes.SequenceChanges{
				{Name: "s", AsType: "bigint", Increment: &increment, OwnedBy: "orders.id"},
			},
			want: `["s"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
		{
			name:    "a schema-qualified sequence keeps its qualified spelling",
			changes: difftypes.SequenceChanges{{Name: "s", Schema: "app"}},
			want:    `["app.s"]`,
			why:     "the identity a consumer keys on is unchanged",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestSequenceChanges_TheDefinitionSurvivesInMemory is the other half.
//
// Ownership is the field that makes it matter: `OWNED BY` is planned from the
// operand now, and a carry that dropped it would plan a sequence whose lifetime
// is no longer tied to its column.
func TestSequenceChanges_TheDefinitionSurvivesInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.SequenceChanges{{Name: "s", Schema: "app", AsType: "bigint", OwnedBy: "orders.id"}}

	c.Assert(changes[0].OwnedBy, qt.Equals, "orders.id")
	c.Assert(changes[0].AsType, qt.Equals, "bigint")
	c.Assert(changes.Names(), qt.DeepEquals, []string{"app.s"})
}

// TestExtensionChanges_TheWireShapeIsUnchanged is the same promise for the
// fourth family off `[]string`.
func TestExtensionChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.ExtensionChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.ExtensionChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing; the comparator initialises these two eagerly",
		},
		{
			name: "the installation schema and version do not reach the wire",
			changes: difftypes.ExtensionChanges{
				{Name: "pgcrypto", Schema: "extensions", Version: "1.3", IfNotExists: true},
			},
			want: `["pgcrypto"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
		{
			name:    "the name is bare, not schema-qualified",
			changes: difftypes.ExtensionChanges{{Name: "pgcrypto", Schema: "extensions"}},
			want:    `["pgcrypto"]`,
			why:     "an extension is named globally; the schema is where it is INSTALLED, not part of its identity",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestExtensionChanges_TheInstallationSchemaSurvivesInMemory is the other half.
//
// The schema is the field that makes it matter: both the CREATE SCHEMA
// precondition and the WITH SCHEMA clause are planned from the operand, so a
// carry that dropped it would install the extension in the wrong place.
func TestExtensionChanges_TheInstallationSchemaSurvivesInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.ExtensionChanges{{Name: "pgcrypto", Schema: "extensions", Version: "1.3"}}

	c.Assert(changes[0].Schema, qt.Equals, "extensions")
	c.Assert(changes[0].Version, qt.Equals, "1.3")
	c.Assert(changes.Names(), qt.DeepEquals, []string{"pgcrypto"})
}

// TestEnumChanges_TheWireShapeIsUnchanged is the same promise for the fifth
// family off `[]string`.
func TestEnumChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.EnumChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.EnumChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name:    "the values do not reach the wire",
			changes: difftypes.EnumChanges{{Name: "status", Values: []string{"draft", "live"}}},
			want:    `["status"]`,
			why:     "a name list is what format_version 1 has always carried here",
		},
		{
			name:    "a schema-qualified enum keeps its qualified spelling",
			changes: difftypes.EnumChanges{{Name: "status", Schema: "app"}},
			want:    `["app.status"]`,
			why:     "the identity a consumer keys on is unchanged",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestEnumChanges_TheValuesSurviveInMemory is the other half.
//
// The values are the whole point for an enum: CREATE TYPE ... AS ENUM is
// planned from them, so a carry that dropped them would plan an empty type.
func TestEnumChanges_TheValuesSurviveInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.EnumChanges{{Name: "status", Schema: "app", Values: []string{"draft", "live"}}}

	c.Assert(changes[0].Values, qt.DeepEquals, []string{"draft", "live"})
	c.Assert(changes.Names(), qt.DeepEquals, []string{"app.status"})
}

// TestDomainChanges_TheWireShapeIsUnchanged is the same promise for the sixth
// family off `[]string`, and the first whose carry needed a rule.
func TestDomainChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.DomainChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.DomainChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the base type, check and default do not reach the wire",
			changes: difftypes.DomainChanges{
				{Name: "positive", BaseType: "integer", Check: "VALUE > 0", DefaultExpr: "now()"},
			},
			want: `["positive"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
		{
			name:    "a schema-qualified domain keeps its qualified spelling",
			changes: difftypes.DomainChanges{{Name: "positive", Schema: "app"}},
			want:    `["app.positive"]`,
			why:     "the identity a consumer keys on is unchanged",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestDomainChanges_TheDefaultKeepsItsKind is the other half, and it is about
// the one field a read cannot transcribe.
//
// A catalog reports one string for a default; the model splits it into a
// literal VALUE and an EXPRESSION, and which slot it lands in decides whether
// the renderer quotes it. A carry that put `now()` in the value slot would
// render the literal text "now()" and the domain would default to a string.
func TestDomainChanges_TheDefaultKeepsItsKind(t *testing.T) {
	tests := []struct {
		name      string
		domain    schemamodel.Domain
		wantValue string
		wantExpr  string
		why       string
	}{
		{
			name:     "an expression stays an expression",
			domain:   schemamodel.Domain{Name: "d", DefaultExpr: "now()"},
			wantExpr: "now()",
			why:      "unquoted SQL is evaluated, and quoting it would default the domain to a string",
		},
		{
			name:      "a quoted literal stays a value",
			domain:    schemamodel.Domain{Name: "d", Default: "'draft'"},
			wantValue: "'draft'",
			why:       "the server's quoting is what marks it a literal",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := difftypes.DomainChanges{test.domain}

			c.Assert(changes[0].Default, qt.Equals, test.wantValue, qt.Commentf("%s", test.why))
			c.Assert(changes[0].DefaultExpr, qt.Equals, test.wantExpr, qt.Commentf("%s", test.why))
		})
	}
}

// TestViewChanges_TheWireShapeIsUnchanged is the same promise for the seventh
// family off `[]string`.
func TestViewChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.ViewChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.ViewChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the body, the check option and the attributes do not reach the wire",
			changes: difftypes.ViewChanges{{
				Name:       "active_users",
				Body:       "SELECT id FROM users",
				WithCheck:  true,
				Attributes: []string{"SCHEMABINDING"},
			}},
			want: `["active_users"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
		{
			name:    "a schema-qualified view keeps its qualified spelling",
			changes: difftypes.ViewChanges{{Name: "reporting.active_users"}},
			want:    `["reporting.active_users"]`,
			why:     "the schema is folded into the name, so the name IS the identity a consumer keys on",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestViewChanges_TheBodySurvivesInMemory is the other half: the field the wire
// drops is the field the planner reads.
//
// A view planned from its name alone renders as an empty SELECT, which is the
// failure this family exists to remove, so the carry is asserted where it is
// used rather than only where it is written.
func TestViewChanges_TheBodySurvivesInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.ViewChanges{{Name: "active_users", Body: "SELECT id FROM users", WithCheck: true}}

	c.Assert(changes[0].Body, qt.Equals, "SELECT id FROM users",
		qt.Commentf("the planner renders this rather than looking the name back up"))
	c.Assert(changes[0].WithCheck, qt.IsTrue,
		qt.Commentf("WITH CHECK OPTION is the view's, and a name cannot carry it"))
	c.Assert(changes.Names(), qt.DeepEquals, []string{"active_users"},
		qt.Commentf("and the name list a consumer reads is unchanged"))
}
