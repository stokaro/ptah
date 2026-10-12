package pgtype_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// changeCodec returns the change codec of kind.
func changeCodec(c *qt.C, kind schemaext.Kind) schemaext.Codec {
	c.Helper()
	for _, candidate := range pgtype.ChangeCodecs() {
		if candidate.Prototype.Kind() == kind {
			return candidate
		}
	}
	c.Fatalf("no change codec of kind %s", kind)
	return schemaext.Codec{}
}

var (
	desiredQty  = &pgtype.DesiredDomain{BaseType: "integer", Check: "VALUE = 7"}
	observedQty = &pgtype.ObservedDomain{BaseType: "integer", Check: "(VALUE = 6)", Checks: []pgtype.DomainCheck{{Name: "qty_check", Expression: "(VALUE = 6)"}}}
)

func TestChangeCodecs_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		change schemaext.ChangeValue
		wire   string
	}{
		{name: "a created domain", change: &pgtype.DomainChange{After: desiredQty},
			wire: `{"before":null,"after":{"base_type":"integer","check":"VALUE = 7"},"differs":null}`},
		{name: "a dropped domain", change: &pgtype.DomainChange{Before: observedQty},
			wire: `{"before":{"base_type":"integer","check":"(VALUE = 6)","checks":[{"name":"qty_check","expression":"(VALUE = 6)"}]},"after":null,"differs":null}`},
		{name: "a changed domain", change: &pgtype.DomainChange{Before: observedQty, After: desiredQty, Differs: []string{pgtype.DiffCheck}},
			wire: `{"before":{"base_type":"integer","check":"(VALUE = 6)","checks":[{"name":"qty_check","expression":"(VALUE = 6)"}]},` +
				`"after":{"base_type":"integer","check":"VALUE = 7"},"differs":["check"]}`},
		{name: "a composite type altered in place",
			change: &pgtype.CompositeChange{
				Before:  &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int"}, {Name: "b", Type: "int"}}},
				After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int"}, {Name: "c", Type: "text"}}},
				Differs: []string{pgtype.DiffAttributes}, Added: []pgtype.Attribute{{Name: "c", Type: "text"}}, Removed: []string{"b"}},
			wire: `{"before":{"attributes":[{"name":"a","type":"int"},{"name":"b","type":"int"}]},` +
				`"after":{"attributes":[{"name":"a","type":"int"},{"name":"c","type":"text"}]},"differs":["attributes"],` +
				`"added":[{"name":"c","type":"text"}],"removed":["b"]}`},
		{name: "a changed range type, its empty lists written null",
			change: &pgtype.RangeChange{Before: &pgtype.ObservedRange{Subtype: "int4", SubtypeDiff: "f"},
				After:   &pgtype.DesiredRange{Subtype: "int4", Cleared: []string{pgtype.RangeSubtypeDiff, pgtype.RangeCanonical}},
				Differs: []string{pgtype.RangeSubtypeDiff}},
			wire: `{"before":{"subtype":"int4","subtype_diff":"f"},"after":{"subtype":"int4","cleared":["canonical","subtype_diff"]},` +
				`"differs":["subtype_diff"]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			model := changeCodec(c, test.change.Kind())

			encoded := must.Must(model.Encode(test.change))
			decoded := must.Must(model.Decode(encoded))

			c.Assert(string(encoded), qt.Equals, test.wire)
			c.Assert(must.Must(model.Encode(decoded)), qt.DeepEquals, encoded)
		})
	}
}

func TestChanges_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		change interface{ Validate() error }
		want   string
	}{
		{name: "neither side", change: &pgtype.DomainChange{}, want: `.*a domain change has a current or a desired side`},
		{name: "a difference beside a creation", change: &pgtype.DomainChange{After: desiredQty, Differs: []string{pgtype.DiffCheck}},
			want: `.*a domain that is created or dropped names no difference`},
		{name: "a surviving domain that names nothing", change: &pgtype.DomainChange{Before: observedQty, After: desiredQty},
			want: `.*a surviving domain's change names what differs`},
		{name: "a difference of another kind", change: &pgtype.DomainChange{Before: observedQty, After: desiredQty,
			Differs: []string{pgtype.DiffAttributes}}, want: `.*"attributes" is not a difference of a domain`},
		{name: "differences out of order", change: &pgtype.DomainChange{Before: observedQty, After: desiredQty,
			Differs: []string{pgtype.DiffNotNull, pgtype.DiffCheck}}, want: `.*a domain change names each difference once, sorted`},
		{name: "a difference twice", change: &pgtype.DomainChange{Before: observedQty, After: desiredQty,
			Differs: []string{pgtype.DiffCheck, pgtype.DiffCheck}}, want: `.*a domain change names each difference once, sorted`},
		{name: "an invalid operand", change: &pgtype.DomainChange{After: &pgtype.DesiredDomain{}}, want: `.*a domain requires a base type`},
		{name: "an attribute delta beside a changed comment",
			change: &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int"}}},
				After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int"}}, Comment: "x"},
				Differs: []string{pgtype.DiffComment}, Removed: []string{"a"}},
			want: `.*a composite type's attribute delta belongs to a changed attribute list`},
		{name: "a removed attribute named twice",
			change: &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int"}}},
				After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "b", Type: "int"}}},
				Differs: []string{pgtype.DiffAttributes}, Removed: []string{"a", "a"}},
			want: `.*composite type removed attribute "a" is named twice`},
		{name: "a range type's base-type difference", change: &pgtype.RangeChange{Before: &pgtype.ObservedRange{Subtype: "int4"},
			After: &pgtype.DesiredRange{Subtype: "int8"}, Differs: []string{pgtype.DiffBaseType}},
			want: `.*"base_type" is not a difference of a range type`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.change.Validate()

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestChangeCodecs_FailurePath pins that a change's wire is refused where
// the struct decoder would accept it: an empty list spelled out, and an
// operand whose model wire is refused.
func TestChangeCodecs_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		kind schemaext.Kind
		wire string
	}{
		{name: "an empty difference list", kind: pgtype.DomainChangeKind, wire: `{"before":null,"after":{"base_type":"int"},"differs":[]}`},
		{name: "a missing key", kind: pgtype.DomainChangeKind, wire: `{"before":null,"after":{"base_type":"int"}}`},
		{name: "an operand key in another case", kind: pgtype.DomainChangeKind,
			wire: `{"before":null,"after":{"Base_type":"int"},"differs":null}`},
		{name: "an added attribute without a type", kind: pgtype.CompositeChangeKind,
			wire: `{"before":{"attributes":[{"name":"a","type":"int"}]},"after":{"attributes":[{"name":"b","type":"int"}]},` +
				`"differs":["attributes"],"added":[{"name":"b"}],"removed":["a"]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := changeCodec(c, test.kind).Decode(json.RawMessage(test.wire))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestChanges_TellAHostWhatTheyDo pins the two answers a host reads off a
// change without importing the owner: whether it takes state away, and which
// type it creates, rebuilds or changes in place, under the common kind.
func TestChanges_TellAHostWhatTheyDo(t *testing.T) {
	ref := pgtype.Ref(pgtype.DomainKind, "app", "qty")
	tests := []struct {
		name        string
		change      schemaext.ChangeValue
		wantRemoves bool
		wantCreates bool
	}{
		{name: "a creation", change: &pgtype.DomainChange{After: desiredQty}, wantCreates: true},
		{name: "a change in place", change: &pgtype.DomainChange{Before: observedQty, After: desiredQty, Differs: []string{pgtype.DiffCheck}},
			wantCreates: true},
		{name: "a drop", change: &pgtype.DomainChange{Before: observedQty}, wantRemoves: true},
		{name: "a dropped composite type", change: &pgtype.CompositeChange{Before: &pgtype.ObservedComposite{}}, wantRemoves: true},
		{name: "a created range type", change: &pgtype.RangeChange{After: &pgtype.DesiredRange{Subtype: "int4"}}, wantCreates: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			created := must.Must(schemaext.CreatedTypes([]schemaext.ChangeRecord{{Subject: ref, Value: test.change}}))

			c.Assert(schemaext.RemovesState(test.change), qt.Equals, test.wantRemoves)
			c.Assert(len(created) == 1, qt.Equals, test.wantCreates)
		})
	}
}

// TestDomainChange_CreatesTheDeclaredDomain pins the type a creation reports:
// the subject under objectidentity.KindDomain, naming the declared base type.
func TestDomainChange_CreatesTheDeclaredDomain(t *testing.T) {
	c := qt.New(t)
	ref := pgtype.Ref(pgtype.DomainKind, "app", "qty")

	created := must.Must(schemaext.CreatedTypes([]schemaext.ChangeRecord{{Subject: ref, Value: &pgtype.DomainChange{After: desiredQty}}}))

	want := ref
	want.Kind = objectidentity.KindDomain
	c.Assert(created, qt.DeepEquals, []schemaext.DefinedType{{ID: want, References: []string{"integer"}}})
}

// TestChanges_PlaceACreatedType pins the schema each change asks a host to
// create first: the one a creation's subject names as its source wrote it,
// and none for a type that survives, one that is dropped, or one the source
// left to the connection's default.
func TestChanges_PlaceACreatedType(t *testing.T) {
	domain := &pgtype.DesiredDomain{BaseType: "text"}
	held := &pgtype.ObservedDomain{BaseType: "text"}
	composite := &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "int"}}}
	span := &pgtype.DesiredRange{Subtype: "int4"}
	tests := []struct {
		name    string
		change  schemaext.SchemaPlacement
		subject objectidentity.ID
		want    string
	}{
		{name: "a domain created in a schema", change: &pgtype.DomainChange{After: domain}, subject: pgtype.Ref(pgtype.DomainKind, "App", "t"), want: "App"},
		{name: "a composite type created in a schema", change: &pgtype.CompositeChange{After: composite},
			subject: pgtype.Ref(pgtype.CompositeKind, "app", "t"), want: "app"},
		{name: "a range type created in a schema", change: &pgtype.RangeChange{After: span}, subject: pgtype.Ref(pgtype.RangeKind, "app", "t"), want: "app"},
		{name: "a domain created in the default schema", change: &pgtype.DomainChange{After: domain}, subject: pgtype.Ref(pgtype.DomainKind, "", "t")},
		{name: "a domain that survives", change: &pgtype.DomainChange{Before: held, After: domain, Differs: []string{pgtype.DiffComment}},
			subject: pgtype.Ref(pgtype.DomainKind, "app", "t")},
		{name: "a domain dropped", change: &pgtype.DomainChange{Before: held}, subject: pgtype.Ref(pgtype.DomainKind, "app", "t")},
		{name: "a range type dropped", change: &pgtype.RangeChange{Before: &pgtype.ObservedRange{Subtype: "int4"}},
			subject: pgtype.Ref(pgtype.RangeKind, "app", "t")},
		{name: "a composite type dropped", change: &pgtype.CompositeChange{Before: composite.Observed()},
			subject: pgtype.Ref(pgtype.CompositeKind, "app", "t")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.PlacementSchema(test.subject), qt.Equals, test.want)
		})
	}
}
