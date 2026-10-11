package pgextension_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// installed is pg_trgm 1.5 in public, which may move.
func installed() *pgextension.ObservedExtension {
	return &pgextension.ObservedExtension{Schema: "public", Version: "1.5", Relocatable: true, Provides: []string{"similarity"}}
}

func TestChange_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		change  *pgextension.Change
		updates bool
		removes bool
		effect  schemaext.Effect
	}{
		{
			name:   "an installation",
			change: &pgextension.Change{After: &pgextension.DesiredExtension{Version: "1.6"}},
			effect: schemaext.Effect{Impact: schemaext.Additive, Reason: "installs an extension"},
		},
		{
			name: "a drop", change: &pgextension.Change{Before: installed()}, removes: true,
			effect: schemaext.Effect{Impact: schemaext.Destructive, Reason: "DROP EXTENSION removes database objects owned by the extension"},
		},
		{
			name: "a version update", change: &pgextension.Change{Before: installed(), After: &pgextension.DesiredExtension{Version: " 1.6 "}}, updates: true,
			effect: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes the version or the schema of an extension and of the objects it owns"},
		},
		{
			name:   "a move",
			change: &pgextension.Change{Before: installed(), After: &pgextension.DesiredExtension{Schema: "ext"}, Schema: "ext"},
			effect: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes the version or the schema of an extension and of the objects it owns"},
		},
		{
			name:   "a comment",
			change: &pgextension.Change{Before: installed(), After: &pgextension.DesiredExtension{Version: "1.5", Comment: "trigrams"}, Comment: true},
			effect: schemaext.Effect{Impact: schemaext.Additive, Reason: "changes only the comment of an extension"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.change.Validate(), qt.IsNil)
			c.Assert(test.change.UpdatesVersion(), qt.Equals, test.updates)
			c.Assert(test.change.RemovesState(), qt.Equals, test.removes)
			c.Assert(test.change.Effect(), qt.Equals, test.effect)

			codec := pgextension.ChangeCodec()
			encoded := must.Must(codec.Encode(test.change))
			decoded := must.Must(codec.Decode(encoded))
			c.Assert(decoded, qt.DeepEquals, codecCanonical(test.change))
		})
	}
}

// codecCanonical is a change as its codec writes it: the members sorted.
func codecCanonical(change *pgextension.Change) *pgextension.Change {
	canonical := change.Copy()
	if canonical.Before != nil {
		canonical.Before.Provides = []string{"similarity"}
	}
	return canonical
}

func TestChange_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		change *pgextension.Change
		want   string
	}{
		{name: "no operand", change: &pgextension.Change{}, want: `.*requires a before or after operand`},
		{name: "a move of an installation", change: &pgextension.Change{After: &pgextension.DesiredExtension{}, Schema: "ext"},
			want: `.*only a surviving extension moves or changes its comment`},
		{name: "a comment of a drop", change: &pgextension.Change{Before: installed(), Comment: true},
			want: `.*only a surviving extension moves or changes its comment`},
		{name: "a surviving extension that changes nothing", change: &pgextension.Change{Before: installed(), After: &pgextension.DesiredExtension{Version: "1.5"}},
			want: `.*changes its version, its schema or its comment`},
		{name: "an invalid operand", change: &pgextension.Change{After: &pgextension.DesiredExtension{Version: "\xff"}},
			want: `.*extension version is not valid UTF-8`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.change.Validate()

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(test.change.Effect(), qt.Equals, schemaext.Effect{})
		})
	}
}

func TestChangeCodec_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{name: "an omitted key", wire: `{"before":null,"after":{},"schema":""}`},
		{name: "an operand in its own wrong spelling", wire: `{"before":null,"after":{"version":""},"schema":"","comment":false}`},
		{name: "a null finding", wire: `{"before":null,"after":{},"schema":null,"comment":false}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := pgextension.ChangeCodec().Decode(json.RawMessage(test.wire))

			c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
			c.Assert(decoded, qt.IsNil)
		})
	}
}
