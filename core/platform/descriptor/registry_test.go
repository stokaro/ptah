package descriptor_test

import (
	"encoding/json"
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

func head(name string, aliases ...string) descriptor.Dialect {
	return descriptor.Dialect{
		Name:        name,
		Aliases:     aliases,
		Family:      name,
		Placeholder: descriptor.PlaceholderQuestion,
		Identifiers: descriptor.Identifiers{IndexNamespace: "table", IndexNames: "exact", TableNames: "exact", ColumnNames: "exact"},
	}
}

func member(name, family string, aliases ...string) descriptor.Dialect {
	described := head(name, aliases...)
	described.Family = family
	return described
}

func TestNewRegistry_HappyPath(t *testing.T) {
	c := qt.New(t)

	registry, err := descriptor.NewRegistry(head("alpha", "a", "alpha+tls"), member("beta", "alpha", "b"), head("alpha", "a", "alpha+tls"))

	c.Assert(err, qt.IsNil)
	c.Assert(registry.Spellings(), qt.DeepEquals, []string{"alpha", "a", "alpha+tls", "beta", "b"})
	c.Assert(registry.Normalize(" ALPHA+TLS "), qt.Equals, "alpha")
	c.Assert(registry.Normalize("b"), qt.Equals, "beta")
	c.Assert(registry.Normalize("gamma"), qt.Equals, "")
	c.Assert(registry.Normalize(""), qt.Equals, "")
	described, found := registry.Lookup("B")
	c.Assert(found, qt.IsTrue)
	c.Assert(described, qt.DeepEquals, member("beta", "alpha", "b"))
	c.Assert(registry.Dialects(), qt.DeepEquals, []descriptor.Dialect{head("alpha", "a", "alpha+tls"), member("beta", "alpha", "b")})
}

// TestRegistry_With_LeavesTheReceiverUnchanged pins that a registry is
// immutable: extending it answers new spellings in the result only, and the
// spellings it already answered keep their answers there.
func TestRegistry_With_LeavesTheReceiverUnchanged(t *testing.T) {
	c := qt.New(t)
	base, err := descriptor.NewRegistry(head("alpha", "a"))
	c.Assert(err, qt.IsNil)

	extended, err := base.With(member("beta", "alpha", "b"), head("alpha", "a"))

	c.Assert(err, qt.IsNil)
	c.Assert(base.Spellings(), qt.DeepEquals, []string{"alpha", "a"})
	c.Assert(base.Normalize("b"), qt.Equals, "")
	c.Assert(extended.Spellings(), qt.DeepEquals, []string{"alpha", "a", "beta", "b"})
	c.Assert(extended.Normalize("a"), qt.Equals, "alpha")
}

// TestRegistry_HandsOutCopies pins that nothing a caller receives aliases the
// registry's own state.
func TestRegistry_HandsOutCopies(t *testing.T) {
	c := qt.New(t)
	given := head("alpha", "a")
	registry, err := descriptor.NewRegistry(given)
	c.Assert(err, qt.IsNil)

	given.Aliases[0] = "given"
	looked, _ := registry.Lookup("alpha")
	looked.Aliases[0] = "looked"
	listed := registry.Dialects()
	listed[0].Aliases[0] = "listed"
	spellings := registry.Spellings()
	spellings[1] = "spelled"

	c.Assert(registry.Spellings(), qt.DeepEquals, []string{"alpha", "a"})
	c.Assert(registry.Normalize("a"), qt.Equals, "alpha")
}

func TestRegistry_NilIsEmpty(t *testing.T) {
	c := qt.New(t)
	var registry *descriptor.Registry

	described, found := registry.Lookup("alpha")

	c.Assert(found, qt.IsFalse)
	c.Assert(described, qt.DeepEquals, descriptor.Dialect{})
	c.Assert(registry.Normalize("alpha"), qt.Equals, "")
	c.Assert(registry.Dialects(), qt.HasLen, 0)
	c.Assert(registry.Spellings(), qt.HasLen, 0)
}

func TestNewRegistry_FailurePath(t *testing.T) {
	withPlaceholder := head("alpha")
	withPlaceholder.Placeholder = "percent"
	withoutPlaceholder := head("alpha")
	withoutPlaceholder.Placeholder = ""
	tests := []struct {
		name     string
		dialects []descriptor.Dialect
		wantIs   error
		wantErr  string
	}{
		{name: "an empty name", dialects: []descriptor.Dialect{head("")},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `invalid dialect descriptor: name "" must be .*`},
		{name: "an upper-case name", dialects: []descriptor.Dialect{head("Alpha")},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `invalid dialect descriptor: name "Alpha" must be .*`},
		{name: "a name beginning with a digit", dialects: []descriptor.Dialect{head("1alpha")},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `invalid dialect descriptor: name "1alpha" must be .*`},
		{name: "an alias with a space", dialects: []descriptor.Dialect{head("alpha", "al pha")},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `invalid dialect descriptor: dialect "alpha": alias "al pha" must be .*`},
		{name: "an alias that repeats the name", dialects: []descriptor.Dialect{head("alpha", "alpha")},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `invalid dialect descriptor: dialect "alpha" spells "alpha" twice`},
		{name: "an undeclared placeholder", dialects: []descriptor.Dialect{withPlaceholder},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `invalid dialect descriptor: dialect "alpha": placeholder "percent" is not one of .*`},
		{name: "no placeholder", dialects: []descriptor.Dialect{withoutPlaceholder},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `invalid dialect descriptor: dialect "alpha": placeholder "" is not one of .*`},
		{name: "a family no dialect is named", dialects: []descriptor.Dialect{member("beta", "alpha")},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `invalid dialect descriptor: dialect "beta" names family "alpha", which is no dialect's name`},
		{name: "a family named by an alias", dialects: []descriptor.Dialect{head("alpha", "a"), member("beta", "a")},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `invalid dialect descriptor: dialect "beta" names family "a", which is no dialect's name`},
		{name: "a family whose head belongs to another", dialects: []descriptor.Dialect{head("alpha"), member("beta", "alpha"), member("gamma", "beta")},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `invalid dialect descriptor: dialect "gamma" names family "beta", whose own family is "alpha" rather than itself`},
		{name: "a name another dialect has as an alias", dialects: []descriptor.Dialect{head("alpha", "beta"), head("beta")},
			wantIs: descriptor.ErrConflict, wantErr: `conflicting dialect descriptor: dialect "beta" claims "beta", which dialect "alpha" already owns`},
		{name: "an alias two dialects claim", dialects: []descriptor.Dialect{head("alpha", "x"), head("beta", "x")},
			wantIs: descriptor.ErrConflict, wantErr: `conflicting dialect descriptor: dialect "beta" claims "x", which dialect "alpha" already owns`},
		{name: "a name given twice with different content", dialects: []descriptor.Dialect{head("alpha"), head("alpha", "a")},
			wantIs: descriptor.ErrConflict, wantErr: `conflicting dialect descriptor: dialect "alpha" is already described differently`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialects...)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}

// TestRegistry_With_FailurePath pins that a refused extension leaves the
// receiver as it was, including the descriptors given before the refused one.
func TestRegistry_With_FailurePath(t *testing.T) {
	c := qt.New(t)
	base, err := descriptor.NewRegistry(head("alpha", "a"))
	c.Assert(err, qt.IsNil)

	extended, err := base.With(head("beta"), head("gamma", "a"))

	c.Assert(err, qt.ErrorIs, descriptor.ErrConflict)
	c.Assert(extended, qt.IsNil)
	c.Assert(base.Spellings(), qt.DeepEquals, []string{"alpha", "a"})
}

func TestDialect_Equal(t *testing.T) {
	c := qt.New(t)
	withoutAliases := head("alpha")
	withoutAliases.Aliases = nil
	withEmptyAliases := head("alpha")
	withEmptyAliases.Aliases = []string{}

	c.Assert(withoutAliases.Equal(withEmptyAliases), qt.IsTrue)
	c.Assert(head("alpha", "a").Equal(head("alpha", "a")), qt.IsTrue)
	c.Assert(head("alpha", "a").Equal(head("alpha", "b")), qt.IsFalse)
	c.Assert(head("alpha").Equal(member("alpha", "")), qt.IsFalse)
}

func TestDialect_Clone(t *testing.T) {
	c := qt.New(t)
	original := head("alpha", "a")

	clone := original.Clone()
	clone.Aliases[0] = "b"

	c.Assert(original.Aliases, qt.DeepEquals, []string{"a"})
	c.Assert(head("alpha").Clone().Aliases, qt.IsNil)
}

// TestDialect_RoundTripsThroughJSON pins that a descriptor survives being
// sent to another process and read back, which is what lets a provider
// outside the process describe an engine.
func TestDialect_RoundTripsThroughJSON(t *testing.T) {
	c := qt.New(t)
	sent := member("beta", "alpha", "b")
	sent.DefaultSchema = "main"

	encoded, err := json.Marshal(sent)
	c.Assert(err, qt.IsNil)
	var received descriptor.Dialect
	c.Assert(json.Unmarshal(encoded, &received), qt.IsNil)

	c.Assert(received, qt.DeepEquals, sent)
	c.Assert(string(encoded), qt.Equals, `{"name":"beta","aliases":["b"],"family":"alpha","default_schema":"main",`+
		`"identifiers":{"index_namespace":"table","index_names":"exact","table_names":"exact","column_names":"exact"},"placeholder":"question"}`)
}

// TestDialect_HoldsDataOnly walks every type a descriptor reaches and refuses
// the kinds that cannot cross a process boundary or that would share state
// between copies. A field added later is held to it without an edit here.
func TestDialect_HoldsDataOnly(t *testing.T) {
	c := qt.New(t)

	c.Assert(behaviorKinds(reflect.TypeFor[descriptor.Dialect](), "Dialect"), qt.HasLen, 0)
}

// behaviorKinds returns the path of every field whose type is not data.
func behaviorKinds(typ reflect.Type, path string) []string {
	switch typ.Kind() {
	case reflect.Struct:
		var found []string
		for field := range typ.Fields() {
			found = append(found, behaviorKinds(field.Type, path+"."+field.Name)...)
		}
		return found
	case reflect.Slice, reflect.Array:
		return behaviorKinds(typ.Elem(), path+"[]")
	case reflect.Map:
		return append(behaviorKinds(typ.Key(), path+"{key}"), behaviorKinds(typ.Elem(), path+"{}")...)
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return nil
	default:
		return []string{path + " is a " + typ.Kind().String()}
	}
}

// TestBehaviorKinds_FailurePath is the acceptance control of the walk above:
// each kind it must refuse, reached the ways a descriptor field could reach
// it, is reported.
func TestBehaviorKinds_FailurePath(t *testing.T) {
	c := qt.New(t)
	type nested struct{ Ready chan bool }
	type shape struct {
		Hook      func()
		Any       any
		Pointer   *string
		Inside    nested
		Listed    []func()
		Keyed     map[string]any
		Plain     map[string][]bool
		Unsafeish uintptr
	}

	c.Assert(behaviorKinds(reflect.TypeFor[shape](), "shape"), qt.DeepEquals, []string{
		"shape.Hook is a func",
		"shape.Any is a interface",
		"shape.Pointer is a ptr",
		"shape.Inside.Ready is a chan",
		"shape.Listed[] is a func",
		"shape.Keyed{} is a interface",
		"shape.Unsafeish is a uintptr",
	})
}
