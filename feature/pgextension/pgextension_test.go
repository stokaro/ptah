package pgextension_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

func TestRef_HappyPath(t *testing.T) {
	c := qt.New(t)
	ref := pgextension.Ref("audit.tools")

	c.Assert(pgextension.ValidateRef(ref), qt.IsNil)
	c.Assert(pgextension.Name(ref), qt.Equals, "audit.tools")
	c.Assert(ref.Kind, qt.Equals, objectidentity.Kind(pgextension.ExtensionKind))
	c.Assert(ref.Schema.Empty(), qt.IsTrue)
	c.Assert(pgextension.Subject(ref), qt.DeepEquals, objectidentity.ID{Kind: objectidentity.KindExtension,
		Name: objectidentity.Part{Source: "audit.tools", Normalized: "audit.tools"}})
	c.Assert(pgextension.Ref("PG_TRGM").Key(), qt.Not(qt.Equals), pgextension.Ref("pg_trgm").Key())
}

func TestValidateRef_FailurePath(t *testing.T) {
	inSchema := pgextension.Ref("pg_trgm")
	inSchema.Schema = objectidentity.Part{Source: "public", Normalized: "public"}
	onTable := pgextension.Ref("pg_trgm")
	onTable.Parent = objectidentity.Part{Source: "orders", Normalized: "orders"}
	folded := pgextension.Ref("PG_TRGM")
	folded.Name.Normalized = "pg_trgm"
	tests := []struct {
		name string
		ref  objectidentity.ID
		want string
	}{
		{name: "no name", ref: pgextension.Ref(""), want: `invalid feature value: an extension is identified by its name alone, not .*`},
		{name: "a schema", ref: inSchema, want: `invalid feature value: an extension is identified by its name alone, not .*`},
		{name: "a table", ref: onTable, want: `invalid feature value: an extension is identified by its name alone, not .*`},
		{name: "a folded name", ref: folded, want: `invalid feature value: an extension is identified by its name alone, not .*`},
		{name: "another kind", ref: objectidentity.ID{Kind: objectidentity.KindExtension, Name: pgextension.Ref("pg_trgm").Name},
			want: `invalid feature value: an extension is identified by its name alone, not .*`},
		{name: "a NUL byte", ref: pgextension.Ref("pg\x00trgm"), want: `invalid feature value: extension name contains a NUL byte`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := pgextension.ValidateRef(test.ref)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

func TestObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	declared := pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true}
	observed := pgextension.ObservedExtension{Schema: "ext", Version: "1.6", Relocatable: true, Comment: new("trigrams"),
		Provides: []string{"gin_trgm_ops", "similarity"}}

	desiredObject := must.Must(pgextension.DesiredObject(pgextension.Ref("pg_trgm"), declared))
	observedObject := must.Must(pgextension.ObservedObject(pgextension.Ref("pg_trgm"), observed))

	c.Assert(desiredObject.Value, qt.DeepEquals, &declared)
	c.Assert(observedObject.Value, qt.DeepEquals, &observed)
	c.Assert(observedObject.Ref, qt.DeepEquals, pgextension.Ref("pg_trgm"))
}

func TestObjects_FailurePath(t *testing.T) {
	t.Run("a declaration with a NUL byte", func(t *testing.T) {
		c := qt.New(t)

		object, err := pgextension.DesiredObject(pgextension.Ref("pg_trgm"), pgextension.DesiredExtension{Comment: "a\x00b"})

		c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
		c.Assert(err, qt.ErrorMatches, `.*extension comment contains a NUL byte`)
		c.Assert(object.Value, qt.IsNil)
	})

	t.Run("an observation without a name", func(t *testing.T) {
		c := qt.New(t)

		object, err := pgextension.ObservedObject(pgextension.Ref(""), pgextension.ObservedExtension{Schema: "public", Version: "1.0"})

		c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
		c.Assert(object.Value, qt.IsNil)
	})

	t.Run("an observation with an invalid member", func(t *testing.T) {
		c := qt.New(t)

		object, err := pgextension.ObservedObject(pgextension.Ref("isn"),
			pgextension.ObservedExtension{Schema: "public", Version: "1.2", Provides: []string{"isbn", "\xff"}})

		c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
		c.Assert(err, qt.ErrorMatches, `.*extension member is not valid UTF-8`)
		c.Assert(object.Value, qt.IsNil)
	})

	t.Run("a nil declaration", func(t *testing.T) {
		c := qt.New(t)

		err := pgextension.ValidateDesired(nil)

		c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
		c.Assert(err, qt.ErrorMatches, `.*nil extension declaration`)
	})

	t.Run("a nil observation", func(t *testing.T) {
		c := qt.New(t)

		err := pgextension.ValidateObserved(nil)

		c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
		c.Assert(err, qt.ErrorMatches, `.*nil extension observation`)
	})
}

// TestObservedExtension_Desired pins the declaration that keeps an installed
// extension as it is, which a rollback creates again with IF NOT EXISTS, with
// the members the read reported for an export to read.
func TestObservedExtension_Desired(t *testing.T) {
	tests := []struct {
		name     string
		observed *pgextension.ObservedExtension
		want     *pgextension.DesiredExtension
	}{
		{
			name:     "with a comment",
			observed: &pgextension.ObservedExtension{Schema: "ext", Version: "1.6", Relocatable: true, Comment: new("trigrams"), Provides: []string{"similarity"}},
			want:     &pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true, Provides: []string{"similarity"}},
		},
		{
			name:     "without one",
			observed: &pgextension.ObservedExtension{Schema: "public", Version: "1.0"},
			want:     &pgextension.DesiredExtension{Schema: "public", Version: "1.0", IfNotExists: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.observed.Desired(), qt.DeepEquals, test.want)
		})
	}

	t.Run("nil", func(t *testing.T) {
		c := qt.New(t)

		c.Assert((*pgextension.ObservedExtension)(nil).Desired(), qt.IsNil)
	})
}

func TestExtension_EqualAndCopy(t *testing.T) {
	declared := &pgextension.DesiredExtension{Schema: "ext", Version: "1.6"}
	observed := &pgextension.ObservedExtension{Schema: "ext", Version: "1.6", Comment: new("x"), Provides: []string{"b", "a", "a"}}

	t.Run("IF NOT EXISTS is part of a declaration's words", func(t *testing.T) {
		c := qt.New(t)
		guarded := declared.Copy()
		guarded.IfNotExists = true

		c.Assert(declared.Equal(declared.Clone()), qt.IsTrue)
		c.Assert(declared.Equal(guarded), qt.IsFalse)
		c.Assert(declared.Equal(observed), qt.IsFalse)
	})

	t.Run("the members are a set", func(t *testing.T) {
		c := qt.New(t)
		reordered := observed.Copy()
		reordered.Provides = []string{"a", "b"}
		other := observed.Copy()
		other.Provides = []string{"a", "c"}
		carried := observed.Desired()
		carriedReordered := carried.Copy()
		carriedReordered.Provides = []string{"a", "b"}
		carriedOther := carried.Copy()
		carriedOther.Provides = []string{"a", "c"}

		c.Assert(observed.Equal(reordered), qt.IsTrue)
		c.Assert(observed.Equal(other), qt.IsFalse)
		c.Assert(carried.Equal(carriedReordered), qt.IsTrue)
		c.Assert(carried.Equal(carriedOther), qt.IsFalse)
	})

	t.Run("a missing comment is not an empty one", func(t *testing.T) {
		c := qt.New(t)
		empty := observed.Copy()
		empty.Comment = new("")
		missing := observed.Copy()
		missing.Comment = nil

		c.Assert(empty.Equal(missing), qt.IsFalse)
		c.Assert(missing.Equal(missing.Clone()), qt.IsTrue)
	})

	t.Run("a copy shares nothing", func(t *testing.T) {
		c := qt.New(t)
		copied := observed.Copy()
		*copied.Comment = "changed"
		copied.Provides[0] = "changed"

		declaredCopy := observed.Desired()
		declaredOriginal := declaredCopy.Copy()
		declaredCopy.Provides[0] = "changed"

		c.Assert(*observed.Comment, qt.Equals, "x")
		c.Assert(observed.Provides[0], qt.Equals, "b")
		c.Assert(declaredOriginal.Provides[0], qt.Equals, "b")
		c.Assert((*pgextension.ObservedExtension)(nil).Copy(), qt.IsNil)
		c.Assert((*pgextension.DesiredExtension)(nil).Copy(), qt.IsNil)
	})
}

// TestNamesAndSelect pins the name view of a collection: extensions by name in
// identity order, and every other object left alone.
func TestNamesAndSelect(t *testing.T) {
	c := qt.New(t)
	other := schemaext.Object{Ref: objectidentity.ID{Kind: "example.org/other", Name: objectidentity.Part{Source: "pg_trgm", Normalized: "pg_trgm"}},
		Value: &otherValue{}}
	objects := must.Must(schemaext.NewObjects(
		must.Must(pgextension.DesiredObject(pgextension.Ref("pg_trgm"), pgextension.DesiredExtension{})),
		must.Must(pgextension.ObservedObject(pgextension.Ref("citext"), pgextension.ObservedExtension{Schema: "public", Version: "1.6"})),
		other,
	))

	kept := pgextension.Select(objects, func(name string) bool { return name != "pg_trgm" })

	c.Assert(pgextension.Names(objects), qt.DeepEquals, []string{"citext", "pg_trgm"})
	c.Assert(pgextension.Names(kept), qt.DeepEquals, []string{"citext"})
	c.Assert(kept.Refs(), qt.DeepEquals, []objectidentity.ID{other.Ref, pgextension.Ref("citext")})
	c.Assert(objects.Len(), qt.Equals, 3)
}

// otherValue is another owner's object that happens to share a name.
type otherValue struct{}

func (*otherValue) Kind() schemaext.Kind             { return "example.org/other" }
func (*otherValue) Clone() schemaext.Value           { return &otherValue{} }
func (*otherValue) Equal(other schemaext.Value) bool { _, ok := other.(*otherValue); return ok }
