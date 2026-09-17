package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/goschema"
	"ptah.run/migration/schemadiff"
)

// A Go schema can say what it does not describe.
//
// A serialized description carries the statement as a directive in its leading
// comment header, which a Go schema has no place for: it is not a document.
// Without an annotation of its own, a Go schema was the one desired-state
// source that could not decline an object family, and its silence about one
// was read as a removal (stokaro/ptah#3377).

// notDescribedSource declines one named extension and one whole family, and
// declares a table so the parse has something to succeed at.
const notDescribedSource = `package entities

//ptah:schema:notdescribed kind="extension" name="pg_trgm"
//ptah:schema:notdescribed kind="role"
type _ struct{}

//ptah:schema:table name="notes"
type Note struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`

func TestParseSource_NotDescribed_HappyPath(t *testing.T) {
	t.Run("a named object and a whole family", func(t *testing.T) {
		c := qt.New(t)

		database, err := goschema.ParseSource("entities.go", notDescribedSource)

		c.Assert(err, qt.IsNil)
		c.Assert(database.NotDescribed.Directives(), qt.DeepEquals, []string{
			`ptah:not-described extension provenance=declared "pg_trgm"`,
			`ptah:not-described role provenance=declared`,
		})
	})

	t.Run("a schema that declines nothing claims everything", func(t *testing.T) {
		c := qt.New(t)

		database, err := goschema.ParseSource("entities.go", `package entities

//ptah:schema:table name="notes"
type Note struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`)

		c.Assert(err, qt.IsNil)
		c.Assert(database.NotDescribed.IsZero(), qt.IsTrue)
	})
}

func TestParseSource_NotDescribed_FailurePath(t *testing.T) {
	t.Run("a kind the closed list does not hold", func(t *testing.T) {
		c := qt.New(t)

		_, err := goschema.ParseSource("entities.go", `package entities

//ptah:schema:notdescribed kind="widget"
type _ struct{}
`)

		c.Assert(err, qt.ErrorMatches, `(?s)unknown coverage kind "widget".*`)
	})

	t.Run("no kind at all", func(t *testing.T) {
		c := qt.New(t)

		_, err := goschema.ParseSource("entities.go", `package entities

//ptah:schema:notdescribed name="pg_trgm"
type _ struct{}
`)

		c.Assert(err, qt.ErrorMatches, `(?s)missing required annotation attribute "kind".*`)
	})

	t.Run("an attribute the directive does not take", func(t *testing.T) {
		c := qt.New(t)

		_, err := goschema.ParseSource("entities.go", `package entities

//ptah:schema:notdescribed kind="extension" reason="because"
type _ struct{}
`)

		c.Assert(err, qt.ErrorMatches, `(?s)unknown annotation attribute "reason".*`)
	})
}

// TestParseSource_NotDescribed_WithholdsTheRemoval is what the directive is
// for. Parsing it is not the claim; the comparison reading it is.
func TestParseSource_NotDescribed_WithholdsTheRemoval(t *testing.T) {
	c := qt.New(t)
	live := &catalog.Database{
		Extensions: []catalog.Extension{{Name: "pg_trgm", Schema: "public"}},
	}

	declining, err := goschema.ParseSource("entities.go", notDescribedSource)
	c.Assert(err, qt.IsNil)
	silent, err := goschema.ParseSource("entities.go", `package entities

//ptah:schema:table name="notes"
type Note struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`)
	c.Assert(err, qt.IsNil)

	withheld := schemadiff.CompareWithOptions(&declining, live, config.DefaultCompareOptions())
	// The control: the same comparison from a schema that declines nothing.
	planned := schemadiff.CompareWithOptions(&silent, live, config.DefaultCompareOptions())

	c.Assert(withheld.ExtensionsRemoved, qt.HasLen, 0, qt.Commentf("removed: %#v", withheld.ExtensionsRemoved))
	c.Assert(planned.ExtensionsRemoved, qt.HasLen, 1, qt.Commentf("removed: %#v", planned.ExtensionsRemoved))
}

// TestParseSource_NotDescribed_AgreesWithTheSerializedSpelling holds the two
// grammars to one meaning. A Go schema declining an object and a description
// carrying the directive in its header are the same statement, so a comparison
// cannot tell them apart.
func TestParseSource_NotDescribed_AgreesWithTheSerializedSpelling(t *testing.T) {
	c := qt.New(t)
	live := &catalog.Database{
		Extensions: []catalog.Extension{{Name: "pg_trgm", Schema: "public"}},
	}

	annotated, err := goschema.ParseSource("entities.go", notDescribedSource)
	c.Assert(err, qt.IsNil)

	byHeader, err := goschema.ParseSource("entities.go", `package entities

//ptah:schema:table name="notes"
type Note struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`)
	c.Assert(err, qt.IsNil)
	byHeader.NotDescribed = byHeader.NotDescribed.WithObject(coverage.Extension, "pg_trgm")

	fromAnnotation := schemadiff.CompareWithOptions(&annotated, live, config.DefaultCompareOptions())
	fromHeader := schemadiff.CompareWithOptions(&byHeader, live, config.DefaultCompareOptions())

	c.Assert(fromAnnotation.ExtensionsRemoved, qt.DeepEquals, fromHeader.ExtensionsRemoved)
}
