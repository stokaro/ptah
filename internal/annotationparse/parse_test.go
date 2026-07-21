package annotationparse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/annotationparse"
)

func TestScanReportsDirectiveAndAttributeRanges(t *testing.T) {
	c := qt.New(t)

	annotations := annotationparse.Scan(`package test

type User struct {
	//migrator:schema:field name="email" type="TEXT" defaul="x"
	Email string
}
`)

	c.Assert(annotations, qt.HasLen, 1)
	annotation := annotations[0]
	c.Assert(annotation.Directive, qt.Equals, "migrator:schema:field")
	c.Assert(annotation.Known, qt.IsTrue)
	c.Assert(annotation.DirectiveRange.Start.Line, qt.Equals, 3)
	c.Assert(annotation.DirectiveRange.Start.Character, qt.Equals, 3)
	c.Assert(annotation.Attributes, qt.HasLen, 3)
	c.Assert(annotation.Attributes[2].Name, qt.Equals, "defaul")
	c.Assert(annotation.Attributes[2].Range.Start.Line, qt.Equals, 3)
	c.Assert(annotation.Attributes[2].Range.Start.Character, qt.Equals, 50)
	c.Assert(annotation.Attributes[2].Range.End.Character, qt.Equals, 56)
}

func TestScanUsesLongestKnownDirectiveMatch(t *testing.T) {
	c := qt.New(t)

	annotations := annotationparse.Scan(`//migrator:schema:rls:policy name="tenant" table="users"`)

	c.Assert(annotations, qt.HasLen, 1)
	c.Assert(annotations[0].Directive, qt.Equals, "migrator:schema:rls:policy")
	c.Assert(annotations[0].Known, qt.IsTrue)
}

func TestScanCapturesUnknownMigratorDirective(t *testing.T) {
	c := qt.New(t)

	annotations := annotationparse.Scan(`//migrator:schema:foreign_key name="fk"`)

	c.Assert(annotations, qt.HasLen, 1)
	c.Assert(annotations[0].Directive, qt.Equals, "migrator:schema:foreign_key")
	c.Assert(annotations[0].Known, qt.IsFalse)
}

func TestScanIgnoresMigratorTextInsideStringLiterals(t *testing.T) {
	c := qt.New(t)

	annotations := annotationparse.Scan(`package test

func example() {
	_ = "//migrator:schema:field defaul=\"x\""
}
`)

	c.Assert(annotations, qt.HasLen, 0)
}
