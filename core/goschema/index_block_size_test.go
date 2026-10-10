package goschema_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

func TestParseSource_IndexBlockSize_FailurePath(t *testing.T) {
	for _, sourceFormat := range []string{
		"package entities\ntype T struct {\n//ptah:schema:constraint name=\"pk\" type=\"PRIMARY KEY\" columns=\"a\" key_block_size=\"%s\"\nA int\n}",
		"package entities\n//ptah:schema:table name=\"t\" primary_key=\"a\" primary_key_block_size=\"%s\"\ntype T struct { A int }",
	} {
		for _, value := range []string{"", "-1", "1.5", "wrong", "18446744073709551616"} {
			t.Run(sourceFormat+"/"+value, func(t *testing.T) {
				c := qt.New(t)
				source := fmt.Sprintf(sourceFormat, value)
				_, err := goschema.ParseSource(noOwners, "entities.go", source)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			})
		}
	}
}

// TestParseSource_IndexBlockSizeIsAnOwnerAttribute pins that the frontend no
// longer reads an index's block size: without the MySQL owner selected, the
// attribute is one the index directive does not declare.
func TestParseSource_IndexBlockSizeIsAnOwnerAttribute(t *testing.T) {
	c := qt.New(t)
	source := "package entities\ntype T struct {\n//ptah:schema:index name=\"k\" fields=\"a\" key_block_size=\"8\"\nA int\n}"
	_, err := goschema.ParseSource(noOwners, "entities.go", source)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
}
