package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

// TestParseSource_BlockSizeIsAnOwnerAttribute pins that the frontend reads no
// block size of its own: without the MySQL owner selected, the attribute is
// one the index, constraint and table directives do not declare.
func TestParseSource_BlockSizeIsAnOwnerAttribute(t *testing.T) {
	for _, source := range []string{
		"package entities\ntype T struct {\n//ptah:schema:index name=\"k\" fields=\"a\" key_block_size=\"8\"\nA int\n}",
		"package entities\ntype T struct {\n//ptah:schema:constraint name=\"pk\" type=\"PRIMARY KEY\" columns=\"a\" key_block_size=\"8\"\nA int\n}",
		"package entities\n//ptah:schema:table name=\"t\" primary_key=\"a\" primary_key_block_size=\"8\"\ntype T struct { A int }",
	} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(noOwners, "entities.go", source)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}
