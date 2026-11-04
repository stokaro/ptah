package annotationmeta_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/annotationmeta"
)

// TestCommentDirectiveName_HappyPath names the directive a comment line is
// written as, whether or not a catalog declares it, reading the line as the
// catalog's matcher reads it.
func TestCommentDirectiveName_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		want    string
	}{
		{name: "a directive with attributes", comment: `//ptah:schema:field name="id"`, want: "ptah:schema:field"},
		{name: "a bare directive", comment: "//ptah:embedded", want: "ptah:embedded"},
		{name: "a space after the marker", comment: `// ptah:schema:table name="users"`, want: "ptah:schema:table"},
		{name: "a tab after the name", comment: "//ptah:schema:column\tname=\"id\"", want: "ptah:schema:column"},
		{name: "an unknown name", comment: `//ptah:schema:not-a-known-directive x`, want: "ptah:schema:not-a-known-directive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := annotationmeta.CommentDirectiveName(test.comment)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestCommentDirectiveName_FailurePath is what is not written as a directive:
// prose, a bare prefix, a block comment and a line that only mentions one.
func TestCommentDirectiveName_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		comment string
	}{
		{name: "prose naming Ptah", comment: "// ptah is the tool"},
		{name: "a bare prefix", comment: "//ptah:"},
		{name: "a prefix followed by prose", comment: "// ptah: keep it short"},
		{name: "a block comment", comment: "/* ptah:schema:field */"},
		{name: "a directive mentioned mid-line", comment: "// see //ptah:schema:field"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := annotationmeta.CommentDirectiveName(test.comment)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
