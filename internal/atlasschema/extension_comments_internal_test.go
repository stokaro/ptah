package atlasschema

// White-box testing required: the rehearsal applies omitControlComments to the
// rebuild it derives from a live target, and its public entry point requires a
// live database connection; the live tests drive the control file query.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

// TestOmitControlComments pins which extension comments the rebuild leaves
// out: exactly those that equal the comment the control file of the installed
// version gives, on an extension the rebuild creates. A changed comment, a
// comment of another version, and the comment of an extension the dev database
// already holds are kept, so they meet the baseline guard.
func TestOmitControlComments(t *testing.T) {
	defaults := map[extensionVersion]string{
		{name: "pgcrypto", version: "1.3"}: "cryptographic functions",
		{name: "pg_trgm", version: "1.6"}:  "text similarity measurement and index searching based on trigrams",
		{name: "citext", version: "1.6"}:   "data type for case-insensitive character strings",
	}
	tests := []struct {
		name      string
		extension schemamodel.Extension
		held      []catalog.Extension
		want      string
	}{
		{name: "the control file comment", extension: schemamodel.Extension{Name: "pgcrypto", Version: "1.3", Comment: "cryptographic functions"}},
		{name: "a changed comment", extension: schemamodel.Extension{Name: "pgcrypto", Version: "1.3", Comment: "hashing"}, want: "hashing"},
		{name: "a comment that differs in case", extension: schemamodel.Extension{Name: "pgcrypto", Version: "1.3", Comment: "Cryptographic functions"},
			want: "Cryptographic functions"},
		{name: "another version", extension: schemamodel.Extension{Name: "pg_trgm", Version: "1.5", Comment: "text similarity measurement and index searching based on trigrams"},
			want: "text similarity measurement and index searching based on trigrams"},
		{name: "an extension the dev database holds", extension: schemamodel.Extension{Name: "citext", Version: "1.6", Comment: "data type for case-insensitive character strings"},
			held: []catalog.Extension{{Name: "citext", Version: "1.6"}}, want: "data type for case-insensitive character strings"},
		{name: "an extension the dev server does not offer", extension: schemamodel.Extension{Name: "postgis", Version: "3.5", Comment: "PostGIS geometry"},
			want: "PostGIS geometry"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			original := []schemamodel.Extension{test.extension}
			target := &schemamodel.Database{Extensions: original}

			omitControlComments(target, test.held, defaults)

			c.Assert(target.Extensions[0].Comment, qt.Equals, test.want)
			c.Assert(original[0].Comment, qt.Equals, test.extension.Comment)
		})
	}
}
