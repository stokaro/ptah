package generator

// White-box testing required: every exported entry point that writes a native
// migration file reads a live database first, and the property under test --
// how the file body ends a statement that is only a comment -- is decided in
// the SQL-only generation stage below them.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// Dropping a table's last RLS policy ends the PostgreSQL plan on a note, a
// statement that is only a comment. The native migration file writes it as a
// plain line: no closing " --" and no semicolon, which together read as "--;"
// (stokaro/ptah#3903). The DROP POLICY before it keeps its semicolon.
func TestGenerateUpMigrationSQL_EndsANoteWithoutASemicolon(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		RLSPoliciesRemoved: []difftypes.RLSPolicyRef{{
			PolicyName: "tenant_only",
			TableName:  "site_media_settings",
		}},
	}

	sql, err := generateUpMigrationSQL(diff, &schemamodel.Database{}, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "DROP POLICY IF EXISTS \"tenant_only\" ON \"site_media_settings\";\n")
	c.Assert(strings.HasSuffix(sql,
		"\n-- NOTE: RLS policies were removed from table site_media_settings - verify if RLS should be disabled"),
		qt.IsTrue, qt.Commentf("migration body:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "--;")
}
