package generator

// White-box testing required: every exported entry point that writes a native
// migration file reads a live database first, and the property under test --
// how the file body ends a statement that is only a comment -- is decided in
// the SQL-only generation stage below them.

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff/difftypes"
)

// Dropping a column ends the PostgreSQL plan on a note, a statement that is
// only a comment: the warning that the drop deletes data. The native migration
// file writes it as a plain line: no closing " --" and no semicolon, which
// together read as "--;" (stokaro/ptah#3903). The ALTER TABLE before it keeps
// its semicolon.
func TestGenerateUpMigrationSQL_EndsANoteWithoutASemicolon(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "site_media_settings",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "legacy"}},
		}},
	}

	sql, err := generateUpMigrationSQL(
		context.Background(), must.Must(builtin.New()),
		diff, &schemamodel.Database{}, platform.Postgres,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "DROP COLUMN \"legacy\" CASCADE;\n")
	c.Assert(strings.HasSuffix(sql,
		"\n-- WARNING: Dropping column site_media_settings.legacy with CASCADE - This will delete data and dependent objects!"),
		qt.IsTrue, qt.Commentf("migration body:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "--;")
}
