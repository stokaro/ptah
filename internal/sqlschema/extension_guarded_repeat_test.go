package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// readExtensionDocument reads sql as PostgreSQL with the built-in owners,
// onto document.
func readExtensionDocument(c *qt.C, sql string, document *sqlschema.Document) (schemamodel.Database, error) {
	c.Helper()
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(sql), platform.Postgres, document,
		sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	return database, err
}

// declaredExtension is the declaration database holds for the extension name.
func declaredExtension(c *qt.C, database schemamodel.Database, name string) *pgextension.DesiredExtension {
	c.Helper()
	object, found, err := database.FeatureObjects.Get(pgextension.Ref(name))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	declared, ok := object.Value.(*pgextension.DesiredExtension)
	c.Assert(ok, qt.IsTrue)
	return declared
}

// TestRead_AGuardedRepeatOfAnExtensionIsSkipped holds that a CREATE EXTENSION
// IF NOT EXISTS of an extension a statement before it declared adds nothing,
// whatever it says: PostgreSQL 18.6 skips it with a notice and keeps the
// extension where the first statement put it.
func TestRead_AGuardedRepeatOfAnExtensionIsSkipped(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want pgextension.DesiredExtension
	}{
		{
			name: "a declared extension, then guarded",
			sql:  "CREATE EXTENSION pgcrypto;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;",
			want: pgextension.DesiredExtension{},
		},
		{
			name: "a guarded extension, then guarded in another schema",
			sql:  "CREATE SCHEMA ext;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;\nCREATE EXTENSION IF NOT EXISTS pgcrypto SCHEMA ext;",
			want: pgextension.DesiredExtension{IfNotExists: true},
		},
		{
			name: "a declared extension, then guarded at a version",
			sql:  "CREATE SCHEMA ext;\nCREATE EXTENSION pgcrypto SCHEMA ext;\nCREATE EXTENSION IF NOT EXISTS pgcrypto VERSION '1.3';",
			want: pgextension.DesiredExtension{Schema: "ext"},
		},
		{
			name: "a guarded repeat before a comment",
			sql: "CREATE EXTENSION pgcrypto;\nCREATE EXTENSION IF NOT EXISTS pgcrypto VERSION '1.3';\n" +
				"COMMENT ON EXTENSION pgcrypto IS 'kept';",
			want: pgextension.DesiredExtension{Comment: "kept"},
		},
		{
			name: "two equal guarded declarations",
			sql:  "CREATE EXTENSION IF NOT EXISTS pgcrypto;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;",
			want: pgextension.DesiredExtension{IfNotExists: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := readExtensionDocument(c, test.sql, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(pgextension.Names(database.FeatureObjects), qt.DeepEquals, []string{"pgcrypto"})
			c.Assert(*declaredExtension(c, database, "pgcrypto"), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_AnUnguardedRepeatOfAnExtension_FailurePath is the control: a
// repeat without the guard is a second declaration, and one that differs from
// the first is refused, as PostgreSQL 18.6 refuses the statement with
// `extension "pgcrypto" already exists`.
func TestRead_AnUnguardedRepeatOfAnExtension_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, err := readExtensionDocument(c, "CREATE SCHEMA ext;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;\nCREATE EXTENSION pgcrypto SCHEMA ext;", nil)

	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(err, qt.ErrorMatches, `duplicate feature identity: conflicting definitions of .*pgcrypto`)
	c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
}

// TestReadOnto_AGuardedRepeatOfAnEarlierFilesExtensionIsSkipped holds that a
// later file of a schema directory adds nothing for a CREATE EXTENSION IF NOT
// EXISTS of an extension an earlier file declared, as the server running the
// files in order skips it, so the merge of the two files has one declaration
// to keep.
func TestReadOnto_AGuardedRepeatOfAnEarlierFilesExtensionIsSkipped(t *testing.T) {
	c := qt.New(t)
	earlier, err := readExtensionDocument(c, "CREATE SCHEMA ext;\nCREATE EXTENSION pgcrypto;", nil)
	c.Assert(err, qt.IsNil)

	later, err := readExtensionDocument(c, "CREATE EXTENSION IF NOT EXISTS pgcrypto SCHEMA ext;", sqlschema.NewDocument(&earlier))

	c.Assert(err, qt.IsNil)
	c.Assert(later.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(*declaredExtension(c, earlier, "pgcrypto"), qt.DeepEquals, pgextension.DesiredExtension{})
}
