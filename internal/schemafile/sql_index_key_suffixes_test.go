package schemafile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemafile"
)

// postgresIndexSuffixSQL is what `ptah-compat schema inspect --format '{{ sql
// . }}'` writes for a PostgreSQL 17.10 database holding one index per key
// suffix. It is copied from a measured run, not composed by hand.
const postgresIndexSuffixSQL = `CREATE TABLE "t" (
  "id" integer PRIMARY KEY NOT NULL,
  "code" text,
  "created_at" timestamptz,
  "score" integer,
  "tsv" tsvector
);
CREATE INDEX IF NOT EXISTS "i_desc" ON "t" ("created_at" DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS "i_nullsfst" ON "t" ("score" NULLS FIRST);
CREATE INDEX IF NOT EXISTS "i_opclass" ON "t" ("code" text_pattern_ops);
CREATE INDEX IF NOT EXISTS "i_plain" ON "t" ("code");
CREATE INDEX IF NOT EXISTS "i_siglen" ON "t" USING gist ("tsv" tsvector_ops(siglen=64));
`

// TestToDBSchema_SQLDocumentCarriesIndexKeySuffixes pins the surface the
// operator-class parameter has to survive alongside the live reader, the HCL
// reader and the HCL writer: Ptah's own `.sql` schema-file surface.
//
// Measured on live PostgreSQL 17.10 before this guard existed, every row below
// except the plain one came back from this loader as a single EXPRESSION -- the
// column name with its suffix glued on -- so diffing the database against the
// document `schema inspect` had just written for it planned a DROP plus a
// CREATE for an identical index, and the CREATE was
// `(("tsv" tsvector_ops(siglen=64)))`, which psql refuses at exit 3. The DROP
// had already committed, so the index was gone and nothing replaced it.
//
// The values on the right are the reader's spellings, because these are the
// values the comparator holds against what the catalog reports.
func TestToDBSchema_SQLDocumentCarriesIndexKeySuffixes(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	path := writeSchemaFile(c, dir, "inspected.sql", postgresIndexSuffixSQL)
	db, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)

	parts := map[string][]dbschematypes.DBIndexPart{}
	for _, index := range schemafile.ToDBSchema(db, platform.Postgres).Indexes {
		parts[index.Name] = index.Parts
	}

	tests := []struct {
		name  string
		index string
		want  []dbschematypes.DBIndexPart
	}{
		{
			name:  "a parameterised default operator class survives whole",
			index: "i_siglen",
			want:  []dbschematypes.DBIndexPart{{Name: "tsv", Operator: "tsvector_ops(siglen=64)"}},
		},
		{
			name:  "a non-default operator class survives",
			index: "i_opclass",
			want:  []dbschematypes.DBIndexPart{{Name: "code", Operator: "text_pattern_ops"}},
		},
		{
			name:  "a direction and its NULLS ordering survive together",
			index: "i_desc",
			want:  []dbschematypes.DBIndexPart{{Name: "created_at", Desc: true, NullsOrder: dbschematypes.NullsOrderLast}},
		},
		{
			name:  "an explicit NULLS ordering without a direction survives",
			index: "i_nullsfst",
			want:  []dbschematypes.DBIndexPart{{Name: "score", NullsOrder: dbschematypes.NullsOrderFirst}},
		},
		{
			name:  "a key list with no suffix stays on the legacy column path",
			index: "i_plain",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(parts[tt.index], qt.DeepEquals, tt.want)
		})
	}
}
