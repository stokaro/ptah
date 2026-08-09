package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRenderInspectedAttributesEverySchemaScopedBlock pins that an inspected
// render names the read's schema on EVERY block type that can carry one, not
// just on the two that reached the fallback first.
//
// A PostgreSQL catalog blanks the schema on exactly the objects the read's own
// search_path made implicit, so an inspected IR arrives with an empty Schema on
// its own schema's objects. A block written without the attribute is created
// wherever the connection that replays it happens to point. Measured on
// PostgreSQL 17.10, one schema `wf1138s` holding one object of each kind below,
// inspected and then planned back into a fresh database with
// `psql -v ON_ERROR_STOP=1`:
//
//	CREATE OR REPLACE FUNCTION "f"(a integer) ...       -> public.f
//	CREATE SEQUENCE "seq1" ...                          -> public.seq1
//	CREATE DOMAIN "positive_int" ...                    -> public.positive_int
//	CREATE TYPE "numrng" AS RANGE ...                   -> public.numrng
//	CREATE TYPE "addr" AS (...)                         -> public.addr
//	CREATE VIEW "v" ...                                 -> public.v
//	CREATE MATERIALIZED VIEW "mv" ...                   -> public.mv
//	CREATE TYPE "wf1138s"."mood" AS ENUM (...)          -> wf1138s.mood, correct
//	CREATE TABLE "wf1138s"."t" (...)                    -> wf1138s.t, correct
//
// Seven of the nine landed in the wrong schema (stokaro/ptah#1138); the two
// that did not are the pair the fallback already served (stokaro/ptah#1276).
//
// The rows are the whole enumeration on purpose. A per-block fix keeps being
// applied to the block an issue happened to name, and the two correct rows are
// here so the table states the rule rather than the exceptions.
func TestRenderInspectedAttributesEverySchemaScopedBlock(t *testing.T) {
	tests := []struct {
		name    string
		declare func(*goschema.Database)
		want    string
	}{
		{
			name: "a sequence",
			declare: func(db *goschema.Database) {
				db.Sequences = []goschema.Sequence{{Name: "seq1"}}
			},
			want: "sequence \"seq1\" {\n  schema = schema.public\n",
		},
		{
			name: "a domain",
			declare: func(db *goschema.Database) {
				db.Domains = []goschema.Domain{{Name: "positive_int", BaseType: "integer"}}
			},
			want: "domain \"positive_int\" {\n  schema = schema.public\n",
		},
		{
			name: "a composite type",
			declare: func(db *goschema.Database) {
				db.CompositeTypes = []goschema.CompositeType{{
					Name:   "addr",
					Fields: []goschema.CompositeTypeField{{Name: "street", Type: "text"}},
				}}
			},
			want: "composite \"addr\" {\n  schema = schema.public\n",
		},
		{
			name: "a range type",
			declare: func(db *goschema.Database) {
				db.Ranges = []goschema.Range{{Name: "numrng", Subtype: "numeric"}}
			},
			want: "range \"numrng\" {\n  schema = schema.public\n",
		},
		{
			name: "a function",
			declare: func(db *goschema.Database) {
				db.Functions = []goschema.Function{{
					Name: "f", Returns: "integer", Language: "sql", Body: "SELECT 1",
				}}
			},
			want: "function \"f\" {\n  schema = schema.public\n",
		},
		{
			name: "a view",
			declare: func(db *goschema.Database) {
				db.Views = []goschema.View{{Name: "v", Body: "SELECT id FROM t"}}
			},
			want: "view \"v\" {\n  schema = schema.public\n",
		},
		{
			name: "a materialized view",
			declare: func(db *goschema.Database) {
				db.MaterializedViews = []goschema.MaterializedView{{
					Name: "mv", Body: "SELECT id FROM t",
				}}
			},
			want: "materialized \"mv\" {\n  schema = schema.public\n",
		},
		{
			// Already correct before stokaro/ptah#1138, and here so the table
			// says so rather than leaving it to be rediscovered.
			name: "an enum",
			declare: func(db *goschema.Database) {
				db.Enums = []goschema.Enum{{Name: "mood", Values: []string{"sad", "ok"}}}
			},
			want: "enum \"mood\" {\n  schema = schema.public\n",
		},
		{
			name:    "a table",
			declare: func(_ *goschema.Database) {},
			want:    "table \"t\" {\n  schema = schema.public\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := inspectedTable("")
			test.declare(db)

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
		})
	}
}

// TestRenderInspectedKeepsASchemaTheReaderReported is the negative half: the
// fallback is a fallback, so a reader that did report a schema is believed for
// every one of the same block types.
//
// Without this row set, "write the read's schema everywhere" and "write the
// read's schema where the object has none" are indistinguishable, and the first
// one moves objects the reader placed correctly.
func TestRenderInspectedKeepsASchemaTheReaderReported(t *testing.T) {
	tests := []struct {
		name    string
		declare func(*goschema.Database)
		want    string
	}{
		{
			name: "a sequence",
			declare: func(db *goschema.Database) {
				db.Sequences = []goschema.Sequence{{Name: "seq1", Schema: "reporting"}}
			},
			want: "sequence \"seq1\" {\n  schema = schema.reporting\n",
		},
		{
			name: "a domain",
			declare: func(db *goschema.Database) {
				db.Domains = []goschema.Domain{{
					Name: "positive_int", Schema: "reporting", BaseType: "integer",
				}}
			},
			want: "domain \"positive_int\" {\n  schema = schema.reporting\n",
		},
		{
			name: "a composite type",
			declare: func(db *goschema.Database) {
				db.CompositeTypes = []goschema.CompositeType{{
					Name:   "addr",
					Schema: "reporting",
					Fields: []goschema.CompositeTypeField{{Name: "street", Type: "text"}},
				}}
			},
			want: "composite \"addr\" {\n  schema = schema.reporting\n",
		},
		{
			name: "a range type",
			declare: func(db *goschema.Database) {
				db.Ranges = []goschema.Range{{
					Name: "numrng", Schema: "reporting", Subtype: "numeric",
				}}
			},
			want: "range \"numrng\" {\n  schema = schema.reporting\n",
		},
		{
			name: "a function",
			declare: func(db *goschema.Database) {
				db.Functions = []goschema.Function{{
					Name: "reporting.f", Returns: "integer", Language: "sql", Body: "SELECT 1",
				}}
			},
			want: "function \"f\" {\n  schema = schema.reporting\n",
		},
		{
			name: "a view",
			declare: func(db *goschema.Database) {
				db.Views = []goschema.View{{Name: "reporting.v", Body: "SELECT id FROM t"}}
			},
			want: "view \"v\" {\n  schema = schema.reporting\n",
		},
		{
			name: "a materialized view",
			declare: func(db *goschema.Database) {
				db.MaterializedViews = []goschema.MaterializedView{{
					Name: "reporting.mv", Body: "SELECT id FROM t",
				}}
			},
			want: "materialized \"mv\" {\n  schema = schema.reporting\n",
		},
		{
			name: "an enum",
			declare: func(db *goschema.Database) {
				db.Enums = []goschema.Enum{{
					Name: "mood", Schema: "reporting", Values: []string{"sad", "ok"},
				}}
			},
			want: "enum \"mood\" {\n  schema = schema.reporting\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := inspectedTable("")
			test.declare(db)

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
		})
	}
}
