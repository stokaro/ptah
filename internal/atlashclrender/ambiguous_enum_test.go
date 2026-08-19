package atlashclrender_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// twoSchemaEnumRealm is the realm stokaro/ptah#1360's second half is about: one
// enum name in two schemas, which is legal PostgreSQL and which Ptah's IR
// models.
//
// Created and read back on PostgreSQL 17.10 as
//
//	CREATE SCHEMA other;
//	CREATE TYPE public.mood AS ENUM ('happy','sad');
//	CREATE TYPE other.mood  AS ENUM ('happy','sad');
func twoSchemaEnumRealm() *goschema.Database {
	return &goschema.Database{
		Schemas: []goschema.Schema{{Name: "public"}, {Name: "other"}},
		Enums: []goschema.Enum{
			{Name: "mood", Schema: "public", Values: []string{"happy", "sad"}},
			{Name: "mood", Schema: "other", Values: []string{"happy", "sad"}},
		},
	}
}

// enumBlockHeaders returns the header line of every enum block in a rendered
// document, and enumColumnTypes every enum reference a column is typed by.
//
// A row states the whole set rather than probing the document with one Contains
// after another: the claim is which labels the document carries, so a block
// that gained a schema label it should not have is a diff here, where a
// substring probe for the label it should have had stays green.
//
// Both are sorted, because the labels are the claim and the order blocks are
// emitted in is not.
func enumBlockHeaders(hcl string) []string {
	return renderedLinesWithPrefix(hcl, "enum ")
}

func enumColumnTypes(hcl string) []string {
	return renderedLinesWithPrefix(hcl, "type = enum")
}

func renderedLinesWithPrefix(hcl, prefix string) []string {
	out := make([]string, 0)
	for line := range strings.SplitSeq(hcl, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, prefix) {
			out = append(out, trimmed)
		}
	}
	slices.Sort(out)
	return out
}

// TestRenderLabelsAnAmbiguousEnumWithItsSchema is the renderer half of
// stokaro/ptah#1360.
//
// Two distinct objects were written with one label. A document holding two
// blocks both headed `enum "mood"` describes a database no reader can
// reconstruct: Ptah read it back as ONE enum before this change, and refused it
// after the loader learned to notice the repeat -- either way, `schema inspect`
// then `schema apply` did not reproduce the database it had just described.
//
// The two-label spelling is not invented here. Measured on PostgreSQL 17.10, the
// pinned Atlas community binary v1.3.0's own `schema inspect` of that realm
// emits `enum "other" "mood"` and `enum "public" "mood"`, and its inspect of a
// realm holding one enum emits the one-label `enum "mood"` -- so qualifying
// only where the bare name is ambiguous is that binary's own rule.
func TestRenderLabelsAnAmbiguousEnumWithItsSchema(t *testing.T) {
	tests := []struct {
		name string
		db   *goschema.Database
		// wantBlocks is every enum block header the document must hold, and
		// wantTypes every enum reference a column is typed by. Both are the
		// whole set: a label the renderer added is as much a failure as one it
		// left out.
		wantBlocks []string
		wantTypes  []string
	}{
		{
			name:       "an ambiguous name is written with its schema",
			db:         twoSchemaEnumRealm(),
			wantBlocks: []string{`enum "other" "mood" {`, `enum "public" "mood" {`},
			wantTypes:  make([]string, 0),
		},
		{
			name: "an unambiguous name keeps one label",
			db: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}},
				Enums: []goschema.Enum{
					{Name: "mood", Schema: "public", Values: []string{"happy", "sad"}},
				},
			},
			wantBlocks: []string{`enum "mood" {`},
			wantTypes:  make([]string, 0),
		},
		{
			// Two enums sharing a name AND a schema are one object declared
			// twice, not two objects needing distinct labels. Writing them with
			// a schema label would hide the repeat behind two blocks that look
			// different, which is the opposite of what this change is for.
			name: "one schema twice keeps one label",
			db: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}},
				Enums: []goschema.Enum{
					{Name: "mood", Schema: "public", Values: []string{"happy", "sad"}},
					{Name: "mood", Schema: "public", Values: []string{"happy", "sad"}},
				},
			},
			wantBlocks: []string{`enum "mood" {`},
			wantTypes:  make([]string, 0),
		},
		{
			// The reference must gain the schema exactly where the block does,
			// or the document names a label that is not there. Measured on the
			// pinned binary for the same realm: `type = enum.public.mood` and
			// `type = enum.other.mood`.
			name: "a column typed by an ambiguous enum references it by schema",
			db: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}, {Name: "other"}},
				Tables:  []goschema.Table{{StructName: "O", Name: "o", Schema: "other"}},
				Fields:  []goschema.Field{{StructName: "O", Name: "m", Type: "other.mood"}},
				Enums: []goschema.Enum{
					{Name: "mood", Schema: "public", Values: []string{"happy", "sad"}},
					{Name: "mood", Schema: "other", Values: []string{"happy", "sad"}},
				},
			},
			wantBlocks: []string{`enum "other" "mood" {`, `enum "public" "mood" {`},
			wantTypes:  []string{`type = enum.other.mood`},
		},
		{
			// One enum, one reference, one label: the shape every existing
			// document has. Without this row the qualification could apply
			// unconditionally and every reference in the tree would change.
			name: "a column typed by an unambiguous enum references it by name",
			db: &goschema.Database{
				Schemas: []goschema.Schema{{Name: "public"}},
				Tables:  []goschema.Table{{StructName: "P", Name: "p", Schema: "public"}},
				Fields:  []goschema.Field{{StructName: "P", Name: "m", Type: "mood"}},
				Enums: []goschema.Enum{
					{Name: "mood", Schema: "public", Values: []string{"happy", "sad"}},
				},
			},
			wantBlocks: []string{`enum "mood" {`},
			wantTypes:  []string{`type = enum.mood`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := test.db
			goschema.Finalize(db)

			rendered, err := atlashclrender.RenderInspected(db, "postgres", "public")

			c.Assert(err, qt.IsNil)
			c.Assert(enumBlockHeaders(string(rendered.Data)), qt.DeepEquals, test.wantBlocks)
			c.Assert(enumColumnTypes(string(rendered.Data)), qt.DeepEquals, test.wantTypes)
		})
	}
}

// TestInspectedTwoSchemaEnumsRoundTrip is the property the renderer and the
// loader owe together: what Ptah writes, Ptah reads.
//
// The pinned Atlas community binary v1.3.0 does not have this property for this
// realm. Measured on PostgreSQL 17.10, its `schema inspect` writes both enums as
// two-label blocks and its own loader then answers `Error: duplicate enum
// "mood"` at exit 1 on the file it just produced. Ptah's default matches that
// refusal, because never exiting 0 where that binary exits 1 is the drop-in
// floor; the variable is the half above the floor, and under it the document
// re-renders byte for byte and both types survive.
func TestInspectedTwoSchemaEnumsRoundTrip(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlashcl.SchemaScopedEnumsEnvVar, "1")

	inspected := twoSchemaEnumRealm()
	goschema.Finalize(inspected)
	first, err := atlashclrender.RenderInspected(inspected, "postgres", "public")
	c.Assert(err, qt.IsNil)

	reparsed, err := atlashcl.Parse(first.Data, "inspected.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(reparsed.Enums, qt.HasLen, 2)

	second, err := atlashclrender.RenderInspected(reparsed, "postgres", "public")
	c.Assert(err, qt.IsNil)
	c.Assert(string(second.Data), qt.Equals, string(first.Data))
}

// TestInspectedTwoSchemaEnumsAreRefusedByDefault is the parity half of the same
// property, and it is here rather than only in internal/atlashcl because it is
// the RENDERER's output that is being read back.
//
// The default must refuse, because the pinned binary refuses. A render that
// produced a document the default reads at exit 0 would be a drop-in violation
// introduced by a rendering change, which is the failure this row exists to
// catch.
func TestInspectedTwoSchemaEnumsAreRefusedByDefault(t *testing.T) {
	c := qt.New(t)

	inspected := twoSchemaEnumRealm()
	goschema.Finalize(inspected)
	rendered, err := atlashclrender.RenderInspected(inspected, "postgres", "public")
	c.Assert(err, qt.IsNil)

	_, err = atlashcl.Parse(rendered.Data, "inspected.hcl")

	c.Assert(err, qt.ErrorMatches, `.*enum "mood" is declared more than once;.*PTAH_HCL_SCHEMA_SCOPED_ENUMS=1.*`)
}

// TestFinalizeKeepsTwoSchemaEnums pins the fold this depends on.
//
// [goschema.Deduplicate] keyed enums by their BARE name, so an inspected realm
// holding public.mood and other.mood arrived at the renderer with one enum and
// the other gone with no diagnostic -- the document then described a database
// that does not exist. The Go-annotation path is unaffected because an enum
// declared there carries no schema, which the second row pins.
func TestFinalizeKeepsTwoSchemaEnums(t *testing.T) {
	tests := []struct {
		name  string
		enums []goschema.Enum
		want  int
	}{
		{
			name: "one name in two schemas is two enums",
			enums: []goschema.Enum{
				{Name: "mood", Schema: "public", Values: []string{"happy"}},
				{Name: "mood", Schema: "other", Values: []string{"happy"}},
			},
			want: 2,
		},
		{
			name: "one name twice in one schema is one enum",
			enums: []goschema.Enum{
				{Name: "mood", Schema: "public", Values: []string{"happy"}},
				{Name: "mood", Schema: "public", Values: []string{"happy"}},
			},
			want: 1,
		},
		{
			name: "one unschemad name twice is one enum",
			enums: []goschema.Enum{
				{Name: "mood", Values: []string{"happy"}},
				{Name: "mood", Values: []string{"happy"}},
			},
			want: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &goschema.Database{Enums: test.enums}

			goschema.Finalize(db)

			c.Assert(db.Enums, qt.HasLen, test.want)
		})
	}
}
