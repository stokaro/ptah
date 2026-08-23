package schemadiff_test

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/schemafile"
)

// roundTripRow is one object family of [goschema.Database], and what has to be
// true of it after the document Ptah writes is read back.
type roundTripRow struct {
	// field is the goschema.Database field name, and the key the reflection
	// guard matches against.
	field string
	// seed puts one object of this family into a schema.
	seed func(*goschema.Database)
	// count reads the family back out of the parsed document.
	count func(*goschema.Database) int
	// kind is the coverage kind that must be recorded when the family does NOT
	// survive. The empty kind means the family is expected to survive, and a
	// row that neither survives nor records one is the defect this sweep is
	// about.
	kind coverage.Kind
}

// roundTripRows is one row per object family the HCL document could carry.
//
// Families that are not objects are absent by construction and named in
// [nonObjectDatabaseFields] instead: a column is not a thing a document can
// omit without omitting its table, and coverage's kind list is closed for
// exactly that reason.
func roundTripRows() []roundTripRow {
	return []roundTripRow{
		{
			field: "Enums",
			seed: func(d *goschema.Database) {
				d.Enums = append(d.Enums, goschema.Enum{Name: "mood", Values: []string{"a", "b"}})
			},
			count: func(d *goschema.Database) int { return len(d.Enums) },
		},
		{
			field: "Extensions",
			seed:  func(d *goschema.Database) { d.Extensions = append(d.Extensions, goschema.Extension{Name: "citext"}) },
			count: func(d *goschema.Database) int { return len(d.Extensions) },
		},
		{
			field: "Functions",
			seed: func(d *goschema.Database) {
				d.Functions = append(d.Functions, goschema.Function{
					StructName: "F", Name: "fn", Returns: "integer", Language: "sql", Body: "SELECT 1",
				})
			},
			count: func(d *goschema.Database) int { return len(d.Functions) },
		},
		{
			field: "Sequences",
			seed: func(d *goschema.Database) {
				d.Sequences = append(d.Sequences, goschema.Sequence{StructName: "S", Name: "s1"})
			},
			count: func(d *goschema.Database) int { return len(d.Sequences) },
		},
		{
			field: "Domains",
			seed: func(d *goschema.Database) {
				d.Domains = append(d.Domains, goschema.Domain{StructName: "D", Name: "d1", BaseType: "text"})
			},
			count: func(d *goschema.Database) int { return len(d.Domains) },
		},
		{
			field: "CompositeTypes",
			seed: func(d *goschema.Database) {
				d.CompositeTypes = append(d.CompositeTypes, goschema.CompositeType{
					StructName: "C", Name: "c1",
					Fields: []goschema.CompositeTypeField{{Name: "a", Type: "integer"}},
				})
			},
			count: func(d *goschema.Database) int { return len(d.CompositeTypes) },
		},
		{
			field: "Ranges",
			seed: func(d *goschema.Database) {
				d.Ranges = append(d.Ranges, goschema.Range{StructName: "R", Name: "r1", Subtype: "integer"})
			},
			count: func(d *goschema.Database) int { return len(d.Ranges) },
		},
		{
			field: "Views",
			seed: func(d *goschema.Database) {
				d.Views = append(d.Views, goschema.View{StructName: "V", Name: "v1", Body: "SELECT 1"})
			},
			count: func(d *goschema.Database) int { return len(d.Views) },
		},
		{
			field: "MaterializedViews",
			seed: func(d *goschema.Database) {
				d.MaterializedViews = append(d.MaterializedViews, goschema.MaterializedView{
					StructName: "M", Name: "m1", Body: "SELECT 1",
				})
			},
			count: func(d *goschema.Database) int { return len(d.MaterializedViews) },
		},
		{
			field: "Triggers",
			seed: func(d *goschema.Database) {
				d.Triggers = append(d.Triggers, goschema.Trigger{
					StructName: "G", Name: "g1", Table: "users", Timing: "BEFORE", Event: "INSERT",
					ForEach: "ROW", Body: "BEGIN RETURN NEW; END;",
				})
			},
			count: func(d *goschema.Database) int { return len(d.Triggers) },
		},
		{
			field: "RLSPolicies",
			seed: func(d *goschema.Database) {
				d.RLSPolicies = append(d.RLSPolicies, goschema.RLSPolicy{
					StructName: "P", Name: "p1", Table: "users", PolicyFor: "ALL",
					ToRoles: "app", UsingExpression: "true",
				})
			},
			count: func(d *goschema.Database) int { return len(d.RLSPolicies) },
		},
		{
			field: "RLSEnabledTables",
			seed: func(d *goschema.Database) {
				d.RLSEnabledTables = append(d.RLSEnabledTables, goschema.RLSEnabledTable{
					StructName: "T", Table: "users",
				})
			},
			count: func(d *goschema.Database) int { return len(d.RLSEnabledTables) },
		},
		{
			field: "Roles",
			seed:  func(d *goschema.Database) { d.Roles = append(d.Roles, goschema.Role{StructName: "R", Name: "app"}) },
			count: func(d *goschema.Database) int { return len(d.Roles) },
		},
		{
			field: "Grants",
			seed: func(d *goschema.Database) {
				d.Grants = append(d.Grants, goschema.Grant{
					StructName: "G", Role: "app", Privileges: []string{"SELECT"}, OnTable: "users",
				})
			},
			count: func(d *goschema.Database) int { return len(d.Grants) },
		},
		{
			field: "Synonyms",
			seed: func(d *goschema.Database) {
				d.Synonyms = append(d.Synonyms, goschema.Synonym{Name: "s1", Target: "other.dbo.users"})
			},
			count: func(d *goschema.Database) int { return len(d.Synonyms) },
			kind:  coverage.Synonym,
		},
		{
			field: "ExtendedProperties",
			seed: func(d *goschema.Database) {
				d.ExtendedProperties = append(d.ExtendedProperties, goschema.ExtendedProperty{
					Name: "MS_Description", Value: "the users", Schema: "public", Table: "users",
				})
			},
			count: func(d *goschema.Database) int { return len(d.ExtendedProperties) },
			kind:  coverage.ExtendedProperty,
		},
	}
}

// nonObjectDatabaseFields are the slice fields of [goschema.Database] that no
// document omits on its own.
//
// Tables, columns, indexes and constraints are absent because a description
// that does not mention one IS saying it should go -- coverage's kind list is
// closed for that reason. The rest are inputs to the model rather than objects
// in it: source-only helper declarations, dependency maps rendered as ordering,
// and seed rows.
var nonObjectDatabaseFields = []string{
	"Constraints", "EmbeddedFields", "Fields", "Indexes", "ManagedData", "Schemas", "Tables",
}

// TestRoundTrip_EveryObjectFamilySurvivesOrSaysItDidNot is the sweep that turns
// stokaro/ptah#1031's two defects into a rule.
//
// The loop is the one an operator runs -- `schema inspect > out.hcl` then
// `schema apply --to file://out.hcl` -- and a family that neither survives it
// nor records a coverage kind is silently dropped, which the comparison then
// reads as a request to remove it. Synonyms and extended properties were
// exactly that, and nothing said so until the round trip was measured.
//
// Fourteen of the sixteen families survive; the two that cannot be written in
// HCL at all carry their kind instead. A row is a claim about ONE of those two
// outcomes rather than about the document's contents, so the sweep stays true
// when a family later gains a block.
func TestRoundTrip_EveryObjectFamilySurvivesOrSaysItDidNot(t *testing.T) {
	for _, row := range roundTripRows() {
		t.Run(row.field, func(t *testing.T) {
			c := qt.New(t)
			db := roundTripFixture()
			row.seed(db)

			parsed := loadPostgresDocument(c, renderPostgresDocument(c, db))

			c.Assert(roundTripOutcome(row, parsed), qt.Equals, true,
				qt.Commentf("%s neither survived the document nor recorded a coverage kind",
					row.field))
		})
	}
}

// roundTripOutcome answers the sweep's one question, so the assertion above
// stays a single claim rather than a branch in a test body.
func roundTripOutcome(row roundTripRow, parsed *goschema.Database) bool {
	if row.kind == "" {
		return row.count(parsed) > 0
	}
	return !parsed.NotDescribed.Describes(row.kind)
}

// TestRoundTrip_SweepCoversEveryObjectFamily is the guard that makes the test
// above a sweep rather than a list someone remembered to extend.
//
// A family added to [goschema.Database] has no row until somebody writes one,
// and this names it -- which is how the next unwritable object gets noticed
// before an apply drops it.
func TestRoundTrip_SweepCoversEveryObjectFamily(t *testing.T) {
	c := qt.New(t)

	covered := make([]string, 0, len(roundTripRows()))
	for _, row := range roundTripRows() {
		covered = append(covered, row.field)
	}
	covered = append(covered, nonObjectDatabaseFields...)
	slices.Sort(covered)

	c.Assert(covered, qt.DeepEquals, databaseSliceFields())
}

// databaseSliceFields derives the family list from the struct rather than
// repeating it.
func databaseSliceFields() []string {
	databaseType := reflect.TypeFor[goschema.Database]()
	fields := make([]string, 0, databaseType.NumField())
	for field := range databaseType.Fields() {
		if field.Type.Kind() != reflect.Slice {
			continue
		}
		fields = append(fields, field.Name)
	}
	slices.Sort(fields)
	return fields
}

// roundTripFixture is the smallest schema a rendered document needs: one schema
// and one table, so every row's object has somewhere to hang.
func roundTripFixture() *goschema.Database {
	return &goschema.Database{
		Schemas: []goschema.Schema{{Name: "public"}},
		Tables:  []goschema.Table{{StructName: "T", Name: "users", Schema: "public"}},
		Fields:  []goschema.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
	}
}

func renderPostgresDocument(c *qt.C, db *goschema.Database) []byte {
	c.Helper()
	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
	c.Assert(err, qt.IsNil)
	return result.Data
}

func loadPostgresDocument(c *qt.C, document []byte) *goschema.Database {
	c.Helper()
	path := filepath.Join(c.TB.(*testing.T).TempDir(), "sweep.hcl")
	c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)
	parsed, err := schemafile.Load(path, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	return parsed
}
