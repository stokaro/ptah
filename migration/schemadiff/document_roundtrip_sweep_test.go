package schemadiff_test

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/migration/schemadiff"
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
			field: "Hypertables",
			seed: func(d *goschema.Database) {
				d.Hypertables = append(d.Hypertables, goschema.Hypertable{
					Table: "users", Column: "created_at",
				})
			},
			count: func(d *goschema.Database) int { return len(d.Hypertables) },
		},
		{
			field: "ContinuousAggregates",
			seed: func(d *goschema.Database) {
				d.ContinuousAggregates = append(d.ContinuousAggregates, goschema.ContinuousAggregate{
					Name: "hourly", Body: "SELECT time_bucket('1 hour', created_at) AS bucket FROM users GROUP BY bucket",
				})
			},
			count: func(d *goschema.Database) int { return len(d.ContinuousAggregates) },
		},
		{
			field: "Synonyms",
			seed: func(d *goschema.Database) {
				d.Synonyms = append(d.Synonyms, goschema.Synonym{Name: "s1", Target: "other.dbo.users"})
			},
			count: func(d *goschema.Database) int { return len(d.Synonyms) },
		},
		{
			field: "ExtendedProperties",
			seed: func(d *goschema.Database) {
				d.ExtendedProperties = append(d.ExtendedProperties, goschema.ExtendedProperty{
					Name: "MS_Description", Value: "the users", Schema: "public", Table: "users",
				})
			},
			count: func(d *goschema.Database) int { return len(d.ExtendedProperties) },
		},
	}
}

// TestHCLDocument_StillRemovesWhatItCouldHaveNamed is the control the YAML
// record needs, and the direction that costs a capability when it is wrong.
//
// HCL has a block for a sequence, a domain, a composite type and a range, so an
// HCL document that omits one IS asking for it to go. A record applied to every
// format rather than to the surface that lacks the key would pass the YAML test
// and the round-trip sweep both, while quietly making those four undroppable
// from the format Ptah itself writes.
func TestHCLDocument_StillRemovesWhatItCouldHaveNamed(t *testing.T) {
	c := qt.New(t)
	live := &catalog.Database{
		Schemas:   []catalog.Schema{{Name: "public"}},
		Tables:    []catalog.Table{{Schema: "public", Name: "users"}},
		Sequences: []catalog.Sequence{{Schema: "public", Name: "s1"}},
		Domains:   []catalog.Domain{{Schema: "public", Name: "d1", BaseType: "text"}},
		Composites: []catalog.CompositeType{{
			Schema: "public", Name: "c1",
			Fields: []catalog.CompositeField{{Name: "a", Type: "integer"}},
		}},
		Ranges: []catalog.Range{{Schema: "public", Name: "r1", Subtype: "integer"}},
	}

	parsed := loadPostgresDocument(c, renderPostgresDocument(c, roundTripFixture()))
	diff := schemadiff.Compare(parsed, live)

	c.Assert(diff.SequencesRemoved, qt.HasLen, 1)
	c.Assert(diff.DomainsRemoved, qt.HasLen, 1)
	c.Assert(diff.CompositeTypesRemoved, qt.HasLen, 1)
	c.Assert(diff.RangesRemoved, qt.HasLen, 1)
}

// yamlUnwritableFields are the object families the YAML surface has no
// top-level key for, and the coverage kind each one is recorded under.
//
// The list is written out rather than derived, because the thing it describes
// lives in another package's unexported struct. That makes it a claim this test
// checks rather than a copy it trusts: a key added to the YAML surface turns
// TestYAMLDocument_RecordsExactlyWhatTheSurfaceCannotName red on the family
// that gained it.
//
// It was measured rather than read off the parser. A YAML schema declaring one
// table, compared against a database holding one of each, planned
// `DROP SEQUENCE`, `DROP DOMAIN` and both `DROP TYPE`s -- silently, because
// nothing recorded that the document could not have named them
// (stokaro/ptah#1031).
var yamlUnwritableFields = map[string]coverage.Kind{
	"Sequences":      coverage.Sequence,
	"Domains":        coverage.Domain,
	"CompositeTypes": coverage.Composite,
	"Ranges":         coverage.Range,
	// HCL gained a block for these two (stokaro/ptah#1031) and the sweep above
	// measures that they survive it; YAML still has no key, so here they stay.
	"Hypertables":          coverage.Hypertable,
	"ContinuousAggregates": coverage.ContinuousAggregate,
	"Synonyms":             coverage.Synonym,
	"ExtendedProperties":   coverage.ExtendedProperty,
}

// TestYAMLDocument_RecordsExactlyWhatTheSurfaceCannotName is the same rule as
// the round-trip sweep, for the format that has no renderer.
//
// A YAML document is written by hand, so there is no round trip to run -- but
// the question is the same one: a family the surface has no key for is a family
// the author could not have named, and reading that silence as intent drops it.
// The complement matters as much as the list: a blanket record would suppress
// every removal a YAML schema legitimately asks for, so the families the
// surface DOES carry are asserted unrecorded.
func TestYAMLDocument_RecordsExactlyWhatTheSurfaceCannotName(t *testing.T) {
	for _, row := range roundTripRows() {
		t.Run(row.field, func(t *testing.T) {
			c := qt.New(t)
			kind, unwritable := yamlUnwritableFields[row.field]

			parsed := loadYAMLDocument(c)

			c.Assert(yamlRecordsKind(parsed, kind), qt.Equals, unwritable,
				qt.Commentf("%s: the YAML surface and this document's coverage record disagree",
					row.field))
		})
	}
}

// yamlRecordsKind answers whether a parsed document declines one kind, and
// answers false for the zero kind so a family the surface carries has one
// question rather than two.
func yamlRecordsKind(parsed *goschema.Database, kind coverage.Kind) bool {
	if kind == "" {
		return false
	}
	return !parsed.NotDescribed.Describes(kind)
}

// loadYAMLDocument writes and loads the smallest YAML schema there is.
func loadYAMLDocument(c *qt.C) *goschema.Database {
	c.Helper()
	path := filepath.Join(c.TB.(*testing.T).TempDir(), "schema.yaml")
	const document = "tables:\n" +
		"  users:\n" +
		"    name: users\n" +
		"    columns:\n" +
		"      id:\n" +
		"        type: INTEGER\n" +
		"        primary: true\n"
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	parsed, err := schemafile.Load(path, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	return parsed
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

// TestRoundTrip_EveryObjectFamilySurvives is the sweep that turns
// stokaro/ptah#1031's two defects into a rule.
//
// The loop is the one an operator runs -- `schema inspect > out.hcl` then
// `schema apply --to file://out.hcl` -- and a family that does not survive it
// is silently dropped, which the comparison then reads as a request to remove
// it. Synonyms and extended properties were exactly that, and nothing said so
// until the round trip was measured.
//
// All sixteen families now survive. The two that did not, until the HCL
// surface gained a `synonym` and an `extended_property` block, are the reason
// the sweep is a sweep: the question is asked of every family rather than of
// the ones somebody suspected.
//
// A family the surface genuinely cannot name is not silently dropped either --
// it records a coverage kind instead, which is what
// [TestYAMLDocument_RecordsExactlyWhatTheSurfaceCannotName] measures on the
// format that still has no key for these two.
func TestRoundTrip_EveryObjectFamilySurvives(t *testing.T) {
	for _, row := range roundTripRows() {
		t.Run(row.field, func(t *testing.T) {
			c := qt.New(t)
			db := roundTripFixture()
			row.seed(db)

			parsed := loadPostgresDocument(c, renderPostgresDocument(c, db))

			c.Assert(row.count(parsed) > 0, qt.Equals, true,
				qt.Commentf("%s did not survive the document Ptah itself wrote", row.field))
		})
	}
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
