package schemadiff_test

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
)

// roundTripRow is one object family of [schemamodel.Database], and what has to be
// true of it after the document Ptah writes is read back.
type roundTripRow struct {
	// field is the schemamodel.Database field name, and the key the reflection
	// guard matches against.
	field string
	// seed puts one object of this family into a schema.
	seed func(*schemamodel.Database)
	// count reads the family back out of the parsed document.
	count func(*schemamodel.Database) int
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
			seed: func(d *schemamodel.Database) {
				d.Enums = append(d.Enums, schemamodel.Enum{Name: "mood", Values: []string{"a", "b"}})
			},
			count: func(d *schemamodel.Database) int { return len(d.Enums) },
		},
		{
			field: "Extensions",
			seed: func(d *schemamodel.Database) {
				d.Extensions = append(d.Extensions, schemamodel.Extension{Name: "citext"})
			},
			count: func(d *schemamodel.Database) int { return len(d.Extensions) },
		},
		{
			field: "Functions",
			seed: func(d *schemamodel.Database) {
				d.Functions = append(d.Functions, schemamodel.Function{
					StructName: "F", Name: "fn", Returns: "integer", Language: "sql", Body: "SELECT 1",
				})
			},
			count: func(d *schemamodel.Database) int { return len(d.Functions) },
		},
		{
			field: "Sequences",
			seed: func(d *schemamodel.Database) {
				d.Sequences = append(d.Sequences, schemamodel.Sequence{StructName: "S", Name: "s1"})
			},
			count: func(d *schemamodel.Database) int { return len(d.Sequences) },
		},
		{
			field: "Domains",
			seed: func(d *schemamodel.Database) {
				d.Domains = append(d.Domains, schemamodel.Domain{StructName: "D", Name: "d1", BaseType: "text"})
			},
			count: func(d *schemamodel.Database) int { return len(d.Domains) },
		},
		{
			field: "CompositeTypes",
			seed: func(d *schemamodel.Database) {
				d.CompositeTypes = append(d.CompositeTypes, schemamodel.CompositeType{
					StructName: "C", Name: "c1",
					Fields: []schemamodel.CompositeField{{Name: "a", Type: "integer"}},
				})
			},
			count: func(d *schemamodel.Database) int { return len(d.CompositeTypes) },
		},
		{
			field: "Ranges",
			seed: func(d *schemamodel.Database) {
				d.Ranges = append(d.Ranges, schemamodel.Range{StructName: "R", Name: "r1", Subtype: "integer"})
			},
			count: func(d *schemamodel.Database) int { return len(d.Ranges) },
		},
		{
			field: "Views",
			seed: func(d *schemamodel.Database) {
				d.Views = append(d.Views, schemamodel.View{StructName: "V", Name: "v1", Body: "SELECT 1"})
			},
			count: func(d *schemamodel.Database) int { return len(d.Views) },
		},
		{
			field: "MaterializedViews",
			seed: func(d *schemamodel.Database) {
				d.MaterializedViews = append(d.MaterializedViews, schemamodel.MaterializedView{
					StructName: "M", Name: "m1", Body: "SELECT 1",
				})
			},
			count: func(d *schemamodel.Database) int { return len(d.MaterializedViews) },
		},
		{
			field: "Triggers",
			seed: func(d *schemamodel.Database) {
				d.Triggers = append(d.Triggers, schemamodel.Trigger{
					StructName: "G", Name: "g1", Table: "users", Timing: "BEFORE", Event: "INSERT",
					ForEach: "ROW", Body: "BEGIN RETURN NEW; END;",
				})
			},
			count: func(d *schemamodel.Database) int { return len(d.Triggers) },
		},
		{
			field: "RLSPolicies",
			seed: func(d *schemamodel.Database) {
				d.RLSPolicies = append(d.RLSPolicies, schemamodel.RLSPolicy{
					StructName: "P", Name: "p1", Table: "users", PolicyFor: "ALL",
					ToRoles: "app", UsingExpression: "true",
				})
			},
			// The shared declaration reads back as the row-security owner's.
			count: func(d *schemamodel.Database) int {
				return len(d.FeatureObjects.Select(func(ref objectidentity.ID) bool {
					return ref.Kind == objectidentity.Kind(pgpolicy.PolicyKind)
				}).Refs())
			},
		},
		{
			field: "RLSEnabledTables",
			seed: func(d *schemamodel.Database) {
				d.RLSEnabledTables = append(d.RLSEnabledTables, schemamodel.RLSEnabledTable{
					StructName: "T", Table: "users",
				})
			},
			count: func(d *schemamodel.Database) int {
				return len(slices.DeleteFunc(slices.Clone(d.Tables), func(table schemamodel.Table) bool {
					return !slices.Contains(table.Facets.Kinds(), pgpolicy.TableStateKind)
				}))
			},
		},
		{
			field: "Roles",
			seed: func(d *schemamodel.Database) {
				d.Roles = append(d.Roles, schemamodel.Role{StructName: "R", Name: "app"})
			},
			count: func(d *schemamodel.Database) int { return len(d.Roles) },
		},
		{
			field: "Grants",
			seed: func(d *schemamodel.Database) {
				d.Grants = append(d.Grants, schemamodel.Grant{
					StructName: "G", Role: "app", Privileges: []string{"SELECT"}, OnTable: "users",
				})
			},
			count: func(d *schemamodel.Database) int { return len(d.Grants) },
		},
		{
			// PUBLIC is the grantee a revoke names most often, and it is not a
			// role the document can declare.
			field: "RevokedGrants",
			seed: func(d *schemamodel.Database) {
				d.RevokedGrants = append(d.RevokedGrants, schemamodel.Grant{
					StructName: "RG", Role: "PUBLIC", Privileges: []string{"TRUNCATE"}, OnTable: "users",
				})
			},
			count: func(d *schemamodel.Database) int { return len(d.RevokedGrants) },
		},
		{
			// The grantor is what forces the family its own block. It is part of
			// a default privilege's identity, and the `permission` block reports
			// a grantor as an export loss rather than writing one, so a family
			// routed through that block would leave this round trip as a warning
			// and nothing else.
			field: "DefaultPrivileges",
			seed: func(d *schemamodel.Database) {
				d.DefaultPrivileges = append(d.DefaultPrivileges, schemamodel.DefaultPrivilege{
					StructName: "D", Grantor: "app_owner", Schema: "public",
					ObjectType: "TABLES", Grantee: "app_reader",
					Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
				})
			},
			count: func(d *schemamodel.Database) int { return len(d.DefaultPrivileges) },
		},
		{
			field: "Synonyms",
			seed: func(d *schemamodel.Database) {
				d.Synonyms = append(d.Synonyms, schemamodel.Synonym{Name: "s1", Target: "other.dbo.users"})
			},
			count: func(d *schemamodel.Database) int { return len(d.Synonyms) },
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
		// A read that asked the catalog about row-level security, as the
		// PostgreSQL reader does, so the fixture's policies compare.
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
	}

	parsed := loadPostgresDocument(c, renderPostgresDocument(c, roundTripFixture()))
	diff := must.Must(schemadiff.CompareWithDatabaseInfo(t.Context(), parsed, live, catalog.ServerInfo{Dialect: "postgres"}, nil, must.Must(builtin.New())))

	c.Assert(diff.SequencesRemoved.Names(), qt.HasLen, 1)
	c.Assert(diff.DomainsRemoved.Names(), qt.HasLen, 1)
	c.Assert(diff.CompositeTypesRemoved.Names(), qt.HasLen, 1)
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
	// HCL gained a block for synonyms (stokaro/ptah#1031) and the sweep above
	// measures that they survive it; YAML still has no key, so here they stay.
	"Synonyms": coverage.Synonym,
}

// The YAML surface has a topics key, so a YAML document that leaves a topic
// out is asking for it to go: unlike the HCL one, it claims the topic
// namespace.
func TestYAMLDocument_DescribesTopics(t *testing.T) {
	c := qt.New(t)

	parsed := loadYAMLDocument(c)

	c.Assert(parsed.FeatureCoverage.Lookup(ydbtopic.Kind, ydbtopic.Ref("", "undeclared")).State, qt.Equals, schemaext.Complete)
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
func yamlRecordsKind(parsed *schemamodel.Database, kind coverage.Kind) bool {
	if kind == "" {
		return false
	}
	return !parsed.NotDescribed.Describes(kind)
}

// loadYAMLDocument writes and loads the smallest YAML schema there is.
func loadYAMLDocument(c *qt.C) *schemamodel.Database {
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
	parsed, err := schemafile.Load(path, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	return parsed
}

// nonObjectDatabaseFields are the slice fields of [schemamodel.Database] that no
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
// Every family in the table survives. The ones that did not, until the HCL
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

			c.Assert(row.count(parsed) > 0, qt.IsTrue,
				qt.Commentf("%s did not survive the document Ptah itself wrote", row.field))
		})
	}
}

// TestRoundTrip_TimescaleStateSurvives is the sweep's row for the TimescaleDB
// models, which are a facet of a table and a named feature object rather than
// families of the common model: both have an HCL block, so the document Ptah
// writes carries them back unchanged.
func TestRoundTrip_TimescaleStateSurvives(t *testing.T) {
	c := qt.New(t)
	db := roundTripFixture()
	hypertable := &tsschema.DesiredHypertable{Column: "created_at", ChunkInterval: "1 day", IfNotExists: true, Comment: "time series"}
	db.Tables[0].Facets = must.Must(schemaext.NewFacets(hypertable))
	aggregate := tsschema.DesiredContinuousAggregateObject("public", "hourly", tsschema.DesiredContinuousAggregate{
		Body: "SELECT time_bucket('1 hour', created_at) AS bucket FROM users GROUP BY bucket", MaterializedOnly: new(true), Comment: "hourly",
	})
	db.FeatureObjects = must.Must(schemaext.NewObjects(aggregate))

	parsed := loadPostgresDocument(c, renderPostgresDocument(c, db))

	parsedHypertable, found, err := schemaext.FacetAs[*tsschema.DesiredHypertable](parsed.Tables[0].Facets, tsschema.HypertableKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(parsedHypertable, qt.DeepEquals, hypertable)
	parsedAggregate, found, err := parsed.FeatureObjects.Get(aggregate.Ref)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(parsedAggregate.Value, qt.DeepEquals, aggregate.Value)
	c.Assert(parsed.FeatureCoverage.Lookup(tsschema.ContinuousAggregateKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}

// TestRoundTrip_UnwritableFamiliesAreRecordedNotDropped is the round trip of
// the feature objects the HCL document cannot carry: the document leaves each
// out and makes no claim about its namespace, so applying it back to a YDB
// database plans no removal -- for a replication, no `DROP ASYNC REPLICATION
// ... CASCADE` of its replica tables. The control is the same document's
// silence about a sequence, which it could have named and so still removes.
func TestRoundTrip_UnwritableFamiliesAreRecordedNotDropped(t *testing.T) {
	c := qt.New(t)
	db := roundTripFixture()
	db.FeatureObjects = must.Must(schemaext.NewObjects(
		ydbworkload.DesiredPoolObject("batch", "", ydbworkload.PoolSpec{}),
		ydbworkload.DesiredClassifierObject("batch_users", "", ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 1}),
		ydbsecret.DesiredObject("", "pg_password", "", "PTAH_SECRET_PG"),
		ydbtopic.DesiredObject("public", "events", "", ydbtopic.Spec{}),
		ydbreplication.DesiredReplicationObject("public", "mirror", "", mirrorRead),
		ydbreplication.DesiredTransferObject("public", "ingest", "", ingestRead),
	))
	live := &catalog.Database{
		Schemas:         []catalog.Schema{{Name: "public"}},
		Tables:          []catalog.Table{{Schema: "public", Name: "users"}},
		Sequences:       []catalog.Sequence{{Schema: "public", Name: "s1"}},
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
	}

	parsed := loadPostgresDocument(c, renderPostgresDocument(c, db))
	diff := must.Must(schemadiff.CompareWithDatabaseInfo(t.Context(), parsed, live, catalog.ServerInfo{Dialect: "postgres"}, nil, must.Must(builtin.New())))

	// Only the fixture's row-level security policies are objects the
	// document can carry.
	c.Assert(parsed.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind != objectidentity.Kind(pgpolicy.PolicyKind)
	}).Len(), qt.Equals, 0)
	c.Assert(parsed.FeatureCoverage.Lookup(ydbworkload.PoolKind, objectidentity.ID{}).State, qt.Equals, schemaext.Uninspected)
	c.Assert(parsed.FeatureCoverage.Lookup(ydbworkload.ClassifierKind, objectidentity.ID{}).State, qt.Equals, schemaext.Uninspected)
	c.Assert(parsed.FeatureCoverage.Lookup(ydbtopic.Kind, objectidentity.ID{}).State, qt.Equals, schemaext.Uninspected)
	c.Assert(parsed.FeatureCoverage.Lookup(ydbreplication.ReplicationKind, objectidentity.ID{}).State, qt.Equals, schemaext.Uninspected)
	c.Assert(parsed.FeatureCoverage.Lookup(ydbreplication.TransferKind, objectidentity.ID{}).State, qt.Equals, schemaext.Uninspected)
	c.Assert(parsed.FeatureCoverage.Lookup(ydbsecret.Kind, objectidentity.ID{}).State, qt.Equals, schemaext.Uninspected)
	c.Assert(diff.SequencesRemoved.Names(), qt.HasLen, 1)
}

// The YAML surface has a secrets key, so a YAML document that leaves a secret
// out is asking for it to go: unlike the HCL one, it claims the namespace.
func TestYAMLDocument_DescribesSecrets(t *testing.T) {
	c := qt.New(t)

	parsed := loadYAMLDocument(c)

	c.Assert(parsed.FeatureCoverage.Lookup(ydbsecret.Kind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}

// The same holds for YDB's external objects: the HCL document leaves both out
// and makes no claim about either namespace, so applying it back plans no drop
// of either, while the YAML surface has keys for both and claims them.
func TestRoundTrip_ExternalObjectsMakeNoHCLClaim(t *testing.T) {
	c := qt.New(t)
	db := roundTripFixture()
	db.FeatureObjects = must.Must(schemaext.NewObjects(
		ydbexternal.DesiredSourceObject("", "s3", "", ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}),
		ydbexternal.DesiredTableObject("", "events", "", ydbexternal.Table{DataSource: "s3", Location: "e/",
			Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}),
	))

	parsed := loadPostgresDocument(c, renderPostgresDocument(c, db))
	yaml := loadYAMLDocument(c)

	// Only the fixture's row-level security policies are objects the
	// document can carry.
	c.Assert(parsed.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind != objectidentity.Kind(pgpolicy.PolicyKind)
	}).Len(), qt.Equals, 0)
	c.Assert(parsed.FeatureCoverage.Lookup(ydbexternal.SourceKind, objectidentity.ID{}).State, qt.Equals, schemaext.Uninspected)
	c.Assert(parsed.FeatureCoverage.Lookup(ydbexternal.TableKind, objectidentity.ID{}).State, qt.Equals, schemaext.Uninspected)
	c.Assert(yaml.FeatureCoverage.Lookup(ydbexternal.SourceKind, ydbexternal.SourceRef("", "undeclared")).State, qt.Equals, schemaext.Complete)
	c.Assert(yaml.FeatureCoverage.Lookup(ydbexternal.TableKind, ydbexternal.TableRef("", "undeclared")).State, qt.Equals, schemaext.Complete)
}

// TestRoundTrip_SweepCoversEveryObjectFamily is the guard that makes the test
// above a sweep rather than a list someone remembered to extend.
//
// A family added to [schemamodel.Database] has no row until somebody writes one,
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
	databaseType := reflect.TypeFor[schemamodel.Database]()
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
func roundTripFixture() *schemamodel.Database {
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Tables:  []schemamodel.Table{{StructName: "T", Name: "users", Schema: "public"}},
		Fields:  []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
	}
}

func renderPostgresDocument(c *qt.C, db *schemamodel.Database) []byte {
	c.Helper()
	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
	c.Assert(err, qt.IsNil)
	return result.Data
}

func loadPostgresDocument(c *qt.C, document []byte) *schemamodel.Database {
	c.Helper()
	path := filepath.Join(c.TB.(*testing.T).TempDir(), "sweep.hcl")
	c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)
	parsed, err := schemafile.Load(path, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	return parsed
}
