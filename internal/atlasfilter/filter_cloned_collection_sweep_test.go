package atlasfilter_test

import (
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
)

// clonedCollectionRow describes one collection cloneDatabase copies, together
// with a selector that names a real object of that kind and a selector that
// names nothing.
//
// This table exists because the same defect was found three times on this
// branch, one object kind further along each time: a collection that is read
// and rendered but never offered to the exclude patterns makes every selector
// naming one of its objects look empty, and the unmatched-selector refusal then
// fails `schema apply` while asserting something factually false. The rows are
// checked against the struct by reflection in
// TestExcludeDatabaseReport_SweepCoversEveryClonedCollection, so a collection
// added to [catalog.Database] reddens this file until it is answered here.
type clonedCollectionRow struct {
	// field is the [catalog.Database] field name, and the key the
	// reflection guard matches against.
	field string
	// present names a real object of this kind in sweptCollectionFixture.
	present string
	// absent is the same shape of selector naming an object that is not there.
	// It keeps "ask the patterns" from degrading into "mark unconditionally",
	// which would satisfy every present row and lose the refusal entirely.
	absent string
	// seed puts this row's objects into the shared fixture.
	seed func(*catalog.Database)
}

func clonedCollectionRows() []clonedCollectionRow {
	return []clonedCollectionRow{
		{
			field: "Schemas", present: "app", absent: "nosuch_schema",
			seed: func(s *catalog.Database) {
				s.Schemas = append(s.Schemas,
					catalog.Schema{Name: "public"},
					catalog.Schema{Name: "app"})
			},
		},
		{
			field: "Tables", present: "users", absent: "nosuch_table",
			seed: func(s *catalog.Database) {
				s.Tables = append(s.Tables, catalog.Table{
					Name:    "users",
					Columns: []catalog.Column{{Name: "id"}, {Name: "name"}},
				})
			},
		},
		{
			field: "Enums", present: "mood", absent: "nosuch_enum",
			seed: func(s *catalog.Database) {
				s.Enums = append(s.Enums, catalog.Enum{Name: "mood"})
			},
		},
		{
			field: "Indexes", present: "users.users_name_idx", absent: "users.nosuch_idx",
			seed: func(s *catalog.Database) {
				s.Indexes = append(s.Indexes, catalog.Index{
					Name: "users_name_idx", TableName: "users", Columns: []string{"name"},
				})
			},
		},
		{
			field: "Constraints", present: "users.users_name_chk", absent: "users.nosuch_chk",
			seed: func(s *catalog.Database) {
				s.Constraints = append(s.Constraints, catalog.Constraint{
					Name: "users_name_chk", TableName: "users", Type: "CHECK",
				})
			},
		},
		{
			// A hypertable answers its TABLE's selector and has none of its
			// own, so `present` is a table name: it is a fact about a table
			// rather than an object beside it, and a selector of its own would
			// let a description carry the fact for a table it does not carry
			// (stokaro/ptah#1026).
			field: "Hypertables", present: "readings", absent: "nosuch_hypertable",
			seed: func(s *catalog.Database) {
				s.Tables = append(s.Tables, catalog.Table{
					Name:    "readings",
					Columns: []catalog.Column{{Name: "time"}},
				})
				s.Hypertables = append(s.Hypertables, catalog.Hypertable{
					Name: "readings", PrimaryDimension: "time", Dimensions: 1,
				})
			},
		},
		{
			field: "ContinuousAggregates", present: "hourly_totals", absent: "nosuch_aggregate",
			seed: func(s *catalog.Database) {
				s.ContinuousAggregates = append(s.ContinuousAggregates,
					catalog.ContinuousAggregate{
						Name: "hourly_totals", HypertableName: "readings",
						Definition: "SELECT 1",
					})
			},
		},
		{
			field: "ExtendedProperties", present: "ptah_flag", absent: "nosuch_property",
			seed: func(s *catalog.Database) {
				s.ExtendedProperties = append(s.ExtendedProperties,
					catalog.ExtendedProperty{
						Name: "ptah_flag", Table: "users", Value: "enabled", ValueType: "nvarchar",
					})
			},
		},
		{
			field: "Extensions", present: "pgcrypto", absent: "nosuch_extension",
			seed: func(s *catalog.Database) {
				s.Extensions = append(s.Extensions, catalog.Extension{Name: "pgcrypto"})
			},
		},
		{
			field: "Functions", present: "fn_audit", absent: "nosuch_function",
			seed: func(s *catalog.Database) {
				s.Functions = append(s.Functions, catalog.Function{Name: "fn_audit"})
			},
		},
		{
			field: "Sequences", present: "order_seq", absent: "nosuch_sequence",
			seed: func(s *catalog.Database) {
				s.Sequences = append(s.Sequences, catalog.Sequence{Name: "order_seq"})
			},
		},
		{
			field: "Domains", present: "positive_int", absent: "nosuch_domain",
			seed: func(s *catalog.Database) {
				s.Domains = append(s.Domains, catalog.Domain{
					Name: "positive_int", BaseType: "integer",
				})
			},
		},
		{
			field: "Composites", present: "addr", absent: "nosuch_composite",
			seed: func(s *catalog.Database) {
				s.Composites = append(s.Composites, catalog.CompositeType{Name: "addr"})
			},
		},
		{
			field: "Ranges", present: "intrange", absent: "nosuch_range",
			seed: func(s *catalog.Database) {
				s.Ranges = append(s.Ranges, catalog.Range{
					Name: "intrange", Subtype: "integer",
				})
			},
		},
		{
			field: "Views", present: "v_users", absent: "nosuch_view",
			seed: func(s *catalog.Database) {
				s.Views = append(s.Views, catalog.View{Name: "v_users"})
			},
		},
		{
			field: "Synonyms", present: "s_users", absent: "nosuch_synonym",
			seed: func(s *catalog.Database) {
				s.Synonyms = append(s.Synonyms, catalog.Synonym{
					Name: "s_users", Target: "dbo.users", TargetSchema: "dbo", TargetObject: "users",
				})
			},
		},
		{
			field: "MatViews", present: "mv_users", absent: "nosuch_matview",
			seed: func(s *catalog.Database) {
				s.MatViews = append(s.MatViews, catalog.MaterializedView{Name: "mv_users"})
			},
		},
		{
			field: "Triggers", present: "users.users_audit_trg", absent: "users.nosuch_trg",
			seed: func(s *catalog.Database) {
				s.Triggers = append(s.Triggers, catalog.Trigger{
					Name: "users_audit_trg", Table: "users",
				})
			},
		},
		{
			field: "RLSPolicies", present: "users.users_policy", absent: "users.nosuch_policy",
			seed: func(s *catalog.Database) {
				s.RLSPolicies = append(s.RLSPolicies, catalog.RLSPolicy{
					Name: "users_policy", Table: "users",
				})
			},
		},
		{
			field: "Roles", present: "app_role", absent: "nosuch_role",
			seed: func(s *catalog.Database) {
				s.Roles = append(s.Roles, catalog.Role{Name: "app_role"})
			},
		},
		{
			// An ownership row names two things a selector can reach: the role
			// that owns and the object owned. This row names the object, and
			// the owner is a name no other row uses.
			field: "ObjectOwners", present: "owned_table", absent: "nosuch_owned_object",
			seed: func(s *catalog.Database) {
				s.ObjectOwners = append(s.ObjectOwners, catalog.ObjectOwner{
					Kind: "table", Name: "owned_table", Owner: "owner_role", OwnerCanLogin: true,
				})
			},
		},
		{
			// A membership names no object of its own; both of its ends are
			// roles, so the `role` selector is what answers it. The seeded
			// role name is one no other row uses, so this row's selector can
			// only be matched by the membership filter.
			field: "RoleMemberships", present: "reporting_role", absent: "nosuch_role_membership",
			seed: func(s *catalog.Database) {
				s.RoleMemberships = append(s.RoleMemberships, catalog.RoleMembership{
					Role: "reporting_role", Member: "app_role",
				})
			},
		},
		{
			// The role-qualified target names the grant and nothing else; the
			// bare object name would be answered by the table instead.
			field: "Grants", present: "app_role.users", absent: "app_role.nosuch_table",
			seed: func(s *catalog.Database) {
				s.Grants = append(s.Grants, catalog.Grant{
					Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users",
				})
			},
		},
		{
			// Not filtered, and correctly so: these roles are outside the
			// description, so there is nothing to subtract. They are still
			// ASKED, because "this selector protected nothing" is false for an
			// object that is already out.
			field: "RolesOutOfScope", present: "cluster_role", absent: "nosuch_cluster_role",
			seed: func(s *catalog.Database) {
				s.RolesOutOfScope = append(s.RolesOutOfScope, catalog.Role{Name: "cluster_role"})
			},
		},
		{
			// Not filtered either, and for a sharper reason than the roles
			// above: this list is what the SQLite comparison refusal reads, and
			// the whole point is that it survives the exclusion of the table it
			// names. Narrowing it would remove the refusal on precisely the run
			// that plans DROP TABLE for a module's storage
			// (stokaro/ptah#1028). It is still ASKED, so `--exclude docs` is
			// not reported as protecting nothing while the run is refused over
			// the object `docs` names.
			field: "UnregisteredVirtualTables", present: "legacy_docs", absent: "nosuch_virtual_table",
			seed: func(s *catalog.Database) {
				s.UnregisteredVirtualTables = append(s.UnregisteredVirtualTables,
					catalog.VirtualTable{Name: "legacy_docs", Module: "fts4"})
			},
		},
	}
}

// sweptCollectionFixture holds one real object of every collection
// cloneDatabase copies, so each row's selector is tested against a state that
// also contains every other kind.
func sweptCollectionFixture() *catalog.Database {
	schema := &catalog.Database{}
	for _, row := range clonedCollectionRows() {
		row.seed(schema)
	}
	return schema
}

// clonedDatabaseCollectionFields derives the collection list from the struct
// rather than repeating it, so the sweep cannot silently fall behind
// [catalog.Database]. Every slice-typed field is a collection
// cloneDatabase copies; NotDescribed is a coverage set rather than a slice and
// carries no objects a selector could name.
func clonedDatabaseCollectionFields() []string {
	schemaType := reflect.TypeFor[catalog.Database]()
	fields := make([]string, 0, schemaType.NumField())
	for field := range schemaType.Fields() {
		if field.Type.Kind() != reflect.Slice {
			continue
		}
		fields = append(fields, field.Name)
	}
	slices.Sort(fields)
	return fields
}

// TestExcludeDatabaseReport_SweepCoversEveryClonedCollection is the guard that
// makes the rest of this file a sweep rather than a list someone remembered to
// extend. A collection added to [catalog.Database] has no row here until
// somebody writes one, and this test names it.
func TestExcludeDatabaseReport_SweepCoversEveryClonedCollection(t *testing.T) {
	c := qt.New(t)

	rows := clonedCollectionRows()
	covered := make([]string, 0, len(rows))
	for _, row := range rows {
		covered = append(covered, row.field)
	}
	slices.Sort(covered)

	c.Assert(covered, qt.DeepEquals, clonedDatabaseCollectionFields())
}

// TestExcludeDatabaseReport_EveryClonedCollectionAnswersItsSelector is the rule
// the branch's own documentation states: a selector is only ever called empty by
// a filter that asked it.
//
// Red without the Schemas fix on the `Schemas` row, which reports ["app"]
// against a description that renders `schema "app"`.
func TestExcludeDatabaseReport_EveryClonedCollectionAnswersItsSelector(t *testing.T) {
	for _, row := range clonedCollectionRows() {
		t.Run(row.field, func(t *testing.T) {
			c := qt.New(t)

			_, report, err := atlasfilter.ExcludeDatabaseReport(
				sweptCollectionFixture(), []string{row.present}, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(report.Unmatched, qt.IsNil)
		})
	}
}

// TestExcludeDatabaseReport_EveryClonedCollectionStillReportsAnAbsentSelector is
// the inverse mutant of the table above, one row per collection.
//
// "Ask the patterns" must stay a name test. A filter that marked every pattern
// whenever its collection was non-empty would satisfy every row above and trade
// a false refusal for a lost one, which is the worse direction: an exclude
// selector that protects nothing would go back to being silent.
func TestExcludeDatabaseReport_EveryClonedCollectionStillReportsAnAbsentSelector(t *testing.T) {
	for _, row := range clonedCollectionRows() {
		t.Run(row.field, func(t *testing.T) {
			c := qt.New(t)

			_, report, err := atlasfilter.ExcludeDatabaseReport(
				sweptCollectionFixture(), []string{row.absent}, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(report.Unmatched, qt.DeepEquals, []string{row.absent})
		})
	}
}

// schemaScopedFixture holds objects in the default schema and in a second one,
// so a schema selector's reach can be asserted rather than assumed.
func schemaScopedFixture() *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "public"}, {Name: "app"}},
		Tables: []catalog.Table{
			{Name: "users", Columns: []catalog.Column{{Name: "id"}}},
			{Schema: "app", Name: "orders", Columns: []catalog.Column{{Name: "id"}}},
		},
		Enums: []catalog.Enum{
			{Name: "mood"},
			{Schema: "app", Name: "color"},
		},
		Sequences: []catalog.Sequence{{Schema: "app", Name: "app_seq"}},
		Views:     []catalog.View{{Schema: "app", Name: "v_orders"}},
		Grants: []catalog.Grant{
			{Role: "app_role", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app"},
		},
	}
}

// databaseTableNames and databaseQualifiedEnumNames qualify with the owning
// schema, so a row can tell "the object in app left" from "the object in public
// left". The package's existing databaseEnumNames reports the bare name, which
// cannot distinguish the two enums this fixture holds.
func databaseTableNames(tables []catalog.Table) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, catalog.QualifyTableName(table.Schema, table.Name))
	}
	return names
}

func databaseQualifiedEnumNames(enums []catalog.Enum) []string {
	names := make([]string, 0, len(enums))
	for _, value := range enums {
		names = append(names, catalog.QualifyTableName(value.Schema, value.Name))
	}
	return names
}

// TestExcludeDatabase_SchemaSelectorTakesTheSchemaContentsWithIt pins the keep
// decision, which is the destructive half of the same defect. Marking the
// selector matched without filtering would satisfy the report tables above and
// still plan `DROP TABLE "app"."orders"` for the object the selector was
// written to protect.
//
// Every row names the same schema a different way, so every row owes the same
// object set: a spelling that dropped the schema but left a sequence inside it
// is a partial sweep, and a partial sweep is the defect. That is why the
// expectation is in the body rather than in the rows.
//
// The expectation is the object set the pinned community binary produced for
// the same selector against the same fixture shape, in the `-s public -s app`
// scope ptah-compat exposes.
func TestExcludeDatabase_SchemaSelectorTakesTheSchemaContentsWithIt(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
	}{
		{
			name:    "a schema selector removes the schema and everything in it",
			pattern: "app",
		},
		{
			name:    "a glob covering the schema name reaches it the same way",
			pattern: "ap*",
		},
		{
			name:    "the type selector names the same schema",
			pattern: "app[type=schema]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
				schemaScopedFixture(), []string{test.pattern}, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(databaseSchemaNames(got.Schemas), qt.DeepEquals, []string{"public"})
			c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, []string{"users"})
			c.Assert(databaseQualifiedEnumNames(got.Enums), qt.DeepEquals, []string{"mood"})
			c.Assert(got.Sequences, qt.HasLen, 0)
			c.Assert(got.Views, qt.HasLen, 0)
			c.Assert(got.Grants, qt.HasLen, 0)
		})
	}
}

// TestExcludeDatabase_SelectorsThatNameNoSchemaKeepEverySchema is the boundary
// the sweep above must not cross, and the control that keeps it honest: a
// filter that removed a schema whenever any object in it matched would satisfy
// every row above and take the schema with it here.
//
// `app` is not a match for the glob `app.*`, so the two-part spelling takes the
// contents and leaves the schema entry; a selector naming an object in the
// default schema leaves both schemas alone. The rows differ only in what
// survives inside the schemas, which is what the row carries.
func TestExcludeDatabase_SelectorsThatNameNoSchemaKeepEverySchema(t *testing.T) {
	tests := []struct {
		name       string
		pattern    string
		wantTables []string
		wantEnums  []string
	}{
		{
			name:       "the two-part spelling removes the contents and keeps the schema",
			pattern:    "app.*",
			wantTables: []string{"users"},
			wantEnums:  []string{"mood"},
		},
		{
			name:       "a selector naming no schema removes no schema",
			pattern:    "users",
			wantTables: []string{"app.orders"},
			wantEnums:  []string{"mood", "app.color"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
				schemaScopedFixture(), []string{test.pattern}, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(databaseSchemaNames(got.Schemas), qt.DeepEquals, []string{"public", "app"})
			c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, test.wantTables)
			c.Assert(databaseQualifiedEnumNames(got.Enums), qt.DeepEquals, test.wantEnums)
		})
	}
}

// TestExcludeGenerated_SchemaSelectorTakesTheSchemaContentsWithIt is the
// desired-side mirror. Both sides of a comparison must subtract the same
// objects: a schema removed from the introspected side alone would come back as
// a CREATE SCHEMA, together with everything the selector was protecting.
func TestExcludeGenerated_SchemaSelectorTakesTheSchemaContentsWithIt(t *testing.T) {
	c := qt.New(t)
	schema := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}, {Name: "app"}},
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Order", Schema: "app", Name: "orders"},
		},
		Enums: []schemamodel.Enum{
			{Name: "mood", Values: []string{"happy"}},
			{Name: "app.color", Values: []string{"red"}},
		},
		Sequences: []schemamodel.Sequence{{Schema: "app", Name: "app_seq"}},
		Functions: []schemamodel.Function{{Name: "app.fn_app"}},
	}

	got, report, err := atlasfilter.ExcludeGeneratedReport(schema, []string{"app"}, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.IsNil)
	c.Assert(generatedSchemaNames(got.Schemas), qt.DeepEquals, []string{"public"})
	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"users"})
	c.Assert(generatedEnumNames(got.Enums), qt.DeepEquals, []string{"mood"})
	c.Assert(got.Sequences, qt.HasLen, 0)
	c.Assert(got.Functions, qt.HasLen, 0)
}
