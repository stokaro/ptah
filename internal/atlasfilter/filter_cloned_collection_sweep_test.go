package atlasfilter_test

import (
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
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
// added to [dbschematypes.DBSchema] reddens this file until it is answered here.
type clonedCollectionRow struct {
	// field is the [dbschematypes.DBSchema] field name, and the key the
	// reflection guard matches against.
	field string
	// present names a real object of this kind in sweptCollectionFixture.
	present string
	// absent is the same shape of selector naming an object that is not there.
	// It keeps "ask the patterns" from degrading into "mark unconditionally",
	// which would satisfy every present row and lose the refusal entirely.
	absent string
	// seed puts this row's objects into the shared fixture.
	seed func(*dbschematypes.DBSchema)
}

func clonedCollectionRows() []clonedCollectionRow {
	return []clonedCollectionRow{
		{
			field: "Schemas", present: "app", absent: "nosuch_schema",
			seed: func(s *dbschematypes.DBSchema) {
				s.Schemas = append(s.Schemas,
					dbschematypes.DBSchemaInfo{Name: "public"},
					dbschematypes.DBSchemaInfo{Name: "app"})
			},
		},
		{
			field: "Tables", present: "users", absent: "nosuch_table",
			seed: func(s *dbschematypes.DBSchema) {
				s.Tables = append(s.Tables, dbschematypes.DBTable{
					Name:    "users",
					Columns: []dbschematypes.DBColumn{{Name: "id"}, {Name: "name"}},
				})
			},
		},
		{
			field: "Enums", present: "mood", absent: "nosuch_enum",
			seed: func(s *dbschematypes.DBSchema) {
				s.Enums = append(s.Enums, dbschematypes.DBEnum{Name: "mood"})
			},
		},
		{
			field: "Indexes", present: "users.users_name_idx", absent: "users.nosuch_idx",
			seed: func(s *dbschematypes.DBSchema) {
				s.Indexes = append(s.Indexes, dbschematypes.DBIndex{
					Name: "users_name_idx", TableName: "users", Columns: []string{"name"},
				})
			},
		},
		{
			field: "Constraints", present: "users.users_name_chk", absent: "users.nosuch_chk",
			seed: func(s *dbschematypes.DBSchema) {
				s.Constraints = append(s.Constraints, dbschematypes.DBConstraint{
					Name: "users_name_chk", TableName: "users", Type: "CHECK",
				})
			},
		},
		{
			field: "Extensions", present: "pgcrypto", absent: "nosuch_extension",
			seed: func(s *dbschematypes.DBSchema) {
				s.Extensions = append(s.Extensions, dbschematypes.DBExtension{Name: "pgcrypto"})
			},
		},
		{
			field: "Functions", present: "fn_audit", absent: "nosuch_function",
			seed: func(s *dbschematypes.DBSchema) {
				s.Functions = append(s.Functions, dbschematypes.DBFunction{Name: "fn_audit"})
			},
		},
		{
			field: "Sequences", present: "order_seq", absent: "nosuch_sequence",
			seed: func(s *dbschematypes.DBSchema) {
				s.Sequences = append(s.Sequences, dbschematypes.DBSequence{Name: "order_seq"})
			},
		},
		{
			field: "Domains", present: "positive_int", absent: "nosuch_domain",
			seed: func(s *dbschematypes.DBSchema) {
				s.Domains = append(s.Domains, dbschematypes.DBDomain{
					Name: "positive_int", BaseType: "integer",
				})
			},
		},
		{
			field: "Composites", present: "addr", absent: "nosuch_composite",
			seed: func(s *dbschematypes.DBSchema) {
				s.Composites = append(s.Composites, dbschematypes.DBComposite{Name: "addr"})
			},
		},
		{
			field: "Ranges", present: "intrange", absent: "nosuch_range",
			seed: func(s *dbschematypes.DBSchema) {
				s.Ranges = append(s.Ranges, dbschematypes.DBRange{
					Name: "intrange", Subtype: "integer",
				})
			},
		},
		{
			field: "Views", present: "v_users", absent: "nosuch_view",
			seed: func(s *dbschematypes.DBSchema) {
				s.Views = append(s.Views, dbschematypes.DBView{Name: "v_users"})
			},
		},
		{
			field: "MatViews", present: "mv_users", absent: "nosuch_matview",
			seed: func(s *dbschematypes.DBSchema) {
				s.MatViews = append(s.MatViews, dbschematypes.DBMatView{Name: "mv_users"})
			},
		},
		{
			field: "Triggers", present: "users.users_audit_trg", absent: "users.nosuch_trg",
			seed: func(s *dbschematypes.DBSchema) {
				s.Triggers = append(s.Triggers, dbschematypes.DBTrigger{
					Name: "users_audit_trg", Table: "users",
				})
			},
		},
		{
			field: "RLSPolicies", present: "users.users_policy", absent: "users.nosuch_policy",
			seed: func(s *dbschematypes.DBSchema) {
				s.RLSPolicies = append(s.RLSPolicies, dbschematypes.DBRLSPolicy{
					Name: "users_policy", Table: "users",
				})
			},
		},
		{
			field: "Roles", present: "app_role", absent: "nosuch_role",
			seed: func(s *dbschematypes.DBSchema) {
				s.Roles = append(s.Roles, dbschematypes.DBRole{Name: "app_role"})
			},
		},
		{
			// The role-qualified target names the grant and nothing else; the
			// bare object name would be answered by the table instead.
			field: "Grants", present: "app_role.users", absent: "app_role.nosuch_table",
			seed: func(s *dbschematypes.DBSchema) {
				s.Grants = append(s.Grants, dbschematypes.DBGrant{
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
			seed: func(s *dbschematypes.DBSchema) {
				s.RolesOutOfScope = append(s.RolesOutOfScope, dbschematypes.DBRole{Name: "cluster_role"})
			},
		},
	}
}

// sweptCollectionFixture holds one real object of every collection
// cloneDatabase copies, so each row's selector is tested against a state that
// also contains every other kind.
func sweptCollectionFixture() *dbschematypes.DBSchema {
	schema := &dbschematypes.DBSchema{}
	for _, row := range clonedCollectionRows() {
		row.seed(schema)
	}
	return schema
}

// clonedDatabaseCollectionFields derives the collection list from the struct
// rather than repeating it, so the sweep cannot silently fall behind
// [dbschematypes.DBSchema]. Every slice-typed field is a collection
// cloneDatabase copies; NotDescribed is a coverage set rather than a slice and
// carries no objects a selector could name.
func clonedDatabaseCollectionFields() []string {
	schemaType := reflect.TypeFor[dbschematypes.DBSchema]()
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
// extend. A collection added to [dbschematypes.DBSchema] has no row here until
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
func schemaScopedFixture() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Schemas: []dbschematypes.DBSchemaInfo{{Name: "public"}, {Name: "app"}},
		Tables: []dbschematypes.DBTable{
			{Name: "users", Columns: []dbschematypes.DBColumn{{Name: "id"}}},
			{Schema: "app", Name: "orders", Columns: []dbschematypes.DBColumn{{Name: "id"}}},
		},
		Enums: []dbschematypes.DBEnum{
			{Name: "mood"},
			{Schema: "app", Name: "color"},
		},
		Sequences: []dbschematypes.DBSequence{{Schema: "app", Name: "app_seq"}},
		Views:     []dbschematypes.DBView{{Schema: "app", Name: "v_orders"}},
		Grants: []dbschematypes.DBGrant{
			{Role: "app_role", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app"},
		},
	}
}

// databaseTableNames and databaseQualifiedEnumNames qualify with the owning
// schema, so a row can tell "the object in app left" from "the object in public
// left". The package's existing databaseEnumNames reports the bare name, which
// cannot distinguish the two enums this fixture holds.
func databaseTableNames(tables []dbschematypes.DBTable) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, dbschematypes.QualifyTableName(table.Schema, table.Name))
	}
	return names
}

func databaseQualifiedEnumNames(enums []dbschematypes.DBEnum) []string {
	names := make([]string, 0, len(enums))
	for _, value := range enums {
		names = append(names, dbschematypes.QualifyTableName(value.Schema, value.Name))
	}
	return names
}

// TestExcludeDatabase_SchemaSelectorTakesTheSchemaContentsWithIt pins the keep
// decision, which is the destructive half of the same defect. Marking the
// selector matched without filtering would satisfy the report tables above and
// still plan `DROP TABLE "app"."orders"` for the object the selector was
// written to protect.
//
// Every expectation below is the object set the pinned community binary
// produced for the same selector against the same fixture shape, in the
// `-s public -s app` scope ptah-compat exposes.
func TestExcludeDatabase_SchemaSelectorTakesTheSchemaContentsWithIt(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		assert  func(*qt.C, *dbschematypes.DBSchema)
	}{
		{
			name:    "a schema selector removes the schema and everything in it",
			pattern: "app",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseSchemaNames(got.Schemas), qt.DeepEquals, []string{"public"})
				c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, []string{"users"})
				c.Assert(databaseQualifiedEnumNames(got.Enums), qt.DeepEquals, []string{"mood"})
				c.Assert(got.Sequences, qt.HasLen, 0)
				c.Assert(got.Views, qt.HasLen, 0)
				c.Assert(got.Grants, qt.HasLen, 0)
			},
		},
		{
			name:    "a glob covering the schema name reaches it the same way",
			pattern: "ap*",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseSchemaNames(got.Schemas), qt.DeepEquals, []string{"public"})
				c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, []string{"users"})
			},
		},
		{
			name:    "the type selector names the same schema",
			pattern: "app[type=schema]",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseSchemaNames(got.Schemas), qt.DeepEquals, []string{"public"})
				c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, []string{"users"})
			},
		},
		{
			// Control, and the boundary that keeps the schema reading from
			// swallowing the two-part spelling: `app` is not a match for the
			// glob `app.*`, so the schema entry stays and only its contents go.
			name:    "the two-part spelling removes the contents and keeps the schema",
			pattern: "app.*",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseSchemaNames(got.Schemas), qt.DeepEquals, []string{"public", "app"})
				c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, []string{"users"})
				c.Assert(databaseQualifiedEnumNames(got.Enums), qt.DeepEquals, []string{"mood"})
			},
		},
		{
			// Control: a selector naming no schema leaves every schema alone.
			name:    "a selector naming no schema removes no schema",
			pattern: "users",
			assert: func(c *qt.C, got *dbschematypes.DBSchema) {
				c.Assert(databaseSchemaNames(got.Schemas), qt.DeepEquals, []string{"public", "app"})
				c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, []string{"app.orders"})
				c.Assert(databaseQualifiedEnumNames(got.Enums), qt.DeepEquals, []string{"mood", "app.color"})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(
				schemaScopedFixture(), []string{test.pattern}, "public")

			c.Assert(err, qt.IsNil)
			test.assert(c, got)
		})
	}
}

// TestExcludeGenerated_SchemaSelectorTakesTheSchemaContentsWithIt is the
// desired-side mirror. Both sides of a comparison must subtract the same
// objects: a schema removed from the introspected side alone would come back as
// a CREATE SCHEMA, together with everything the selector was protecting.
func TestExcludeGenerated_SchemaSelectorTakesTheSchemaContentsWithIt(t *testing.T) {
	c := qt.New(t)
	schema := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "public"}, {Name: "app"}},
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Order", Schema: "app", Name: "orders"},
		},
		Enums: []goschema.Enum{
			{Name: "mood", Values: []string{"happy"}},
			{Name: "app.color", Values: []string{"red"}},
		},
		Sequences: []goschema.Sequence{{Schema: "app", Name: "app_seq"}},
		Functions: []goschema.Function{{Name: "app.fn_app"}},
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
