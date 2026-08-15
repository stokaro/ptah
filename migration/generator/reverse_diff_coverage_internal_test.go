package generator

// White-box testing required: reverseSchemaDiffWithSchema is unexported, and
// this gate has to build the reverse plan directly in order to observe which
// input fields it actually reads. The exported API surfaces only rendered SQL,
// which cannot tell "the builder ignored this field" apart from "the planner
// rendered this field to nothing".

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestReverseSchemaDiff_AccountsForEverySchemaDiffField is the gate issue #1287
// asks for: it fails when a field is added to SchemaDiff and the reverse builder
// does not handle it.
//
// The enumeration is by reflection on purpose. reverseSchemaDiffWithSchema
// builds a fresh struct literal, and a literal is silent about the fields it
// omits: nine of them -- views, materialized views and triggers -- were missing
// for as long as those categories existed, so every rollback dropped them
// without a word. The tests that were in place asserted the swap itself
// (result.TablesAdded == input.TablesRemoved), which is a tautology that can
// never notice an absent field, and a hand-written field list is exactly what
// rotted.
//
// The property checked here is that every field REACHES the reverse plan:
// zeroing one field of a fully populated diff must change the plan the builder
// produces. A field the builder never reads leaves the plan identical, and the
// subtest named after that field goes red.
func TestReverseSchemaDiff_AccountsForEverySchemaDiffField(t *testing.T) {
	c := qt.New(t)

	schema, dbSchema := reverseCoverageContext()
	baseline := reverseSchemaDiffWithSchema(reverseCoverageDiff(), schema, dbSchema)

	diffType := reflect.TypeFor[types.SchemaDiff]()
	c.Assert(diffType.NumField() > 0, qt.IsTrue,
		qt.Commentf("reflection found no fields on SchemaDiff; the gate would pass vacuously"))

	for i := range diffType.NumField() {
		field := diffType.Field(i)
		t.Run(field.Name, func(t *testing.T) {
			c := qt.New(t)
			assertReverseCoverageField(c, baseline, schema, dbSchema, field, i)
		})
	}
}

func assertReverseCoverageField(
	c *qt.C,
	baseline *types.SchemaDiff,
	schema *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	field reflect.StructField,
	fieldIndex int,
) {
	c.Helper()
	if field.Name == "ForeignKeysRemovedWithTables" {
		// This collection is supplemental metadata for ordering the forward
		// removals already present in ConstraintsRemovedWithTables. It must not
		// independently create a reverse operation; the reverse collection is
		// derived from the forward constraint additions and desired schema.
		return
	}
	withoutField := reverseCoverageDiff()
	reflect.ValueOf(withoutField).Elem().Field(fieldIndex).SetZero()

	reversed := reverseSchemaDiffWithSchema(withoutField, schema, dbSchema)

	c.Assert(reflect.DeepEqual(baseline, reversed), qt.IsFalse,
		qt.Commentf(
			"SchemaDiff.%s never reaches the reverse plan: zeroing it left the down plan "+
				"byte-identical. Reverse it in reverseSchemaDiffWithSchema, or record there "+
				"why the down direction cannot carry it -- and if a generic value cannot "+
				"exercise it, give it an entry in reverseCoverageDiff.",
			field.Name))
}

// TestReverseSchemaDiff_EveryModifiedCategoryReachesTheRenderedRollback is the
// companion the gate above needs, and the one that would have caught the failure
// it missed.
//
// Reaching the reversed SchemaDiff is not reaching the rollback. A field can be
// swapped faithfully and still render nothing at all, and the "...Modified"
// fields are where that happens: a modified object is not described by the diff,
// it is re-rendered from the pre-change schema, which the planner finds BY NAME.
// When the two sides spell the name differently the lookup misses, the category
// renders nothing, and the file says "No rollback operations needed" while the
// reflection gate above stays green. That is exactly what every modified view
// outside the default schema did -- and every modified view at all on MySQL and
// MariaDB, whose reader reports a schema for all of them (issue #1287).
//
// So this one renders. Each subtest plans a rollback whose ONLY populated field
// is one modified category, and requires the down file to carry the prior
// definition. Every object lives in a NAMED schema, because that is the spelling
// the two sides disagree about and an unqualified fixture proves nothing here.
//
// The scope is the three categories issue #1287 put into the rollback. The other
// "...Modified" fields -- tables, enums, functions, sequences, domains,
// composite types, RLS policies, roles -- reached the rollback before this
// change and are covered by their own tests; a rendered gate over all of them
// wants a fixture that declares one of every object kind, which is worth
// building and is not built here. The three names are read back off SchemaDiff
// by reflection, so renaming a field breaks this test rather than quietly
// emptying it.
func TestReverseSchemaDiff_EveryModifiedCategoryReachesTheRenderedRollback(t *testing.T) {
	schema, dbSchema := modifiedCategoryContext()
	diffType := reflect.TypeFor[types.SchemaDiff]()

	tests := []struct {
		field string
		diff  *types.SchemaDiff
		wants string
	}{
		{
			field: "ViewsModified",
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{{
				ViewName: modifiedCategoryView,
				Changes:  map[string]string{"body": "old -> new"},
			}}},
			wants: modifiedCategoryPriorViewBody,
		},
		{
			field: "MaterializedViewsModified",
			diff: &types.SchemaDiff{MaterializedViewsModified: []types.MaterializedViewDiff{{
				ViewName: modifiedCategoryMatView,
				Changes:  map[string]string{"body": "old -> new"},
			}}},
			wants: modifiedCategoryPriorMatViewBody,
		},
		{
			field: "TriggersModified",
			diff: &types.SchemaDiff{TriggersModified: []types.TriggerDiff{{
				TriggerName: modifiedCategoryTrigger,
				TableName:   modifiedCategoryTable,
				Changes:     map[string]string{"timing": "AFTER -> BEFORE"},
			}}},
			wants: modifiedCategoryTrigger,
		},
	}

	for _, test := range tests {
		t.Run(test.field, func(t *testing.T) {
			c := qt.New(t)
			_, exists := diffType.FieldByName(test.field)
			c.Assert(exists, qt.IsTrue,
				qt.Commentf("SchemaDiff has no field %s; this gate is naming something that moved", test.field))

			downSQL, err := generateDownMigrationSQL(test.diff, schema, dbSchema, "postgres")
			c.Assert(err, qt.IsNil)
			downSQL = legacyRenderedSQL(downSQL)

			c.Assert(downSQL, qt.Contains, test.wants,
				qt.Commentf(
					"SchemaDiff.%s reaches the reversed diff but not the rendered rollback. A "+
						"modified object is re-rendered from the pre-change schema, which the "+
						"planner finds by name, and that schema qualifies every name with %q.\n"+
						"rendered:\n%s",
					test.field, modifiedCategorySchema, downSQL))
		})
	}
}

// The fixture below is deliberately spelled the way the two sides really are: the
// Go schema names each object bare, and the pre-change database reports it under
// a schema, which is what dbschematogo qualifies it with.
const (
	modifiedCategorySchema           = "rev_reporting"
	modifiedCategoryTable            = "rev_named_users"
	modifiedCategoryView             = "rev_named_active"
	modifiedCategoryMatView          = "rev_named_stats"
	modifiedCategoryTrigger          = "rev_named_touch"
	modifiedCategoryPriorViewBody    = "SELECT id FROM rev_named_users"
	modifiedCategoryTargetViewBody   = "SELECT id, email FROM rev_named_users"
	modifiedCategoryPriorMatViewBody = "SELECT count(*) AS total FROM rev_named_users"
)

func modifiedCategoryContext() (*goschema.Database, *dbschematypes.DBSchema) {
	schema := &goschema.Database{
		Tables: []goschema.Table{{StructName: "RevNamedUser", Name: modifiedCategoryTable}},
		Fields: []goschema.Field{
			{StructName: "RevNamedUser", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "RevNamedUser", Name: "email", Type: "TEXT"},
		},
		Views: []goschema.View{
			{StructName: "RevNamedActive", Name: modifiedCategoryView, Body: modifiedCategoryTargetViewBody},
		},
		MaterializedViews: []goschema.MaterializedView{{
			StructName:      "RevNamedStats",
			Name:            modifiedCategoryMatView,
			Body:            "SELECT count(*) AS total FROM rev_named_users WHERE id > 0",
			RefreshStrategy: "manual",
		}},
		Triggers: []goschema.Trigger{{
			StructName: "RevNamedUser",
			Name:       modifiedCategoryTrigger,
			Table:      modifiedCategoryTable,
			Timing:     "BEFORE",
			Event:      "UPDATE",
			ForEach:    "ROW",
			Body:       "RETURN NEW;",
		}},
	}
	goschema.Finalize(schema)

	dbSchema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{
			Name:   modifiedCategoryTable,
			Schema: modifiedCategorySchema,
			Type:   "TABLE",
			Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "email", DataType: "text", IsNullable: "NO", OrdinalPosition: 2},
			},
		}},
		Views: []dbschematypes.DBView{
			{Name: modifiedCategoryView, Schema: modifiedCategorySchema, Body: modifiedCategoryPriorViewBody},
		},
		MatViews: []dbschematypes.DBMatView{{
			Name:            modifiedCategoryMatView,
			Schema:          modifiedCategorySchema,
			Body:            modifiedCategoryPriorMatViewBody,
			RefreshStrategy: "manual",
		}},
		Triggers: []dbschematypes.DBTrigger{{
			Name:    modifiedCategoryTrigger,
			Table:   modifiedCategoryTable,
			Schema:  modifiedCategorySchema,
			Timing:  "AFTER",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "RETURN NEW;",
		}},
	}
	return schema, dbSchema
}

// TestReverseCoverageDiff_PopulatesEverySchemaDiffField guards the fixture the
// gate above depends on. A field whose type the generic filler does not know how
// to populate would arrive zero, the gate would compare two identical plans and
// report the builder's omission as the fixture's -- so the fixture states its own
// completeness separately.
func TestReverseCoverageDiff_PopulatesEverySchemaDiffField(t *testing.T) {
	populated := reflect.ValueOf(*reverseCoverageDiff())
	diffType := populated.Type()

	for i := range diffType.NumField() {
		field := diffType.Field(i)
		t.Run(field.Name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(populated.Field(i).IsZero(), qt.IsFalse,
				qt.Commentf(
					"reverseCoverageDiff left SchemaDiff.%s at its zero value; teach fillDistinctly "+
						"about %s or set the field explicitly", field.Name, field.Type))
		})
	}
}

// reverseCoverageDiff returns a SchemaDiff with every field carrying a distinct
// non-zero value.
//
// The bulk is filled generically from the field name so that a field added
// tomorrow is populated without anyone touching this file. A few fields need
// more than a generic value because the builder does not swap them: it rebuilds
// them from the pre-change database, and a value that names no real constraint
// would rebuild to nothing.
func reverseCoverageDiff() *types.SchemaDiff {
	diff := &types.SchemaDiff{}
	fillDistinctly(reflect.ValueOf(diff).Elem(), "")

	// ConstraintBackedIndexRemovals is the subset of IndexesRemoved whose object
	// is a UNIQUE constraint of the same name on the same table, and
	// reverseIndexRemovals reverses exactly that subset into constraint
	// additions instead of index additions. A generic value satisfies neither
	// half of that relation -- it names a removal no removal list holds and a
	// constraint no host has -- so every removal falls through as an index
	// addition and zeroing the field leaves the plan identical. The entry is
	// therefore spelled out, and the same reference is appended to the removals
	// it has to be a subset of; reverseCoverageContext introspects the UNIQUE
	// constraint it names.
	constraintBacked := types.IndexRef{Name: revCoverageUniqueName, TableName: revCoverageTable}
	diff.IndexesRemoved = append(diff.IndexesRemoved, constraintBacked)
	diff.ConstraintBackedIndexRemovals = []types.IndexRef{constraintBacked}

	// reverseConstraintAdditions restores the prior body of each removed
	// constraint from the introspected schema, so the entry has to name a
	// constraint type it reconstructs and a host reverseCoverageContext has.
	diff.ConstraintsRemovedWithTables = []types.ConstraintRemovalInfo{{
		Name:      revCoverageCheckName,
		TableName: revCoverageTable,
		Type:      "CHECK",
	}}
	// reverseConstraintRemovals resolves each added constraint's owning table,
	// and skips any addition that does not name one.
	diff.ConstraintsAddedWithTables = []types.ConstraintAdditionInfo{{
		Name:      revCoverageCheckName,
		TableName: revCoverageTable,
		Type:      "CHECK",
	}}
	// The planner refuses a snapshot that disagrees with the dialect it is
	// planning for, so this one field cannot carry an invented value: a generic
	// filler would make every rendered-rollback subtest fail on the same error
	// instead of on the field it names.
	postgresSemantics := identifier.ForDialect("postgres")
	diff.IdentifierSemantics = &postgresSemantics
	return diff
}

const (
	revCoverageTable      = "rev_coverage_hosts"
	revCoverageCheckName  = "rev_coverage_check"
	revCoverageUniqueName = "rev_coverage_unique"
)

// reverseCoverageContext supplies the generated schema and the pre-change
// database the reverse builder consults for the fields it derives rather than
// swaps.
func reverseCoverageContext() (*goschema.Database, *dbschematypes.DBSchema) {
	checkClause := "id > 0"

	schema := &goschema.Database{
		Tables: []goschema.Table{{StructName: "RevCoverageHost", Name: revCoverageTable}},
		Fields: []goschema.Field{{StructName: "RevCoverageHost", Name: "id", Type: "BIGINT", Primary: true}},
		Constraints: []goschema.Constraint{{
			StructName:      "RevCoverageHost",
			Name:            revCoverageCheckName,
			Table:           revCoverageTable,
			Type:            "CHECK",
			CheckExpression: checkClause,
		}},
	}
	goschema.Finalize(schema)

	dbSchema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{
			Name: revCoverageTable,
			Type: "TABLE",
			Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			},
		}},
		Constraints: []dbschematypes.DBConstraint{
			{
				Name:        revCoverageCheckName,
				TableName:   revCoverageTable,
				Type:        "CHECK",
				CheckClause: &checkClause,
			},
			// The UNIQUE constraint behind the constraint-backed index removal
			// in reverseCoverageDiff. reverseIndexRemovals reads its columns to
			// rebuild the ALTER TABLE ... ADD CONSTRAINT the rollback needs;
			// without a body here the removal would fall back to an index
			// addition and the field would look unreached.
			{
				Name:        revCoverageUniqueName,
				TableName:   revCoverageTable,
				Type:        "UNIQUE",
				ColumnNames: []string{"id"},
			},
		},
	}
	return schema, dbSchema
}

// fillDistinctly writes a non-zero value derived from the field path into every
// reachable leaf, so that two different fields never collide and a reversed
// "old -> new" description is observably different from the original.
//
// A kind it does not know about is left at its zero value, which
// TestReverseCoverageDiff_PopulatesEverySchemaDiffField turns into a failure
// naming the field and its type.
func fillDistinctly(value reflect.Value, label string) {
	switch value.Kind() {
	case reflect.String:
		value.SetString(label)
	case reflect.Bool:
		value.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value.SetInt(1)
	case reflect.Pointer:
		pointed := reflect.New(value.Type().Elem())
		fillDistinctly(pointed.Elem(), label)
		value.Set(pointed)
	case reflect.Slice:
		element := reflect.New(value.Type().Elem()).Elem()
		fillDistinctly(element, label)
		value.Set(reflect.Append(reflect.MakeSlice(value.Type(), 0, 1), element))
	case reflect.Map:
		key := reflect.New(value.Type().Key()).Elem()
		fillDistinctly(key, label)
		element := reflect.New(value.Type().Elem()).Elem()
		// A change map records "old -> new"; giving the two sides different text
		// is what makes a flipped map distinguishable from an unflipped one.
		fillDistinctly(element, label+"_old -> "+label+"_new")
		created := reflect.MakeMap(value.Type())
		created.SetMapIndex(key, element)
		value.Set(created)
	case reflect.Struct:
		structType := value.Type()
		for i := range structType.NumField() {
			fillDistinctly(value.Field(i), distinctLabel(label, structType.Field(i).Name))
		}
	}
}

func distinctLabel(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "_" + name
}
