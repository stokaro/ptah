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

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/deporder"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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

	diffType := reflect.TypeFor[difftypes.SchemaDiff]()
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
	baseline *difftypes.SchemaDiff,
	schema *schemamodel.Database,
	dbSchema *catalog.Database,
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
	if field.Name == "FunctionsRemovedWithSignatures" || field.Name == "ProceduresRemovedWithSignatures" {
		// These are OUTPUTS of the reverse rather than inputs to it. The
		// reverse of a removal is an addition, and an addition is recorded by
		// name -- the planner reads the rest off the declaration -- so zeroing
		// the forward signatures cannot change the down plan.
		//
		// The down direction fills them itself, from the desired schema, for
		// the reason the forward direction needs them: the rollback of an ADDED
		// overload is as ambiguous as the forward drop was
		// (stokaro/ptah#2296). TestReverseSchemaDiff_ADroppedOverloadKeepsIts
		// Signature is what holds that, since this gate structurally cannot.
		return
	}
	if field.Name == "DeclaredTables" {
		// An OUTPUT of the reverse, for the reason DeclaredUserTypes below is.
		// A rollback restores the tables the pre-change database held, and a
		// foreign key of theirs names a table as THAT database had it -- so the
		// reverse derives the list from the introspected schema and the forward
		// value cannot reach it.
		//
		// TestReverseSchemaDiff_ARolledBackForeignKeyResolvesAgainstThePriorTables
		// is what holds that, since this gate structurally cannot.
		return
	}
	if field.Name == "DeclaredViewLikes" {
		// An OUTPUT of the reverse, for the reason the two above are. A
		// rollback recreates what a cascade took from the PRE-CHANGE database,
		// so the collateral set is that database's views; the forward value
		// names the views the change was moving to and cannot reach it.
		//
		// TestReverseSchemaDiff_ARolledBackCascadeRecreatesThePriorViews is
		// what holds that, since this gate structurally cannot.
		return
	}
	if field.Name == "DeclaredTableDependencies" {
		// An OUTPUT of the reverse, for the reason the five below are. A
		// rollback drops the tables the change created, and the edges between
		// them are the PRE-CHANGE database's -- a table it never held has no
		// edges there, which is what leaves this direction's own ordering of
		// TablesRemoved standing.
		//
		// TestReverseSchemaDiff_ARolledBackDropOrderComesFromThePriorGraph is
		// what holds that, since this gate structurally cannot.
		return
	}
	if field.Name == "DeclaredConstraintHosts" {
		// An OUTPUT of the reverse, for the reason the four above are. A
		// rollback rebuilds the table the PRE-CHANGE database held, so the
		// columns, indexes and triggers that rebuild renders are that
		// database's -- the forward value describes the table the change was
		// moving to and cannot reach it.
		//
		// TestReverseSchemaDiff_ARolledBackRebuildUsesThePriorTableBody is
		// what holds that, since this gate structurally cannot.
		return
	}
	if field.Name == "DeclaredForeignKeys" {
		// An OUTPUT of the reverse, for the reason the three above are. A
		// rollback restores the column the PRE-CHANGE database had, so the
		// keys it must drop and put back around that column are that
		// database's -- the forward value names the keys the change was moving
		// to and cannot reach it.
		//
		// TestReverseSchemaDiff_ARolledBackColumnCarriesThePriorForeignKeys is
		// what holds that, since this gate structurally cannot.
		return
	}
	if field.Name == "DeclaredUserTypes" {
		// An OUTPUT of the reverse rather than an input, for the reason the
		// signatures above are. The vocabulary the down direction needs is the
		// PRE-CHANGE declaration's -- the tables it creates are the ones that
		// database held -- so the reverse derives it from the introspected
		// schema and the forward value cannot reach it.
		//
		// Carrying the forward one across would be wrong rather than merely
		// unnecessary: it names the types the DESIRED schema declares, and a
		// rollback creating a table that database held would resolve its
		// columns through a vocabulary that may not contain their types at all.
		// TestReverseSchemaDiff_ARolledBackTableIsTypedByThePriorVocabulary is
		// what holds that, since this gate structurally cannot.
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
	diffType := reflect.TypeFor[difftypes.SchemaDiff]()

	tests := []struct {
		field string
		diff  *difftypes.SchemaDiff
		wants string
	}{
		{
			field: "ViewsModified",
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{
				ViewName: modifiedCategoryView,
				Changes:  map[string]string{"body": "old -> new"},
			}}},
			wants: modifiedCategoryPriorViewBody,
		},
		{
			field: "MaterializedViewsModified",
			diff: &difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{{
				ViewName: modifiedCategoryMatView,
				Changes:  map[string]string{"body": "old -> new"},
			}}},
			wants: modifiedCategoryPriorMatViewBody,
		},
		{
			field: "TriggersModified",
			diff: &difftypes.SchemaDiff{TriggersModified: []difftypes.TriggerDiff{{
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

func modifiedCategoryContext() (*schemamodel.Database, *catalog.Database) {
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "RevNamedUser", Name: modifiedCategoryTable}},
		Fields: []schemamodel.Field{
			{StructName: "RevNamedUser", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "RevNamedUser", Name: "email", Type: "TEXT"},
		},
		Views: []schemamodel.View{
			{StructName: "RevNamedActive", Name: modifiedCategoryView, Body: modifiedCategoryTargetViewBody},
		},
		MaterializedViews: []schemamodel.MaterializedView{{
			StructName: "RevNamedStats",
			Name:       modifiedCategoryMatView,
			Body:       "SELECT count(*) AS total FROM rev_named_users WHERE id > 0",
		}},
		Triggers: []schemamodel.Trigger{{
			StructName: "RevNamedUser",
			Name:       modifiedCategoryTrigger,
			Table:      modifiedCategoryTable,
			Timing:     "BEFORE",
			Event:      "UPDATE",
			ForEach:    "ROW",
			Body:       "RETURN NEW;",
		}},
	}
	schemamodel.Finalize(schema)

	dbSchema := &catalog.Database{
		Tables: []catalog.Table{{
			Name:   modifiedCategoryTable,
			Schema: modifiedCategorySchema,
			Type:   "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "email", DataType: "text", IsNullable: "NO", OrdinalPosition: 2},
			},
		}},
		Views: []catalog.View{
			{Name: modifiedCategoryView, Schema: modifiedCategorySchema, Body: modifiedCategoryPriorViewBody},
		},
		MatViews: []catalog.MaterializedView{{
			Name:   modifiedCategoryMatView,
			Schema: modifiedCategorySchema,
			Body:   modifiedCategoryPriorMatViewBody,
		}},
		Triggers: []catalog.Trigger{{
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
func reverseCoverageDiff() *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
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
	constraintBacked := difftypes.IndexRef{Name: revCoverageUniqueName, TableName: revCoverageTable}
	diff.IndexesRemoved = append(diff.IndexesRemoved, constraintBacked)
	diff.ConstraintBackedIndexRemovals = []difftypes.IndexRef{constraintBacked}

	// reverseConstraintAdditions restores the prior body of each removed
	// constraint from the introspected schema, so the entry has to name a
	// constraint type it reconstructs and a host reverseCoverageContext has.
	diff.ConstraintsRemovedWithTables = []difftypes.ConstraintRemovalInfo{{
		Name:      revCoverageCheckName,
		TableName: revCoverageTable,
		Type:      "CHECK",
	}}
	// reverseConstraintRemovals resolves each added constraint's owning table,
	// and skips any addition that does not name one.
	diff.ConstraintsAddedWithTables = []difftypes.ConstraintAdditionInfo{{
		Name:      revCoverageCheckName,
		TableName: revCoverageTable,
		Type:      "CHECK",
	}}
	// DeclaredUserTypes is only observable through a table this direction
	// CREATES whose column names a user type: the vocabulary resolves
	// `rev_coverage_domain` to its schema, and a generic value resolves
	// nothing because no creation names a type. The removal below is what the
	// rollback turns into that creation (stokaro/ptah#2315).
	diff.TablesRemoved = append(diff.TablesRemoved, revCoverageTypedTable)

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

	// The table whose column names a user type, and the domain it names. The
	// domain lives outside the default schema, because that is the only case
	// where qualifying it changes the rendered type.
	revCoverageTypedTable  = "revcov.rev_coverage_typed"
	revCoverageDomainName  = "rev_coverage_domain"
	revCoverageDomainOwner = "revcov"
)

// reverseCoverageContext supplies the generated schema and the pre-change
// database the reverse builder consults for the fields it derives rather than
// swaps.
func reverseCoverageContext() (*schemamodel.Database, *catalog.Database) {
	checkClause := "id > 0"

	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "RevCoverageHost", Name: revCoverageTable}},
		Fields: []schemamodel.Field{{StructName: "RevCoverageHost", Name: "id", Type: "BIGINT", Primary: true}},
		Constraints: []schemamodel.Constraint{{
			StructName:      "RevCoverageHost",
			Name:            revCoverageCheckName,
			Table:           revCoverageTable,
			Type:            "CHECK",
			CheckExpression: checkClause,
		}},
	}
	schemamodel.Finalize(schema)

	dbSchema := &catalog.Database{
		Domains: []catalog.Domain{{
			Schema: revCoverageDomainOwner, Name: revCoverageDomainName, BaseType: "integer",
		}},
		Tables: []catalog.Table{{
			Name: revCoverageTable,
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			},
		}, {
			Schema: revCoverageDomainOwner,
			Name:   "rev_coverage_typed",
			Type:   "TABLE",
			Columns: []catalog.Column{{
				Name: "c", DataType: revCoverageDomainName, IsNullable: "YES", OrdinalPosition: 1,
				DomainName: revCoverageDomainName, DomainSchema: revCoverageDomainOwner,
			}},
		}},
		Constraints: []catalog.Constraint{
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

// TestReverseSchemaDiff_ADroppedOverloadKeepsItsSignature holds what the
// coverage gate above structurally cannot.
//
// That gate zeroes an INPUT field and asks whether the down plan changed. These
// two fields are outputs, so it is exempt from it — and an exemption with
// nothing behind it is how a field stops being checked. This drives the reverse
// directly: a routine added forward comes back as a removal, and that removal
// has to name the overload or the rollback is refused with
// `function name "f" is not unique` (stokaro/ptah#2296).
func TestReverseSchemaDiff_ADroppedOverloadKeepsItsSignature(t *testing.T) {
	c := qt.New(t)

	// The addition carries its own declaration now, so the signature the
	// rollback drops by comes from the change rather than from a lookup in the
	// schema beside it (stokaro/ptah#2315). The schema is still passed, and
	// still empty of this routine, which is what shows the carry is the source.
	reversed := reverseSchemaDiffWithSchema(
		&difftypes.SchemaDiff{FunctionsAdded: difftypes.FunctionChanges{{
			Function: schemamodel.Function{Name: "f", Parameters: "a text", Returns: "text", Body: "SELECT $1"},
		}}}, &schemamodel.Database{}, nil)

	c.Assert(reversed.FunctionsRemoved.Removals(), qt.DeepEquals,
		[]difftypes.RoutineRemoval{{Name: "f", Signature: "a text"}})
}

// TestReverseSchemaDiff_ARoutineTheSchemaNoLongerDeclaresDropsByName is the
// control: an empty signature is the answer the bare list already gave, and it
// drops correctly wherever the name is unique.
//
// What makes it empty changed with stokaro/ptah#2315. It used to be a lookup
// that found nothing; it is now an addition that declared no parameters, which
// is the same answer reached without depending on the schema still being there.
func TestReverseSchemaDiff_ARoutineTheSchemaNoLongerDeclaresDropsByName(t *testing.T) {
	c := qt.New(t)

	reversed := reverseSchemaDiffWithSchema(
		&difftypes.SchemaDiff{FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "gone"}}}}, &schemamodel.Database{}, nil)

	c.Assert(reversed.FunctionsRemoved.Removals(), qt.DeepEquals,
		[]difftypes.RoutineRemoval{{Name: "gone", Signature: ""}})
}

// TestReverseSchemaDiff_ARolledBackTableIsTypedByThePriorVocabulary is the
// assertion the coverage gate above structurally cannot make.
//
// A rollback creates the tables the up direction dropped, and their columns name
// the user types the PRE-CHANGE database declared. Resolving them through the
// desired schema's vocabulary would type a restored column by a declaration that
// no longer describes it -- and where the desired schema declares no such type,
// by nothing at all, which renders the bare name and applies to whatever the
// connection's search path finds.
func TestReverseSchemaDiff_ARolledBackTableIsTypedByThePriorVocabulary(t *testing.T) {
	c := qt.New(t)

	schema, dbSchema := reverseCoverageContext()
	forward := &difftypes.SchemaDiff{
		TablesRemoved: []string{revCoverageTypedTable},
		// The desired schema's vocabulary, which is empty here: the rollback
		// must not resolve through it.
		DeclaredUserTypes: difftypes.UserTypeVocabularyOf(schema),
	}

	reversed := reverseSchemaDiffWithSchema(forward, schema, dbSchema)

	c.Assert(reversed.TablesAdded, qt.HasLen, 1)
	c.Assert(reversed.DeclaredUserTypes.Domains, qt.HasLen, 1,
		qt.Commentf("the vocabulary comes from the database that held the table"))
	c.Assert(reversed.DeclaredUserTypes.Domains[0].Name, qt.Equals, revCoverageDomainName)

	qualified := reversed.TablesAdded.Qualified(reversed.DeclaredUserTypes, "postgres")
	c.Assert(qualified, qt.HasLen, 1)
	c.Assert(qualified[0].Fields, qt.HasLen, 1)
	c.Assert(qualified[0].Fields[0].Type, qt.Equals,
		revCoverageDomainOwner+"."+revCoverageDomainName,
		qt.Commentf("the restored column is typed by the domain that database declared"))
}

// TestReverseSchemaDiff_ARolledBackForeignKeyResolvesAgainstThePriorTables is
// the companion to the vocabulary assertion above, for the other schema-wide
// fact a rollback needs.
//
// A restored table's foreign key names the table it references, and that table
// is one the PRE-CHANGE database held. Resolving it against the desired
// schema's tables would qualify it by a declaration that no longer describes
// the database being rolled back to -- or leave it unqualified, to be resolved
// by whatever the connection's search path finds.
func TestReverseSchemaDiff_ARolledBackForeignKeyResolvesAgainstThePriorTables(t *testing.T) {
	c := qt.New(t)

	schema, dbSchema := reverseCoverageContext()
	forward := &difftypes.SchemaDiff{
		TablesRemoved: []string{revCoverageTypedTable},
		// The desired schema's tables, which do not include the one the
		// rollback restores: the reverse must not resolve through them.
		DeclaredTables: schema.Tables,
	}

	reversed := reverseSchemaDiffWithSchema(forward, schema, dbSchema)

	names := make([]string, 0, len(reversed.DeclaredTables))
	for _, table := range reversed.DeclaredTables {
		names = append(names, table.QualifiedName())
	}
	c.Assert(names, qt.Contains, revCoverageTypedTable,
		qt.Commentf("the reference vocabulary comes from the database that held the table"))
}

// TestReverseSchemaDiff_ARolledBackCascadeRecreatesThePriorViews is the third
// schema-wide fact a rollback resolves rather than inherits.
//
// A DROP that cascades reaches whichever views SELECT from what it dropped, and
// the rollback has to put those back. Which views those are is a fact about the
// database being rolled back TO, not about the declaration the change was
// moving to: a view the desired schema declares may not exist there at all, and
// a view that database held may have been removed by the very change being
// undone.
//
// The gate above cannot see this, because the reverse derives the vocabulary
// and zeroing the forward field leaves the down plan identical.
func TestReverseSchemaDiff_ARolledBackCascadeRecreatesThePriorViews(t *testing.T) {
	c := qt.New(t)

	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Host", Name: "rev_view_host"}},
		Fields: []schemamodel.Field{{StructName: "Host", Name: "id", Type: "BIGINT", Primary: true}},
		Views: []schemamodel.View{{
			Name: "rev_view_desired",
			Body: "SELECT id FROM rev_view_host",
		}},
	}
	schemamodel.Finalize(schema)

	dbSchema := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "rev_view_host",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			},
		}},
		Views: []catalog.View{{
			Name: "rev_view_prior",
			Body: "SELECT id FROM rev_view_host",
		}},
	}

	forward := &difftypes.SchemaDiff{
		ViewsAdded: difftypes.ViewChanges{{Name: "rev_view_desired"}},
		// The DESIRED schema's views, which the reverse must not resolve
		// through: they describe the state being rolled back FROM.
		DeclaredViewLikes: difftypes.ViewLikeVocabularyOf(schema),
	}

	reversed := reverseSchemaDiffWithSchema(forward, schema, dbSchema)

	names := make([]string, 0, len(reversed.DeclaredViewLikes.Views))
	for _, view := range reversed.DeclaredViewLikes.Views {
		names = append(names, view.Name)
	}
	c.Assert(names, qt.Contains, "rev_view_prior",
		qt.Commentf("the collateral set comes from the database being rolled back to"))
	c.Assert(names, qt.Not(qt.Contains), "rev_view_desired",
		qt.Commentf("a view only the desired schema declares is not there to be recreated"))
}

// TestReverseSchemaDiff_ARolledBackColumnCarriesThePriorForeignKeys is the
// fourth schema-wide fact a rollback resolves rather than inherits.
//
// The MySQL family cannot MODIFY a column a foreign key references, so it drops
// the keys first and puts them back after. Which keys those are is a fact about
// the database being rolled back TO: a key the desired schema declares may not
// be there at all, and one that database holds may be absent from the
// declaration -- which is exactly what a rollback of "add a foreign key" is.
//
// Inheriting the forward value would drop nothing and re-add a key the
// pre-change database never had.
func TestReverseSchemaDiff_ARolledBackColumnCarriesThePriorForeignKeys(t *testing.T) {
	c := qt.New(t)

	// The declaration holds one foreign key; the database holds a different
	// one. Neither list is empty, so a reverse that inherited the forward value
	// would still look populated.
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Order", Name: "buyer_id", Type: "BIGINT", Foreign: "buyers(id)", ForeignKeyName: "fk_orders_buyer"},
		},
	}
	schemamodel.Finalize(schema)
	foreignTable, foreignColumn := "customers", "id"
	dbSchema := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "orders",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "customer_id", DataType: "bigint", IsNullable: "NO", OrdinalPosition: 2},
			},
		}},
		Constraints: []catalog.Constraint{{
			Name:          "fk_orders_customer",
			TableName:     "orders",
			Type:          "FOREIGN KEY",
			ColumnName:    "customer_id",
			ForeignTable:  &foreignTable,
			ForeignColumn: &foreignColumn,
		}},
	}
	forward := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{TableName: "orders"}},
		// The declaration's keys, which the rollback must not act on.
		DeclaredForeignKeys: difftypes.ForeignKeyDeclarationsOf(schema),
	}

	reversed := reverseSchemaDiffWithSchema(forward, schema, dbSchema)

	names := make([]string, 0, len(reversed.DeclaredForeignKeys))
	for _, declared := range reversed.DeclaredForeignKeys {
		names = append(names, declared.Name)
	}
	c.Assert(names, qt.Contains, "fk_orders_customer",
		qt.Commentf("the keys to drop and restore come from the database being rolled back to"))
	c.Assert(names, qt.Not(qt.Contains), "fk_orders_buyer",
		qt.Commentf("the declaration's own key is not in that database and must not be re-added"))
}

// TestReverseSchemaDiff_ARolledBackTableCarriesThePriorConstraints covers the
// half of a creation that has no second chance to arrive.
//
// A rollback recreates the tables the change removed, and on a target with no
// ADD CONSTRAINT the constraints have to be inside that CREATE. They come from
// the database being rolled back to, which is where the table was.
//
// The reverse census cannot see this: the forward diff's TablesRemoved is a
// list of names, so there is no forward value to zero.
func TestReverseSchemaDiff_ARolledBackTableCarriesThePriorConstraints(t *testing.T) {
	c := qt.New(t)

	schema, dbSchema := reverseCoverageContext()
	forward := &difftypes.SchemaDiff{TablesRemoved: []string{revCoverageTable}}

	reversed := reverseSchemaDiffWithSchema(forward, schema, dbSchema)

	c.Assert(reversed.TablesAdded, qt.HasLen, 1)
	names := make([]string, 0, len(reversed.TablesAdded[0].Constraints))
	for _, constraint := range reversed.TablesAdded[0].Constraints {
		names = append(names, constraint.Name)
	}
	c.Assert(names, qt.Contains, revCoverageCheckName,
		qt.Commentf("the recreated table brings back the constraints that database's table had"))
}

// TestReverseSchemaDiff_ARolledBackRebuildUsesThePriorTableBody is the fifth
// schema-wide fact a rollback resolves rather than inherits.
//
// A target with no ALTER for a constraint change rebuilds the table around it,
// and a rebuild renders the table entire. Which columns, indexes and triggers
// that is, is a fact about the database being rolled back TO: the declaration
// describes the table the change was moving to, and rebuilding from it would
// write the post-change table while restoring the pre-change constraint.
//
// The two bodies differ by one column here, and both are non-empty, so a
// reversal that inherited the forward value would still look populated.
func TestReverseSchemaDiff_ARolledBackRebuildUsesThePriorTableBody(t *testing.T) {
	c := qt.New(t)

	const table, constraintName = "widgets", "ck_widgets_id"
	// The declaration has the column the change ADDS; the database does not.
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Widget", Name: table}},
		Fields: []schemamodel.Field{
			{StructName: "Widget", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Widget", Name: "note", Type: "TEXT"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Widget", Name: constraintName, Table: table,
			Type: "CHECK", CheckExpression: "id > 0",
		}},
	}
	schemamodel.Finalize(schema)
	checkClause := "id > 0"
	dbSchema := &catalog.Database{
		Tables: []catalog.Table{{
			Name: table, Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			},
		}},
		// The constraint the forward change removes has to BE there, or the
		// rollback has nothing to restore and carries no host either.
		Constraints: []catalog.Constraint{{
			Name: constraintName, TableName: table, Type: "CHECK", CheckClause: &checkClause,
		}},
	}
	removal := []difftypes.ConstraintRemovalInfo{{
		Name: constraintName, TableName: table, Type: "CHECK",
	}}
	forward := &difftypes.SchemaDiff{
		ConstraintsRemoved:           []string{constraintName},
		ConstraintsRemovedWithTables: removal,
		// The declaration's own hosts, carrying the added column. A rollback
		// rebuilding from these would write a table the database never had.
		DeclaredConstraintHosts: difftypes.ConstraintHostDeclarationsOf(
			schema, nil, removal, identifier.Semantics{}),
	}

	reversed := reverseSchemaDiffWithSchema(forward, schema, dbSchema)
	c.Assert(reversed.DeclaredConstraintHosts, qt.HasLen, 1)
	columns := make([]string, 0, len(reversed.DeclaredConstraintHosts[0].Fields))
	for _, field := range reversed.DeclaredConstraintHosts[0].Fields {
		columns = append(columns, field.Name)
	}
	c.Assert(columns, qt.Contains, "id")
	c.Assert(columns, qt.Not(qt.Contains), "note",
		qt.Commentf("the rebuild renders the table body that database had, not the one the change was moving to"))
}

// TestReverseSchemaDiff_ARolledBackDropOrderComesFromThePriorGraph is the sixth
// schema-wide fact a rollback resolves rather than inherits.
//
// The graph the removals are ordered by describes the database being rolled
// back TO. Inheriting the forward one would order a rollback's drops by edges
// between tables that database does not have.
func TestReverseSchemaDiff_ARolledBackDropOrderComesFromThePriorGraph(t *testing.T) {
	c := qt.New(t)

	// The declaration and the database each hold one edge, and they are
	// different edges, so a reversal that inherited the forward value would
	// still look populated.
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Desired", Name: "desired_child"},
			{StructName: "DesiredParent", Name: "desired_parent"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Desired", Name: "parent_id", Type: "BIGINT", Foreign: "desired_parent(id)"},
			{StructName: "DesiredParent", Name: "id", Type: "BIGINT", Primary: true},
		},
	}
	schemamodel.Finalize(schema)
	parent, parentColumn := "prior_parent", "id"
	dbSchema := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "prior_parent", Type: "TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			}},
			{Name: "prior_child", Type: "TABLE", Columns: []catalog.Column{
				{Name: "parent_id", DataType: "bigint", OrdinalPosition: 1},
			}},
		},
		Constraints: []catalog.Constraint{{
			Name: "fk_prior_child_parent", TableName: "prior_child", Type: "FOREIGN KEY",
			ColumnName: "parent_id", ForeignTable: &parent, ForeignColumn: &parentColumn,
		}},
	}
	forward := &difftypes.SchemaDiff{
		TablesRemoved:             []string{"prior_child", "prior_parent"},
		DeclaredTableDependencies: deporder.GeneratedTableDependencies(schema),
	}

	reversed := reverseSchemaDiffWithSchema(forward, schema, dbSchema)

	c.Assert(reversed.DeclaredTableDependencies["prior_child"], qt.Contains, "prior_parent",
		qt.Commentf("the edges come from the database being rolled back to"))
	_, declaredEdge := reversed.DeclaredTableDependencies["desired_child"]
	c.Assert(declaredEdge, qt.IsFalse,
		qt.Commentf("the declaration's own graph describes tables that database does not have"))
}
