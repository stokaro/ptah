package schemachange_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemachange"
	"go.5x5.cz/ptah/internal/schemastate"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestPipeline_ProducesTheChangeTheInputsDescribe walks the whole prototype for
// the three operations, and asserts on the CHANGE rather than on its SQL.
//
// The change is what the rest of the pipeline reads. A test that only checked
// statements would pass on a change carrying no risk, no reversibility and no
// provenance, which is exactly the change stokaro/ptah#1350 exists to replace.
func TestPipeline_ProducesTheChangeTheInputsDescribe(t *testing.T) {
	tests := []struct {
		name              string
		description       *goschema.Database
		catalog           *dbschematypes.DBSchema
		wantOperation     schemachange.Operation
		wantChanged       []string
		wantRisk          schemachange.Risk
		wantReversibility schemachange.Reversibility
	}{
		{
			name:              "a foreign key the database does not have",
			description:       parentChildDescription(""),
			catalog:           emptyCatalog(),
			wantOperation:     schemachange.Add,
			wantRisk:          schemachange.RiskDataDependent,
			wantReversibility: schemachange.Reversible,
		},
		{
			name:              "a foreign key the desired schema does not declare",
			description:       &goschema.Database{Tables: parentChildDescription("").Tables, Fields: fieldsWithoutForeignKey()},
			catalog:           parentChildCatalog("NO ACTION"),
			wantOperation:     schemachange.Remove,
			wantRisk:          schemachange.RiskGuaranteeLoss,
			wantReversibility: schemachange.ReversibleWithData,
		},
		{
			// The #189 shape: a changed referential action. The existing
			// comparator expresses it as a removal and an addition of one name
			// and leaves every planner to work out that the two belong
			// together; here it is one change that says what moved.
			name:              "a foreign key whose referential action changed",
			description:       parentChildDescription("CASCADE"),
			catalog:           parentChildCatalog("NO ACTION"),
			wantOperation:     schemachange.Modify,
			wantChanged:       []string{"on delete"},
			wantRisk:          schemachange.RiskDataDependent,
			wantReversibility: schemachange.ReversibleWithData,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes, err := orderedChanges(c, test.description, test.catalog, postgresProfile())

			c.Assert(err, qt.IsNil)
			c.Assert(changes, qt.HasLen, 1)
			c.Assert(changes[0].Operation, qt.Equals, test.wantOperation)
			c.Assert(changes[0].Changed, qt.DeepEquals, test.wantChanged)
			c.Assert(changes[0].Risk, qt.Equals, test.wantRisk)
			c.Assert(changes[0].Reversibility, qt.Equals, test.wantReversibility)
			c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
			c.Assert(changes[0].Evidence, qt.Not(qt.Equals), "")
			c.Assert(changes[0].Provenance.Source, qt.Not(qt.Equals), "")
			c.Assert(changes[0].ID.Name.Source, qt.Equals, "fk_child_parent")
		})
	}
}

// TestPipeline_UnchangedSchemaPlansNothing is the control for the table above.
// A comparator that reported a change for everything would satisfy all three
// rows, and this is the input where the right answer is silence.
//
// It is also where the referential-action default earns its place: the
// description writes no ON DELETE and the catalog reports NO ACTION, so a
// comparison over the raw values would report a modification here.
func TestPipeline_UnchangedSchemaPlansNothing(t *testing.T) {
	c := qt.New(t)

	changes, err := orderedChanges(c, parentChildDescription(""), parentChildCatalog("NO ACTION"), postgresProfile())

	c.Assert(err, qt.IsNil)
	c.Assert(changes, qt.HasLen, 0)
}

// TestPipeline_RendersWithoutTheSourceDescription is the boundary claim of
// ADR 0001 decision 8, stated as a test.
//
// [schemachange.Plan] takes changes and a profile. It does not take a schema,
// so it cannot consult one, and the compiler is what enforces that. The
// existing path's planner takes `(diff, generated, dialect)` and recovers from
// `generated` what the diff dropped.
func TestPipeline_RendersWithoutTheSourceDescription(t *testing.T) {
	c := qt.New(t)
	changes, err := orderedChanges(c, parentChildDescription("CASCADE"), emptyCatalog(), postgresProfile())
	c.Assert(err, qt.IsNil)

	operations, err := schemachange.Plan(changes, postgresProfile())

	c.Assert(err, qt.IsNil)
	c.Assert(operations, qt.HasLen, 1)
	c.Assert(operations[0].SQL, qt.Contains, "ADD CONSTRAINT")
	c.Assert(operations[0].SQL, qt.Contains, "fk_child_parent")
	c.Assert(operations[0].SQL, qt.Contains, "ON DELETE CASCADE")
	// The rendered statement traces back to the change that produced it, which
	// a list of names cannot do.
	c.Assert(operations[0].Change.Operation, qt.Equals, schemachange.Add)
}

// TestPipeline_ModificationRendersDropThenAdd pins that one change becomes two
// statements in the right order, and stays one change.
//
// No engine Ptah targets alters a foreign key's referential actions in place.
// Splitting the modification into two changes is what lets a later stage carry
// one half and drop the other.
func TestPipeline_ModificationRendersDropThenAdd(t *testing.T) {
	c := qt.New(t)

	operations := pipeline(c, parentChildDescription("CASCADE"), parentChildCatalog("NO ACTION"), postgresProfile())

	c.Assert(operations, qt.HasLen, 2)
	c.Assert(operations[0].SQL, qt.Contains, "DROP CONSTRAINT")
	c.Assert(operations[1].SQL, qt.Contains, "ADD CONSTRAINT")
	c.Assert(operations[0].Change.ID.Key(), qt.Equals, operations[1].Change.ID.Key())
	c.Assert(operations[0].Change.Operation, qt.Equals, schemachange.Modify)
}

// TestPipeline_BlockedOnATargetThatCannotHostTheFamily is the required-target-
// fact path, measured rather than hypothetical.
//
// ClickHouse parses no FOREIGN KEY clause at all, and the capability probe
// measures foreign_keys false on every ClickHouse matrix cell. The change is
// blocked with the key named, so an operator learns which measurement is
// missing rather than that something failed.
func TestPipeline_BlockedOnATargetThatCannotHostTheFamily(t *testing.T) {
	c := qt.New(t)

	changes, err := orderedChanges(c, parentChildDescription(""), emptyCatalog(), clickhouseProfile())
	c.Assert(err, qt.IsNil)
	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Blocked)
	c.Assert(changes[0].MissingFacts, qt.DeepEquals, []capability.Capability{capability.ForeignKeys})
	c.Assert(changes[0].Diagnostic, qt.Contains, "foreign_keys")

	_, planErr := schemachange.Plan(changes, clickhouseProfile())

	c.Assert(planErr, qt.ErrorIs, schemachange.ErrBlocked)
}

// TestPipeline_ForwardAndRollbackShareTheSameEdges pins the property that a
// rollback derived from its own rules cannot have: the two orders are one
// traversal and its reverse.
func TestPipeline_ForwardAndRollbackShareTheSameEdges(t *testing.T) {
	c := qt.New(t)
	desired, current, err := states(parentChildDescription("CASCADE"), parentChildCatalog("NO ACTION"), postgresProfile())
	c.Assert(err, qt.IsNil)
	changes, err := schemachange.Compare(current, desired, postgresProfile())
	c.Assert(err, qt.IsNil)
	graph, err := schemachange.BuildGraph(changes, current, desired)
	c.Assert(err, qt.IsNil)

	forward, forwardErr := graph.Forward()
	rollback, rollbackErr := graph.Rollback()

	c.Assert(forwardErr, qt.IsNil)
	c.Assert(rollbackErr, qt.IsNil)
	c.Assert(rollback, qt.HasLen, len(forward))
	c.Assert(identityLine(rollback), qt.Equals, reversedLine(forward))
}

// TestPipeline_DeterministicAcrossRuns runs the whole pipeline repeatedly and
// asserts the statements do not move.
//
// Go randomizes map iteration per run, and the state, the graph and the change
// list all pass through maps. A plan whose statement order changes between two
// runs over one input is a plan nobody can review.
func TestPipeline_DeterministicAcrossRuns(t *testing.T) {
	c := qt.New(t)
	description := manyForeignKeys(12)
	catalog := manyForeignKeysCatalog(12)

	first := strings.Join(statementsOf(c, pipeline(c, description, catalog, postgresProfile())), "\n")

	for run := range 24 {
		again := strings.Join(statementsOf(c, pipeline(c, description, catalog, postgresProfile())), "\n")
		c.Assert(again, qt.Equals, first, qt.Commentf("run %d", run))
	}
}

// TestPipeline_RefusesAStateItDidNotNormalize pins ADR 0001 invariant 8 at the
// stage boundary: the referential-action default is applied in normalization,
// so comparing an unnormalized side reports a modification for every foreign
// key whose ON DELETE nobody wrote.
func TestPipeline_RefusesAStateItDidNotNormalize(t *testing.T) {
	c := qt.New(t)
	profile := postgresProfile()
	raw, err := schemastate.FromDescription(parentChildDescription(""), profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	normalized, err := schemastate.Normalize(raw, profile)
	c.Assert(err, qt.IsNil)

	_, compareErr := schemachange.Compare(raw, normalized, profile)

	c.Assert(compareErr, qt.ErrorIs, schemastate.ErrUnnormalized)
}

// TestPipeline_RefusesToNormalizeTwice is the other half of invariant 8. A
// second fold is what the verbatim constructors in objectidentity exist to work
// around today, and this is the check that would have caught it.
func TestPipeline_RefusesToNormalizeTwice(t *testing.T) {
	c := qt.New(t)
	profile := postgresProfile()
	raw, err := schemastate.FromDescription(parentChildDescription(""), profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	once, err := schemastate.Normalize(raw, profile)
	c.Assert(err, qt.IsNil)

	_, twiceErr := schemastate.Normalize(once, profile)

	c.Assert(twiceErr, qt.IsNotNil)
	c.Assert(twiceErr.Error(), qt.Contains, "already normalized")
}

// TestPipeline_UnknownStateNeverPlansDestruction is the fail-closed rule, at
// the three points where a guess would target the wrong object.
func TestPipeline_UnknownStateNeverPlansDestruction(t *testing.T) {
	tests := []struct {
		name        string
		description *goschema.Database
		catalog     *dbschematypes.DBSchema
		wantErr     string
	}{
		{
			// A reference to a table nothing in the schema is. Rendering it
			// would emit a constraint the target refuses at apply time, and
			// dropping the clause would emit one that guards nothing.
			name:        "a reference to a table that does not exist",
			description: referencingAMissingTable(),
			catalog:     emptyCatalog(),
			wantErr:     "dangling reference",
		},
		{
			// A referential action Ptah does not understand. Passing it through
			// renders a clause whose behavior on delete nobody can state.
			name:        "a referential action the model does not understand",
			description: withOnDelete("SET EVERYTHING"),
			catalog:     emptyCatalog(),
			wantErr:     "unknown referential action",
		},
		{
			// A reference naming no column. Defaulting to a primary key this
			// adapter cannot see would target whichever column happens to be
			// the key.
			name:        "a reference that names no column",
			description: withForeignSpelling("parent"),
			catalog:     emptyCatalog(),
			wantErr:     "names no column",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := orderedChanges(c, test.description, test.catalog, postgresProfile())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.wantErr)
		})
	}
}

// TestPipeline_ScopeIsNotSilence pins that a reader which did not look at the
// family cannot have its silence read as a removal.
func TestPipeline_ScopeIsNotSilence(t *testing.T) {
	c := qt.New(t)
	profile := postgresProfile()
	// A state whose builder declares it looked at tables only, which is what a
	// reader that skipped constraints produces.
	tablesOnly := schemastate.New(profile.Dialect, objectidentity.KindTable)
	normalized, err := schemastate.Normalize(tablesOnly, profile)
	c.Assert(err, qt.IsNil)
	desired, _, err := states(parentChildDescription(""), emptyCatalog(), profile)
	c.Assert(err, qt.IsNil)

	_, compareErr := schemachange.Compare(normalized, desired, profile)

	c.Assert(compareErr, qt.ErrorIs, schemastate.ErrOutsideScope)
}

// TestPipeline_ExplainNamesTheChangeAndItsSource pins that the plan is
// explainable, which is a definition-of-done item that needs output to be
// checkable at all.
func TestPipeline_ExplainNamesTheChangeAndItsSource(t *testing.T) {
	c := qt.New(t)

	explanation := schemachange.Explain(
		pipeline(c, parentChildDescription("CASCADE"), emptyCatalog(), postgresProfile()))

	c.Assert(explanation, qt.Contains, "fk_child_parent")
	c.Assert(explanation, qt.Contains, "risk data_dependent")
	c.Assert(explanation, qt.Contains, "description")
	c.Assert(explanation, qt.Contains, "absent from the database")
}

// TestPipeline_MatchesTheExistingPath is the differential test.
//
// It renders the same input through the existing comparator and planner and
// through the prototype, and compares the statements. A difference is either a
// defect or an entry documented in the intentional-differences table below;
// there is no third outcome.
func TestPipeline_MatchesTheExistingPath(t *testing.T) {
	tests := []struct {
		name        string
		description *goschema.Database
		catalog     *dbschematypes.DBSchema
	}{
		{name: "adding a foreign key", description: parentChildDescription(""), catalog: emptyCatalog()},
		{name: "adding one with a referential action", description: parentChildDescription("CASCADE"), catalog: emptyCatalog()},
		{name: "an unchanged schema", description: parentChildDescription(""), catalog: parentChildCatalog("NO ACTION")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			profile := postgresProfile()

			existing := existingPathStatements(c, test.description, test.catalog, profile)
			prototype := statementsOf(c, pipeline(c, test.description, test.catalog, profile))

			c.Assert(foreignKeyStatements(c, prototype), qt.DeepEquals, foreignKeyStatements(c, existing),
				qt.Commentf("existing=%v prototype=%v", existing, prototype))
		})
	}
}

// existingPathStatements runs the comparator and planner the prototype is
// measured against.
func existingPathStatements(
	c *qt.C,
	description *goschema.Database,
	catalog *dbschematypes.DBSchema,
	profile schemastate.Profile,
) []string {
	c.Helper()
	diff := schemadiff.CompareWithDialect(description, catalog, profile.Dialect)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff, description, profile.Dialect, profile.Capabilities)
	c.Assert(err, qt.IsNil)
	return statements
}

// foreignKeyStatements keeps only the LINES about this slice's family.
//
// The existing path emits a block per table -- comments, column alterations and
// the constraint together -- where the prototype emits one statement per change.
// Comparing the blocks whole would compare two different questions: whether the
// two paths plan the same foreign key, and whether the prototype also plans the
// column changes it declares itself out of scope for.
//
// Filtering to the constraint lines is the documented scope of this comparison
// and not a filter that hides a difference. Every line it drops is asserted to
// mention no foreign key, so a constraint statement cannot leave through it.
func foreignKeyStatements(c *qt.C, statements []string) []string {
	c.Helper()
	kept := make([]string, 0)
	for _, statement := range statements {
		for line := range strings.SplitSeq(statement, "\n") {
			trimmed := strings.TrimSpace(line)
			upper := strings.ToUpper(trimmed)
			mentionsFamily := strings.Contains(upper, "FOREIGN KEY") || strings.Contains(upper, "FK_CHILD_PARENT")
			isConstraintStatement := strings.HasPrefix(upper, "ALTER TABLE") && mentionsFamily
			c.Assert(mentionsFamily && !isConstraintStatement, qt.IsFalse,
				qt.Commentf("a line naming the family would be dropped: %q", trimmed))
			kept = appendWhen(kept, isConstraintStatement, normalizeStatement(trimmed))
		}
	}
	return kept
}

// appendWhen keeps the loop above branch-free at the point the repository's
// test style cares about.
func appendWhen(values []string, keep bool, value string) []string {
	appenders := map[bool]func() []string{
		true:  func() []string { return append(values, value) },
		false: func() []string { return values },
	}
	return appenders[keep]()
}

// normalizeStatement collapses whitespace and trailing semicolons, which the
// two paths spell differently and which no target distinguishes.
func normalizeStatement(statement string) string {
	return strings.TrimSuffix(strings.Join(strings.Fields(statement), " "), ";")
}

func identityLine(changes []schemachange.Change) string {
	names := make([]string, 0, len(changes))
	for _, change := range changes {
		names = append(names, change.ID.String())
	}
	return strings.Join(names, " -> ")
}

func reversedLine(changes []schemachange.Change) string {
	names := make([]string, 0, len(changes))
	for _, change := range slices.Backward(changes) {
		names = append(names, change.ID.String())
	}
	return strings.Join(names, " -> ")
}

// fieldsWithoutForeignKey is the child schema with the reference removed, which
// is what a removal's desired side looks like.
func fieldsWithoutForeignKey() []goschema.Field {
	fields := parentChildDescription("").Fields
	kept := make([]goschema.Field, 0, len(fields))
	for _, field := range fields {
		kept = append(kept, clearedForeign(field))
	}
	return kept
}

func clearedForeign(field goschema.Field) goschema.Field {
	field.Foreign = ""
	field.ForeignKeyName = ""
	field.OnDelete = ""
	return field
}

func referencingAMissingTable() *goschema.Database {
	return withForeignSpelling("nowhere(id)")
}

func withForeignSpelling(spelling string) *goschema.Database {
	description := parentChildDescription("")
	description.Fields[2].Foreign = spelling
	return description
}

func withOnDelete(action string) *goschema.Database {
	return parentChildDescription(action)
}

// manyForeignKeys builds a description with enough constraints that a
// nondeterministic order would show up rather than happen to agree.
func manyForeignKeys(count int) *goschema.Database {
	description := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Parent", Name: "parent"}},
		Fields: []goschema.Field{{StructName: "Parent", Name: "id", Type: "int", Primary: true}},
	}
	for index := range count {
		child := fmt.Sprintf("child%02d", index)
		description.Tables = append(description.Tables, goschema.Table{StructName: child, Name: child})
		description.Fields = append(description.Fields,
			goschema.Field{StructName: child, Name: "id", Type: "int", Primary: true},
			goschema.Field{
				StructName:     child,
				Name:           "parent_id",
				Type:           "int",
				Foreign:        "parent(id)",
				ForeignKeyName: fmt.Sprintf("fk_%s_parent", child),
			})
	}
	return description
}

// manyForeignKeysCatalog is the same shape with the tables present and no
// constraints, so every one of them is an addition.
func manyForeignKeysCatalog(count int) *dbschematypes.DBSchema {
	catalog := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "parent", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
		},
	}
	for index := range count {
		catalog.Tables = append(catalog.Tables, dbschematypes.DBTable{
			Name:   fmt.Sprintf("child%02d", index),
			Schema: "public",
			Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			},
		})
	}
	return catalog
}

var _ = errors.Is

// BenchmarkPipeline_LargeSchema measures the prototype against the existing
// path on the same input.
//
// The acceptance bar ADR 0001 sets is the same order of magnitude: a canonical
// state that costs one more allocation per object is expected, and one that
// costs a second full comparison is not. The two benchmarks are here rather
// than in separate files so a reader compares them without assembling two
// inputs that might differ.
func BenchmarkPipeline_LargeSchema(b *testing.B) {
	description := manyForeignKeys(500)
	catalog := manyForeignKeysCatalog(500)
	profile := postgresProfile()

	b.Run("prototype", func(b *testing.B) {
		for b.Loop() {
			desired, _ := schemastate.FromDescription(description, profile.Dialect, profile.Semantics)
			current, _ := schemastate.FromCatalog(catalog, profile.Dialect, profile.Semantics)
			normalizedDesired, _ := schemastate.Normalize(desired, profile)
			normalizedCurrent, _ := schemastate.Normalize(current, profile)
			changes, _ := schemachange.Compare(normalizedCurrent, normalizedDesired, profile)
			graph, _ := schemachange.BuildGraph(changes, normalizedCurrent, normalizedDesired)
			ordered, _ := graph.Forward()
			_, _ = schemachange.Plan(ordered, profile)
		}
	})

	b.Run("existing path", func(b *testing.B) {
		for b.Loop() {
			diff := schemadiff.CompareWithDialect(description, catalog, profile.Dialect)
			_, _ = planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
				diff, description, profile.Dialect, profile.Capabilities)
		}
	})
}

// TestPipeline_BlockedWhenTheReferenceIsNotAKey pins the schema precondition,
// as opposed to the target-capability one above.
//
// PostgreSQL, MySQL and MariaDB all refuse a foreign key whose referenced
// columns are not a key. The prototype blocks the change with a diagnostic
// naming the columns, so an author learns which declaration to fix; the
// existing path refuses inside the renderer, where no change is attached to the
// error.
func TestPipeline_BlockedWhenTheReferenceIsNotAKey(t *testing.T) {
	c := qt.New(t)
	description := parentChildDescription("")
	// The parent's key stops being one. Everything else is unchanged, so this
	// is the single fact under test.
	description.Fields[0].Primary = false

	changes, err := orderedChanges(c, description, emptyCatalogWithoutKeys(), postgresProfile())

	c.Assert(err, qt.IsNil)
	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Blocked)
	c.Assert(changes[0].Diagnostic, qt.Contains, "no unique constraint covers")
	c.Assert(changes[0].Diagnostic, qt.Contains, "id")
	// The existing path refuses the same schema, which is what makes this a
	// shared answer rather than the prototype inventing a rule.
	diff := schemadiff.CompareWithDialect(description, emptyCatalogWithoutKeys(), "postgres")
	_, existingErr := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff, description, "postgres", postgresProfile().Capabilities)
	c.Assert(existingErr, qt.IsNotNil)
	c.Assert(existingErr.Error(), qt.Contains, "unique")
}

// TestPipeline_EdgeOrderIsStable pins both what the graph's edges ARE and that
// they do not move between runs.
//
// The plan order for this slice does not depend on them -- every edge runs to a
// table outside the change set -- but the edges are what a cycle diagnostic
// prints, so a diagnostic whose reasons reorder or go missing between two runs
// over one input is one nobody can diff. Asserting only stability would let an
// edge class disappear entirely and stay green.
func TestPipeline_EdgeOrderIsStable(t *testing.T) {
	c := qt.New(t)
	description := manyForeignKeys(3)
	catalog := manyForeignKeysCatalog(3)

	first := edgeLine(c, description, catalog)

	c.Assert(first, qt.Equals, strings.Join([]string{
		"referenced table: constraint public.child00.fk_child00_parent references table public.parent",
		"owning table: constraint public.child00.fk_child00_parent is carried by table public.child00",
		"referenced table: constraint public.child01.fk_child01_parent references table public.parent",
		"owning table: constraint public.child01.fk_child01_parent is carried by table public.child01",
		"referenced table: constraint public.child02.fk_child02_parent references table public.parent",
		"owning table: constraint public.child02.fk_child02_parent is carried by table public.child02",
	}, "\n"))
	for run := range 16 {
		c.Assert(edgeLine(c, description, catalog), qt.Equals, first, qt.Commentf("run %d", run))
	}
}

// TestPipeline_OneNameOnTwoTablesIsTwoConstraints is the #197 shape: a foreign
// key contributed by an embedded mixin carries one name on every host table.
//
// A key without the owning table collapses them, and the plan then emits one
// ALTER for whichever host it saw last -- against the other host's schema.
func TestPipeline_OneNameOnTwoTablesIsTwoConstraints(t *testing.T) {
	c := qt.New(t)

	changes, err := orderedChanges(c, sharedForeignKeyName(), sharedForeignKeyCatalog(), postgresProfile())

	c.Assert(err, qt.IsNil)
	c.Assert(changes, qt.HasLen, 2)
	c.Assert(changes[0].ID.Name.Source, qt.Equals, changes[1].ID.Name.Source)
	c.Assert(changes[0].ID.Key(), qt.Not(qt.Equals), changes[1].ID.Key())
	c.Assert(changes[0].ID.Parent.Source, qt.Not(qt.Equals), changes[1].ID.Parent.Source)

	operations, planErr := schemachange.Plan(changes, postgresProfile())

	c.Assert(planErr, qt.IsNil)
	c.Assert(operations, qt.HasLen, 2)
	c.Assert(operations[0].SQL, qt.Contains, `"one"`)
	c.Assert(operations[1].SQL, qt.Contains, `"two"`)
}

// TestPipeline_RollbackReversesAMultiChangeOrder is the version of the
// forward-and-rollback property that a one-change set cannot test: with one
// change, a rollback computed by its own traversal agrees by accident.
func TestPipeline_RollbackReversesAMultiChangeOrder(t *testing.T) {
	c := qt.New(t)
	desired, current, err := states(manyForeignKeys(8), manyForeignKeysCatalog(8), postgresProfile())
	c.Assert(err, qt.IsNil)
	changes, err := schemachange.Compare(current, desired, postgresProfile())
	c.Assert(err, qt.IsNil)
	graph, err := schemachange.BuildGraph(changes, current, desired)
	c.Assert(err, qt.IsNil)

	forward, forwardErr := graph.Forward()
	rollback, rollbackErr := graph.Rollback()

	c.Assert(forwardErr, qt.IsNil)
	c.Assert(rollbackErr, qt.IsNil)
	c.Assert(forward, qt.HasLen, 8)
	c.Assert(identityLine(rollback), qt.Equals, reversedLine(forward))
	// The control: with eight changes the two orders must actually differ, or
	// the assertion above would hold for a rollback that never reversed.
	c.Assert(identityLine(rollback), qt.Not(qt.Equals), identityLine(forward))
}

// edgeLine renders a graph's edges as one comparable string.
func edgeLine(c *qt.C, description *goschema.Database, catalog *dbschematypes.DBSchema) string {
	c.Helper()
	desired, current, err := states(description, catalog, postgresProfile())
	c.Assert(err, qt.IsNil)
	changes, err := schemachange.Compare(current, desired, postgresProfile())
	c.Assert(err, qt.IsNil)
	graph, err := schemachange.BuildGraph(changes, current, desired)
	c.Assert(err, qt.IsNil)
	lines := make([]string, 0)
	for _, edge := range graph.Edges() {
		lines = append(lines, string(edge.Kind)+": "+edge.Why)
	}
	return strings.Join(lines, "\n")
}

// sharedForeignKeyName declares one foreign-key NAME on two host tables, which
// is what an embedded mixin produces.
func sharedForeignKeyName() *goschema.Database {
	description := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parent"},
			{StructName: "One", Name: "one"},
			{StructName: "Two", Name: "two"},
		},
		Fields: []goschema.Field{{StructName: "Parent", Name: "id", Type: "int", Primary: true}},
	}
	for _, host := range []string{"One", "Two"} {
		description.Fields = append(description.Fields, goschema.Field{
			StructName:     host,
			Name:           "parent_id",
			Type:           "int",
			Foreign:        "parent(id)",
			ForeignKeyName: "fk_shared_parent",
		})
	}
	return description
}

func sharedForeignKeyCatalog() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "parent", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "one", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
			{Name: "two", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
	}
}

// emptyCatalogWithoutKeys is [emptyCatalog] with the parent's primary key
// removed, so nothing on either side makes the reference a key.
func emptyCatalogWithoutKeys() *dbschematypes.DBSchema {
	catalog := emptyCatalog()
	catalog.Tables[0].Columns[0].IsPrimaryKey = false
	return catalog
}
