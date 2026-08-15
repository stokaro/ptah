package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestSequenceLookupResolvesAcrossSchemaSpellings pins findSequence at each of
// its three call sites.
//
// The shared objectlookup rows pin objectlookup.Find; they say nothing about
// whether THIS function calls it. Reverting findSequence to the raw
// `QualifiedName() == name` loop left the whole suite green, which is what makes
// these rows necessary rather than decorative: the sequence simply vanishes from
// the plan, and a missing CREATE SEQUENCE is a column DEFAULT that will not
// resolve at apply time.
func TestSequenceLookupResolvesAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name           string
		sequenceSchema string
		diffName       string
	}{
		{
			// Control: both sides already agree, and the plan is what it has
			// always been.
			name:           "both sides spell the sequence the same way",
			sequenceSchema: "",
			diffName:       "order_id_seq",
		},
		{
			name:           "the diff qualifies public and the declaration does not",
			sequenceSchema: "",
			diffName:       "public.order_id_seq",
		},
		{
			name:           "the declaration qualifies public and the diff does not",
			sequenceSchema: "public",
			diffName:       "order_id_seq",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			increment := int64(5)
			generated := &goschema.Database{
				Sequences: []goschema.Sequence{{
					Name:      "order_id_seq",
					Schema:    test.sequenceSchema,
					AsType:    "bigint",
					Increment: &increment,
					OwnedBy:   "orders.id",
				}},
			}

			added, err := planner.GenerateSchemaDiffSQLStatements(
				&types.SchemaDiff{SequencesAdded: []string{test.diffName}},
				generated,
				"postgres",
			)
			c.Assert(err, qt.IsNil)
			addedPlan := strings.Join(added, "\n")
			// addNewSequences and addSequenceOwnership are two separate call
			// sites and both have to resolve, or the sequence is created
			// without the ownership that ties it to its column's lifetime.
			c.Assert(addedPlan, qt.Contains, "CREATE SEQUENCE", qt.Commentf("plan:\n%s", addedPlan))
			c.Assert(addedPlan, qt.Contains, "OWNED BY", qt.Commentf("plan:\n%s", addedPlan))

			modified, err := planner.GenerateSchemaDiffSQLStatements(
				&types.SchemaDiff{SequencesModified: []types.SequenceDiff{{
					SequenceName: test.diffName,
					Changes:      map[string]string{"increment": "1 -> 5"},
				}}},
				generated,
				"postgres",
			)
			c.Assert(err, qt.IsNil)
			modifiedPlan := strings.Join(modified, "\n")
			c.Assert(modifiedPlan, qt.Contains, "ALTER SEQUENCE", qt.Commentf("plan:\n%s", modifiedPlan))
		})
	}
}

// TestSequenceLookupDoesNotGuessBetweenSchemas is the control the widened match
// must not swallow: a sequence declared in one schema is not the sequence a diff
// names in another, and creating it would put the object in the wrong place.
func TestSequenceLookupDoesNotGuessBetweenSchemas(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Sequences: []goschema.Sequence{{Name: "order_id_seq", Schema: "reporting", AsType: "bigint"}},
	}
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		&types.SchemaDiff{SequencesAdded: []string{"app.order_id_seq"}},
		generated,
		"postgres",
	)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "CREATE SEQUENCE")
}

// TestEnumLookupResolvesAcrossSchemaSpellings pins addNewEnums and
// postgresEnumValues, the two enum sites the sweep converted.
//
// Both were still green with the raw `QualifiedName() == enumName` loop back in
// place. The first drops the CREATE TYPE for a new enum, so the columns typed
// against it fail to apply. The second is worse in kind: it does not fail, it
// emits a WARNING comment and plans no value removal at all.
func TestEnumLookupResolvesAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name       string
		enumSchema string
		diffName   string
	}{
		{
			name:       "both sides spell the enum the same way",
			enumSchema: "",
			diffName:   "status",
		},
		{
			name:       "the diff qualifies public and the declaration does not",
			enumSchema: "",
			diffName:   "public.status",
		},
		{
			name:       "the declaration qualifies public and the diff does not",
			enumSchema: "public",
			diffName:   "status",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{
				Enums: []goschema.Enum{{
					Name:   "status",
					Schema: test.enumSchema,
					Values: []string{"draft", "live"},
				}},
			}

			added, err := planner.GenerateSchemaDiffSQLStatements(
				&types.SchemaDiff{EnumsAdded: []string{test.diffName}},
				generated,
				"postgres",
			)
			c.Assert(err, qt.IsNil)
			addedPlan := strings.Join(added, "\n")
			c.Assert(addedPlan, qt.Contains, "CREATE TYPE", qt.Commentf("plan:\n%s", addedPlan))

			removed, err := planner.GenerateSchemaDiffSQLStatements(
				&types.SchemaDiff{EnumsModified: []types.EnumDiff{{
					EnumName:      test.diffName,
					ValuesRemoved: []string{"draft"},
				}}},
				generated,
				"postgres",
			)
			c.Assert(err, qt.IsNil)
			removedPlan := strings.Join(removed, "\n")
			c.Assert(removedPlan, qt.Not(qt.Contains), "the target enum definition was not found",
				qt.Commentf("plan:\n%s", removedPlan))
		})
	}
}

// TestEnumLookupDoesNotGuessBetweenSchemas is the enum control. public.mood and
// extra.mood are two types with different value sets (stokaro/ptah#1276), so a
// lookup that crossed schemas would rebuild one enum from the other's values.
func TestEnumLookupDoesNotGuessBetweenSchemas(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Enums: []goschema.Enum{{Name: "status", Schema: "reporting", Values: []string{"draft", "live"}}},
	}
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		&types.SchemaDiff{EnumsAdded: []string{"app.status"}},
		generated,
		"postgres",
	)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "CREATE TYPE")
}

// TestUserTypeLookupResolvesAcrossSchemaSpellings pins findCompositeType and
// findRange the way the existing symmetry test already pins findDomain.
//
// A modified user type is planned as DROP TYPE followed by CREATE TYPE. Both
// halves go through the same lookup, so the row that matters is the one where
// the two sides spell the schema differently: the pair must still be a pair.
func TestUserTypeLookupResolvesAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name      string
		generated *goschema.Database
		diff      *types.SchemaDiff
	}{
		{
			name: "a composite type qualified in the diff and bare in the schema",
			generated: &goschema.Database{
				CompositeTypes: []goschema.CompositeType{{
					Name: "addr",
					Fields: []goschema.CompositeTypeField{
						{Name: "line1", Type: "text"},
						{Name: "line2", Type: "text"},
					},
				}},
			},
			diff: &types.SchemaDiff{CompositeTypesModified: []types.CompositeTypeDiff{{
				TypeName: "public.addr",
				Changes:  map[string]string{"fields": "line1 text -> line1 text, line2 text"},
			}}},
		},
		{
			name: "a composite type bare in the diff and qualified in the schema",
			generated: &goschema.Database{
				CompositeTypes: []goschema.CompositeType{{
					Name:   "addr",
					Schema: "public",
					Fields: []goschema.CompositeTypeField{
						{Name: "line1", Type: "text"},
						{Name: "line2", Type: "text"},
					},
				}},
			},
			diff: &types.SchemaDiff{CompositeTypesModified: []types.CompositeTypeDiff{{
				TypeName: "addr",
				Changes:  map[string]string{"fields": "line1 text -> line1 text, line2 text"},
			}}},
		},
		{
			name: "a range type qualified in the diff and bare in the schema",
			generated: &goschema.Database{
				Ranges: []goschema.Range{{Name: "span", Subtype: "int8"}},
			},
			diff: &types.SchemaDiff{RangesModified: []types.RangeDiff{{
				RangeName:      "public.span",
				Changes:        map[string]string{"subtype": "int4 -> int8"},
				CurrentSubtype: "int4",
			}}},
		},
		{
			name: "a range type bare in the diff and qualified in the schema",
			generated: &goschema.Database{
				Ranges: []goschema.Range{{Name: "span", Schema: "public", Subtype: "int8"}},
			},
			diff: &types.SchemaDiff{RangesModified: []types.RangeDiff{{
				RangeName:      "span",
				Changes:        map[string]string{"subtype": "int4 -> int8"},
				CurrentSubtype: "int4",
			}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.generated, "postgres")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Contains, "DROP TYPE", qt.Commentf("plan:\n%s", plan))
			c.Assert(plan, qt.Contains, "CREATE TYPE", qt.Commentf("plan:\n%s", plan))
			c.Assert(plan, qt.Not(qt.Contains), "was not found", qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestPlannerWritesNoDDLForARelationTheSchemaDoesNotDeclare is blocker 1: the
// third resolution tier used to answer a name across two DIFFERENT schemas.
//
// The statement is rendered against the name the DIFF carries and the definition
// found under another schema, so the DDL applies cleanly to a relation the
// desired schema never declared. Measured on PostgreSQL 17.10 against a database
// holding both `app.users` and `reporting.users`, `ALTER TABLE "app"."users" ADD
// COLUMN "note" TEXT NOT NULL` exited 0 and information_schema.columns afterwards
// showed the column on `app.users`. The live row in the companion file asserts
// that catalog; these rows assert the plan for every object kind that reaches
// the same tier.
func TestPlannerWritesNoDDLForARelationTheSchemaDoesNotDeclare(t *testing.T) {
	reportingUsers := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users", Schema: "reporting"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "note", Type: "TEXT"},
		},
	}

	tests := []struct {
		name        string
		generated   *goschema.Database
		diff        *types.SchemaDiff
		unwantedSQL string
	}{
		{
			name:      "a column addition on a table declared in another schema",
			generated: reportingUsers,
			diff: &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName:    "app.users",
				ColumnsAdded: []string{"note"},
			}}},
			unwantedSQL: "ADD COLUMN",
		},
		{
			name:      "a column modification on a table declared in another schema",
			generated: reportingUsers,
			diff: &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName: "app.users",
				ColumnsModified: []types.ColumnDiff{{
					ColumnName: "note",
					Changes:    map[string]string{"type": "varchar(10) -> TEXT"},
				}},
			}}},
			unwantedSQL: "ALTER COLUMN",
		},
		{
			name: "a domain modification on a type declared in another schema",
			generated: &goschema.Database{
				Domains: []goschema.Domain{{Name: "zip", Schema: "reporting", BaseType: "VARCHAR(10)"}},
			},
			diff: &types.SchemaDiff{DomainsModified: []types.DomainDiff{{
				DomainName:      "app.zip",
				Changes:         map[string]string{"type": "character varying(5) -> VARCHAR(10)"},
				CurrentBaseType: "character varying(5)",
			}}},
			unwantedSQL: "DROP DOMAIN",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.generated, "postgres")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Not(qt.Contains), test.unwantedSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}
