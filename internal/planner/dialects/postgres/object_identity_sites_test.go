package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
// The ADD path is no longer part of this: an added sequence carries its own
// definition (stokaro/ptah#2315), so there is no name to resolve and no
// spelling for the two sides to disagree about. What remains here is the
// MODIFIED path, which still names a sequence and still has to find it.
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
			desired := &schemamodel.Database{
				Sequences: []schemamodel.Sequence{{
					Name:      "order_id_seq",
					Schema:    test.sequenceSchema,
					AsType:    "bigint",
					Increment: &increment,
					OwnedBy:   "orders.id",
				}},
			}

			modified, err := planner.GenerateSchemaDiffSQLStatements(
				&difftypes.SchemaDiff{SequencesModified: []difftypes.SequenceDiff{{
					SequenceName: test.diffName,
					Changes:      map[string]string{"increment": "1 -> 5"},
				}}},
				desired,
				"postgres",
			)
			c.Assert(err, qt.IsNil)
			modifiedPlan := strings.Join(modified, "\n")
			c.Assert(modifiedPlan, qt.Contains, "ALTER SEQUENCE", qt.Commentf("plan:\n%s", modifiedPlan))
		})
	}
}

// TestSequenceAdditionFollowsItsOperand replaces the control that asked the
// lookup not to guess between schemas.
//
// That control existed because an addition was a NAME, and a name resolved
// against the wrong declaration would create the object in the wrong schema. It
// refused by finding nothing. An addition now carries the sequence itself
// (stokaro/ptah#2315), so there is nothing to resolve and nothing to guess: the
// object lands where its operand says, and a desired schema that disagrees
// cannot move it.
//
// This is the stronger property, and it is the one the change is FOR. The
// scenario the old control described -- a diff naming a sequence the desired
// schema does not declare -- is unreachable through the comparator, which
// builds the operand from that same schema.
func TestSequenceAdditionFollowsItsOperand(t *testing.T) {
	c := qt.New(t)

	// The desired schema deliberately declares a DIFFERENT sequence, to prove
	// the plan no longer reads placement out of it.
	desired := &schemamodel.Database{
		Sequences: []schemamodel.Sequence{{Name: "order_id_seq", Schema: "reporting", AsType: "bigint"}},
	}
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		&difftypes.SchemaDiff{SequencesAdded: difftypes.SequenceChanges{
			{Name: "order_id_seq", Schema: "app", AsType: "bigint"},
		}},
		desired,
		"postgres",
	)
	c.Assert(err, qt.IsNil)
	plan := strings.Join(statements, "\n")
	c.Assert(plan, qt.Contains, "CREATE SEQUENCE", qt.Commentf("plan:\n%s", plan))
	c.Assert(plan, qt.Contains, "app", qt.Commentf("the operand's schema decides placement:\n%s", plan))
	c.Assert(plan, qt.Not(qt.Contains), "reporting",
		qt.Commentf("the desired schema's spelling must not reach the plan:\n%s", plan))
}

// TestEnumLookupResolvesAcrossSchemaSpellings pins addNewEnums and
// postgresEnumValues, the two enum sites the sweep converted.
//
// Both were still green with the raw `QualifiedName() == enumName` loop back in
// place. The first drops the CREATE TYPE for a new enum, so the columns typed
// against it fail to apply. The second is worse in kind: it does not fail, it
// emits a WARNING comment and plans no value removal at all.
// The ADD path is no longer part of this: an added enum carries its own values
// (stokaro/ptah#2315), so there is no name to resolve. What remains is the
// MODIFIED path, which still names an enum and still has to find it.
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
			desired := &schemamodel.Database{
				Enums: []schemamodel.Enum{{
					Name:   "status",
					Schema: test.enumSchema,
					Values: []string{"draft", "live"},
				}},
			}

			removed, err := planner.GenerateSchemaDiffSQLStatements(
				&difftypes.SchemaDiff{EnumsModified: []difftypes.EnumDiff{{
					EnumName:      test.diffName,
					ValuesRemoved: []string{"draft"},
				}}},
				desired,
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
// TestEnumAdditionFollowsItsOperand replaces the control that asked the lookup
// not to guess between schemas, for the reason
// TestSequenceAdditionFollowsItsOperand gives: an addition carries the enum, so
// there is nothing to resolve and nothing to guess.
func TestEnumAdditionFollowsItsOperand(t *testing.T) {
	c := qt.New(t)

	// The desired schema deliberately declares the enum in a DIFFERENT schema,
	// to prove the plan no longer reads placement out of it.
	desired := &schemamodel.Database{
		Enums: []schemamodel.Enum{{Name: "status", Schema: "reporting", Values: []string{"draft", "live"}}},
	}
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		&difftypes.SchemaDiff{EnumsAdded: difftypes.EnumChanges{
			{Name: "status", Schema: "app", Values: []string{"draft", "live"}},
		}},
		desired,
		"postgres",
	)
	c.Assert(err, qt.IsNil)
	plan := strings.Join(statements, "\n")
	c.Assert(plan, qt.Contains, "CREATE TYPE", qt.Commentf("plan:\n%s", plan))
	c.Assert(plan, qt.Contains, "app", qt.Commentf("the operand's schema decides placement:\n%s", plan))
	c.Assert(plan, qt.Not(qt.Contains), "reporting",
		qt.Commentf("the desired schema's spelling must not reach the plan:\n%s", plan))
}

// TestUserTypeLookupResolvesAcrossSchemaSpellings pins findCompositeType and
// findRange the way the existing symmetry test already pins findDomain.
//
// A modified user type is planned as DROP TYPE followed by CREATE TYPE. Both
// halves go through the same lookup, so the row that matters is the one where
// the two sides spell the schema differently: the pair must still be a pair.
func TestUserTypeLookupResolvesAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		diff    *difftypes.SchemaDiff
	}{
		{
			name: "a composite type qualified in the diff and bare in the schema",
			desired: &schemamodel.Database{
				CompositeTypes: []schemamodel.CompositeType{{
					Name: "addr",
					Fields: []schemamodel.CompositeField{
						{Name: "line1", Type: "text"},
						{Name: "line2", Type: "text"},
					},
				}},
			},
			diff: &difftypes.SchemaDiff{CompositeTypesModified: []difftypes.CompositeTypeDiff{{
				TypeName: "public.addr",
				Changes:  map[string]string{"fields": "line1 text -> line1 text, line2 text"},
			}}},
		},
		{
			name: "a composite type bare in the diff and qualified in the schema",
			desired: &schemamodel.Database{
				CompositeTypes: []schemamodel.CompositeType{{
					Name:   "addr",
					Schema: "public",
					Fields: []schemamodel.CompositeField{
						{Name: "line1", Type: "text"},
						{Name: "line2", Type: "text"},
					},
				}},
			},
			diff: &difftypes.SchemaDiff{CompositeTypesModified: []difftypes.CompositeTypeDiff{{
				TypeName: "addr",
				Changes:  map[string]string{"fields": "line1 text -> line1 text, line2 text"},
			}}},
		},
		{
			name: "a range type qualified in the diff and bare in the schema",
			desired: &schemamodel.Database{
				Ranges: []schemamodel.Range{{Name: "span", Subtype: "int8"}},
			},
			diff: &difftypes.SchemaDiff{RangesModified: []difftypes.RangeDiff{{
				RangeName:      "public.span",
				Changes:        map[string]string{"subtype": "int4 -> int8"},
				CurrentSubtype: "int4",
			}}},
		},
		{
			name: "a range type bare in the diff and qualified in the schema",
			desired: &schemamodel.Database{
				Ranges: []schemamodel.Range{{Name: "span", Schema: "public", Subtype: "int8"}},
			},
			diff: &difftypes.SchemaDiff{RangesModified: []difftypes.RangeDiff{{
				RangeName:      "span",
				Changes:        map[string]string{"subtype": "int4 -> int8"},
				CurrentSubtype: "int4",
			}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.desired, "postgres")
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
	reportingUsers := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users", Schema: "reporting"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "note", Type: "TEXT"},
		},
	}

	tests := []struct {
		name        string
		desired     *schemamodel.Database
		diff        *difftypes.SchemaDiff
		unwantedSQL string
	}{
		{
			name:    "a column addition on a table declared in another schema",
			desired: reportingUsers,
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:    "app.users",
				ColumnsAdded: []string{"note"},
			}}},
			unwantedSQL: "ADD COLUMN",
		},
		{
			name:    "a column modification on a table declared in another schema",
			desired: reportingUsers,
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName: "app.users",
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName: "note",
					Changes:    map[string]string{"type": "varchar(10) -> TEXT"},
				}},
			}}},
			unwantedSQL: "ALTER COLUMN",
		},
		{
			name: "a domain modification on a type declared in another schema",
			desired: &schemamodel.Database{
				Domains: []schemamodel.Domain{{Name: "zip", Schema: "reporting", BaseType: "VARCHAR(10)"}},
			},
			diff: &difftypes.SchemaDiff{DomainsModified: []difftypes.DomainDiff{{
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
			statements, err := planner.GenerateSchemaDiffSQLStatements(test.diff, test.desired, "postgres")
			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Not(qt.Contains), test.unwantedSQL, qt.Commentf("plan:\n%s", plan))
		})
	}
}
