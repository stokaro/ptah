package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
)

// twoHostCandidates is two tables, so a declaration that names no host cannot be
// answered by "there is only one table it could mean". With one table in the
// schema the families behave differently again, which is itself part of what
// stokaro/ptah#2612 reports.
func twoHostCandidates() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "A", Name: "a"},
			{StructName: "B", Name: "b"},
		},
		Fields: []schemamodel.Field{
			{StructName: "A", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "B", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
		},
		Roles: []schemamodel.Role{{Name: "reader"}},
		Functions: []schemamodel.Function{{
			Name: "touch", Returns: "trigger", Language: "plpgsql",
			Body: "BEGIN RETURN NEW; END;",
		}},
		Extensions: []schemamodel.Extension{{Name: "timescaledb"}},
	}
}

// hostedSchema returns the two-table schema with one declaration of the named
// family, hosted by table "b" when host is "B" and hosted by nothing when it is
// empty.
func hostedSchema(family, host string) schemamodel.Database {
	schema := twoHostCandidates()
	switch family {
	case "constraint":
		schema.Constraints = []schemamodel.Constraint{{
			StructName: host, Name: "chk", Type: "CHECK", CheckExpression: "id > 0",
		}}
	case "index":
		schema.Indexes = []schemamodel.Index{{
			StructName: host, Name: "idx", Fields: []string{"id"},
		}}
	case "rls enable":
		schema.RLSEnabledTables = []schemamodel.RLSEnabledTable{{StructName: host}}
	case "policy":
		schema.RLSPolicies = []schemamodel.RLSPolicy{{
			StructName: host, Name: "p", PolicyFor: "SELECT",
			ToRoles: "reader", UsingExpression: "true",
		}}
	case "trigger":
		schema.Triggers = []schemamodel.Trigger{{
			StructName: host, Name: "tr", Timing: "BEFORE", Event: "UPDATE",
			ForEach: "ROW", ExecuteFunction: "touch()",
		}}
	case "hypertable":
		schema.Hypertables = []schemamodel.Hypertable{{StructName: host, Column: "id"}}
	}
	return schema
}

// hostedFamilies is every family that reaches a target through a host it names.
//
// The wanted text is the kind the refusal has to name, so a refusal that fired
// for the wrong declaration is a failure rather than a pass.
var hostedFamilies = []struct {
	name string
	kind string
}{
	{name: "constraint", kind: `constraint "chk" names no table`},
	{name: "index", kind: `index "idx" names no table`},
	{name: "rls enable", kind: "a declared row-level security enablement names no table"},
	{name: "policy", kind: `policy "p" names no table`},
	{name: "trigger", kind: `trigger "tr" names no table`},
	{name: "hypertable", kind: "a declared hypertable names no table"},
}

// TestHostlessDeclaration_EveryFamilyIsRefused is stokaro/ptah#2612.
//
// Measured on PostgreSQL before this change, all six at exit 0: the constraint
// was dropped from the render entirely, and the other five rendered against an
// empty identifier: an empty quoted name after ON, after ALTER TABLE, and as
// the create_hypertable argument. No server takes an empty relation name, and a
// constraint that vanishes is the loss the refusal exists to report.
func TestHostlessDeclaration_EveryFamilyIsRefused(t *testing.T) {
	for _, family := range hostedFamilies {
		t.Run(family.name, func(t *testing.T) {
			c := qt.New(t)

			schema := hostedSchema(family.name, "")
			statements, err := renderer.GetOrderedCreateStatements(&schema, "postgres")

			c.Assert(err, qt.IsNotNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err.Error(), qt.Contains, family.kind)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestHostlessDeclaration_ANamedHostStillRenders is the acceptance control: a
// refusal that fired for every declaration would pass the test above.
func TestHostlessDeclaration_ANamedHostStillRenders(t *testing.T) {
	for _, family := range hostedFamilies {
		t.Run(family.name, func(t *testing.T) {
			c := qt.New(t)

			schema := hostedSchema(family.name, "B")
			statements, err := renderer.GetOrderedCreateStatements(&schema, "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(len(statements) > 0, qt.IsTrue)
		})
	}
}

// TestHostlessDeclaration_AnExtendedPropertyNeedsNoTable is the deliberate
// exclusion, and it is a test rather than a sentence because the list of
// families is the whole content of the check.
//
// SQL Server takes an extended property at database scope, so
// `EXEC sp_addextendedproperty @name = N'MS_Description'` naming no table is a
// complete statement rather than a loss.
func TestHostlessDeclaration_AnExtendedPropertyNeedsNoTable(t *testing.T) {
	c := qt.New(t)

	schema := twoHostCandidates()
	schema.ExtendedProperties = []schemamodel.ExtendedProperty{{
		Name: "MS_Description", Value: "the database",
	}}

	statements, err := renderer.GetOrderedCreateStatements(&schema, "sqlserver")

	c.Assert(err, qt.IsNil)
	c.Assert(joinStatements(statements), qt.Contains, "sp_addextendedproperty")
}

// joinStatements is the rendered script as one string.
func joinStatements(statements []string) string {
	joined := ""
	for _, statement := range statements {
		joined += statement + "\n"
	}
	return joined
}
