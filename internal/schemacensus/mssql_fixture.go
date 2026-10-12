package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlschema"
)

// securityPolicyFixture declares a SQL Server security policy with every value
// away from its default, so removing any of them changes what is rendered: a
// disabled policy without schema binding, left out of replication, with a
// filter and a block predicate for one operation.
func securityPolicyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	tenant, table := mssqlschema.ObjectName{Schema: "rls", Name: "fn_tenant"}, mssqlschema.ObjectName{Schema: "dbo", Name: "t"}
	db.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(mssqlschema.DesiredSecurityPolicyObject(
		mssqlschema.SecurityPolicyRef("rls", "tenancy"), mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{
				{Type: mssqlschema.Filter, Function: tenant, Arguments: []string{"id"}, Table: table},
				{Type: mssqlschema.Block, Function: tenant, Arguments: []string{"id"}, Table: table, Operation: mssqlschema.AfterInsert},
			},
			Enabled: new(false), SchemaBinding: new(false), NotForReplication: true, StructName: "SP",
		}))))
	db.FeatureCoverage = must.Must(mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

// routineOptionsFixture declares a SQL Server function that states every
// option of the routine options facet, so removing one changes what is
// rendered: EXECUTE AS OWNER in place of EXECUTE AS N'auditor', and a WITH list
// without the option. RECOMPILE is a procedure's option, which the server
// refuses on a function; the census only renders.
func routineOptionsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Functions = []schemamodel.Function{{
		StructName: "F", Name: "dbo.audit_it", Returns: "int", Language: "sql", Security: "DEFINER",
		Body: "BEGIN RETURN 1; END",
		Facets: must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{
			Principal: "auditor", SchemaBinding: true, Recompile: true, ReturnsNullOnNullInput: true,
		})),
	}}
	db.FeatureCoverage = must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}
