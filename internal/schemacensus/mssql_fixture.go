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
