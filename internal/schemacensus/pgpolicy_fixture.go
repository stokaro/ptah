package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
)

// tablePolicyFixture declares a restrictive UPDATE policy on a table with both
// clauses, a named role beside a role keyword, and a comment, every declared
// field set, so the census measures each one. The normalized spelling is the
// server's, which a live comparison attaches; a statement writes the
// declaration, which is why ablating it moves nothing.
func tablePolicyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "t", "tenant_rows"),
		pgpolicy.DesiredPolicy{
			Command: pgpolicy.CommandUpdate, Roles: []pgpolicy.RoleSelector{{Name: "reader"}, {Keyword: pgpolicy.CurrentUser}},
			Using: new("id > 0"), WithCheck: new("id < 100"), Composition: pgpolicy.Restrictive, Comment: "tenant rows", StructName: "T",
			Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{{Name: "app"}, {Name: "reader"}},
				Using: new("(id > 0)"), WithCheck: new("(id < 100)")},
		}))))
	db.FeatureCoverage = must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))
	return db
}

// tableRowSecurityFixture enables and forces row security on a table, with the
// comment written before the statements, so the census measures each switch.
func tableRowSecurityFixture() schemamodel.Database {
	state := &pgpolicy.DesiredTableState{Enabled: true, Forced: true, Comment: "tenant isolation", StructName: "T"}
	db := oneTable("T", schemamodel.Table{Name: "t", Facets: must.Must(schemaext.NewFacets(state))})
	db.FeatureCoverage = must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))
	return db
}
