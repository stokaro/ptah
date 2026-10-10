package schemafile_test

import (
	"maps"
	"slices"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
)

// ownerPolicies returns the policies a parsed document hands the row-security
// owner, keyed by schema, table and name as the document spells them.
func ownerPolicies(c *qt.C, db *schemamodel.Database) map[string]pgpolicy.DesiredPolicy {
	c.Helper()
	all, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	policies := make(map[string]pgpolicy.DesiredPolicy)
	for _, object := range all {
		if policy, ok := object.Value.(*pgpolicy.DesiredPolicy); ok {
			policies[object.Ref.Schema.Source+"."+object.Ref.Parent.Source+"."+object.Ref.Name.Source] = *policy
		}
	}
	return policies
}

// ownerSwitches returns the row-security switches each table of a parsed
// document declares, keyed by the table's qualified name.
func ownerSwitches(c *qt.C, db *schemamodel.Database) map[string]pgpolicy.DesiredTableState {
	c.Helper()
	switches := make(map[string]pgpolicy.DesiredTableState)
	for _, table := range db.Tables {
		value, found, err := table.Facets.Get(pgpolicy.TableStateKind)
		c.Assert(err, qt.IsNil)
		if found {
			switches[table.QualifiedName()] = *value.(*pgpolicy.DesiredTableState)
		}
	}
	return switches
}

// rowSecurityNames is where a parsed file records its one enablement and its
// one policy: the shared model where the dialect keeps them there, and the
// PostgreSQL row-security owner where it hands them over.
func rowSecurityNames(c *qt.C, db *schemamodel.Database) (enabled, policy, policyTable string) {
	c.Helper()
	if len(db.RLSEnabledTables) == 1 && len(db.RLSPolicies) == 1 {
		return db.RLSEnabledTables[0].Table, db.RLSPolicies[0].Name, db.RLSPolicies[0].Table
	}
	switches := slices.Collect(maps.Keys(ownerSwitches(c, db)))
	refs := db.FeatureObjects.Refs()
	c.Assert(switches, qt.HasLen, 1)
	c.Assert(refs, qt.HasLen, 1)
	return switches[0], refs[0].Name.Source, refs[0].Parent.Source
}
