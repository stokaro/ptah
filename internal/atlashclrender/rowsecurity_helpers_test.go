package atlashclrender_test

import (
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
