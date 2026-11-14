package atlasfilter

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
)

// A whole-kind policy exclusion also leaves the row-security switches to the
// caller. They belong to the same provider, but live as table facets rather
// than policy objects. Keeping them would plan DISABLE ROW LEVEL SECURITY
// against a source that deliberately leaves policies out.
func (s *exclusionState) excludesAllPolicies() bool {
	for _, pattern := range s.patterns {
		if pattern.glob != "" && strings.Trim(pattern.glob, "*.") == "" && len(pattern.fields) == 0 &&
			pattern.matches("policy", "table.object", "schema.table.object") {
			return true
		}
	}
	return false
}

func (s *exclusionState) excludeObservedRowSecurity(tables []catalog.Table) {
	if !s.excludesAllPolicies() {
		return
	}
	for i := range tables {
		tables[i].RLSEnabled = false
		tables[i].RLSForced = false
		tables[i].Facets = tables[i].Facets.Without(pgpolicy.TableStateKind)
	}
}

func (s *exclusionState) excludeDesiredRowSecurity(schema *schemamodel.Database) {
	if !s.excludesAllPolicies() {
		return
	}
	for i := range schema.Tables {
		schema.Tables[i].Facets = schema.Tables[i].Facets.Without(pgpolicy.TableStateKind)
	}
	schema.RLSEnabledTables = nil
}
