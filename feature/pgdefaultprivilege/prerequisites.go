package pgdefaultprivilege

import "ptah.run/core/objectidentity"

// PlacementSchema names the explicitly declared IN SCHEMA scope. The global
// scope contributes no schema and never defaults to public.
func (*Desired) PlacementSchema(subject objectidentity.ID) string { return subject.Schema.Source }

// PlacementSchema names a schema needed by a grant or a restoration. A removal
// alone addresses a schema the inspected ACL already establishes.
func (v *Change) PlacementSchema(subject objectidentity.ID) string {
	if v == nil || v.Restore == nil && len(v.Granted)+len(v.GrantOptionsAdded) == 0 {
		return ""
	}
	return subject.Schema.Source
}
