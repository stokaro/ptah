package schemaext

import "ptah.run/core/objectidentity"

// HasTargetScopes reports whether any object carries a source binding.
func (o Objects) HasTargetScopes() bool {
	for _, object := range o.values {
		if len(object.Targets) != 0 {
			return true
		}
	}
	return false
}

// ForTarget projects the collection onto target without consulting the
// objects' codecs: an object whose binding excludes target is absent from the
// result, and every other object keeps its binding. An unresolved target is an
// error.
//
// Unlike [Facets.ForTarget], the result keeps no record of an exclusion. An
// object is named, so a caller that must not read its absence as deletion
// intent names the excluded objects before projecting, as
// schemamodel.OmissionsForTarget does. The collection shares only immutable
// private snapshots.
func (o Objects) ForTarget(target TargetSelection) (Objects, error) {
	if err := target.Validate(); err != nil {
		return Objects{}, err
	}
	return o.Select(func(ref objectidentity.ID) bool {
		return target.Includes(o.values[ref.Key()].Targets)
	}), nil
}
