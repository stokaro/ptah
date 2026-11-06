package devclean

import (
	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// WithoutUnreadObjects returns target, the desired schema a rehearsal
// recreates on a dev database, without the feature objects the dev
// database's read makes no claim about, and without the coverage records of
// their subjects. A nil target or read comes back as it is, and target is not
// changed.
//
// Such an object cannot be compared on the dev database: the comparison
// reports it undecided, and the rehearsal stops before its plan runs, though
// the plan may not touch the object. A YDB dev realm is the case: the
// database's resource pools belong to the database the realm is in, the
// target's own, so the realm reads none, and every target on a server with
// resource pools holds the pool default. Leaving such an object out lets no
// change through unrehearsed. A plan that changes it is planned again for the
// dev database, which reads the object as unknown there too and plans no
// change to it, so the two plans differ and the rehearsal refuses the apply.
func WithoutUnreadObjects(target *schemamodel.Database, read *catalog.Database) *schemamodel.Database {
	if target == nil || read == nil {
		return target
	}
	unread := make(map[objectidentity.Key]bool)
	for _, ref := range target.FeatureObjects.Refs() {
		if read.FeatureCoverage.Lookup(schemaext.Kind(ref.Kind), ref).State == schemaext.Uninspected {
			unread[ref.Key()] = true
		}
	}
	if len(unread) == 0 {
		return target
	}
	kept := func(ref objectidentity.ID) bool { return !unread[ref.Key()] }
	result := *target
	result.FeatureObjects = target.FeatureObjects.Select(kept)
	result.FeatureCoverage = target.FeatureCoverage.SelectSubjects(kept)
	return &result
}
