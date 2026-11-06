package devclean

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

// databaseGrantObject is the object type a catalog grant on the database
// itself carries.
const databaseGrantObject = "DATABASE"

// WithoutDatabaseGrants returns current and desired without their permissions
// on the database root: the grants of current whose object is the database,
// and the grants and revoked grants of desired that are on the database. Each
// is a shallow copy with new grant lists, so the caller's values are not
// changed, and a nil side comes back nil.
//
// A dev realm is a directory under a database root, and the root's
// permissions are the database's: the realm's root does not stand in for
// them. A comparison that recreates a database in a realm of it, or compares
// the two, leaves them out of both sides. Kept, a realm would revoke the
// database's root permissions, and the database would grant them on the
// realm's root, which a realm made for one run no longer names after it.
func WithoutDatabaseGrants(current *catalog.Database, desired *schemamodel.Database) (*catalog.Database, *schemamodel.Database) {
	if current != nil {
		kept := *current
		kept.Grants = slices.DeleteFunc(slices.Clone(current.Grants), func(grant catalog.Grant) bool {
			return grant.ObjectType == databaseGrantObject
		})
		current = &kept
	}
	if desired != nil {
		onDatabase := func(grant schemamodel.Grant) bool { return grant.OnDatabase }
		kept := *desired
		kept.Grants = slices.DeleteFunc(slices.Clone(desired.Grants), onDatabase)
		kept.RevokedGrants = slices.DeleteFunc(slices.Clone(desired.RevokedGrants), onDatabase)
		desired = &kept
	}
	return current, desired
}
