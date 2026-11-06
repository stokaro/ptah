package atlasschema

import (
	"net/url"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/devclean"
	"ptah.run/internal/schemafile"
)

// omitRealmRootGrants leaves the permissions on the database root out of a
// baseline that rebuilds the target in a dev realm under that root, on both
// sides of the comparison the baseline is planned from.
//
// The realm's root would stand in for the target's root, but a reset in the
// middle of a run removes what is under the realm and keeps the realm's root
// with its permissions, and nothing the cleanup reads can see a permission. A
// grant written there would outlive the target it came from and reach the
// next one rehearsed in the realm, so the baseline writes none, and a
// permission an earlier run left on the root plans no REVOKE either. Every
// other target is left as it is.
//
// A database root is what an engine has whose root reader
// [schemafile.RootReader] returns, and the dev realm is the one that reader
// reads from the dev connection's URL. On an engine an executable provider
// serves, the dev connection names a realm when the database its session
// works in is not the root its authored references are read against
// (schemaext.DatabaseContext). A URL whose realm cannot be read omits nothing.
func omitRealmRootGrants(info catalog.ServerInfo, target *schemamodel.Database, dev *catalog.Database) {
	if !namesDevRealm(info) {
		return
	}
	keptDev, keptTarget := devclean.WithoutDatabaseGrants(dev, target)
	if target != nil {
		target.Grants, target.RevokedGrants = keptTarget.Grants, keptTarget.RevokedGrants
	}
	if dev != nil {
		dev.Grants = keptDev.Grants
	}
}

// namesDevRealm reports whether the URL of info names a dev realm under its
// database root, as the dialect's root reader reads it. A dialect with no
// root names no realm.
func namesDevRealm(info catalog.ServerInfo) bool {
	if bound := info.DatabaseContext; !bound.IsZero() {
		return bound.Database != bound.SourceDatabase
	}
	reader, err := schemafile.RootReader(info.Dialect)
	if err != nil || reader == nil {
		return false
	}
	parsed, err := url.Parse(info.URL)
	if err != nil {
		return false
	}
	root, err := reader.Root(parsed)
	return err == nil && root.Realm != ""
}
