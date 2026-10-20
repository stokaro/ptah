package generator_test

import (
	"slices"

	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
)

// declaringExtensions is a desired schema that declares each name to the
// extension owner, as a Go //ptah:schema:extension annotation does, and
// claims that it declares every extension.
func declaringExtensions(names ...string) *schemamodel.Database {
	objects := make([]schemaext.Object, 0, len(names))
	for _, name := range names {
		objects = append(objects, must.Must(pgextension.DesiredObject(pgextension.Ref(name), pgextension.DesiredExtension{IfNotExists: true})))
	}
	return &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
}

// installedExtension is one extension a PostgreSQL read reports.
type installedExtension struct{ name, schema, version string }

// holdingExtensions is a read that holds each extension, as the PostgreSQL
// reader hands them to the extension owner, and describes every extension.
func holdingExtensions(installed ...installedExtension) *catalog.Database {
	objects := make([]schemaext.Object, 0, len(installed))
	for _, extension := range installed {
		objects = append(objects, must.Must(pgextension.ObservedObject(pgextension.Ref(extension.name),
			pgextension.ObservedExtension{Schema: extension.schema, Version: extension.version})))
	}
	return &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
}

// extensionChanges names the extensions a diff installs and drops, from the
// extension owner's changes, each list sorted.
func extensionChanges(diff *difftypes.SchemaDiff) (added, removed []string) {
	for _, record := range diff.FeatureChanges {
		change, ok := record.Value.(*pgextension.Change)
		switch {
		case !ok:
		case change.Before == nil:
			added = append(added, pgextension.Name(record.Subject))
		case change.After == nil:
			removed = append(removed, pgextension.Name(record.Subject))
		}
	}
	slices.Sort(added)
	slices.Sort(removed)
	return added, removed
}
