package postgres

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// readExtensionObjects reports the installed extensions to the PostgreSQL
// extension owner, each as an observed object, and claims complete knowledge
// of them: the read asked pg_extension for every one, so an extension it does
// not report is not installed.
func (r *Reader) readExtensionObjects(ctx context.Context, schema *catalog.Database) error {
	extensions, err := r.readExtensions(ctx)
	if err != nil {
		return err
	}
	for _, extension := range extensions {
		object, err := pgextension.ObservedObject(pgextension.Ref(extension.name), extension.observed)
		if err != nil {
			return err
		}
		if schema.FeatureObjects, err = schema.FeatureObjects.With(object); err != nil {
			return err
		}
	}
	known, err := pgextension.CompleteCoverage(schemaext.Observed)
	if err != nil {
		return err
	}
	schema.FeatureCoverage, err = schema.FeatureCoverage.Combine(known)
	return err
}
