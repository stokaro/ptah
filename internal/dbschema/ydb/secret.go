package ydb

import (
	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
)

// secret observes the secret name in the directory schema by its path alone:
// the listing names it, and no request the reader sends returns its value.
func (r *Reader) secret(schema, name string, db *catalog.Database) error {
	if !r.inScope(schema) {
		return nil
	}
	var err error
	db.FeatureObjects, err = db.FeatureObjects.With(ydbsecret.ObservedObject(schema, name))
	return err
}

// unmanagedSecrets turns, on a server without [capability.Secrets], every
// secret the walk listed into an uninspected record, in one pass after the
// walk: Ptah plans no secret statement there, so a listed secret is neither
// kept nor dropped, and its silence never reads as absence.
func (r *Reader) unmanagedSecrets(db *catalog.Database) error {
	if r.caps.Has(capability.Secrets) {
		return nil
	}
	isSecret := func(ref objectidentity.ID) bool { return ref.Kind == objectidentity.Kind(ydbsecret.Kind) }
	listed := db.FeatureObjects.Select(isSecret).Refs()
	if len(listed) == 0 {
		return nil
	}
	records := db.FeatureCoverage.SubjectRecords()
	for _, ref := range listed {
		records = append(records, schemaext.SubjectCoverage{Kind: ydbsecret.Kind, Subject: ref,
			Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbsecret.UnsupportedReason}})
	}
	known, err := schemaext.NewCoverage(schemaext.Observed, db.FeatureCoverage.KindRecords(), records)
	if err != nil {
		return err
	}
	db.FeatureCoverage = known
	db.FeatureObjects = db.FeatureObjects.Select(func(ref objectidentity.ID) bool { return !isSecret(ref) })
	return nil
}
