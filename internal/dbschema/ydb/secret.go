package ydb

import (
	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
)

// secret observes the secret name in the directory schema by its path alone:
// the listing names it, and no request the reader sends returns its value. On
// a server without [capability.Secrets] the secret is recorded as unread, so
// its silence never reads as absence.
func (r *Reader) secret(schema, name string, db *catalog.Database) error {
	if !r.inScope(schema) {
		return nil
	}
	if !r.caps.Has(capability.Secrets) {
		records := append(db.FeatureCoverage.SubjectRecords(), schemaext.SubjectCoverage{
			Kind: ydbsecret.Kind, Subject: ydbsecret.Ref(schema, name),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "target capability secrets is unavailable"},
		})
		known, err := schemaext.NewCoverage(schemaext.Observed, db.FeatureCoverage.KindRecords(), records)
		if err != nil {
			return err
		}
		db.FeatureCoverage = known
		return nil
	}
	var err error
	db.FeatureObjects, err = db.FeatureObjects.With(ydbsecret.ObservedObject(schema, name))
	return err
}
