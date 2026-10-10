package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_CarriesASecretAsADeclarationOfItsDefaultVariable holds a secret
// a YDB read found to a declaration of the same path. The read holds no value
// and names no variable, so the declaration names none either, which selects
// the default variable for its path: a document written from the read declares
// every secret, and applying it back plans nothing for them. The read's claim
// to have listed every secret carries over.
func TestConvert_CarriesASecretAsADeclarationOfItsDefaultVariable(t *testing.T) {
	c := qt.New(t)
	read := &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbsecret.ObservedObject("", "pg_password"), ydbsecret.ObservedObject("ext/aws", "s3-key"))),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}

	converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), read, "ydb", must.Must(builtin.New())))

	objects, err := converted.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.ContentEquals, []schemaext.Object{
		{Ref: ydbsecret.Ref("", "pg_password"), Value: &ydbsecret.Desired{}},
		{Ref: ydbsecret.Ref("ext/aws", "s3-key"), Value: &ydbsecret.Desired{}},
	})
	c.Assert(converted.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("", "other")).State, qt.Equals, schemaext.Complete)
}
