package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/internal/convert/goschematogo"
)

// TestRenderSecretsRoundTripThroughParser writes each YDB secret as the
// annotation the parser reads back as the same secret, its path and the
// variable its value comes from, on a holder struct of its own: a database
// holding nothing but secrets still gets the struct the comments attach to. A
// secret that names no variable is written with the default one for its
// path, which is the one it selects.
func TestRenderSecretsRoundTripThroughParser(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbsecret.DesiredObject("", "pg_password", "", "PTAH_SECRET_PG"),
			schemaext.Object{Ref: ydbsecret.Ref("ext/aws", "s3.key"), Value: &ydbsecret.Desired{}},
		)),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	files, err := goschematogo.Render(c.Context(), desired, goschematogo.Options{PackageName: "models", SingleFile: true})
	c.Assert(err, qt.IsNil)
	dir := t.TempDir()
	c.Assert(goschematogo.WriteDir(dir, files), qt.IsNil)

	parsed, err := goschema.ParseDir(dir)

	c.Assert(err, qt.IsNil)
	objects, err := parsed.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.ContentEquals, []schemaext.Object{
		ydbsecret.DesiredObject("", "pg_password", "PtahSchemaObjects", "PTAH_SECRET_PG"),
		ydbsecret.DesiredObject("ext/aws", "s3.key", "PtahSchemaObjects", "PTAH_SECRET_EXT_AWS_S3_KEY"),
	})
}

// TestRenderSecrets_RefusesARotationRequest refuses a declaration carrying a
// rotation request rather than writing it without one: the request belongs to
// one invocation and has no annotation.
func TestRenderSecrets_RefusesARotationRequest(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: ydbsecret.Ref("ext", "pw"), Value: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW", Rotate: true}}))}

	files, err := goschematogo.Render(c.Context(), desired, goschematogo.Options{PackageName: "models", SingleFile: true})

	c.Assert(err, qt.ErrorMatches, ".*Go annotations cannot preserve the rotation request on secret ext/pw")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(files, qt.IsNil)
}
