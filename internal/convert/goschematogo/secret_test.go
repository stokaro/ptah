package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRenderSecretsRoundTripThroughParser writes each YDB secret as the
// annotation the parser reads back as the same secret, its path and the
// variable its value comes from, on a holder struct of its own: a database
// holding nothing but secrets still gets the struct the comments attach to.
func TestRenderSecretsRoundTripThroughParser(t *testing.T) {
	c := qt.New(t)
	secrets := []schemamodel.Secret{
		{Name: "pg_password", ValueEnv: "PTAH_SECRET_PG_PASSWORD"},
		{Name: "s3.key", Schema: "ext/aws", ValueEnv: "PTAH_SECRET_EXT_AWS_S3_KEY"},
	}
	files, err := goschematogo.Render(&schemamodel.Database{Secrets: secrets},
		goschematogo.Options{PackageName: "models", SingleFile: true})
	c.Assert(err, qt.IsNil)
	dir := t.TempDir()
	c.Assert(goschematogo.WriteDir(dir, files), qt.IsNil)

	parsed, err := goschema.ParseDir(dir)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Secrets, qt.DeepEquals, []schemamodel.Secret{
		{StructName: "PtahSchemaObjects", Name: "s3.key", Schema: "ext/aws", ValueEnv: "PTAH_SECRET_EXT_AWS_S3_KEY"},
		{StructName: "PtahSchemaObjects", Name: "pg_password", ValueEnv: "PTAH_SECRET_PG_PASSWORD"},
	})
}
