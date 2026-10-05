package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_CarriesASecretAsADeclarationNamingItsDefaultVariable holds a
// secret a YDB read found to a declaration of the same path. The read holds no
// value and names no variable, so the declaration names the one its path
// gives: a document written from the read declares every secret, and applying
// it back plans nothing for them.
func TestConvert_CarriesASecretAsADeclarationNamingItsDefaultVariable(t *testing.T) {
	c := qt.New(t)

	converted := dbschematogo.ConvertDBSchemaToGoSchema(&catalog.Database{
		Secrets: []catalog.Secret{{Name: "pg_password"}, {Name: "s3-key", Schema: "ext/aws"}},
	}, "ydb")

	c.Assert(converted.Secrets, qt.DeepEquals, []schemamodel.Secret{
		{Name: "pg_password", ValueEnv: "PTAH_SECRET_PG_PASSWORD"},
		{Name: "s3-key", Schema: "ext/aws", ValueEnv: "PTAH_SECRET_EXT_AWS_S3_KEY"},
	})
}
