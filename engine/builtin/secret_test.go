package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// secretlessTargets are the targets of every engine but YDB, which has the one
// secret Ptah models.
var secretlessTargets = []struct {
	dialect string
	caps    capability.Capabilities
}{
	{dialect: platform.Postgres, caps: capability.Postgres18()},
	{dialect: platform.MySQL, caps: capability.MySQL84()},
	{dialect: platform.MariaDB, caps: capability.MariaDB1011()},
	{dialect: platform.SQLite, caps: capability.SQLite3()},
	{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
	{dialect: platform.SQLServer, caps: capability.SQLServer2022()},
	{dialect: platform.Oracle, caps: capability.Oracle23()},
}

// secretSchema declares one secret beside a table.
func secretSchema(schema, name string) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "T", Name: "notes", Schema: "app"}},
		Fields:          []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbsecret.DesiredObject(schema, name, "", "PTAH_SECRET_PW"))),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	schemamodel.Finalize(database)
	return database
}

// TestRender_Secret_HappyPath writes a declared secret through its owner
// ahead of every other statement, with its value the reference to its
// variable, on every whole-schema entry point.
func TestRender_Secret_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := secretSchema("ext", "pw")

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(database, platform.YDB, capability.YDB262())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n",
		"CREATE TABLE `app/notes` (\n    `id` Int64 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n",
	})
	assertOwnedSchemaEntryPoints(c, database, capability.YDB262(), "CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);")
}

// TestRender_Secret_FailurePath refuses a declared secret on every target
// without a selected, enabled owner: built as nothing, the declaration would
// report a secret created that the target does not hold, and an external data
// source naming it would fail at its first read. A secret whose path a
// declared table holds is refused too, since YDB keeps one object at a path.
func TestRender_Secret_FailurePath(t *testing.T) {
	type target struct {
		name     string
		dialect  string
		caps     capability.Capabilities
		database *schemamodel.Database
		wantErr  string
		wantIs   error
	}
	tests := []target{
		{name: "ydb 25.1", dialect: platform.YDB, caps: capability.YDB251(), database: secretSchema("ext", "pw"),
			wantErr: `secret ext/pw, which requires target capability secrets, unavailable on this ydb target`, wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "a table's path", dialect: platform.YDB, caps: capability.YDB262(), database: secretSchema("app", "notes"),
			wantErr: `.*secret create conflicts with create at scheme path.*`, wantIs: ptaherr.ErrInvalidSchemaDiff},
	}
	for _, other := range secretlessTargets {
		tests = append(tests, target{name: other.dialect, dialect: other.dialect, caps: other.caps.With(capability.Secrets, true),
			database: secretSchema("ext", "pw"), wantErr: `.*`, wantIs: ptaherr.ErrUnsupportedFeature})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.database, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRender_SecretOperation_NonOwningRenderersRefuse hands each statement on a
// secret to every renderer: only YDB's owner writes one, and every other
// renderer refuses the payload through the common extension boundary without
// knowing what it is.
func TestRender_SecretOperation_NonOwningRenderersRefuse(t *testing.T) {
	for _, test := range secretlessTargets {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			for _, operation := range []*ydbast.Secret{
				{Operation: ydbast.SecretCreate, Schema: "ext", Name: "pw", ValueEnv: "PTAH_SECRET_PW"},
				{Operation: ydbast.SecretRotate, Schema: "ext", Name: "pw", ValueEnv: "PTAH_SECRET_PW"},
				{Operation: ydbast.SecretDrop, Schema: "ext", Name: "pw"},
			} {
				sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps.With(capability.Secrets, true), &ast.ExtensionStatement{Payload: operation})
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature, qt.Commentf("%s", operation.Operation))
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// TestSecretFeatures_RefuseUnsupportedTargets refuses a declared secret, and a
// secret change, on every target but YDB: the shared model no longer
// enumerates secrets, so no other path can drop one silently.
func TestSecretFeatures_RefuseUnsupportedTargets(t *testing.T) {
	for _, test := range secretlessTargets {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(ydbsecret.DesiredObject("", "pw", "", "PTAH_SECRET_PW")))}
			c.Assert(builtin.ValidateSchema(desired, test.dialect), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, &catalog.Database{}, catalog.ServerInfo{Dialect: test.dialect}, nil, runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(diff, qt.IsNil)
			change := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("", "pw"), Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}}}}
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, change, test.dialect, planner.Options{})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}
