package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

func TestReadYQLSecrets(t *testing.T) {
	c := qt.New(t)
	source := "CREATE SECRET `a.b` WITH (value = $PTAH_SECRET_ROOT); CREATE SECRET `a/b` WITH (VALUE = $PTAH_SECRET_NESTED);"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.ContentEquals, []schemaext.Object{
		ydbsecret.DesiredObject("", "a.b", "", "PTAH_SECRET_ROOT"),
		ydbsecret.DesiredObject("a", "b", "", "PTAH_SECRET_NESTED"),
	})
	c.Assert(database.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("", "other")).State, qt.Equals, schemaext.Complete)
	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(&database, "ydb", capability.YDB262())
	c.Assert(err, qt.IsNil)
	again, _, err := sqlschema.Read([]byte(strings.Join(statements, "\n")), "ydb")
	c.Assert(err, qt.IsNil)
	reread, err := again.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(reread, qt.ContentEquals, objects)
	rendered, err := builtin.GetOrderedCreateStatementsWithCapabilities(&again, "ydb", capability.YDB262())
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.DeepEquals, statements)
}

func TestReadYQLSecretRefusalsRedactDeclaration(t *testing.T) {
	for _, source := range []string{
		"CREATE SECRET s WITH (value = 'SENTINEL');",
		"CREATE SECRET s WITH (value = \"SENTINEL\");",
		"CREATE SECRET s WITH (value = @@SENTINEL@@);",
		"CREATE SECRET s WITH (value = 'SENTINEL);",
		"CREATE SECRET s WITH (value = $SENTINEL);",
		"CREATE SECRET s WITH (value = $PTAH_SECRET_);",
		"CREATE SECRET s WITH (value = $PTAH_SECRET_OK || 'SENTINEL');",
		"CREATE SECRET s WITH (value = $PTAH_SECRET_OK, other = 'SENTINEL');",
		"CREATE SECRET s WITH (value = $PTAH_SECRET_OK, value = 'SENTINEL');",
		"CREATE SECRET s WITH (SENTINEL = $PTAH_SECRET_OK);",
		"CREATE SECRET s WITH (value = $PTAH_SECRET_OK) 'SENTINEL';",
		"CREATE SECRET 'SENTINEL' WITH (value = $PTAH_SECRET_OK);",
		"CREATE SECRET `/local/SENTINEL` WITH (value = $PTAH_SECRET_OK);",
		"CREATE SECRET `dir/` WITH (value = $PTAH_SECRET_OK);",
		"CREATE SECRET `ext/pw/` WITH (value = $PTAH_SECRET_OK);",
		"CREATE SECRET s;",
	} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(source), "ydb")
			c.Assert(err, qt.ErrorMatches, "YQL schema at position [0-9]+: invalid CREATE SECRET declaration;.*")
			c.Assert(err.Error(), qt.Not(qt.Contains), "SENTINEL")
			c.Assert(statements, qt.IsNil)
			c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

func TestReadYQLSecretRefusedOnOlderLine(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE SECRET s WITH (value = $PTAH_SECRET_TEST);"), "ydb")
	c.Assert(err, qt.IsNil)
	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(&database, "ydb", capability.YDB251())
	c.Assert(err, qt.ErrorMatches, "secret s, which requires target capability secrets, unavailable on this ydb target")
	c.Assert(statements, qt.IsNil)
}

// TestReadYQLSecretDeclaredTwice refuses a document that creates one secret
// twice: a YQL document declares a secret once.
func TestReadYQLSecretDeclaredTwice(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE SECRET `a/b` WITH (value = $PTAH_SECRET_X); CREATE SECRET `a/b` WITH (value = $PTAH_SECRET_X);"), "ydb")
	c.Assert(err, qt.ErrorMatches, `.*secret a/b is declared twice`)
	c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
}
