package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

func TestReadYQLSecrets(t *testing.T) {
	c := qt.New(t)
	source := "CREATE SECRET `a.b` WITH (value = $PTAH_SECRET_ROOT); CREATE SECRET `a/b` WITH (VALUE = $PTAH_SECRET_NESTED);"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Secrets, qt.DeepEquals, []schemamodel.Secret{
		{Name: "a.b", ValueEnv: "PTAH_SECRET_ROOT"},
		{Name: "b", Schema: "a", ValueEnv: "PTAH_SECRET_NESTED"},
	})
	c.Assert(database.NotDescribed.Describes(coverage.Secret), qt.IsTrue)
	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(&database, "ydb", capability.YDB262())
	c.Assert(err, qt.IsNil)
	again, _, err := sqlschema.Read([]byte(strings.Join(statements, "\n")), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(again.Secrets, qt.DeepEquals, database.Secrets)
	rendered, err := renderer.GetOrderedCreateStatementsWithCapabilities(&again, "ydb", capability.YDB262())
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
		"CREATE SECRET s;",
	} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(source), "ydb")
			c.Assert(err, qt.ErrorMatches, "YQL schema at position [0-9]+: invalid CREATE SECRET declaration;.*")
			c.Assert(err.Error(), qt.Not(qt.Contains), "SENTINEL")
			c.Assert(statements, qt.IsNil)
			c.Assert(database.Secrets, qt.HasLen, 0)
		})
	}
}

func TestReadYQLSecretRefusedOnOlderLine(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE SECRET s WITH (value = $PTAH_SECRET_TEST);"), "ydb")
	c.Assert(err, qt.IsNil)
	_, err = renderer.GetOrderedCreateStatementsWithCapabilities(&database, "ydb", capability.YDB251())
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "secrets")
}
