package ydb_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestGenerateMigrationAST_Secrets_HappyPath pins where secrets go in a YDB
// plan. A removed secret is dropped early, before any table is created, so a
// table created under its path finds the path free; an added secret is
// created, and a rotated one altered, after the tables are dropped, so a
// secret created under a dropped table's path finds the path free, and before
// the views. No statement holds a value: each refers to the variable it is
// read from when the statement runs. A plan of this shape applied to
// local-ydb 26.2.1.14 one statement per query and read back as declared.
func TestGenerateMigrationAST_Secrets_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{
			Name:   "old_pw",
			Table:  schemamodel.Table{StructName: "T", Name: "old_pw"},
			Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		}},
		TablesRemoved:  []string{"ext.pw"},
		SecretsRemoved: difftypes.SecretChanges{{Name: "old_pw"}},
		SecretsAdded:   difftypes.SecretChanges{{Name: "pw", Schema: "ext", ValueEnv: "PTAH_SECRET_PW"}},
		SecretsRotated: difftypes.SecretChanges{{Name: "token", ValueEnv: "PTAH_SECRET_TOKEN"}},
		ViewsAdded:     difftypes.ViewChanges{{Name: "v", Body: "SELECT id FROM old_pw"}},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "DROP SECRET `old_pw`;\n"+
		"CREATE TABLE `old_pw` (\n"+
		"    `id` Int64 NOT NULL,\n"+
		"    PRIMARY KEY (`id`)\n"+
		");\n"+
		"DROP TABLE `ext/pw`;\n"+
		"CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n"+
		"ALTER SECRET `token` WITH (value = $PTAH_SECRET_TOKEN);\n"+
		"CREATE VIEW `v` WITH (security_invoker = TRUE) AS\n"+
		"SELECT id FROM old_pw\n"+
		";\n")
}

// A replication must find its credential when it starts. Both families can
// enter one plan, so testing each family's statements alone misses the order.
func TestGenerateMigrationAST_SecretsPrecedeReplications(t *testing.T) {
	c := qt.New(t)
	spec := replicationOf("accounts", "replica/accounts")
	spec.Connection.TokenSecretPath = "token"
	diff := &difftypes.SchemaDiff{
		SecretsAdded:           difftypes.SecretChanges{{Name: "token", ValueEnv: "PTAH_SECRET_TOKEN"}},
		AsyncReplicationsAdded: difftypes.AsyncReplicationChanges{{Name: "mirror", Spec: spec}},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(strings.HasPrefix(got, "CREATE SECRET `token` WITH (value = $PTAH_SECRET_TOKEN);\n"+
		"CREATE ASYNC REPLICATION `mirror`"), qt.IsTrue, qt.Commentf("plan: %s", got))
}

// TestGenerateMigrationAST_Secrets_FailurePath refuses a secret change on a
// line without the key, naming it, and a creation whose variable is not one a
// value may come from, before any node is returned.
func TestGenerateMigrationAST_Secrets_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{name: "a creation on 25.1", caps: capability.YDB251(),
			diff:    &difftypes.SchemaDiff{SecretsAdded: difftypes.SecretChanges{{Name: "pw", ValueEnv: "PTAH_SECRET_PW"}}},
			wantErr: "secret pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a drop on 25.3", caps: capability.YDB253(),
			diff:    &difftypes.SchemaDiff{SecretsRemoved: difftypes.SecretChanges{{Name: "pw"}}},
			wantErr: "secret pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a rotation on 25.2", caps: capability.YDB252(),
			diff:    &difftypes.SchemaDiff{SecretsRotated: difftypes.SecretChanges{{Name: "pw", ValueEnv: "PTAH_SECRET_PW"}}},
			wantErr: "secret pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a variable outside the prefix", caps: capability.YDB262(),
			diff:    &difftypes.SchemaDiff{SecretsAdded: difftypes.SecretChanges{{Name: "pw", ValueEnv: "HOME"}}},
			wantErr: `secret pw: invalid value_env: "HOME" does not start with PTAH_SECRET_ .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				test.diff,
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			_, refused := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(refused, qt.IsTrue)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
