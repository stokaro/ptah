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
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// secretCreated, secretDropped and secretRotated are the owner's changes a
// comparison hands the planner for one secret.
func secretCreated(schema, name, valueEnv string) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbsecret.Ref(schema, name), Value: &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: valueEnv}}}
}

func secretDropped(schema, name string) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbsecret.Ref(schema, name), Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}}
}

func secretRotated(schema, name, valueEnv string) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbsecret.Ref(schema, name),
		Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}, After: &ydbsecret.Desired{ValueEnv: valueEnv}}}
}

// TestGenerateMigrationAST_Secrets_HappyPath pins where the secret owner puts
// its statements in a YDB plan. A secret depends on nothing but its path, so a
// drop and a rotation run first, and a creation too unless a table the plan
// drops holds its path: then it follows that drop. No statement holds a
// value: each refers to the variable it is read from when the statement runs.
func TestGenerateMigrationAST_Secrets_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{
			Name:   "old_pw",
			Table:  schemamodel.Table{StructName: "T", Name: "old_pw"},
			Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		}},
		TablesRemoved: difftypes.TableRemovals{{Name: "ext.pw", Current: observedFeeds(t, "ext", "pw")}},
		FeatureChanges: []schemaext.ChangeRecord{
			secretDropped("", "old_pw"), secretCreated("ext", "pw", "PTAH_SECRET_PW"), secretRotated("", "token", "PTAH_SECRET_TOKEN"),
		},
		ViewsAdded: difftypes.ViewChanges{{Name: "v", Body: "SELECT id FROM old_pw"}},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "DROP SECRET `old_pw`;\n"+
		"ALTER SECRET `token` WITH (value = $PTAH_SECRET_TOKEN);\n"+
		"CREATE TABLE `old_pw` (\n"+
		"    `id` Int64 NOT NULL,\n"+
		"    PRIMARY KEY (`id`)\n"+
		");\n"+
		"DROP TABLE `ext/pw`;\n"+
		"CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n"+
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
		FeatureChanges:         []schemaext.ChangeRecord{secretCreated("", "token", "PTAH_SECRET_TOKEN")},
		AsyncReplicationsAdded: difftypes.AsyncReplicationChanges{{Name: "mirror", Spec: spec}},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(strings.HasPrefix(got, "CREATE SECRET `token` WITH (value = $PTAH_SECRET_TOKEN);\n"+
		"CREATE ASYNC REPLICATION `mirror`"), qt.IsTrue, qt.Commentf("plan: %s", got))
}

// TestGenerateMigrationAST_Secrets_FailurePath refuses a secret change on a
// line without the key, naming it, before any node is returned.
func TestGenerateMigrationAST_Secrets_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		change  schemaext.ChangeRecord
		wantErr string
	}{
		{name: "a creation on 25.1", caps: capability.YDB251(), change: secretCreated("", "pw", "PTAH_SECRET_PW"),
			wantErr: "secret pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a drop on 25.3", caps: capability.YDB253(), change: secretDropped("ext", "pw"),
			wantErr: "secret ext/pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a rotation on 25.2", caps: capability.YDB252(), change: secretRotated("", "pw", "PTAH_SECRET_PW"),
			wantErr: "secret pw, which requires target capability secrets, unavailable on this ydb target"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				&difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{test.change}},
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			_, refused := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(refused, qt.IsTrue)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestGenerateMigrationAST_Secrets_RefusesAMalformedChange refuses a change
// whose variable no value may come from before any node is returned: it is
// malformed input, not a target limit.
func TestGenerateMigrationAST_Secrets_RefusesAMalformedChange(t *testing.T) {
	c := qt.New(t)
	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		&difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{secretCreated("", "pw", "HOME")}},
	)
	c.Assert(err, qt.ErrorMatches, `.*invalid value_env: "HOME" does not start with PTAH_SECRET_ .*`)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(nodes, qt.IsNil)
}
