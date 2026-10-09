package ydbscheme_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbsecret"
)

// TestCommonEffects_ReadTheSecretsAStatementNames gives each statement that
// names a secret by its path a read of that secret, so the secret's owner can
// order its creation first. A path written absolute names the same secret
// relative to the database root, and reads nothing when the root is not
// known. A deprecated object secret named by name reads nothing Ptah manages,
// and a secret named twice is read once.
func TestCommonEffects_ReadTheSecretsAStatementNames(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	tests := []struct {
		name string
		root string
		node ast.Node
		want []plangraph.Effect
	}{
		{name: "an external data source, the root unknown",
			node: &ast.CreateExternalDataSourceNode{Name: "ext.pg", Options: map[string]string{ // #nosec G101 -- secret paths and an object secret's name, not credentials
				"PASSWORD_SECRET_PATH": "ext/pg.pw", "SERVICE_ACCOUNT_SECRET_NAME": "legacy", "TOKEN_SECRET_PATH": "/local/ext/token"}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("ext", "pg"), Action: plangraph.Create},
				{Subject: ydbsecret.Ref("ext", "pg.pw"), Action: plangraph.Read}}},
		{name: "an external data source in the database /local", root: "/local/",
			node: &ast.CreateExternalDataSourceNode{Name: "ext.pg", Options: map[string]string{ // #nosec G101 -- secret paths, not credentials
				"PASSWORD_SECRET_PATH": "/local/ext/pg.pw", "TOKEN_SECRET_PATH": "/local/./ext/token", "AWS_SECRET_PATH": "ext/pg.pw"}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("ext", "pg"), Action: plangraph.Create},
				{Subject: ydbsecret.Ref("ext", "pg.pw"), Action: plangraph.Read},
				{Subject: ydbsecret.Ref("ext", "token"), Action: plangraph.Read}}},
		{name: "an absolute replication credential", root: "/local", node: &ast.CreateAsyncReplicationNode{Name: "app.copy", Spec: ast.AsyncReplicationSpec{
			Connection: ast.ReplicationConnectionSpec{TokenSecretPath: "/local/token"}}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "copy"), Action: plangraph.Create},
				{Subject: ydbsecret.Ref("", "token"), Action: plangraph.Read}}},
		{name: "a replaced data source", node: &ast.CreateExternalDataSourceNode{Name: "ext.pg", Replace: true,
			Options: map[string]string{"PASSWORD_SECRET_PATH": "pw"}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("ext", "pg"), Action: plangraph.Alter},
				{Subject: ydbsecret.Ref("", "pw"), Action: plangraph.Read}}},
		{name: "an async replication", node: &ast.CreateAsyncReplicationNode{Name: "app.copy", Spec: ast.AsyncReplicationSpec{
			Connection: ast.ReplicationConnectionSpec{TokenSecretPath: "secrets/token", PasswordSecretPath: "secrets/token"}}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "copy"), Action: plangraph.Create},
				{Subject: ydbsecret.Ref("secrets", "token"), Action: plangraph.Read}}},
		{name: "a transfer whose connection changes", node: &ast.AlterTransferNode{Name: "app.move", Spec: ast.TransferSpec{
			Connection: ast.ReplicationConnectionSpec{PasswordSecretPath: "secrets/pw"}}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "move"), Action: plangraph.Alter},
				{Subject: ydbsecret.Ref("secrets", "pw"), Action: plangraph.Read}}},
		{name: "a replication naming an object secret", node: &ast.CreateAsyncReplicationNode{Name: "app.copy", Spec: ast.AsyncReplicationSpec{
			Connection: ast.ReplicationConnectionSpec{TokenSecretName: "token"}}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "copy"), Action: plangraph.Create}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			effects, err := ydbscheme.CommonEffects(builder, test.root, test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(effects, qt.DeepEquals, test.want)
		})
	}
}

// TestCommonEffects_RefusesASecretOutsideTheDatabase refuses a statement that
// names a secret by an absolute path outside the database the plan runs in,
// including one that leaves it through a parent segment.
func TestCommonEffects_RefusesASecretOutsideTheDatabase(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{name: "another database", node: &ast.CreateExternalDataSourceNode{Name: "ext.pg", Options: map[string]string{"PASSWORD_SECRET_PATH": "/other/pw"}}, // #nosec G101 -- a secret path, not a credential
			wantErr: `.*secret path "/other/pw" is outside the database /local, so no statement of it can read the secret.*`},
		{name: "a parent segment", node: &ast.AlterTransferNode{Name: "app.move", Spec: ast.TransferSpec{
			Connection: ast.ReplicationConnectionSpec{PasswordSecretPath: "/local/../other/pw"}}},
			wantErr: `.*secret path "/local/../other/pw" is outside the database /local.*`},
		{name: "the database itself", node: &ast.CreateAsyncReplicationNode{Name: "app.copy", Spec: ast.AsyncReplicationSpec{
			Connection: ast.ReplicationConnectionSpec{TokenSecretPath: "/local"}}},
			wantErr: `.*secret path "/local" is outside the database /local.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			effects, err := ydbscheme.CommonEffects(builder, "/local", test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(effects, qt.IsNil)
		})
	}
}
