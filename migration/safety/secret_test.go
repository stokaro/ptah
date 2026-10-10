package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestClassify_Secret judges what a YDB secret statement loses, from the
// owner's effect, before and after rendering alike: a drop takes a value
// nothing can read back, a rotation replaces the value every data source
// naming the secret connects with, and a creation loses nothing.
func TestClassify_Secret(t *testing.T) {
	tests := []struct {
		name         string
		operation    *ydbast.Secret
		wantSeverity safety.Severity
		wantReason   string
	}{
		{name: "a secret dropped", operation: &ydbast.Secret{Operation: ydbast.SecretDrop, Schema: "ext", Name: "pw"},
			wantSeverity: safety.Destructive, wantReason: "DROP SECRET removes a YDB secret whose value nothing can read back"},
		{name: "a secret rotated", operation: &ydbast.Secret{Operation: ydbast.SecretRotate, Schema: "ext", Name: "pw", ValueEnv: "PTAH_SECRET_PW"},
			wantSeverity: safety.Warning, wantReason: "ALTER SECRET replaces the value every external data source naming the secret uses"},
		{name: "a secret created", operation: &ydbast.Secret{Operation: ydbast.SecretCreate, Schema: "ext", Name: "pw", ValueEnv: "PTAH_SECRET_PW"},
			wantSeverity: safety.Safe, wantReason: ydbsecret.CreateReason},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.ExtensionStatement{Payload: test.operation}

			assessments := safety.Assess([]ast.Node{node})
			rendered, err := safety.AssessRenderedWithCapabilities(c.Context(), must.Must(builtin.New()), []ast.Node{node}, "ydb", capability.YDB262())

			c.Assert(err, qt.IsNil)
			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.wantSeverity)
			c.Assert(assessments[0].Reason, qt.Equals, test.wantReason)
			c.Assert(rendered, qt.HasLen, 1)
			c.Assert(rendered[0].Severity, qt.Equals, test.wantSeverity)
		})
	}
}

// A DROP SECRET read as text is destructive too, as a migration file a person
// wrote carries it.
func TestAssessSQL_DroppingASecretIsDestructive(t *testing.T) {
	c := qt.New(t)

	got := safety.AssessSQL("DROP SECRET `ext/pw`")

	c.Assert(got.Severity, qt.Equals, safety.Destructive)
	c.Assert(got.Reason, qt.Equals, "DROP SECRET removes a YDB secret whose value nothing can read back")
}

// A diff that drops a secret is destructive, one that rotates one is a
// warning, and one that creates one is safe, from the owner's change effect.
func TestClassifySchemaDiff_Secrets(t *testing.T) {
	tests := []struct {
		name   string
		change *ydbdiff.Secret
		want   safety.Severity
	}{
		{name: "dropped", change: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}, want: safety.Destructive},
		{name: "rotated", change: &ydbdiff.Secret{Before: &ydbsecret.Observed{}, After: &ydbsecret.Desired{}}, want: safety.Warning},
		{name: "created", change: &ydbdiff.Secret{After: &ydbsecret.Desired{}}, want: safety.Safe},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("", "pw"), Value: test.change}}}
			c.Assert(safety.Highest(safety.ClassifySchemaDiff(diff)), qt.Equals, test.want)
		})
	}
}
