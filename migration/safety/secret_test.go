package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestClassify_Secret judges what a YDB secret statement loses: a drop takes
// a value nothing can read back, a rotation replaces the value every data
// source naming the secret connects with, and a creation loses nothing.
func TestClassify_Secret(t *testing.T) {
	tests := []struct {
		name         string
		node         ast.Node
		wantSeverity safety.Severity
		wantReason   string
	}{
		{name: "a secret dropped", node: ast.NewDropSecret("ext.pw"), wantSeverity: safety.Destructive,
			wantReason: "DROP SECRET removes a YDB secret whose value nothing can read back"},
		{name: "a secret rotated", node: ast.NewAlterSecret("ext.pw", "PTAH_SECRET_PW"), wantSeverity: safety.Warning,
			wantReason: "ALTER SECRET replaces the value every external data source naming the secret uses"},
		{name: "a secret created", node: ast.NewCreateSecret("ext.pw", "PTAH_SECRET_PW"), wantSeverity: safety.Safe,
			wantReason: "does not remove data or tighten constraints"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			assessments := safety.Assess([]ast.Node{test.node})

			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.wantSeverity)
			c.Assert(assessments[0].Reason, qt.Equals, test.wantReason)
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
// warning, and one that creates one is safe.
func TestClassifySchemaDiff_Secrets(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want safety.Severity
	}{
		{name: "dropped", diff: &difftypes.SchemaDiff{SecretsRemoved: difftypes.SecretChanges{{Name: "pw"}}},
			want: safety.Destructive},
		{name: "rotated", diff: &difftypes.SchemaDiff{SecretsRotated: difftypes.SecretChanges{{Name: "pw"}}},
			want: safety.Warning},
		{name: "created", diff: &difftypes.SchemaDiff{SecretsAdded: difftypes.SecretChanges{{Name: "pw"}}},
			want: safety.Safe},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(safety.Highest(safety.ClassifySchemaDiff(test.diff)), qt.Equals, test.want)
		})
	}
}
