package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// declaredSecrets declares the secrets pg_password at the root and s3 in the
// directory ext.
func declaredSecrets() *schemamodel.Database {
	return &schemamodel.Database{Secrets: []schemamodel.Secret{
		{Name: "pg_password", ValueEnv: "PTAH_SECRET_PG_PASSWORD"},
		{Name: "s3", Schema: "ext", ValueEnv: "PTAH_SECRET_S3"},
	}}
}

// TestCompare_YDBSecretsCompareByPresence holds a secret both sides hold
// equal, whatever the value its variable holds, because the server never
// returns one; plans the creation of a declared secret the database lacks; and
// plans the drop of a held secret the declaration leaves out. Every declared
// secret is carried for a rotation request to find.
func TestCompare_YDBSecretsCompareByPresence(t *testing.T) {
	tests := []struct {
		name        string
		held        []catalog.Secret
		wantAdded   difftypes.SecretChanges
		wantRemoved difftypes.SecretChanges
	}{
		{name: "both held", held: []catalog.Secret{{Name: "pg_password"}, {Name: "s3", Schema: "ext"}}},
		{name: "one missing", held: []catalog.Secret{{Name: "pg_password"}},
			wantAdded: difftypes.SecretChanges{{Name: "s3", Schema: "ext", ValueEnv: "PTAH_SECRET_S3"}}},
		{name: "one more held", held: []catalog.Secret{{Name: "pg_password"}, {Name: "s3", Schema: "ext"}, {Name: "old"}},
			wantRemoved: difftypes.SecretChanges{{Name: "old"}}},
		{name: "a secret of the same name in another directory is another secret",
			held:        []catalog.Secret{{Name: "pg_password"}, {Name: "s3"}},
			wantAdded:   difftypes.SecretChanges{{Name: "s3", Schema: "ext", ValueEnv: "PTAH_SECRET_S3"}},
			wantRemoved: difftypes.SecretChanges{{Name: "s3"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), declaredSecrets(), &catalog.Database{Secrets: test.held}, platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.SecretsAdded, qt.DeepEquals, test.wantAdded)
			c.Assert(diff.SecretsRemoved, qt.DeepEquals, test.wantRemoved)
			c.Assert(diff.SecretsRotated, qt.HasLen, 0)
			// Ordered by canonical reference, `ext.s3` before `pg_password`.
			c.Assert(diff.DeclaredSecrets, qt.DeepEquals, []schemamodel.Secret{
				{Name: "s3", Schema: "ext", ValueEnv: "PTAH_SECRET_S3"},
				{Name: "pg_password", ValueEnv: "PTAH_SECRET_PG_PASSWORD"},
			})
			c.Assert(diff.HasChanges(), qt.Equals, len(test.wantAdded)+len(test.wantRemoved) > 0)
		})
	}
}

// TestCompare_YDBSecretKeptWhereTheDesiredStateCannotNameIt plans no drop of
// a held secret when the desired state records that it does not describe
// secrets, as an HCL document does, and still plans the drop where it does.
func TestCompare_YDBSecretKeptWhereTheDesiredStateCannotNameIt(t *testing.T) {
	held := &catalog.Database{Secrets: []catalog.Secret{{Name: "pg_password"}}}
	tests := []struct {
		name        string
		desired     *schemamodel.Database
		wantRemoved int
	}{
		{name: "a document that cannot name a secret",
			desired: &schemamodel.Database{NotDescribed: coverage.Set{}.With(coverage.Object{
				Kind: coverage.Secret, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromFact})}},
		{name: "a document that can, and names none", desired: &schemamodel.Database{}, wantRemoved: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, held, platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.SecretsRemoved, qt.HasLen, test.wantRemoved)
		})
	}
}

// TestCompare_YDBSecretNotCreatedWhereTheReadDidNotLook withholds the
// creation of a declared secret when the read of the database recorded that it
// did not describe secrets: CREATE SECRET has no guard on the lines Ptah
// measured, so planning it over a secret that exists would fail.
func TestCompare_YDBSecretNotCreatedWhereTheReadDidNotLook(t *testing.T) {
	c := qt.New(t)
	held := &catalog.Database{NotDescribed: coverage.Set{}.With(coverage.Object{
		Kind: coverage.Secret, Name: "pg_password", Reason: coverage.Unsupported, Provenance: coverage.Observed})}

	diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), declaredSecrets(), held, &config.CompareOptions{Dialect: platform.YDB}, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.Common, qt.HasLen, 1)

	c.Assert(diff.SecretsAdded, qt.DeepEquals, difftypes.SecretChanges{{Name: "s3", Schema: "ext", ValueEnv: "PTAH_SECRET_S3"}})
}

// TestRotateSecrets_HappyPath adds a declared secret the database holds to
// the rotations, by its path, once however often it is named; a secret the
// plan creates is not rotated as well, since its creation takes the value.
func TestRotateSecrets_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		requested []string
		want      difftypes.SecretChanges
	}{
		{name: "by path", requested: []string{"ext/s3"},
			want: difftypes.SecretChanges{{Name: "s3", Schema: "ext", ValueEnv: "PTAH_SECRET_S3"}}},
		{name: "twice, and at the root, sorted", requested: []string{"pg_password", "/ext/s3", "pg_password"},
			want: difftypes.SecretChanges{
				{Name: "s3", Schema: "ext", ValueEnv: "PTAH_SECRET_S3"},
				{Name: "pg_password", ValueEnv: "PTAH_SECRET_PG_PASSWORD"},
			}},
		{name: "a secret the plan creates", requested: []string{"new_one"}},
		{name: "none"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := declaredSecrets()
			desired.Secrets = append(desired.Secrets, schemamodel.Secret{Name: "new_one", ValueEnv: "PTAH_SECRET_NEW"})
			held := &catalog.Database{Secrets: []catalog.Secret{{Name: "pg_password"}, {Name: "s3", Schema: "ext"}}}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, held, platform.YDB, must.Must(builtin.New())))

			err := diff.RotateSecrets(test.requested)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.SecretsRotated, qt.DeepEquals, test.want)
			c.Assert(diff.HasChanges(), qt.IsTrue)
		})
	}
}

// TestRotateSecrets_FailurePath refuses a name the declaration does not hold,
// so a typo cannot read as a rotation done, and leaves the rotations empty.
func TestRotateSecrets_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		requested []string
		wantErr   string
	}{
		{name: "an unknown secret", requested: []string{"nope"},
			wantErr: `rotate secret "nope": the desired schema declares no such secret`},
		{name: "a secret named by the wrong directory", requested: []string{"s3"},
			wantErr: `rotate secret "s3": the desired schema declares no such secret`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			held := &catalog.Database{Secrets: []catalog.Secret{{Name: "pg_password"}, {Name: "s3", Schema: "ext"}}}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), declaredSecrets(), held, platform.YDB, must.Must(builtin.New())))

			err := diff.RotateSecrets(test.requested)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, difftypes.ErrRotateUndeclaredSecret)
			c.Assert(diff.SecretsRotated, qt.HasLen, 0)
		})
	}
}
