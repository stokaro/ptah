package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// declaredSecrets declares the secrets pg_password at the root and s3 in the
// directory ext, from a source that describes the secret namespace.
func declaredSecrets() *schemamodel.Database {
	return &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbsecret.DesiredObject("", "pg_password", "", "PTAH_SECRET_PG_PASSWORD"),
			ydbsecret.DesiredObject("ext", "s3", "", "PTAH_SECRET_S3"))),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// heldSecrets is a read that listed every secret and found these.
func heldSecrets(objects ...schemaext.Object) *catalog.Database {
	return &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

func created(schema, name, valueEnv string) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbsecret.Ref(schema, name), Value: &ydbdiff.Secret{After: &ydbsecret.Desired{ValueEnv: valueEnv}}}
}

func dropped(schema, name string) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbsecret.Ref(schema, name), Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}}
}

// TestCompare_YDBSecretsCompareByPresence holds a secret both sides hold
// equal, whatever the value its variable holds, because the server never
// returns one; plans the creation of a declared secret the database lacks; and
// plans the drop of a held secret the declaration leaves out.
func TestCompare_YDBSecretsCompareByPresence(t *testing.T) {
	tests := []struct {
		name string
		held []schemaext.Object
		want []schemaext.ChangeRecord
	}{
		{name: "both held", held: []schemaext.Object{ydbsecret.ObservedObject("", "pg_password"), ydbsecret.ObservedObject("ext", "s3")}},
		{name: "one missing", held: []schemaext.Object{ydbsecret.ObservedObject("", "pg_password")},
			want: []schemaext.ChangeRecord{created("ext", "s3", "PTAH_SECRET_S3")}},
		{name: "one more held",
			held: []schemaext.Object{ydbsecret.ObservedObject("", "pg_password"), ydbsecret.ObservedObject("ext", "s3"), ydbsecret.ObservedObject("", "old")},
			want: []schemaext.ChangeRecord{dropped("", "old")}},
		{name: "a secret of the same name in another directory is another secret",
			held: []schemaext.Object{ydbsecret.ObservedObject("", "pg_password"), ydbsecret.ObservedObject("", "s3")},
			want: []schemaext.ChangeRecord{dropped("", "s3"), created("ext", "s3", "PTAH_SECRET_S3")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), declaredSecrets(), heldSecrets(test.held...), platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.FeatureChanges, qt.DeepEquals, test.want)
			c.Assert(diff.HasChanges(), qt.Equals, len(test.want) > 0)
		})
	}
}

// TestCompare_YDBSecretKeptWhereTheDesiredStateCannotNameIt plans no drop of
// a held secret when the desired state makes no claim about secrets, as an
// HCL document does not, and still plans the drop where it claims to describe
// them and names none.
func TestCompare_YDBSecretKeptWhereTheDesiredStateCannotNameIt(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    []schemaext.ChangeRecord
	}{
		{name: "a document that cannot name a secret", desired: &schemamodel.Database{}},
		{name: "a document that can, and names none",
			desired: &schemamodel.Database{FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))},
			want:    []schemaext.ChangeRecord{dropped("", "pg_password")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, heldSecrets(ydbsecret.ObservedObject("", "pg_password")), platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.FeatureChanges, qt.DeepEquals, test.want)
		})
	}
}

// TestCompare_YDBSecretNotCreatedWhereTheReadDidNotLook withholds the
// creation of a declared secret when the read of the database recorded that it
// could not read it: CREATE SECRET has no guard on the lines Ptah measured, so
// planning it over a secret that exists would fail. The withheld creation is
// reported, and a secret the read did look for is still created.
func TestCompare_YDBSecretNotCreatedWhereTheReadDidNotLook(t *testing.T) {
	c := qt.New(t)
	held := &catalog.Database{FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: ydbsecret.Kind, Subject: ydbsecret.Ref("", "pg_password"),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "target capability secrets is unavailable"}}}))}

	diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), declaredSecrets(), held, &config.CompareOptions{Dialect: platform.YDB}, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.Features, qt.HasLen, 1)
	c.Assert(diagnostics.Features[0].Subject, qt.DeepEquals, ydbsecret.Ref("", "pg_password"))
	c.Assert(diff.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{created("ext", "s3", "PTAH_SECRET_S3")})
}

// TestCompare_YDBSecretRotatedOnlyWhenAsked plans ALTER SECRET for a declared
// secret the database holds only when the declaration asks for a rotation,
// once however often it is named; a secret the plan creates is created rather
// than rotated, since its creation takes the value.
func TestCompare_YDBSecretRotatedOnlyWhenAsked(t *testing.T) {
	rotated := func(schema, name, valueEnv string) schemaext.ChangeRecord {
		return schemaext.ChangeRecord{Subject: ydbsecret.Ref(schema, name),
			Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}, After: &ydbsecret.Desired{ValueEnv: valueEnv, Rotate: true}}}
	}
	tests := []struct {
		name      string
		requested []string
		want      []schemaext.ChangeRecord
	}{
		{name: "none", want: []schemaext.ChangeRecord{created("", "new_one", "PTAH_SECRET_NEW")}},
		{name: "by path", requested: []string{"ext/s3"},
			want: []schemaext.ChangeRecord{created("", "new_one", "PTAH_SECRET_NEW"), rotated("ext", "s3", "PTAH_SECRET_S3")}},
		{name: "twice, and at the root", requested: []string{"pg_password", "/ext/s3", "pg_password"},
			want: []schemaext.ChangeRecord{created("", "new_one", "PTAH_SECRET_NEW"), rotated("", "pg_password", "PTAH_SECRET_PG_PASSWORD"), rotated("ext", "s3", "PTAH_SECRET_S3")}},
		{name: "a secret the plan creates", requested: []string{"new_one"},
			want: []schemaext.ChangeRecord{created("", "new_one", "PTAH_SECRET_NEW")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := declaredSecrets()
			desired.FeatureObjects = must.Must(desired.FeatureObjects.With(ydbsecret.DesiredObject("", "new_one", "", "PTAH_SECRET_NEW")))
			desired = must.Must(ydbsecret.RequestRotation(desired, test.requested))
			held := heldSecrets(ydbsecret.ObservedObject("", "pg_password"), ydbsecret.ObservedObject("ext", "s3"))

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, held, platform.YDB, must.Must(builtin.New())))

			c.Assert(diff.FeatureChanges, qt.DeepEquals, test.want)
		})
	}
}
