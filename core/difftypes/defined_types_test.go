package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/definedtypetest"
)

// A created table's column names a domain a feature owner's object defines,
// and the vocabulary a declaration hands a plan qualifies the column with the
// domain's schema, as it does for a common domain (stokaro/ptah#1138). The
// control is the same declaration without the owner's object: the column
// keeps the name the author wrote.
func TestUserTypeVocabularyOf_QualifiesAnOwnersType(t *testing.T) {
	tests := []struct {
		name    string
		objects []schemaext.Object
		want    string
	}{
		{name: "an owner's domain", objects: []schemaext.Object{definedtypetest.Object(objectidentity.KindDomain, "app", "zip", "text")},
			want: "app.zip"},
		{name: "control: no owner's type", want: "zip"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables:         []schemamodel.Table{{StructName: "T", Name: "t"}},
				Fields:         []schemamodel.Field{{StructName: "T", Name: "c", Type: "zip"}},
				FeatureObjects: must.Must(schemaext.NewObjects(test.objects...)),
			}
			created := difftypes.TableCreationsFor(desired, identifier.ForDialect("postgres"), "t")

			qualified := created.Qualified(difftypes.UserTypeVocabularyOf(desired), "postgres")

			c.Assert(qualified, qt.HasLen, 1)
			c.Assert(qualified[0].Fields, qt.HasLen, 1)
			c.Assert(qualified[0].Fields[0].Type, qt.Equals, test.want)
		})
	}
}
