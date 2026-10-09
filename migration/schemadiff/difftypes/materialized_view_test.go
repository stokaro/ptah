package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff/difftypes"
)

type viewSetting struct{ Replace bool }

func (*viewSetting) Kind() schemaext.Kind                 { return "example.org/view-setting" }
func (v *viewSetting) CloneChange() schemaext.ChangeValue { return &viewSetting{Replace: v.Replace} }
func (v *viewSetting) ReplacesOwner() bool                { return v.Replace }

type plainSetting struct{}

func (*plainSetting) Kind() schemaext.Kind               { return "example.org/plain-setting" }
func (*plainSetting) CloneChange() schemaext.ChangeValue { return &plainSetting{} }

// Every modification replaces the view except one whose only changes are
// attached settings their owner applies in place.
func TestMaterializedViewDiffReplaces(t *testing.T) {
	subject := objectidentity.ID{Kind: objectidentity.KindMatView, Name: objectidentity.Part{Source: "daily", Normalized: "daily"}}
	inPlace := schemaext.ChangeRecord{Subject: subject, Value: &viewSetting{}}
	replacing := schemaext.ChangeRecord{Subject: subject, Value: &viewSetting{Replace: true}}
	plain := schemaext.ChangeRecord{Subject: subject, Value: &plainSetting{}}
	tests := []struct {
		name string
		diff difftypes.MaterializedViewDiff
		want bool
	}{
		{name: "an entry with no recorded change", diff: difftypes.MaterializedViewDiff{ViewName: "daily"}, want: true},
		{name: "a changed definition", diff: difftypes.MaterializedViewDiff{Changes: map[string]string{"body": "a -> b"}}, want: true},
		{name: "a definition and a setting", diff: difftypes.MaterializedViewDiff{Changes: map[string]string{"body": "a -> b"}, FeatureChanges: []schemaext.ChangeRecord{inPlace}}, want: true},
		{name: "a setting applied in place", diff: difftypes.MaterializedViewDiff{FeatureChanges: []schemaext.ChangeRecord{inPlace}}, want: false},
		{name: "a setting without the contract", diff: difftypes.MaterializedViewDiff{FeatureChanges: []schemaext.ChangeRecord{plain}}, want: false},
		{name: "one setting that replaces the view", diff: difftypes.MaterializedViewDiff{FeatureChanges: []schemaext.ChangeRecord{inPlace, replacing}}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.diff.Replaces(), qt.Equals, test.want)
		})
	}
}
