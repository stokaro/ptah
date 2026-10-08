package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// A read's resource pools and classifiers become declarations, except the
// pool default while it holds no setting: YDB creates it with every setting
// unset, so a declaration of it would change nothing, and every model
// introspected from a database would carry it. A default with a setting is
// kept, since the declaration then says something.
func TestConvert_ResourcePools(t *testing.T) {
	tests := []struct {
		name string
		read []catalog.ResourcePool
		want []schemamodel.ResourcePool
	}{
		{
			name: "an untouched default is left out",
			read: []catalog.ResourcePool{{Name: "default"}, {Name: "batch",
				Spec: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(3))}}},
			want: []schemamodel.ResourcePool{{Name: "batch",
				Spec: ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(3))}}},
		},
		{
			name: "a default with a setting is kept",
			read: []catalog.ResourcePool{{Name: "default", Spec: ast.ResourcePoolSpec{ResourceWeight: new(30.0)}}},
			want: []schemamodel.ResourcePool{{Name: "default", Spec: ast.ResourcePoolSpec{ResourceWeight: new(30.0)}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			classifier := catalog.ResourcePoolClassifier{Name: "etl",
				Spec: ast.ResourcePoolClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: 10}}

			converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
				ResourcePools: test.read, ResourcePoolClassifiers: []catalog.ResourcePoolClassifier{classifier},
			}, "ydb", must.Must(builtin.New())))

			c.Assert(converted.ResourcePools, qt.DeepEquals, test.want)
			c.Assert(converted.ResourcePoolClassifiers, qt.DeepEquals, []schemamodel.ResourcePoolClassifier{
				{Name: "etl", Spec: classifier.Spec},
			})
		})
	}
}
