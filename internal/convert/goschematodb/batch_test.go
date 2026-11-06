package goschematodb_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/google/go-cmp/cmp"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
)

// batchDocuments are documents that each declare one table of their own, with
// the properties dialect projects and converts for a CREATE, and an index
// carrying index.
func batchDocuments(dialect string, index schemaext.Facets) []*schemamodel.Database {
	documents := make([]*schemamodel.Database, 0, 3)
	for i, properties := range []map[string]string{
		{"row_format": "COMPRESSED", "order_by": "a"},
		{"order_by": "a"},
		{"row_format": "DYNAMIC", "order_by": "a", "primary_key": ""},
	} {
		structName, table := fmt.Sprintf("T%d", i), fmt.Sprintf("t%d", i)
		documents = append(documents, &schemamodel.Database{
			Tables: []schemamodel.Table{{StructName: structName, Name: table, Overrides: map[string]map[string]string{dialect: properties}}},
			Fields: []schemamodel.Field{
				{StructName: structName, Name: "a", Type: "INT", Primary: true},
				{StructName: structName, Name: "b", Type: "INT", Nullable: true},
			},
			Indexes: []schemamodel.Index{{StructName: structName, Name: table + "_b", Fields: []string{"b"}, Facets: index}},
		})
	}
	return documents
}

// countingRuntime counts the calls a conversion makes to the services that
// may sit behind a provider's process boundary.
type countingRuntime struct {
	goschematodb.Runtime
	projections, conversions int
}

func (r *countingRuntime) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	r.projections++
	return r.Runtime.ProjectTableCreations(ctx, request)
}

func (r *countingRuntime) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	r.conversions++
	return r.Runtime.ConvertFeatures(ctx, request)
}

// TestToDBSchemas_ConvertsEachDatabaseAsAlone pins that a batch converts each
// document as ToDBSchema converts it alone, facets, coverage and creation
// observations included, and that the batch asks the creation projection and
// the conversion service once for all of them.
func TestToDBSchemas_ConvertsEachDatabaseAsAlone(t *testing.T) {
	everyField := cmp.Exporter(func(reflect.Type) bool { return true })
	blockSize := must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))
	for _, test := range []struct {
		dialect string
		index   schemaext.Facets
	}{
		{"mysql", blockSize}, {"mariadb", blockSize}, {"clickhouse", schemaext.Facets{}}, {"postgres", schemaext.Facets{}}, {"sqlite", schemaext.Facets{}},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			alone := make([]any, 0, 3)
			for _, document := range batchDocuments(test.dialect, test.index) {
				converted, err := goschematodb.ToDBSchema(c.Context(), document, test.dialect, runtime)
				c.Assert(err, qt.IsNil)
				alone = append(alone, converted)
			}
			counted := &countingRuntime{Runtime: runtime}

			batch, err := goschematodb.ToDBSchemas(c.Context(), batchDocuments(test.dialect, test.index), test.dialect, counted)

			c.Assert(err, qt.IsNil)
			together := make([]any, 0, len(batch))
			for _, converted := range batch {
				together = append(together, converted)
			}
			c.Assert(together, qt.CmpEquals(everyField), alone)
			c.Assert(counted.projections, qt.Equals, 1)
			c.Assert(counted.conversions, qt.Equals, 1)
		})
	}
}
