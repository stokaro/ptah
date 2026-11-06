package generator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// projectionCounter counts the creation projections a plan asks for.
type projectionCounter struct {
	generator.Runtime
	projections int
}

func (r *projectionCounter) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	r.projections++
	return r.Runtime.ProjectTableCreations(ctx, request)
}

// The reverse of a migration that creates tables projects the state each
// removal destroys from one conversion of all the created tables: through a
// provider each projection is a round trip, and one per table made a migration
// of 500 new tables a thousand calls. Each removal still holds its own table's
// projection.
func TestReverseOfCreatedTablesProjectsThemTogether(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	names := []string{"events", "logs", "metrics"}
	source := &schemamodel.Database{}
	for _, name := range names {
		source.Tables = append(source.Tables, schemamodel.Table{Name: name, StructName: name, Facets: must.Must(schemaext.NewFacets(&chschema.DesiredTable{
			OrderBy: chschema.Setting{State: chschema.Explicit, Value: "id"},
		}))})
		source.Fields = append(source.Fields, schemamodel.Field{Name: "id", StructName: name, Type: "UInt64"})
	}
	current := &catalog.Database{}
	diff, err := schemadiff.CompareWithDialect(c.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	counted := &projectionCounter{Runtime: runtime}

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: counted, Diff: diff, DesiredSchema: source, CurrentSchema: current, Dialect: "clickhouse",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(counted.projections, qt.Equals, 1)
	c.Assert(plan.Reverse.Diff.TablesRemoved.Names(), qt.ContentEquals, names)
	builder := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	for _, removal := range plan.Reverse.Diff.TablesRemoved {
		c.Assert(removal.Current.Table.Name, qt.Equals, removal.Name)
		c.Assert(removal.Current.FeatureCoverage.Lookup(chschema.TableKind, builder.TableParts("", removal.Name)).State, qt.Equals, schemaext.Complete)
	}
}
