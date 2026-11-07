package goschematodb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/engine"
	"ptah.run/internal/convert/goschematodb"
)

// statedIndexes is a target's table preparation that states, for every index
// a table declares, the kind, the NULLS orders and the predicate its reader
// reports a held index with: the kind "global sync", an empty order for a key
// on column first and LAST for any other key, whatever the source wrote, and
// a predicate in parentheses where one is declared.
type statedIndexes struct{}

func (statedIndexes) PrepareTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	result := schemapreparation.Result{Complete: true, Tables: request.Clone().Tables}
	for i := range result.Tables {
		table := &result.Tables[i]
		table.IndexMethods, table.IndexNulls, table.IndexConditions = make(map[string]string), make(map[string][]string), make(map[string]string)
		for _, index := range table.Desired.Indexes {
			table.IndexMethods[index.Name] = "global sync"
			for _, field := range index.Fields {
				order := "LAST"
				if field == "first" {
					order = ""
				}
				table.IndexNulls[index.Name] = append(table.IndexNulls[index.Name], order)
			}
			table.IndexConditions[index.Name] = ""
			if index.Condition != "" {
				table.IndexConditions[index.Name] = "(" + index.Condition + ")"
			}
		}
	}
	return result, nil
}

// statedIndexRuntime selects one target whose preparation is preparation.
func statedIndexRuntime(preparation schemapreparation.Service) *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: "example.org/stated", Targets: []engine.Target{{Name: "stated", Preparation: preparation, Creations: schemaprojection.IdentityCreations{}}},
	}))
}

// statedIndexDocument declares table items with an index on first and second
// whose source spells no kind and the predicate given, and an index described
// by its parts, with the orders the source wrote.
func statedIndexDocument(condition string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Item", Name: "first", Type: "INTEGER"},
			{StructName: "Item", Name: "second", Type: "INTEGER"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Item", Name: "items_fields", Fields: []string{"first", "second"}, Condition: condition},
			{StructName: "Item", Name: "items_parts", Fields: []string{"first"}, Type: "SYNC",
				Parts: []schemamodel.IndexPart{{Name: "first", NullsOrder: "FIRST"}}},
		},
	}
}

// A document converted to stand for the current side describes each index as
// the target's preparation states it, as the comparison reads the declared
// side: the stated kind as its method, the stated orders on its parts and the
// stated predicate. An index described by its columns gets parts where an
// order is not the default.
func TestToDBSchema_DescribesIndexesAsTheTargetPreparesThem_HappyPath(t *testing.T) {
	c := qt.New(t)

	converted, err := goschematodb.ToDBSchema(c.Context(), statedIndexDocument("first > 0"), "stated", statedIndexRuntime(statedIndexes{}))

	c.Assert(err, qt.IsNil)
	c.Assert(converted.Indexes, qt.HasLen, 2)
	c.Assert(converted.Indexes[0].Method, qt.Equals, "global sync")
	c.Assert(converted.Indexes[0].Parts, qt.DeepEquals, []catalog.IndexPart{{Name: "first"}, {Name: "second", NullsOrder: "LAST"}})
	c.Assert(converted.Indexes[0].Condition, qt.Equals, "(first > 0)")
	c.Assert(converted.Indexes[1].Method, qt.Equals, "global sync")
	c.Assert(converted.Indexes[1].Parts, qt.DeepEquals, []catalog.IndexPart{{Name: "first"}})
	c.Assert(converted.Indexes[1].Condition, qt.Equals, "")
}

// The preparation of a whole batch is one call: each document's tables are
// prepared on their own, so sharing the call changes no result.
func TestToDBSchemas_PreparesTheIndexesOfABatchOnce(t *testing.T) {
	c := qt.New(t)
	counted := &countingPreparation{}
	second := statedIndexDocument("")
	second.Tables[0].Name = "others"

	converted, err := goschematodb.ToDBSchemas(c.Context(), []*schemamodel.Database{statedIndexDocument(""), second}, "stated", statedIndexRuntime(counted))

	c.Assert(err, qt.IsNil)
	c.Assert(counted.calls, qt.Equals, 1)
	c.Assert(converted[1].Indexes[0].TableName, qt.Equals, "others")
	c.Assert(converted[1].Indexes[0].Method, qt.Equals, "global sync")
}

// countingPreparation is statedIndexes, counting its calls.
type countingPreparation struct {
	calls int
}

func (p *countingPreparation) PrepareTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	p.calls++
	return statedIndexes{}.PrepareTables(ctx, request)
}

// A document whose tables declare no index asks the target nothing, so a
// target without a preparation service converts it.
func TestToDBSchema_PreparesNothingWithoutAnIndex(t *testing.T) {
	c := qt.New(t)
	document := statedIndexDocument("")
	document.Indexes = nil

	converted, err := goschematodb.ToDBSchema(c.Context(), document, "stated", statedIndexRuntime(nil))

	c.Assert(err, qt.IsNil)
	c.Assert(converted.Tables, qt.HasLen, 1)
	c.Assert(converted.Indexes, qt.HasLen, 0)
}

// An index's description depends on the target's preparation, so a target
// that has none cannot describe an indexed table, and the conversion returns
// no schema rather than the source's own spelling.
func TestToDBSchema_DescribesIndexesAsTheTargetPreparesThem_FailurePath(t *testing.T) {
	c := qt.New(t)

	converted, err := goschematodb.ToDBSchema(c.Context(), statedIndexDocument(""), "stated", statedIndexRuntime(nil))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(converted, qt.IsNil)
}
