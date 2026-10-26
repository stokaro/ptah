package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

type inspectingColumns struct {
	request schemacomparison.ColumnsRequest
}

func (s *inspectingColumns) CompareColumns(_ context.Context, r schemacomparison.ColumnsRequest) (schemacomparison.ColumnsResult, error) {
	s.request = r.Clone()
	result := schemacomparison.ColumnsResult{Complete: true}
	for _, table := range r.Tables {
		output := schemacomparison.ColumnTableReceipt{Desired: table.DesiredSubject, Current: table.CurrentSubject}
		for _, pair := range table.Pairs {
			receipt := schemacomparison.ColumnReceipt{Desired: pair.Desired, Current: pair.Current}
			for _, aspect := range pair.Aspects {
				value := schemacomparison.ColumnAspectReceipt{Aspect: aspect}
				switch aspect {
				case schemacomparison.ColumnType, schemacomparison.ColumnDefault:
					value.Difference = &schemacomparison.ColumnDifference{Relation: schemacomparison.ColumnSame}
				case schemacomparison.ColumnNullability:
					value.Nullable = new(true)
				case schemacomparison.ColumnSequence:
					value.Sequence = &schemacomparison.ColumnSequenceChanges{}
				}
				receipt.Aspects = append(receipt.Aspects, value)
			}
			output.Columns = append(output.Columns, receipt)
		}
		result.Tables = append(result.Tables, output)
	}
	return result, nil
}
func (*inspectingColumns) ReadColumnDefaults(_ context.Context, r schemacomparison.ColumnDefaultsRequest) (schemacomparison.ColumnDefaultsResult, error) {
	result := schemacomparison.ColumnDefaultsResult{Complete: true}
	for _, column := range r.Columns {
		result.Columns = append(result.Columns, schemacomparison.ColumnDefaultReceipt{Subject: column.Subject, Kind: schemacomparison.DefaultAbsent})
	}
	return result, nil
}

func TestColumnBatchRetainsDomainOwnershipAndCapturedParents(t *testing.T) {
	c := qt.New(t)
	service := &inspectingColumns{}
	runtime := acceptedRuntime{Runtime: mustRuntime(c, engine.Provider{ID: "example.org/columns", Targets: []engine.Target{{Name: "custom", Preparation: schemapreparation.Identity{}, Comparison: schemacomparison.Rules{ColumnService: service}}}})}
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "items", StructName: "Item", Comment: "source"}}, Fields: []schemamodel.Field{
		{Name: "domain", StructName: "Item", Type: "TEXT", Nullable: true},
		{Name: "plain", StructName: "Item", Type: "BIGINT", Nullable: true},
		{Name: "generated", StructName: "Item", Type: "TEXT", Nullable: true},
	}}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "items", Type: "TABLE", Comment: "catalog", Columns: []catalog.Column{
		{Name: "domain", DataType: "domain_type", DomainName: "domain_type", DomainSchema: "app", IsNullable: "YES"},
		{Name: "plain", DataType: "Int64", IsNullable: "YES"},
		{Name: "generated", DataType: "TEXT", IsNullable: "YES", GeneratedKind: "STORED", GeneratedExpression: new("plain")},
	}}}}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, catalog.ServerInfo{Dialect: "custom", Capabilities: capability.Of(map[capability.Capability]bool{capability.DomainTypes: true})}, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(service.request.Tables, qt.HasLen, 1)
	table := service.request.Tables[0]
	c.Assert(table.Desired.Fields, qt.HasLen, 3)
	c.Assert(table.Current.Table.Columns, qt.HasLen, 3)
	c.Assert(table.Desired.Table.Comment, qt.Equals, "source")
	c.Assert(table.Current.Table.Comment, qt.Equals, "catalog")
	c.Assert(table.Pairs, qt.HasLen, 2)
	c.Assert(table.Pairs[0].Aspects, qt.Not(qt.Contains), schemacomparison.ColumnType)
	c.Assert(table.Pairs[1].Aspects, qt.Contains, schemacomparison.ColumnType)
	c.Assert(table.Pairs[1].Aspects, qt.Not(qt.Contains), schemacomparison.ColumnSequence)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].ColumnName, qt.Equals, "domain")
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["type"], qt.Equals, "domain_type -> TEXT")
}
