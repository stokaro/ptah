package compare_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

type columnRouteOwner struct {
	request schemacomparison.ColumnsRequest
}

func (o *columnRouteOwner) CompareColumns(_ context.Context, request schemacomparison.ColumnsRequest) (schemacomparison.ColumnsResult, error) {
	o.request = request.Clone()
	result := schemacomparison.ColumnsResult{Complete: true}
	for _, table := range request.Tables {
		receipt := schemacomparison.ColumnTableReceipt{Desired: table.DesiredSubject, Current: table.CurrentSubject}
		for _, pair := range table.Pairs {
			column := schemacomparison.ColumnReceipt{Desired: pair.Desired, Current: pair.Current}
			for _, aspect := range pair.Aspects {
				fact := schemacomparison.ColumnAspectReceipt{Aspect: aspect}
				if aspect == schemacomparison.ColumnNullability {
					fact.Nullable = new(false)
				} else {
					fact.Difference = &schemacomparison.ColumnDifference{Relation: schemacomparison.ColumnSame}
				}
				column.Aspects = append(column.Aspects, fact)
			}
			receipt.Columns = append(receipt.Columns, column)
		}
		result.Tables = append(result.Tables, receipt)
	}
	return result, nil
}

func (*columnRouteOwner) ReadColumnDefaults(_ context.Context, request schemacomparison.ColumnDefaultsRequest) (schemacomparison.ColumnDefaultsResult, error) {
	result := schemacomparison.ColumnDefaultsResult{Complete: true}
	for _, column := range request.Columns {
		result.Columns = append(result.Columns, schemacomparison.ColumnDefaultReceipt{Subject: column.Subject, Kind: schemacomparison.DefaultAbsent})
	}
	return result, nil
}

func TestColumnOwnerReceivesFormattedTypesOutsideDomainTargets(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		typeName  string
		domain    string
		caps      capability.Capabilities
		ownedType bool
	}{
		{name: "absent domain capability", typeName: "Int64", ownedType: true},
		{name: "false domain capability", typeName: "Utf8", caps: capability.Capabilities{}.With(capability.DomainTypes, false), ownedType: true},
		{name: "explicit domain remains common", typeName: "positive", domain: "positive", caps: capability.Capabilities{}.With(capability.DomainTypes, true)},
		{name: "formatted domain remains common", typeName: "positive", caps: capability.Capabilities{}.With(capability.DomainTypes, true)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "items", StructName: "Item"}},
				Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "BIGINT"}}}
			current := &catalog.Database{Tables: []catalog.Table{{Name: "items", Columns: []catalog.Column{{Name: "id", DataType: test.typeName,
				FormattedType: test.typeName, DomainName: test.domain, IsNullable: "NO", OrdinalPosition: 1}}}}}
			owner := &columnRouteOwner{}
			facts, err := compare.ResolveColumnFacts(c.Context(), desired, current, "fixture", identifier.Semantics{},
				test.caps, nil, owner)
			c.Assert(err, qt.IsNil)
			c.Assert(owner.request.Tables, qt.HasLen, 1)
			c.Assert(owner.request.Tables[0].Pairs, qt.HasLen, 1)
			pair := owner.request.Tables[0].Pairs[0]
			c.Assert(slices.Contains(pair.Aspects, schemacomparison.ColumnType), qt.Equals, test.ownedType)
			_, accepted := facts.Compared[pair.Desired.Key()][schemacomparison.ColumnType]
			c.Assert(accepted, qt.Equals, test.ownedType)
		})
	}
}
