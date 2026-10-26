package schemacomparison_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
)

func columnReceiptExchange() (schemacomparison.ColumnsRequest, schemacomparison.ColumnsResult) {
	builder := objectidentity.NewBuilder(identifier.Semantics{})
	table, column := builder.TableParts("app", "items"), builder.ColumnParts("app", "items", "id")
	request := schemacomparison.ColumnsRequest{Tables: []schemacomparison.ColumnTable{{DesiredSubject: table, CurrentSubject: table,
		Pairs: []schemacomparison.ColumnPair{{Desired: column, Current: column, Aspects: []schemacomparison.ColumnAspect{schemacomparison.ColumnNullability}}}}}}
	result := schemacomparison.ColumnsResult{Complete: true, Tables: []schemacomparison.ColumnTableReceipt{{Desired: table, Current: table,
		Columns: []schemacomparison.ColumnReceipt{{Desired: column, Current: column, Aspects: []schemacomparison.ColumnAspectReceipt{{Aspect: schemacomparison.ColumnNullability, Nullable: new(false)}}}}}}}
	return request, result
}

func TestColumnReceiptsRetainExplicitFalseAndIndependentValues(t *testing.T) {
	c := qt.New(t)
	request, result := columnReceiptExchange()
	accepted, err := schemacomparison.AcceptColumns(request, result)
	c.Assert(err, qt.IsNil)
	*result.Tables[0].Columns[0].Aspects[0].Nullable = true
	c.Assert(*accepted.Tables[0].Columns[0].Aspects[0].Nullable, qt.IsFalse)
}

func TestColumnReceiptsRefuseIncompleteOrForeignResults(t *testing.T) {
	for _, row := range []struct {
		name   string
		mutate func(*schemacomparison.ColumnsResult)
	}{
		{"incomplete", func(r *schemacomparison.ColumnsResult) { r.Complete = false }},
		{"foreign current", func(r *schemacomparison.ColumnsResult) { r.Tables[0].Columns[0].Current.Name.Source = "other" }},
		{"missing pair", func(r *schemacomparison.ColumnsResult) { r.Tables[0].Columns = nil }},
		{"missing aspect", func(r *schemacomparison.ColumnsResult) { r.Tables[0].Columns[0].Aspects = nil }},
		{"missing false", func(r *schemacomparison.ColumnsResult) { r.Tables[0].Columns[0].Aspects[0].Nullable = nil }},
		{"extra receipt", func(r *schemacomparison.ColumnsResult) {
			r.Tables[0].Columns[0].Aspects[0].Difference = &schemacomparison.ColumnDifference{Relation: schemacomparison.ColumnSame}
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := columnReceiptExchange()
			row.mutate(&result)
			accepted, err := schemacomparison.AcceptColumns(request, result)
			c.Assert(err, qt.ErrorIs, schemacomparison.ErrColumns)
			c.Assert(accepted, qt.DeepEquals, schemacomparison.ColumnsResult{})
		})
	}
}

func TestColumnDefaultReceiptsPreserveTypedEmptyValues(t *testing.T) {
	c := qt.New(t)
	_, comparison := columnReceiptExchange()
	subject := comparison.Tables[0].Columns[0].Current
	request := schemacomparison.ColumnDefaultsRequest{Columns: []schemacomparison.ObservedColumnDefault{{Subject: subject, Column: catalog.Column{Name: "id", ColumnDefault: new("''")}}}}
	result := schemacomparison.ColumnDefaultsResult{Complete: true, Columns: []schemacomparison.ColumnDefaultReceipt{{Subject: subject, Kind: schemacomparison.DefaultLiteral}}}
	accepted, err := schemacomparison.AcceptColumnDefaults(request, result)
	c.Assert(err, qt.IsNil)
	c.Assert(accepted.Columns[0].Value, qt.Equals, "")
	result.Columns[0].Kind = schemacomparison.DefaultAbsent
	accepted, err = schemacomparison.AcceptColumnDefaults(request, result)
	c.Assert(err, qt.ErrorIs, schemacomparison.ErrColumns)
	c.Assert(accepted, qt.DeepEquals, schemacomparison.ColumnDefaultsResult{})
	request.Columns[0].Column.ColumnDefault = nil
	accepted, err = schemacomparison.AcceptColumnDefaults(request, result)
	c.Assert(err, qt.IsNil)
	c.Assert(accepted.Columns[0].Kind, qt.Equals, schemacomparison.DefaultAbsent)
}
