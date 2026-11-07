package schemacomparison_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemacomparison"
)

// collationExchange asks the collation of one pair and answers it with the
// receipt given.
func collationExchange(receipt schemacomparison.ColumnAspectReceipt) (schemacomparison.ColumnsRequest, schemacomparison.ColumnsResult) {
	request, result := columnReceiptExchange()
	request.Tables[0].Pairs[0].Aspects = []schemacomparison.ColumnAspect{schemacomparison.ColumnCollation}
	result.Tables[0].Columns[0].Aspects = []schemacomparison.ColumnAspectReceipt{receipt}
	return request, result
}

func TestColumnCollationReceipt_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name       string
		difference schemacomparison.ColumnDifference
	}{
		{"the same collation", schemacomparison.ColumnDifference{Relation: schemacomparison.ColumnSame, Current: "NOCASE", Desired: "nocase"}},
		{"a collation the declaration adds", schemacomparison.ColumnDifference{Relation: schemacomparison.ColumnChanged, Desired: "NOCASE"}},
		{"both on the default", schemacomparison.ColumnDifference{Relation: schemacomparison.ColumnSame}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := collationExchange(schemacomparison.ColumnAspectReceipt{Aspect: schemacomparison.ColumnCollation, Difference: &test.difference})

			accepted, err := schemacomparison.AcceptColumns(request, result)

			c.Assert(err, qt.IsNil)
			c.Assert(*accepted.Tables[0].Columns[0].Aspects[0].Difference, qt.Equals, test.difference)
		})
	}
}

// A collation receipt is one explicit difference, and a change names two
// different collations.
func TestColumnCollationReceipt_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		receipt schemacomparison.ColumnAspectReceipt
		want    string
	}{
		{"no difference", schemacomparison.ColumnAspectReceipt{Aspect: schemacomparison.ColumnCollation},
			`invalid column semantics exchange: a collation needs one explicit relation`},
		{"no relation", schemacomparison.ColumnAspectReceipt{Aspect: schemacomparison.ColumnCollation, Difference: &schemacomparison.ColumnDifference{Desired: "NOCASE"}},
			`invalid column semantics exchange: a collation needs one explicit relation`},
		{"a nullability beside the difference", schemacomparison.ColumnAspectReceipt{Aspect: schemacomparison.ColumnCollation,
			Difference: &schemacomparison.ColumnDifference{Relation: schemacomparison.ColumnSame}, Nullable: new(true)},
			`invalid column semantics exchange: a collation needs one explicit relation`},
		{"a change that names one collation twice", schemacomparison.ColumnAspectReceipt{Aspect: schemacomparison.ColumnCollation,
			Difference: &schemacomparison.ColumnDifference{Relation: schemacomparison.ColumnChanged, Current: "NOCASE", Desired: "NOCASE"}},
			`invalid column semantics exchange: a changed collation names two different collations`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := collationExchange(test.receipt)

			accepted, err := schemacomparison.AcceptColumns(request, result)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, schemacomparison.ErrColumns)
			c.Assert(accepted, qt.DeepEquals, schemacomparison.ColumnsResult{})
		})
	}
}
