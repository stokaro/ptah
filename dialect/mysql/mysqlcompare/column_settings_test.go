package mysqlcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlcompare"
	"ptah.run/dialect/mysql/mysqlschema"
)

func columnRequest(target string, owner objectidentity.ID, declared, observed schemaext.Facets) schemaext.FacetComparisonRequest {
	return schemaext.FacetComparisonRequest{
		Target: target, Kinds: []schemaext.Kind{mysqlschema.ColumnSettingsKind},
		Owners:  []schemaext.ParentState{{Subject: owner, Desired: true, Current: true}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: owner, Values: declared}}},
		Current: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: owner, Values: observed}}},
	}
}

// A declaration that differs from what the column holds is no change, and
// what the column holds is not adopted: the comparison keeps the declared
// settings as they are, which is what a later MODIFY COLUMN writes.
func TestColumnService_KeepsTheDeclarationAndReportsNoChange(t *testing.T) {
	c := qt.New(t)
	column := objectidentity.NewBuilder(identifier.ForDialect("mysql")).ColumnParts("", "users", "seen")
	declared := must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{OnUpdate: "CURRENT_TIMESTAMP"}))
	observed := must.Must(mysqlschema.WithObservedColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{Charset: "latin1", OnUpdate: "NOW()"}))

	result, err := mysqlcompare.ColumnService{}.CompareFacets(t.Context(), columnRequest("mysql", column, declared, observed))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Desired.Records, qt.HasLen, 1)
	c.Assert(result.Desired.Records[0].Values.Equal(declared), qt.IsTrue)
}

func TestColumnService_FailurePath(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("mysql"))
	tests := []struct {
		name    string
		request schemaext.FacetComparisonRequest
		wantIs  error
		wantErr string
	}{
		{name: "another target", request: columnRequest("postgres", builder.ColumnParts("", "users", "seen"), schemaext.Facets{}, schemaext.Facets{}),
			wantIs: ptaherr.ErrUnsupportedDialect, wantErr: `.*MySQL column comparison on "postgres"`},
		{name: "a table owner", request: columnRequest("mysql", builder.TableParts("", "users"), schemaext.Facets{}, schemaext.Facets{}),
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*MySQL column settings attached to .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := mysqlcompare.ColumnService{}.CompareFacets(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}
