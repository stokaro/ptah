package columnsemantics_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/internal/columnsemantics"
)

type defaultsService struct {
	schemacomparison.ColumnService
	read func(context.Context, schemacomparison.ColumnDefaultsRequest) (schemacomparison.ColumnDefaultsResult, error)
}

func (s defaultsService) ReadColumnDefaults(ctx context.Context, r schemacomparison.ColumnDefaultsRequest) (schemacomparison.ColumnDefaultsResult, error) {
	return s.read(ctx, r)
}

func TestDefaultsBindsLiteralNamesAndProtectsCapture(t *testing.T) {
	c := qt.New(t)
	tables := []catalog.Table{{Schema: "a.b", Name: "c", Columns: []catalog.Column{{Name: "value", ColumnDefault: new("typed()")}}}, {Schema: "a", Name: "b.c", Columns: []catalog.Column{{Name: "value"}}}}
	service := defaultsService{read: func(_ context.Context, r schemacomparison.ColumnDefaultsRequest) (schemacomparison.ColumnDefaultsResult, error) {
		c.Assert(r.Columns, qt.HasLen, 2)
		c.Assert(r.Columns[0].Subject.Schema.Source, qt.Equals, "a.b")
		c.Assert(r.Columns[1].Subject.Parent.Source, qt.Equals, "b.c")
		*r.Columns[0].Column.ColumnDefault = "mutated"
		return schemacomparison.ColumnDefaultsResult{Complete: true, Columns: []schemacomparison.ColumnDefaultReceipt{{Subject: r.Columns[0].Subject, Kind: schemacomparison.DefaultLiteral}, {Subject: r.Columns[1].Subject, Kind: schemacomparison.DefaultAbsent}}}, nil
	}}
	result, err := columnsemantics.Defaults(c.Context(), service, "fixture", identifier.Semantics{}, capability.Capabilities{}, tables)
	c.Assert(err, qt.IsNil)
	c.Assert(result[columnsemantics.Column{Schema: "a.b", Table: "c", Name: "value"}].Kind, qt.Equals, schemacomparison.DefaultLiteral)
	c.Assert(result[columnsemantics.Column{Schema: "a", Table: "b.c", Name: "value"}].Kind, qt.Equals, schemacomparison.DefaultAbsent)
	c.Assert(*tables[0].Columns[0].ColumnDefault, qt.Equals, "typed()")
}

func TestDefaultsRefusesPartialCancellationAndDuplicateNames(t *testing.T) {
	sentinel := errors.New("owner refused")
	complete := func(context.CancelFunc) (schemacomparison.ColumnDefaultsResult, error) {
		return schemacomparison.ColumnDefaultsResult{Complete: true}, nil
	}
	keep := func(context.CancelFunc) {}
	cancelNow := func(cancel context.CancelFunc) { cancel() }
	columns := []catalog.Column{{Name: "id"}}
	for _, row := range []struct {
		name    string
		before  func(context.CancelFunc)
		read    func(context.CancelFunc) (schemacomparison.ColumnDefaultsResult, error)
		columns []catalog.Column
		want    error
		calls   int
	}{
		{name: "partial", before: keep, read: complete, columns: columns, want: schemacomparison.ErrColumns, calls: 1},
		{name: "error", before: keep, read: func(context.CancelFunc) (schemacomparison.ColumnDefaultsResult, error) {
			return schemacomparison.ColumnDefaultsResult{}, sentinel
		}, columns: columns, want: sentinel, calls: 1},
		{name: "canceled during call", before: keep, read: func(cancel context.CancelFunc) (schemacomparison.ColumnDefaultsResult, error) {
			cancel()
			return schemacomparison.ColumnDefaultsResult{Complete: true}, nil
		}, columns: columns, want: context.Canceled, calls: 1},
		{name: "canceled before call", before: cancelNow, read: complete, columns: columns, want: context.Canceled},
		{name: "duplicate", before: keep, read: complete, columns: []catalog.Column{{Name: "id"}, {Name: "ID"}}, want: schemacomparison.ErrColumns},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(c.Context())
			defer cancel()
			row.before(cancel)
			calls := 0
			service := defaultsService{read: func(context.Context, schemacomparison.ColumnDefaultsRequest) (schemacomparison.ColumnDefaultsResult, error) {
				calls++
				return row.read(cancel)
			}}
			tables := []catalog.Table{{Name: "records", Columns: row.columns}}
			result, err := columnsemantics.Defaults(ctx, service, "fixture", identifier.Semantics{ColumnNames: identifier.ComparisonASCIIInsensitive}, capability.Capabilities{}, tables)
			c.Assert(err, qt.ErrorIs, row.want)
			c.Assert(result, qt.IsNil)
			c.Assert(calls, qt.Equals, row.calls)
		})
	}
}
