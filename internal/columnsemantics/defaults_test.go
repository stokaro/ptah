package columnsemantics_test

import (
	"context"
	"errors"
	qt "github.com/frankban/quicktest"
	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/internal/columnsemantics"
	"testing"
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
	for _, mode := range []string{"partial", "error", "canceled during call", "canceled before call", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(c.Context())
			defer cancel()
			calls := 0
			service := defaultsService{read: func(_ context.Context, _ schemacomparison.ColumnDefaultsRequest) (schemacomparison.ColumnDefaultsResult, error) {
				calls++
				if mode == "error" {
					return schemacomparison.ColumnDefaultsResult{}, sentinel
				}
				if mode == "canceled during call" {
					cancel()
				}
				return schemacomparison.ColumnDefaultsResult{Complete: true}, nil
			}}
			tables := []catalog.Table{{Name: "records", Columns: []catalog.Column{{Name: "id"}}}}
			if mode == "duplicate" {
				tables[0].Columns = append(tables[0].Columns, catalog.Column{Name: "ID"})
			}
			if mode == "canceled before call" {
				cancel()
			}
			result, err := columnsemantics.Defaults(ctx, service, "fixture", identifier.Semantics{ColumnNames: identifier.ComparisonASCIIInsensitive}, capability.Capabilities{}, tables)
			c.Assert(err, qt.IsNotNil)
			c.Assert(result, qt.IsNil)
			if mode == "error" {
				c.Assert(err, qt.ErrorIs, sentinel)
			}
			if mode == "canceled during call" || mode == "canceled before call" {
				c.Assert(err, qt.ErrorIs, context.Canceled)
			}
			if mode == "duplicate" || mode == "canceled before call" {
				c.Assert(calls, qt.Equals, 0)
			} else {
				c.Assert(calls, qt.Equals, 1)
			}
		})
	}
}
