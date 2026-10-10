package ydbreverse

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbfamily"
)

// ColumnFamiliesService reconstructs a row table's column families from
// complete operands. Its zero value is ready for concurrent use and never
// reads a database.
type ColumnFamiliesService struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. Each reversal asks for the captured prior families in
// place: each column back in the family it held, and each setting back to the
// value it held. ForwardState predicts the families the forward change leaves;
// it is a planning input, never inspection evidence.
//
// YQL drops no family and resets no family setting, so a family the forward
// change added stays, and so does a setting it stated where the table held
// none; the reversal says so.
func (ColumnFamiliesService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseFacet(ctx, request, "YDB column family", reverseColumnFamilies)
}

func reverseColumnFamilies(record schemaext.ChangeRecord, change *ydbdiff.ColumnFamilies) (schemaext.Reversal, error) {
	if err := ydbdiff.ValidateColumnFamilies(change); err != nil {
		return schemaext.Reversal{}, err
	}
	var held []ydbschema.ColumnFamily
	if change.Before != nil {
		held = change.Before.Families
	}
	after, err := change.After.Observed()
	if err != nil {
		return schemaext.Reversal{}, err
	}
	reversed := &ydbdiff.ColumnFamilies{
		Before: after,
		After:  &ydbschema.DesiredColumnFamilies{Families: ydbfamily.Applied(held, after.Families)},
	}
	result := schemaext.Reversal{
		Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: ydbschema.ColumnFamiliesKind, Value: after.Clone()}},
		Strategy:     "move each column back to its family and restore each family setting in place",
	}
	if kept := unrestored(held, after.Families); len(kept) > 0 {
		result.Limitations = []string{fmt.Sprintf("YQL drops no column family and resets no family setting, so the "+
			"rollback keeps %s, which the forward change added.", kept)}
	}
	return result, nil
}

// unrestored names what a rollback to held cannot undo: the families after
// holds and held does not, and the settings after states where held states
// none. Every row table has the default family, so one held does not list
// is a default family stating nothing, which a read leaves out.
func unrestored(held, after []ydbschema.ColumnFamily) string {
	var families, settings []string
	for _, family := range ydbfamily.Normalize(after) {
		index := slices.IndexFunc(held, func(prior ydbschema.ColumnFamily) bool { return prior.Name == family.Name })
		var prior ydbschema.ColumnFamily
		switch {
		case index >= 0:
			prior = held[index]
		case family.Name != ydbschema.DefaultColumnFamily:
			families = append(families, family.Name)
			continue
		default:
		}
		for _, setting := range []struct{ name, stated, held string }{
			{"DATA", family.Data, prior.Data}, {"COMPRESSION", family.Compression, prior.Compression},
			{"CACHE_MODE", family.CacheMode, prior.CacheMode},
		} {
			if setting.stated != "" && setting.held == "" {
				settings = append(settings, fmt.Sprintf("the %s of family %s", setting.name, family.Name))
			}
		}
	}
	var parts []string
	switch len(families) {
	case 0:
	case 1:
		parts = append(parts, "family "+families[0])
	default:
		parts = append(parts, "families "+strings.Join(families, ", "))
	}
	parts = append(parts, settings...)
	return strings.Join(parts, " and ")
}
