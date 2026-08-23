package compare

import (
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/tableref"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// Hypertables compares declared TimescaleDB hypertables against the ones the
// database reports.
//
// The identity is the TABLE. A hypertable is not an object with a name of its
// own -- `timescaledb_information.hypertables` is keyed by the relation -- so a
// table is partitioned or it is not, and there is nothing to rename.
//
// A removal and a modification are both reported and neither is planned. There
// is no statement that undoes create_hypertable: measured on TimescaleDB
// 2.29.2, `drop_hypertable` answers `function drop_hypertable(unknown) does not
// exist`, and changing a dimension is not a statement either. The planner
// refuses on both, which is what makes the divergence visible instead of
// silently permanent (stokaro/ptah#1026).
//
// An empty declared chunk interval is not a difference. It takes TimescaleDB's
// own default -- 7 days for a timestamptz column, measured on the same server
// -- and comparing it against the interval the catalog reports would plan a
// change on every run for a declaration that asked for whatever the server
// chose.
func Hypertables(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	declared := make(map[string]goschema.Hypertable, len(generated.Hypertables))
	for _, hypertable := range generated.Hypertables {
		declared[hypertableKey(hypertable.Table)] = hypertable
	}
	live := make(map[string]types.DBHypertable, len(database.Hypertables))
	for _, hypertable := range database.Hypertables {
		live[hypertableKey(hypertable.QualifiedName())] = hypertable
	}

	for key, hypertable := range declared {
		reported, exists := live[key]
		if !exists {
			diff.HypertablesAdded = append(diff.HypertablesAdded, hypertable.Table)
			continue
		}
		if changed := hypertableChange(hypertable, reported); changed != nil {
			diff.HypertablesModified = append(diff.HypertablesModified, *changed)
		}
	}

	for key, reported := range live {
		if _, ok := declared[key]; ok {
			continue
		}
		// A description that could not say a table is partitioned has not said
		// it should stop being one. The table itself is in the document, so its
		// silence about the partitioning looks like a complete description of
		// an ordinary table -- which is exactly what a replay of it would
		// create.
		name := reported.QualifiedName()
		if !cov.PlansRemoval(coverage.Hypertable, reported.Schema, reported.Name, name) {
			continue
		}
		diff.HypertablesRemoved = append(diff.HypertablesRemoved, name)
	}

	sort.Strings(diff.HypertablesAdded)
	sort.Strings(diff.HypertablesRemoved)
	sort.Slice(diff.HypertablesModified, func(i, j int) bool {
		return diff.HypertablesModified[i].Table < diff.HypertablesModified[j].Table
	})
}

// hypertableChange reports how a declaration differs from the catalog, or nil
// when it does not.
func hypertableChange(
	declared goschema.Hypertable,
	reported types.DBHypertable,
) *difftypes.HypertableDiff {
	sameColumn := strings.EqualFold(
		strings.TrimSpace(declared.Column),
		strings.TrimSpace(reported.PrimaryDimension),
	)
	sameInterval := strings.TrimSpace(declared.ChunkInterval) == "" ||
		strings.EqualFold(
			strings.TrimSpace(declared.ChunkInterval),
			strings.TrimSpace(reported.ChunkInterval),
		)
	if sameColumn && sameInterval {
		return nil
	}
	return &difftypes.HypertableDiff{
		Table:            declared.Table,
		OldColumn:        reported.PrimaryDimension,
		NewColumn:        declared.Column,
		OldChunkInterval: reported.ChunkInterval,
		NewChunkInterval: declared.ChunkInterval,
	}
}

// hypertableKey folds a table name for comparison the way the rest of this
// package folds one: an unqualified declaration and a qualified catalog row
// name the same table.
func hypertableKey(table string) string {
	ref, ok := tableref.Parse(table)
	if !ok {
		return strings.ToLower(strings.TrimSpace(table))
	}
	return strings.ToLower(strings.TrimSpace(ref.Name))
}
