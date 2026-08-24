package timescale

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
)

// ReportUndescribed writes a note naming the TimescaleDB objects the
// description does not carry, and nothing at all when there are none.
//
// It belongs on the read surfaces, which refuse nothing: `ptah db read` and
// `schema inspect` produce a description, and for a TimescaleDB database that
// description can be wrong in a way the reader can name and the operator cannot
// see.
//
// One object is named, and only when the description of it is incomplete: a
// HYPERTABLE is described as an ordinary table, and correctly so as far as the
// statement goes. Measured on 2.29.2 / PostgreSQL 17.11: after
// `create_hypertable('conditions', by_range('time'))`, pg_class reports relkind
// 'r' and the rendered `CREATE TABLE "conditions"` is exactly what the columns
// say. A declaration carries one range dimension, so a table partitioned on two
// is described as partitioned on its first, and a diff between the description
// and the server reports no difference (stokaro/ptah#1026).
//
// A CONTINUOUS AGGREGATE is not named here. It has a declaration of its own --
// the `continuous_aggregate` block and `//ptah:schema:continuousaggregate` --
// so the description carries it and the round trip converges. A format that
// cannot express one records that as a coverage limit instead, which is a
// statement about the document rather than a note beside it.
//
// It names the objects rather than counting them, which is the choice
// [go.5x5.cz/ptah/internal/sqlitevirtual.ReportUnclassified] makes and the
// opposite of [go.5x5.cz/ptah/internal/rolescope.ReportUndescribed]. That note
// withholds names because they come from outside the inspected scope and can
// belong to another tenant; these are the operator's own tables, already in the
// document they are looking at, and the note is useless without saying which of
// the statements below it are the incomplete ones.
//
// w may be nil, which is how the inspect surfaces spell "no diagnostics
// stream"; the note is then dropped rather than panicking. Write errors are
// dropped too: a diagnostic that fails to print must not fail a read that
// succeeded.
func ReportUndescribed(w io.Writer, schema *types.DBSchema) {
	if w == nil || schema == nil {
		return
	}
	reportHypertables(w, schema)
}

// reportHypertables names the hypertables this description carries INCOMPLETELY.
//
// A declaration carries one range dimension, which is what `create_hypertable`
// takes. A second dimension is a separate call -- `add_dimension` with
// `by_hash` -- and nothing declares one, so a table partitioned on two is
// described as partitioned on its first: replaying that description produces a
// hypertable with one dimension where the server has two, and a diff between
// the two reports no difference (stokaro/ptah#1026).
//
// A single-dimension hypertable is named nowhere, because the description is
// now complete for it: the `hypertable` block and `//ptah:schema:hypertable`
// both carry the dimension and the chunk interval, and the round trip converges.
//
// One whose dimension the catalog did not report is named even though it counts
// as one, because a declaration needs the column and there is none to carry.
//
// Only the tables the document contains, because selection runs before this:
// `--exclude conditions` removes the table from the document, and naming it
// here would send the reader looking for a statement that is not in front of
// them.
func reportHypertables(w io.Writer, schema *types.DBSchema) {
	described := describedTables(schema)
	var named []string
	for _, hypertable := range schema.Hypertables {
		key := tableKey(hypertable.Schema, hypertable.Name)
		if _, ok := described[key]; !ok {
			continue
		}
		if hypertable.PrimaryDimension != "" && hypertable.Dimensions <= 1 {
			continue
		}
		named = append(named, describeHypertable(hypertable))
	}
	if len(named) == 0 {
		return
	}
	sort.Strings(named)
	fmt.Fprintf(w,
		"note: %s described with the first partitioning dimension only, because a declaration"+
			" carries one range dimension and a second is a separate call; replaying this"+
			" description partitions on less than the server does, and a diff between the two"+
			" reports no difference: %s.\n",
		countedNoun(len(named), "1 hypertable is", "hypertables are"), joinNames(named))
}

// describeHypertable renders one hypertable the way the note names it: the
// table, the column it is partitioned on, and how many further dimensions there
// are. A note that named only the table would leave the reader to go and look
// up what was lost.
//
// The count is why [types.DBHypertable.Dimensions] is read at all. A hypertable
// partitioned by range on `time` AND by hash on `device` reports two, and a note
// naming only `time` would say less than the truth about what a replay drops --
// the failure this note exists to prevent, one level down.
func describeHypertable(hypertable types.DBHypertable) string {
	if hypertable.PrimaryDimension == "" {
		return hypertable.QualifiedName()
	}
	return fmt.Sprintf("%s (on %s%s)",
		hypertable.QualifiedName(), hypertable.PrimaryDimension,
		furtherDimensions(hypertable.Dimensions))
}

// furtherDimensions names the partitioning the note does not spell out, and
// says nothing for the ordinary single-dimension hypertable.
func furtherDimensions(dimensions int) string {
	switch {
	case dimensions > 2:
		return fmt.Sprintf(" and %d more dimensions", dimensions-1)
	case dimensions == 2:
		return " and 1 more dimension"
	default:
		return ""
	}
}

// describedTables keys the tables this description carries, folded the way
// PostgreSQL folds an unquoted name.
func describedTables(schema *types.DBSchema) map[string]struct{} {
	described := make(map[string]struct{}, len(schema.Tables))
	for _, table := range schema.Tables {
		described[tableKey(table.Schema, table.Name)] = struct{}{}
	}
	return described
}

func tableKey(schemaName, name string) string {
	semantics := identifier.ForDialect(platform.Postgres)
	return semantics.QualifiedTableIdentityKey(types.QualifyTableName(schemaName, name))
}

// countedNoun renders a count with its singular or plural phrase, in the shape
// rolescope's countedRoles uses for the same kind of note.
func countedNoun(count int, singular, plural string) string {
	if count == 1 {
		return singular
	}
	return fmt.Sprintf("%d %s", count, plural)
}

func joinNames(names []string) string {
	return strings.Join(names, ", ")
}
