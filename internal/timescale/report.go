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
// description is wrong in a way the reader can name and the operator cannot
// see. Two omissions, and they are different facts:
//
//   - A HYPERTABLE is described as an ordinary table, and correctly so as far
//     as the statement goes. Measured on 2.29.2 / PostgreSQL 17.11: after
//     `create_hypertable('conditions', by_range('time'))`, pg_class reports
//     relkind 'r' and the rendered `CREATE TABLE "conditions"` is exactly what
//     the columns say. Replayed, it produces a table that is not partitioned,
//     and a diff between the two reports no difference -- which is the false
//     convergence stokaro/ptah#1026 is open about.
//   - A CONTINUOUS AGGREGATE is described NOWHERE. It arrives in the view read,
//     is removed from it because describing it as a view is wrong in both
//     directions, and nothing takes its place. A reader who sees no block for
//     it cannot tell a database without one from a description that declined
//     to mention it.
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
	reportContinuousAggregates(w, schema)
}

// reportHypertables names the hypertables the document still contains.
//
// Only the ones it contains, because selection runs before this: `--exclude
// conditions` removes the table from the document, and naming it here would
// send the reader looking for a statement that is not in front of them. The
// note is about statements the operator can see and would replay.
func reportHypertables(w io.Writer, schema *types.DBSchema) {
	described := describedTables(schema)
	var named []string
	for _, hypertable := range schema.Hypertables {
		key := tableKey(hypertable.Schema, hypertable.Name)
		if _, ok := described[key]; !ok {
			continue
		}
		named = append(named, describeHypertable(hypertable))
	}
	if len(named) == 0 {
		return
	}
	sort.Strings(named)
	fmt.Fprintf(w,
		"note: %s described as ordinary tables, because no declaration syntax can say that a"+
			" table is partitioned yet; replaying this description creates tables that are not"+
			" hypertables, and a diff between the two reports no difference: %s.\n",
		countedNoun(len(named), "1 hypertable is", "hypertables are"), joinNames(named))
}

// reportContinuousAggregates names the aggregates the description leaves out
// entirely.
//
// It is not filtered by what the document contains, because the document
// contains nothing for them by construction: the aggregate is removed from the
// view read and nothing renders one. There is no statement to point at, which
// is exactly what the note has to say.
func reportContinuousAggregates(w io.Writer, schema *types.DBSchema) {
	if len(schema.ContinuousAggregates) == 0 {
		return
	}
	named := make([]string, 0, len(schema.ContinuousAggregates))
	for _, aggregate := range schema.ContinuousAggregates {
		named = append(named, aggregate.QualifiedName())
	}
	sort.Strings(named)
	fmt.Fprintf(w,
		"note: %s in this description at all, because Ptah renders none and describing one as a"+
			" view is wrong in both directions; a declaration naming one is refused rather than"+
			" applied: %s.\n",
		countedNoun(len(named), "1 continuous aggregate is not", "continuous aggregates are not"),
		joinNames(named))
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
