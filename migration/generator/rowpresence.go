package generator

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"go.5x5.cz/ptah/internal/sqlident"
)

// rowExistenceQuery is the question the concurrent-index decision asks, and it
// asks the TABLE.
//
// Whether an index build must avoid the write lock depends on one fact: does
// the relation hold a row right now. Every cheaper stand-in for that fact has
// been measured wrong on this branch.
//
//   - pg_class.reltuples is -1 for ANY never-analyzed relation, so a table
//     holding five thousand rows and an empty one report the same number.
//   - pg_stat_all_tables.n_live_tup is 0 after a counter reset, a
//     crash-recovery restart, or a restored dump, for the same reason.
//   - pg_relation_size is storage, not row presence. PostgreSQL's DELETE does
//     not free heap pages -- only VACUUM does -- so a table emptied by
//     DELETE reports a non-zero main fork with zero rows in it. Measured on
//     PostgreSQL 17.10 with autovacuum disabled on the relation:
//     reltuples -1, n_live_tup 0, pg_relation_size 229376, actual rows 0.
//     It is also 0 for a partitioned parent (relkind 'p'), whose rows all live
//     in its partitions.
//
// The query below has none of those failure modes because it is not a proxy:
// it stops at the first row the executor finds, and reports exactly whether
// one exists. On a partitioned parent it searches the partitions, which is the
// answer that decision wants.
//
// The shape is PostgreSQL-family SQL (a scalar EXISTS subquery), and the only
// caller is gated on platform.IsPostgresFamily. LIMIT 1 is redundant to
// PostgreSQL's own EXISTS short-circuit and is written anyway, because the
// statement is also read by humans deciding whether it can be slow.
//
// The identifiers are quoted through internal/sqlident, so a table named
// `"; DROP TABLE x --` is one identifier rather than a statement.
func rowExistenceQuery(dialect, schema, table string) string {
	return "SELECT EXISTS (SELECT 1 FROM " + sqlident.Qualified(dialect, schema, table) + " LIMIT 1)"
}

// tableRowPresence answers, for one relation, whether it currently holds a row.
type tableRowPresence interface {
	TableHasRows(ctx context.Context, schema, table string) (bool, error)
}

// rowQuerier is the database/sql surface [liveRowPresence] needs.
// *dbschema.DatabaseConnection satisfies it.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// liveRowPresence runs [rowExistenceQuery] against a live database.
type liveRowPresence struct {
	db      rowQuerier
	dialect string
}

// newLiveRowPresence builds the probe one planning pass uses: the live query,
// behind a memo.
func newLiveRowPresence(db rowQuerier, dialect string) tableRowPresence {
	return memoizeRowPresence(liveRowPresence{db: db, dialect: dialect})
}

// TableHasRows reports whether schema.table holds at least one row.
func (p liveRowPresence) TableHasRows(ctx context.Context, schema, table string) (bool, error) {
	query := rowExistenceQuery(p.dialect, schema, table)
	var hasRows bool
	if err := p.db.QueryRowContext(ctx, query).Scan(&hasRows); err != nil {
		return false, fmt.Errorf("row-existence probe for table %q: %w", qualifiedRelation(schema, table), err)
	}
	return hasRows, nil
}

// memoRowPresence asks each relation at most once.
//
// Several new indexes on one table are ordinary, and the decision must not turn
// one migration into N sequential round trips. The memo lives for one planning
// pass, which is short enough that a row arriving mid-pass cannot make the two
// halves of a single split disagree with each other. A failed probe is not
// remembered: the next ref naming that relation asks again.
type memoRowPresence struct {
	inner   tableRowPresence
	answers map[[2]string]bool
}

func memoizeRowPresence(inner tableRowPresence) *memoRowPresence {
	return &memoRowPresence{inner: inner, answers: make(map[[2]string]bool)}
}

func (p *memoRowPresence) TableHasRows(ctx context.Context, schema, table string) (bool, error) {
	key := [2]string{schema, table}
	if answer, asked := p.answers[key]; asked {
		return answer, nil
	}
	answer, err := p.inner.TableHasRows(ctx, schema, table)
	if err != nil {
		return false, err
	}
	p.answers[key] = answer
	return answer, nil
}

// qualifiedRelation spells a relation for a diagnostic, matching
// dbschematypes.DBTable.QualifiedName.
func qualifiedRelation(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// anyRelationHasRows reports whether any of the relations an index ref's table
// spelling can name holds a row.
//
// A probe that cannot be answered -- no SELECT privilege on the table, or the
// relation dropped between the catalog read and this question -- resolves the
// ref toward the concurrent build and says so in the log. That is the
// recoverable side of the choice: a non-transactional migration file on a
// table that turns out to be empty costs a file, while a blocking build on a
// table that turns out to hold rows costs writes for the length of the scan.
// It is an error path, not a fallback heuristic: nothing below reads a
// statistic or a storage size.
func anyRelationHasRows(ctx context.Context, rows tableRowPresence, relations []tableRelation) bool {
	for _, relation := range relations {
		hasRows, err := rows.TableHasRows(ctx, relation.schema, relation.name)
		if err != nil {
			slog.Warn(
				"could not ask whether a table holds rows; choosing the concurrent index build",
				"table", qualifiedRelation(relation.schema, relation.name),
				"error", err,
			)
			return true
		}
		if hasRows {
			return true
		}
	}
	return false
}
