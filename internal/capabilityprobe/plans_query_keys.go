package capabilityprobe

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withQueryKeys adds the questions the query builder decides with: whether a
// write returns its rows through RETURNING, and whether a SELECT can carry a
// WITH clause, a correlated subquery, a JOIN condition that is not an
// equality, and an OFFSET without a LIMIT.
//
// Every engine answers them in the standard spelling, so a refusal is the
// measurement and the day an engine takes the statement the row turns red. A
// statement the server accepts is read back: each check is a query whose
// answer only the construct produces, so a clause the server parsed and
// ignored reads as false with the evidence beside it.
func withQueryKeys(p plan, dialect string) plan {
	spelling, ok := queryKeySpellingFor(dialect)
	if !ok {
		return p
	}
	p.experiments = append(p.experiments, spelling.experiments()...)
	return p
}

// queryKeySpelling is one dialect's spelling of the query experiments.
type queryKeySpelling struct {
	// table spells a throwaway table with an integer key id and an integer
	// column n.
	table func(name string) string
	// uniqueTable spells the same table with a unique index on n. It is the
	// shape the RETURNING question is asked on, because YDB 25.1 and 25.2
	// run `UPDATE ... RETURNING` on a table without one and fail it on a
	// table with one.
	uniqueTable func(name string) []string
}

func queryKeySpellingFor(dialect string) (queryKeySpelling, bool) {
	keyed := func(integer string) func(string) string {
		return func(name string) string {
			return fmt.Sprintf("CREATE TABLE %s (id %s NOT NULL, n %s, PRIMARY KEY (id))", name, integer, integer)
		}
	}
	withUniqueIndex := func(table func(string) string) func(string) []string {
		return func(name string) []string {
			return []string{table(name), fmt.Sprintf("CREATE UNIQUE INDEX %s_n ON %s (n)", name, name)}
		}
	}
	switch platform.NormalizeDialect(dialect) {
	case platform.ClickHouse:
		// ClickHouse has no unique index, and no RETURNING to ask about on
		// one: the table without it is the whole setup.
		table := func(name string) string {
			return fmt.Sprintf("CREATE TABLE %s (id Int32, n Int32) ENGINE=MergeTree ORDER BY id", name)
		}
		return queryKeySpelling{table: table, uniqueTable: func(name string) []string { return []string{table(name)} }}, true
	case platform.YDB:
		// YDB declares an index inside CREATE TABLE; there is no CREATE INDEX.
		return queryKeySpelling{
			table: keyed("Int64"),
			uniqueTable: func(name string) []string {
				return []string{fmt.Sprintf(
					"CREATE TABLE %s (id Int64 NOT NULL, n Int64, PRIMARY KEY (id), INDEX %s_n GLOBAL UNIQUE SYNC ON (n))",
					name, name)}
			},
		}, true
	case platform.Oracle:
		table := keyed("NUMBER(10)")
		return queryKeySpelling{table: table, uniqueTable: withUniqueIndex(table)}, true
	case platform.SQLite:
		table := keyed("INTEGER")
		return queryKeySpelling{table: table, uniqueTable: withUniqueIndex(table)}, true
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
		platform.MySQL, platform.MariaDB, platform.SQLServer:
		table := keyed("int")
		return queryKeySpelling{table: table, uniqueTable: withUniqueIndex(table)}, true
	default:
		return queryKeySpelling{}, false
	}
}

// insertRows writes one INSERT per row: Oracle 21 has no multi-row VALUES.
func insertRows(table string, values ...[2]int) []string {
	out := make([]string, 0, len(values))
	for _, row := range values {
		out = append(out, fmt.Sprintf("INSERT INTO %s (id, n) VALUES (%d, %d)", table, row[0], row[1]))
	}
	return out
}

func (q queryKeySpelling) experiments() []experiment {
	twoTables := func(left, right string) []string {
		setup := []string{q.table(left), q.table(right)}
		setup = append(setup, insertRows(left, [2]int{1, 1}, [2]int{2, 2})...)
		return append(setup, insertRows(right, [2]int{1, 1}, [2]int{3, 3})...)
	}
	return []experiment{
		answers(capability.ReturningClause,
			append(q.uniqueTable("qk_ret"), insertRows("qk_ret", [2]int{1, 1})...),
			counts("INSERT INTO qk_ret (id, n) VALUES (2, 2) RETURNING id", 2),
			counts("UPDATE qk_ret SET n = 3 WHERE id = 2 RETURNING n", 3),
			counts("DELETE FROM qk_ret WHERE id = 2 RETURNING id", 2),
		),
		answers(capability.CommonTableExpressions,
			append([]string{q.table("qk_cte")}, insertRows("qk_cte", [2]int{1, 1})...),
			counts("WITH c AS (SELECT n FROM qk_cte) SELECT COUNT(*) FROM c", 1),
		),
		// Only row 1 of qk_ca has a partner in qk_cb, and only through the
		// outer reference.
		answers(capability.CorrelatedSubqueries,
			twoTables("qk_ca", "qk_cb"),
			counts("SELECT COUNT(*) FROM qk_ca a WHERE EXISTS (SELECT 1 FROM qk_cb b WHERE b.n = a.n)", 1),
		),
		// The pairs with a.n < b.n are (1, 3) and (2, 3).
		answers(capability.NonEquiJoins,
			twoTables("qk_ja", "qk_jb"),
			counts("SELECT COUNT(*) FROM qk_ja a INNER JOIN qk_jb b ON a.n < b.n", 2),
		),
		// The first row after skipping one is n = 2.
		answers(capability.OffsetWithoutLimit,
			append([]string{q.table("qk_off")}, insertRows("qk_off", [2]int{1, 1}, [2]int{2, 2})...),
			counts("SELECT n FROM qk_off ORDER BY n OFFSET 1", 2),
		),
	}
}

// answers decides one key by checks whose results are the evidence: the key
// holds when every check holds. A refused check is the server lacking the
// construct. One the server accepted and answered differently reads false too,
// with what it answered, because a construct parsed and ignored is not one
// Ptah can render.
func answers(key capability.Capability, setup []string, checks ...check) experiment {
	return experiment{
		decides: []capability.Capability{key},
		setup:   setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			var attempts []Attempt
			for _, evidence := range checks {
				attempt, held, did := evidence.run(ctx, s)
				attempts = append(attempts, attempt)
				switch {
				case !attempt.Accepted:
					return verdicts{key: decided(false)}, attempts
				case !held:
					return verdicts{key: annotated(false,
						"the statement was accepted and did not do what the key names: expected "+
							evidence.expectation()+", and it "+did,
					)}, attempts
				}
			}
			return verdicts{key: decided(true)}, attempts
		},
	}
}
