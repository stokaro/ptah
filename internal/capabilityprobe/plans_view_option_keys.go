package capabilityprobe

import (
	"context"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// withViewOptionKeys adds the questions a renderer decides a view's or a
// materialized view's PostgreSQL options with: whether the target keeps the
// options a CREATE states and reports them in pg_class.reloptions, where Ptah's
// reader looks for them.
//
// Every engine is asked, in its own table spelling, because acceptance alone
// is not the answer: measured on the Spanner emulator behind PGAdapter 0.56.1,
// `CREATE VIEW v WITH (security_invoker = true)` is accepted and reloptions
// reports nothing, so a declared option there would never read back. The
// other engines refuse the clause, which decides the key false before the
// catalog is asked.
//
// CockroachDB is asked about security_invoker, the one view option it has, and
// every other engine about security_barrier, which PostgreSQL has on every
// line Ptah declares; security_invoker arrived in PostgreSQL 15, and a line
// that refuses it refuses the option rather than the clause.
func withViewOptionKeys(p plan, dialect string) plan {
	normalized := platform.NormalizeDialect(dialect)
	spelling, ok := typeKeySpellingFor(normalized)
	if !ok {
		return p
	}
	viewOption := "security_barrier = true"
	if normalized == platform.CockroachDB {
		viewOption = "security_invoker = true"
	}
	// Each experiment reads a table of its own, so neither depends on what
	// the other left behind.
	view := storedViewOption(capability.ViewOptions, []string{spelling.table("vo_src", "n "+spelling.integer)},
		"CREATE VIEW vo_v WITH ("+viewOption+") AS SELECT n FROM vo_src",
		"SELECT COUNT(*) FROM pg_class WHERE relname = 'vo_v' AND array_length(reloptions, 1) > 0")
	view.requires = []capability.Capability{capability.Views}
	materialized := storedViewOption(capability.MaterializedViewOptions, []string{spelling.table("vom_src", "n "+spelling.integer)},
		"CREATE MATERIALIZED VIEW vo_m WITH (fillfactor = 70) AS SELECT n FROM vom_src",
		"SELECT COUNT(*) FROM pg_class WHERE relname = 'vo_m' AND array_length(reloptions, 1) > 0")
	materialized.requires = []capability.Capability{capability.MaterializedViews}
	p.experiments = append(p.experiments, view, materialized)
	return p
}

// storedViewOption decides key by whether the option the CREATE states is
// stored where Ptah's reader reads it, for the reason [storedRowTTL] gives: a
// server that parses the clause and drops it accepts the statement too. An
// inspection the server refuses decides false, because an option Ptah cannot
// read back is one no comparison converges on.
func storedViewOption(key capability.Capability, setup []string, create, inspect string) experiment {
	return experiment{
		decides: []capability.Capability{key},
		setup:   setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			created := s.exec(ctx, create)
			attempts := []Attempt{created}
			if !created.Accepted {
				return verdicts{key: decided(false)}, attempts
			}
			stored, inspected := s.query(ctx, inspect)
			attempts = append(attempts, inspected)
			if !inspected.Accepted {
				return verdicts{key: annotated(false,
					"the CREATE was accepted but pg_class.reloptions cannot be asked here ("+
						collapse(inspected.ServerErr)+"), and an option Ptah cannot read is one it cannot "+
						"converge on, so the key is false whatever the server stores internally",
				)}, attempts
			}
			return verdicts{key: annotated(stored == 1,
				"decided by whether pg_class.reloptions reports the option the CREATE stated, because a "+
					"server that accepts the clause and drops the option also accepts the statement",
			)}, attempts
		},
	}
}
