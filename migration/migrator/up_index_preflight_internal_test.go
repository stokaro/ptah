package migrator

// White-box testing required: whether the up preflight opens a session, and
// which statements it reads the catalog for, is decided by an unexported plan
// before any connection is touched. The public API shows it only as a session
// count on a live PostgreSQL server, which
// integration/migrations_up_index_preflight_sessions_e2e_test.go measures.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/migratorext/builtin"
)

// TestPlanUpIndexPreflight_HappyPath pins, for each body, what the inspection
// loop does with every statement and whether the preflight opens a session.
// Both read the same plan, so a statement the loop inspects is one the session
// decision counts.
func TestPlanUpIndexPreflight_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		resumeFrom int
		want       []upIndexPreflightStep
		inspects   bool
	}{
		{
			name:     "a table and nothing else",
			body:     "CREATE TABLE notes (id int PRIMARY KEY, body text);",
			want:     []upIndexPreflightStep{upIndexPreflightSkip},
			inspects: false,
		},
		{
			name:     "an index built without IF NOT EXISTS",
			body:     "CREATE TABLE notes (id int PRIMARY KEY, body text); CREATE INDEX notes_body ON notes (body);",
			want:     []upIndexPreflightStep{upIndexPreflightSkip, upIndexPreflightSkip},
			inspects: false,
		},
		{
			name:     "a concurrent build without IF NOT EXISTS",
			body:     "CREATE INDEX CONCURRENTLY notes_body ON notes (body);",
			want:     []upIndexPreflightStep{upIndexPreflightSkip},
			inspects: false,
		},
		{
			name:     "an index created with IF NOT EXISTS",
			body:     "CREATE TABLE notes (id int PRIMARY KEY, body text); CREATE INDEX IF NOT EXISTS notes_body ON notes (body);",
			want:     []upIndexPreflightStep{upIndexPreflightSkip, upIndexPreflightInspect},
			inspects: true,
		},
		{
			name:     "a concurrent unique build with IF NOT EXISTS",
			body:     "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS notes_body ON notes (body);",
			want:     []upIndexPreflightStep{upIndexPreflightInspect},
			inspects: true,
		},
		{
			name:     "a dropped index",
			body:     "DROP INDEX IF EXISTS notes_body;",
			want:     []upIndexPreflightStep{upIndexPreflightInspect},
			inspects: true,
		},
		{
			name:     "a search path the create resolves through",
			body:     "SET search_path = app; CREATE INDEX IF NOT EXISTS notes_body ON notes (body);",
			want:     []upIndexPreflightStep{upIndexPreflightReplay, upIndexPreflightInspect},
			inspects: true,
		},
		{
			name:     "a search path and nothing to inspect",
			body:     "SET search_path = app; CREATE TABLE notes (id int PRIMARY KEY);",
			want:     []upIndexPreflightStep{upIndexPreflightReplay, upIndexPreflightSkip},
			inspects: false,
		},
		{
			name:     "a lock before the create",
			body:     "LOCK TABLE notes; CREATE INDEX IF NOT EXISTS notes_body ON notes (body);",
			want:     []upIndexPreflightStep{upIndexPreflightSkip, upIndexPreflightInspect},
			inspects: true,
		},
		{
			name:       "a create an earlier attempt committed",
			body:       "CREATE INDEX IF NOT EXISTS notes_body ON notes (body); CREATE TABLE later (id int);",
			resumeFrom: 2,
			want:       []upIndexPreflightStep{upIndexPreflightSkip, upIndexPreflightSkip},
			inspects:   false,
		},
		{
			name:       "a create after the resume point",
			body:       "CREATE TABLE later (id int); CREATE INDEX IF NOT EXISTS notes_body ON notes (body);",
			resumeFrom: 2,
			want:       []upIndexPreflightStep{upIndexPreflightSkip, upIndexPreflightInspect},
			inspects:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			statements := splitSQLStatementsForDialect(tt.body, platform.Postgres)

			plan := planUpIndexPreflight(
				builtin.Lookup(platform.Postgres).Indexes, grammarForDialect(platform.Postgres), statements, tt.resumeFrom, ownTransactionRun{},
			)

			steps := make([]upIndexPreflightStep, len(plan))
			for i, statement := range plan {
				steps[i] = statement.step
			}
			c.Assert(steps, qt.DeepEquals, tt.want)
			c.Assert(plan.inspects(), qt.Equals, tt.inspects)
		})
	}
}
