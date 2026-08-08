package postgres_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/postgres"
)

// objectKindCase is one schema object the PostgreSQL-family renderer emits,
// paired with the capability key that decides whether this target hosts it.
//
// create and drop build their nodes rather than holding them, because a node is
// consumed by a render and the same case is rendered more than once — once with
// the key on and once with it off.
type objectKindCase struct {
	name   string
	key    capability.Capability
	kind   string // the words the skip comment uses for this object kind
	object string // the object's own name, which the skip comment must carry
	create func() ast.Node
	drop   func() ast.Node
	ddl    string // a fragment only the real DDL contains
}

func objectKindCases() []objectKindCase {
	return []objectKindCase{
		{
			name:   "view",
			key:    capability.Views,
			kind:   "view",
			object: "active_users",
			create: func() ast.Node { return ast.NewCreateView("active_users").SetBody("SELECT id FROM users") },
			drop:   func() ast.Node { return ast.NewDropView("active_users") },
			ddl:    "CREATE VIEW",
		},
		{
			name:   "materialized view",
			key:    capability.MaterializedViews,
			kind:   "materialized view",
			object: "user_counts",
			create: func() ast.Node {
				return ast.NewCreateMaterializedView("user_counts").SetBody("SELECT count(*) FROM users")
			},
			drop: func() ast.Node { return ast.NewDropMaterializedView("user_counts") },
			ddl:  "CREATE MATERIALIZED VIEW",
		},
		{
			name:   "function",
			key:    capability.Functions,
			kind:   "function",
			object: "touch_updated",
			create: func() ast.Node {
				return ast.NewCreateFunction("touch_updated").
					SetReturns("trigger").
					SetLanguage("plpgsql").
					SetBody("BEGIN RETURN NEW; END;")
			},
			drop: func() ast.Node { return ast.NewDropFunction("touch_updated") },
			ddl:  "CREATE OR REPLACE FUNCTION",
		},
		{
			name:   "trigger",
			key:    capability.Triggers,
			kind:   "trigger",
			object: "users_touch",
			create: func() ast.Node {
				return ast.NewCreateTrigger("users_touch", "users").
					SetTiming("BEFORE").
					SetEvent("UPDATE").
					SetBody("NEW.updated_at = NOW(); RETURN NEW;")
			},
			drop: func() ast.Node { return ast.NewDropTrigger("users_touch", "users") },
			ddl:  "CREATE TRIGGER",
		},
	}
}

// objectKindDeniedCaps is a valid PostgreSQL set with one object kind denied.
//
// Denying a kind is not a single flag flip, because the registry carries
// implication edges: materialized views require views, and replace syntax
// requires triggers. The dependants come off together with the object so that
// the result is a set Validate accepts — a renderer must never be handed a set
// describing a target that cannot exist.
func objectKindDeniedCaps(key capability.Capability) capability.Capabilities {
	return capability.Postgres16().
		With(key, false).
		With(capability.MaterializedViews, false).
		With(capability.CreateOrReplaceTrigger, false)
}

// TestPostgreSQLRenderer_ObjectKindCapabilities is the renderer half of
// stokaro/ptah#929 item 5.
//
// One renderer serves PostgreSQL, CockroachDB, YugabyteDB and Spanner. Before
// these keys, nothing in it could express that a family member does not host a
// plpgsql function or a trigger — the only suppression lived in the offline
// converter, as a comparison against the literal dialect name, and the live
// planner never consulted it. The gate belongs here because both callers pass
// through this renderer, so both get the same answer.
//
// Each case renders against the SAME dialect name twice with only the key
// flipped, so the dialect cannot be what decided the outcome.
func TestPostgreSQLRenderer_ObjectKindCapabilities(t *testing.T) {
	for _, tc := range objectKindCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("key on renders DDL", func(t *testing.T) {
				c := qt.New(t)

				renderer := postgres.NewWithCapabilities(capability.Postgres16().With(tc.key, true), platform.Postgres)

				sql, err := renderer.Render(tc.create())

				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Contains, tc.ddl)
				c.Assert(sql, qt.Not(qt.Contains), "is not supported by this target")
			})

			t.Run("key off names the object it skipped", func(t *testing.T) {
				c := qt.New(t)

				caps := objectKindDeniedCaps(tc.key)
				c.Assert(caps.Validate(), qt.IsNil)
				renderer := postgres.NewWithCapabilities(caps, platform.Postgres)

				sql, err := renderer.Render(tc.create())

				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Contains,
					fmt.Sprintf("-- POSTGRES: %s %s is not supported by this target; skipped.", tc.kind, tc.object))
				c.Assert(sql, qt.Not(qt.Contains), tc.ddl)
			})

			t.Run("the drop is skipped with the same comment", func(t *testing.T) {
				c := qt.New(t)

				renderer := postgres.NewWithCapabilities(objectKindDeniedCaps(tc.key), platform.Postgres)

				sql, err := renderer.Render(tc.drop())

				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Contains,
					fmt.Sprintf("-- POSTGRES: %s %s is not supported by this target; skipped.", tc.kind, tc.object))
				c.Assert(sql, qt.Not(qt.Contains), "DROP")
			})
		})
	}
}

// TestPostgreSQLRenderer_TriggerCarriesItsFunction pins the reason
// VisitCreateTrigger consults Triggers and not Functions.
//
// A PostgreSQL trigger renders as a linked CREATE OR REPLACE FUNCTION plus the
// CREATE TRIGGER. That function is not an object the schema author declared, so
// a target that hosts triggers must still get it where declared functions are
// refused — and a target that refuses triggers must not receive a stray
// function body for a trigger that will never exist.
func TestPostgreSQLRenderer_TriggerCarriesItsFunction(t *testing.T) {
	trigger := func() ast.Node {
		return ast.NewCreateTrigger("users_touch", "users").
			SetTiming("BEFORE").
			SetEvent("UPDATE").
			SetBody("NEW.updated_at = NOW(); RETURN NEW;")
	}

	t.Run("triggers on, declared functions off", func(t *testing.T) {
		c := qt.New(t)

		renderer := postgres.NewWithCapabilities(capability.Postgres16().With(capability.Functions, false), platform.Postgres)

		sql, err := renderer.Render(trigger())

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, "CREATE OR REPLACE FUNCTION")
		c.Assert(sql, qt.Contains, "CREATE TRIGGER")
	})

	t.Run("triggers off takes the linked function with it", func(t *testing.T) {
		c := qt.New(t)

		caps := capability.Postgres16().
			With(capability.Triggers, false).
			With(capability.CreateOrReplaceTrigger, false)
		renderer := postgres.NewWithCapabilities(caps, platform.Postgres)

		sql, err := renderer.Render(trigger())

		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Not(qt.Contains), "CREATE OR REPLACE FUNCTION")
		c.Assert(sql, qt.Contains, "-- POSTGRES: trigger users_touch is not supported by this target; skipped.")
	})
}

// TestPostgreSQLRenderer_ObjectKindsFollowThePreset checks that the presets
// reach the renderer, rather than the renderer being exercised only against
// hand-built sets.
//
// Spanner is the row that motivated the keys: it shares this renderer with
// PostgreSQL and hosts none of the three refused kinds. The PostgreSQL control
// beside it is what separates "the preset decided" from "the renderer refuses
// everything".
func TestPostgreSQLRenderer_ObjectKindsFollowThePreset(t *testing.T) {
	for _, tc := range objectKindCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			spannerRenderer := postgres.NewWithCapabilities(capability.SpannerPostgres(), platform.Spanner)
			postgresRenderer := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)

			spannerSQL, err := spannerRenderer.Render(tc.create())
			c.Assert(err, qt.IsNil)
			postgresSQL, err := postgresRenderer.Render(tc.create())
			c.Assert(err, qt.IsNil)

			skipped := fmt.Sprintf("-- SPANNER: %s %s is not supported by this target; skipped.\n", tc.kind, tc.object)
			c.Assert(spannerSQL == skipped, qt.Equals, !capability.SpannerPostgres().Has(tc.key),
				qt.Commentf("spanner rendered:\n%s", spannerSQL))
			c.Assert(postgresSQL, qt.Contains, tc.ddl,
				qt.Commentf("the PostgreSQL control must still emit the %s", tc.kind))
		})
	}
}
