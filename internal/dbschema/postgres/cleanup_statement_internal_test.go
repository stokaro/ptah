package postgres

// White-box testing required: the builder stands in for a server-side format()
// call inside the cleanup query, and no exported call reports which side
// assembled a DROP.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
)

// builtDropRow is one object the query can return and the statement Go must
// assemble for it when the server cannot.
type builtDropRow struct {
	name   string
	object postgresCleanupObject
	caps   capability.Capabilities
	want   string
}

func TestBuildCleanupStatementMatchesTheServerSideForm(t *testing.T) {
	rows := []builtDropRow{{
		name:   "a table is dropped by its qualified name",
		object: postgresCleanupObject{Kind: "table", Schema: "public", Name: "users"},
		caps:   capability.Postgres17(),
		want:   `DROP TABLE IF EXISTS "public"."users" RESTRICT`,
	}, {
		name:   "a materialized view keeps both words",
		object: postgresCleanupObject{Kind: "materialized view", Schema: "public", Name: "user_stats"},
		caps:   capability.Postgres17(),
		want:   `DROP MATERIALIZED VIEW IF EXISTS "public"."user_stats" RESTRICT`,
	}, {
		// An extension is database-scoped; qualifying it is a syntax error.
		name:   "an extension is dropped unqualified",
		object: postgresCleanupObject{Kind: "extension", Schema: "public", Name: "hstore"},
		caps:   capability.Postgres17(),
		want:   `DROP EXTENSION IF EXISTS "hstore" RESTRICT`,
	}, {
		// A constraint is reached through its table, which is why the query
		// carries a qualifier column at all.
		name: "a constraint is dropped through its table",
		object: postgresCleanupObject{
			Kind: "constraint", Schema: "public", Name: "users_parent_fkey", Qualifier: "users",
		},
		caps: capability.Postgres17(),
		want: `ALTER TABLE "public"."users" DROP CONSTRAINT IF EXISTS "users_parent_fkey" RESTRICT`,
	}, {
		// Spanner's PostgreSQL interface accepts the guard on DROP and refuses
		// it inside ALTER, so the constraint drop is the one statement here
		// that loses it. Measured: `<IF [NOT] EXISTS> is not supported in
		// <ALTER> statement operations` (SQLSTATE P0001), while the same
		// statement without the guard is accepted (stokaro/ptah#1811).
		name: "a target refusing the guard inside ALTER drops the constraint unguarded",
		object: postgresCleanupObject{
			Kind: "constraint", Schema: "public", Name: "users_parent_fkey", Qualifier: "users",
		},
		caps: capability.SpannerPostgres(),
		want: `ALTER TABLE "public"."users" DROP CONSTRAINT "users_parent_fkey" RESTRICT`,
	}, {
		// And the same target keeps the guard everywhere it is accepted, so the
		// capability narrows one statement rather than the whole cleanup.
		name:   "the same target keeps the guard on a table drop",
		object: postgresCleanupObject{Kind: "table", Schema: "public", Name: "users"},
		caps:   capability.SpannerPostgres(),
		want:   `DROP TABLE IF EXISTS "public"."users" RESTRICT`,
	}, {
		// Spanner accepts the guard here too, which is what lets the index
		// drop stay unconditional. Measured on the emulator through PGAdapter
		// 0.55.2: `DROP INDEX IF EXISTS "public"."absent" RESTRICT` answers ok
		// on an index that was never there (stokaro/ptah#1901).
		name:   "an index is dropped by its qualified name",
		object: postgresCleanupObject{Kind: "index", Schema: "public", Name: "users_email_idx"},
		caps:   capability.SpannerPostgres(),
		want:   `DROP INDEX IF EXISTS "public"."users_email_idx" RESTRICT`,
	}, {
		// The identifiers come from a catalog, and %I in the query this
		// replaces is what keeps a hostile one from ending the statement early.
		name:   "an identifier with a quote in it stays one identifier",
		object: postgresCleanupObject{Kind: "table", Schema: "public", Name: `a"b`},
		caps:   capability.Postgres17(),
		want:   `DROP TABLE IF EXISTS "public"."a""b" RESTRICT`,
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statement, err := buildCleanupStatement(row.object, row.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(statement, qt.Equals, row.want)
		})
	}
}

func TestBuildCleanupStatementRefusesRatherThanSkips(t *testing.T) {
	c := qt.New(t)

	// A kind with no known DROP must stop the cleanup. Skipping it would report
	// a clean database that still holds the object, which is the failure this
	// whole path exists to avoid.
	_, err := buildCleanupStatement(postgresCleanupObject{Kind: "function", Schema: "public", Name: "f"}, capability.Postgres17())
	c.Assert(err, qt.ErrorMatches, `cleanup: no DROP statement is known for function "f".*`)

	// A constraint without its table is the same class of problem: the
	// statement cannot be built, and guessing the table would drop the wrong
	// constraint on a schema that has two by that name.
	_, err = buildCleanupStatement(postgresCleanupObject{Kind: "constraint", Schema: "public", Name: "fk"}, capability.Postgres17())
	c.Assert(err, qt.ErrorMatches, `cleanup: constraint "fk" names no table.*`)
}

func TestCleanupQueryAsksTheServerToAssembleWhereItCan(t *testing.T) {
	c := qt.New(t)

	withFormat := applyCleanupDropExpressions("{{DROP_EXPR_1}}", assembleDropsOnServer)
	withoutFormat := applyCleanupDropExpressions("{{DROP_EXPR_1}}", assembleDropsInGo)

	c.Assert(withFormat, qt.Contains, "format(")
	c.Assert(withoutFormat, qt.Equals, "NULL::text")
}
