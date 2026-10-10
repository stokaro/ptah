//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// spannerRowDeletionSource declares table through the annotations a user
// writes: a bigint key, a timestamptz column per name in columns, and the
// Spanner row deletion properties in policy, none when it is empty.
func spannerRowDeletionSource(c *qt.C, table, policy string, columns ...string) *schemamodel.Database {
	c.Helper()
	var fields strings.Builder
	for _, column := range columns {
		fmt.Fprintf(&fields, "\t//ptah:schema:field name=%q type=\"TIMESTAMPTZ\"\n\tF%s string\n", column, column)
	}
	database, err := goschema.ParseSource("events.go", fmt.Sprintf(`package entities

//ptah:schema:table name=%q %s
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
%s}
`, table, policy, fields.String()))
	c.Assert(err, qt.IsNil)
	return &database
}

// spannerRowDeletionPolicy names a Spanner row deletion policy in the
// annotation spelling.
func spannerRowDeletionPolicy(column, interval string) string {
	return fmt.Sprintf(`platform.spanner.row_deletion_column=%q platform.spanner.row_deletion_interval=%q`, column, interval)
}

// spannerLiveTable narrows a read of the shared test database to one table, so
// a plan against it says nothing about the tables other tests leave behind.
func spannerLiveTable(c *qt.C, conn *dbschema.DatabaseConnection, table string) *catalog.Database {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	return &catalog.Database{
		// The coverage keeps no record of a table the narrowed read leaves out,
		// since a record whose table is not there names no owner.
		FeatureCoverage: live.FeatureCoverage.SelectSubjects(func(subject objectidentity.ID) bool {
			return subject.Kind != objectidentity.KindTable || subject.Name.Source == table
		}),
		Schemas:      live.Schemas,
		Tables:       slices.DeleteFunc(slices.Clone(live.Tables), func(t catalog.Table) bool { return t.Name != table }),
		Indexes:      slices.DeleteFunc(slices.Clone(live.Indexes), func(i catalog.Index) bool { return i.TableName != table }),
		Constraints:  slices.DeleteFunc(slices.Clone(live.Constraints), func(k catalog.Constraint) bool { return k.TableName != table }),
		NotDescribed: live.NotDescribed,
	}
}

// spannerPlan plans the statements that take table to declared.
func spannerPlan(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database, table string) []string {
	c.Helper()
	info := conn.Info()
	engine := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, spannerLiveTable(c, conn, table), info, nil, engine)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		c.Context(), engine, diff, info.Dialect, planner.Options{Capabilities: info.Capabilities},
	)
	c.Assert(err, qt.IsNil)
	return statements
}

// spannerApply runs each statement on its own, since Spanner refuses DDL
// inside an explicit transaction.
func spannerApply(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

// spannerObservedPolicy is the row deletion policy a read found on table, nil
// for none.
func spannerObservedPolicy(c *qt.C, conn *dbschema.DatabaseConnection, table string) *spannerschema.ObservedRowDeletion {
	c.Helper()
	live := spannerLiveTable(c, conn, table)
	c.Assert(live.Tables, qt.HasLen, 1)
	observed, _, err := schemaext.FacetAs[*spannerschema.ObservedRowDeletion](live.Tables[0].Facets, spannerschema.RowDeletionKind)
	c.Assert(err, qt.IsNil)
	return observed
}

// spannerPolicyNote is the comment the plan writes above a row deletion
// policy statement.
func spannerPolicyNote(table string) string {
	return "-- Row deletion policy on table: " + table + "\n-- ALTER statements: --\n"
}

// spannerDropTable drops table, for cleanup.
func spannerDropTable(conn *dbschema.DatabaseConnection, table string) {
	_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`)
}

// TestSpannerLiveRowDeletionPolicyRoundTrip creates a table whose row deletion
// policy is declared through the annotations, reads it back in the spelling
// Spanner stores, and plans nothing after.
func TestSpannerLiveRowDeletionPolicyRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	table := fmt.Sprintf("ptah_sp_rdp_%d", time.Now().UnixNano())
	defer spannerDropTable(conn, table)

	declared := spannerRowDeletionSource(c, table, spannerRowDeletionPolicy("created_at", "30 days"), "created_at")
	created := spannerPlan(c, conn, declared, table)
	c.Assert(created, qt.DeepEquals, []string{
		"-- SPANNER TABLE: " + table + " --\n" +
			`CREATE TABLE "` + table + `" (
  "id" BIGINT PRIMARY KEY NOT NULL,
  "created_at" TIMESTAMPTZ
) TTL INTERVAL '30 days' ON "created_at"`,
	})
	spannerApply(c, conn, created)

	c.Assert(spannerPlan(c, conn, declared, table), qt.HasLen, 0)
	c.Assert(spannerObservedPolicy(c, conn, table), qt.DeepEquals,
		&spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"}})
}

// TestSpannerLiveRowDeletionPolicyChangesInPlace walks a policy through each
// transition on a table holding rows: added to a table without one, given
// another interval, moved to a column the same plan adds while the column it
// read is dropped, and removed. ADD and ALTER are not interchangeable on
// Spanner, a policy cannot name a column that does not exist yet, and a column
// a policy names cannot be dropped, so each plan is the statements in the
// order the server takes them, and each ends with nothing left to plan and the
// rows in place.
//
// The column drop carries no CASCADE: Spanner takes only RESTRICT, and the
// clause made every planned column removal fail (stokaro/ptah#4280).
func TestSpannerLiveRowDeletionPolicyChangesInPlace(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	table := fmt.Sprintf("ptah_sp_rdc_%d", time.Now().UnixNano())
	defer spannerDropTable(conn, table)

	spannerApply(c, conn, spannerPlan(c, conn, spannerRowDeletionSource(c, table, "", "created_at"), table))
	spannerApply(c, conn, []string{`INSERT INTO "` + table + `" ("id", "created_at") VALUES (1, NULL), (2, now())`})

	steps := []struct {
		name     string
		declared *schemamodel.Database
		want     []string
		observed *spannerschema.ObservedRowDeletion
	}{
		{
			name:     "a policy added",
			declared: spannerRowDeletionSource(c, table, spannerRowDeletionPolicy("created_at", "30 days"), "created_at"),
			want:     []string{spannerPolicyNote(table) + `ALTER TABLE "` + table + `" ADD TTL INTERVAL '30 days' ON "created_at"`},
			observed: &spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "4 WEEKS 2 DAYS"}},
		},
		{
			name:     "another interval",
			declared: spannerRowDeletionSource(c, table, spannerRowDeletionPolicy("created_at", "7 days"), "created_at"),
			want:     []string{spannerPolicyNote(table) + `ALTER TABLE "` + table + `" ALTER TTL INTERVAL '7 days' ON "created_at"`},
			observed: &spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "created_at", Interval: "7 DAYS"}},
		},
		{
			name:     "another column, added in the same plan, and the old one dropped",
			declared: spannerRowDeletionSource(c, table, spannerRowDeletionPolicy("expires_at", "7 days"), "expires_at"),
			want: []string{
				"-- Add/modify columns for table: " + table + "\n-- ALTER statements: --\n" +
					`ALTER TABLE "` + table + `" ADD COLUMN "expires_at" TIMESTAMPTZ`,
				spannerPolicyNote(table) + `ALTER TABLE "` + table + `" ALTER TTL INTERVAL '7 days' ON "expires_at"`,
				"-- Remove columns from table: " + table + "\n-- ALTER statements: --\n" +
					`ALTER TABLE "` + table + `" DROP COLUMN "created_at"`,
				"-- WARNING: Dropping column " + table + ".created_at - This will delete data!",
			},
			observed: &spannerschema.ObservedRowDeletion{Policy: spannerschema.Policy{Column: "expires_at", Interval: "7 DAYS"}},
		},
		{
			name:     "no policy",
			declared: spannerRowDeletionSource(c, table, "", "expires_at"),
			want:     []string{spannerPolicyNote(table) + `ALTER TABLE "` + table + `" DROP TTL`},
		},
	}
	for _, step := range steps {
		planned := spannerPlan(c, conn, step.declared, table)
		c.Assert(planned, qt.DeepEquals, step.want, qt.Commentf("step %q", step.name))
		spannerApply(c, conn, planned)
		c.Assert(spannerPlan(c, conn, step.declared, table), qt.HasLen, 0, qt.Commentf("step %q", step.name))
		c.Assert(spannerObservedPolicy(c, conn, table), qt.DeepEquals, step.observed, qt.Commentf("step %q", step.name))
	}
	var rows int64
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT COUNT(*) FROM "`+table+`"`).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, int64(2))
}
