//go:build integration

package generator

import (
	"context"
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// TestMultiHostMixinFKModify_DownRoundTrip_Integration is the live-database gate
// for the issue #197 DOWN path. It drives the REAL generator (UP then the
// generated DOWN) against a live PostgreSQL / MySQL / MariaDB and proves the
// rollback applies cleanly and restores each host's PRIOR FK action per host.
//
// Scenario: an FK lives on several host tables at the prior ON DELETE NO ACTION.
// The UP changes it to ON DELETE CASCADE on every host (a multi-host modify, the
// shape an embedded inline-relation mixin produces). The generated DOWN must,
// for EACH host, drop the CASCADE constraint and re-add it with the prior action.
// Before the fix the reversed diff carried no ConstraintsAddedWithTables, so the
// DOWN add-path fell back to the name-only field scan: it re-added a single host
// and (on Postgres) dropped a single host via the information_schema LIMIT 1 DO
// block, so the 2nd host's re-add collided ("constraint already exists",
// SQLSTATE 42710) and the rollback aborted half-applied. With the fix the DOWN is
// table-qualified per host and applies clean.
//
// FK naming is dialect-aware: Postgres scopes constraint names per table, so the
// canonical mixin case shares ONE name (fk_entity_tenant) across all hosts — the
// exact #197 reproduction. MySQL/MariaDB scope FK names schema-GLOBALLY (a shared
// name is rejected at DDL time with errno 1826/121), so a representable
// multi-host mixin there uses a distinct name per host; the DOWN still exercises
// the same per-host DROP FOREIGN KEY + re-ADD generator path.
//
// Counterfactual (proven separately by neutering the fix): the pre-fix generator
// produces a DOWN that re-adds/drops only one host, so on Postgres the
// shared-name re-add errors 42710 at the "DOWN must apply cleanly" gate below.
func TestMultiHostMixinFKModify_DownRoundTrip_Integration(t *testing.T) {
	cases := []struct {
		dialect string
		envKey  string
	}{
		{"postgres", "POSTGRES_URL"},
		{"mysql", "MYSQL_URL"},
		{"mariadb", "MARIADB_URL"},
	}

	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			url := os.Getenv(tc.envKey)
			if url == "" {
				t.Skipf("skipping %s: %s not set", tc.dialect, tc.envKey)
			}

			c := qt.New(t)
			ctx := context.Background()

			conn, err := dbschema.ConnectToDatabase(ctx, url)
			if err != nil {
				t.Skipf("skipping %s: cannot connect: %v", tc.dialect, err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			dialect := conn.Info().Dialect
			hosts := []string{"ptah_loc", "ptah_area", "ptah_comm"}

			// Always start from a clean slate and tear down afterwards.
			dropAll := func() {
				for _, h := range hosts {
					_, _ = conn.Exec(dropTableSQL(dialect, h))
				}
				_, _ = conn.Exec(dropTableSQL(dialect, "ptah_tenants"))
			}
			dropAll()
			t.Cleanup(dropAll)

			// 1. Install the PRIOR schema: parent table + host tables, then add the
			//    tenant FK at the prior ON DELETE NO ACTION via explicit ALTER per
			//    host. We add the FK explicitly (not via the renderer's inline
			//    CREATE-TABLE FK) because the MySQL renderer omits field-level
			//    inline FKs — the ALTER form is dialect-portable.
			genPrior := roundTripSchema(dialect, "", hosts...)
			applyTablesOnly(c, conn, dialect, genPrior)
			for _, h := range hosts {
				_, err := conn.Exec("ALTER TABLE " + h + " ADD CONSTRAINT " + tenantFKName(dialect, h) +
					" FOREIGN KEY (tenant_id) REFERENCES ptah_tenants(id)")
				c.Assert(err, qt.IsNil, qt.Commentf("setup: add prior FK on %s", h))
			}

			dbPrior, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			priorRules := tenantDeleteRules(dialect, dbPrior, hosts)
			c.Assert(len(priorRules), qt.Equals, len(hosts),
				qt.Commentf("prior FK must exist on every host, got %v", priorRules))

			// 2. Target schema: the tenant FK now ON DELETE CASCADE.
			genCascade := roundTripSchema(dialect, "CASCADE", hosts...)

			upDiff := schemadiff.CompareWithDialect(genCascade, dbPrior, dialect)
			c.Assert(upDiff.HasChanges(), qt.IsTrue, qt.Commentf("UP must have changes"))

			// 3. Generate + apply the UP (the multi-host modify).
			upSQL, err := generateUpMigrationSQL(upDiff, genCascade, dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(upSQL, qt.Not(qt.Equals), "")
			execScript(c, conn, upSQL, "UP")

			dbAfterUp, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			afterUp := tenantDeleteRules(dialect, dbAfterUp, hosts)
			for _, h := range hosts {
				c.Assert(strings.ToUpper(afterUp[h]), qt.Equals, "CASCADE",
					qt.Commentf("after UP, %s tenant FK must be ON DELETE CASCADE, got %q", h, afterUp[h]))
			}

			// 4. Generate the DOWN from the original up diff + the PRIOR db schema
			//    (exactly what the generator does), then apply it. THIS IS THE GATE:
			//    the pre-fix DOWN errors with 42710 here on Postgres.
			downSQL, err := generateDownMigrationSQL(upDiff, genCascade, dbPrior, dialect)
			c.Assert(err, qt.IsNil)
			t.Logf("[%s] generated DOWN:\n%s", dialect, downSQL)
			execScript(c, conn, downSQL, "DOWN")

			// 5. Per-host action restored to the prior value, on every host.
			dbAfterDown, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			restored := tenantDeleteRules(dialect, dbAfterDown, hosts)
			for _, h := range hosts {
				c.Assert(restored[h], qt.Equals, priorRules[h],
					qt.Commentf("after DOWN, %s tenant FK action must be restored to %q, got %q",
						h, priorRules[h], restored[h]))
			}

			// 6. Idempotency: the DB now matches the prior schema again, so a fresh
			//    diff against the prior generated schema is clean (no churn loop).
			idemDiff := schemadiff.CompareWithDialect(genPrior, dbAfterDown, dialect)
			c.Assert(idemDiff.HasChanges(), qt.IsFalse,
				qt.Commentf("post-DOWN diff must be clean; added=%v removed=%v",
					idemDiff.ConstraintsAdded, idemDiff.ConstraintsRemoved))
		})
	}
}

// tenantFKName returns the FK constraint name for the tenant_id FK on the given
// host. Postgres scopes constraint names per table, so the canonical mixin case
// reuses ONE shared name on every host (the #197 reproduction). MySQL/MariaDB
// scope FK names schema-globally and reject a duplicate at DDL time, so there we
// use a distinct name per host (the same multi-host modify shape, representable).
func tenantFKName(dialect, host string) string {
	if dialect == "postgres" {
		return "fk_entity_tenant"
	}
	return "fk_" + host + "_tenant"
}

// roundTripSchema builds the generated schema for the round-trip: a parent
// ptah_tenants table plus the given host tables, each carrying a tenant_id field
// with the tenant FK (named per tenantFKName). onDelete sets the FK's ON DELETE
// action ("" = leave default / NO ACTION).
func roundTripSchema(dialect, onDelete string, hosts ...string) *goschema.Database {
	// VARCHAR(36) (not TEXT) so MySQL/MariaDB can index the PK and back the FK —
	// a TEXT column needs a key length there ("BLOB/TEXT used in key
	// specification without a key length").
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "PtahTenants", Name: "ptah_tenants"}},
		Fields: []goschema.Field{
			{StructName: "PtahTenants", Name: "id", Type: "VARCHAR(36)", Primary: true},
		},
	}
	for _, h := range hosts {
		structName := structNameFor(h)
		db.Tables = append(db.Tables, goschema.Table{StructName: structName, Name: h})
		db.Fields = append(db.Fields,
			goschema.Field{StructName: structName, Name: "id", Type: "VARCHAR(36)", Primary: true},
			goschema.Field{
				StructName:     structName,
				Name:           "tenant_id",
				Type:           "VARCHAR(36)",
				Nullable:       true,
				Foreign:        "ptah_tenants(id)",
				ForeignKeyName: tenantFKName(dialect, h),
				OnDelete:       onDelete,
			},
		)
	}
	return db
}

// tenantDeleteRules returns host -> ON DELETE rule for the host's tenant FK as
// the database reports it.
func tenantDeleteRules(dialect string, db *dbschematypes.DBSchema, hosts []string) map[string]string {
	wantName := make(map[string]string, len(hosts)) // host -> expected FK name
	for _, h := range hosts {
		wantName[h] = tenantFKName(dialect, h)
	}
	rules := make(map[string]string)
	for _, cs := range db.Constraints {
		if cs.Type != "FOREIGN KEY" {
			continue
		}
		if name, ok := wantName[cs.TableName]; !ok || name != cs.Name {
			continue
		}
		rule := ""
		if cs.DeleteRule != nil {
			rule = *cs.DeleteRule
		}
		rules[cs.TableName] = rule
	}
	return rules
}

// applyTablesOnly renders + applies the CREATE TABLE statements for the schema
// with all foreign= fields stripped, so the host tables exist with their columns
// but no FK yet. The named prior FK is then added explicitly by the caller (the
// renderer's inline FK emission is dialect-asymmetric — MySQL omits field-level
// inline FKs — so we keep setup engine-agnostic).
func applyTablesOnly(c *qt.C, conn *dbschema.DatabaseConnection, dialect string, gen *goschema.Database) {
	c.Helper()
	bare := *gen
	bare.Fields = make([]goschema.Field, 0, len(gen.Fields))
	for _, f := range gen.Fields {
		f.Foreign = ""
		f.ForeignKeyName = ""
		f.OnDelete = ""
		f.OnUpdate = ""
		bare.Fields = append(bare.Fields, f)
	}
	stmts := fromschema.FromDatabase(bare, dialect)
	sqlText, err := renderer.RenderSQL(dialect, stmts.Statements...)
	c.Assert(err, qt.IsNil)
	execScript(c, conn, sqlText, "SETUP")
}

// execScript splits a multi-statement SQL script the same way the migrator does
// (DO-block / dollar-quote aware) and executes each statement, failing the test
// on the first error. label names the phase for diagnostics — the DOWN phase is
// the gate that errored on the pre-fix generator. We use conn.Exec (the raw
// driver) rather than the Writer's transaction-scoped ExecuteSQL because DDL on
// MySQL/MariaDB auto-commits and the round-trip applies statements directly.
func execScript(c *qt.C, conn *dbschema.DatabaseConnection, sqlText, label string) {
	c.Helper()
	for _, stmt := range migrator.SplitSQLStatements(sqlText) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		_, err := conn.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("%s statement must apply cleanly:\n%s", label, stmt))
	}
}

func structNameFor(table string) string {
	parts := strings.Split(table, "_")
	for i, p := range parts {
		if p != "" {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}

func dropTableSQL(dialect, table string) string {
	if dialect == "mysql" || dialect == "mariadb" {
		return "DROP TABLE IF EXISTS " + table
	}
	return "DROP TABLE IF EXISTS " + table + " CASCADE"
}
