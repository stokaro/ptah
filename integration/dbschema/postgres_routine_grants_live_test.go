//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestPostgresLiveRoutineGrantReadKeepsOwnerAfterPublicRevoke pins the
// catalog distinction a rollback needs: a routine with no PUBLIC EXECUTE
// still has an owner marker, while a read that excluded grants has no marker.
func TestPostgresLiveRoutineGrantReadKeepsOwnerAfterPublicRevoke(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_fnacl_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	_, err = conn.ExecContext(ctx, `CREATE FUNCTION "`+schemaName+`"."f"() RETURNS integer LANGUAGE sql AS $$SELECT 1$$`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `REVOKE EXECUTE ON FUNCTION "`+schemaName+`"."f"() FROM PUBLIC`)
	c.Assert(err, qt.IsNil)

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	grants := slices.DeleteFunc(slices.Clone(live.Grants), func(grant catalog.Grant) bool {
		return grant.ObjectType != "FUNCTION" || grant.ObjectName != "f"
	})
	c.Assert(slices.ContainsFunc(grants, func(grant catalog.Grant) bool {
		return grant.Implicit && grant.Role != "PUBLIC" && grant.Privilege == "EXECUTE"
	}), qt.IsTrue)
	c.Assert(slices.ContainsFunc(grants, func(grant catalog.Grant) bool {
		return grant.Role == "PUBLIC"
	}), qt.IsFalse)

	_, err = conn.ExecContext(ctx, `REVOKE EXECUTE ON FUNCTION "`+schemaName+`"."f"() FROM CURRENT_USER`)
	c.Assert(err, qt.IsNil)
	live, err = dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	grants = slices.DeleteFunc(slices.Clone(live.Grants), func(grant catalog.Grant) bool {
		return grant.ObjectType != "FUNCTION" || grant.ObjectName != "f"
	})
	c.Assert(slices.ContainsFunc(grants, func(grant catalog.Grant) bool {
		return grant.Implicit && grant.Role != "PUBLIC" && grant.Privilege == ""
	}), qt.IsTrue)
	c.Assert(slices.ContainsFunc(grants, func(grant catalog.Grant) bool {
		return grant.Privilege == "EXECUTE"
	}), qt.IsFalse)
}
