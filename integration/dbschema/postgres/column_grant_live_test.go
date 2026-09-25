//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestPostgresColumnGrant_OnlyTheGrantedColumnsStayWritable applies the shape
// mosamlife/wpmgr's schema file writes -- a role that ALTER DEFAULT PRIVILEGES
// gives UPDATE on every new table, a revoke of the table privilege, and a
// grant of UPDATE on some columns -- and asks the server which columns the
// role can update. Only the server can say: the table revoke also clears the
// column ACLs, so the plan is right only if it revokes before it grants, and
// the second comparison is empty only if the catalog read reports the column
// ACLs the way the comparison keys them. After drift, a column granted by
// hand is revoked again.
func TestPostgresColumnGrant_OnlyTheGrantedColumnsStayWritable(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
	defer cancel()
	fixture := prepareRoutineGrantFixture(c, ctx)
	table := fixture.schema + ".proposals"
	desired, _, err := sqlschema.Read([]byte(
		"CREATE TABLE "+table+" (id bigint PRIMARY KEY, state text, note text, owner_id bigint);\n"+
			"REVOKE UPDATE ON "+table+" FROM "+fixture.role+";\n"+
			"GRANT UPDATE (state, note) ON "+table+" TO "+fixture.role+";"), platform.Postgres)
	c.Assert(err, qt.IsNil)

	fixture.apply(c, ctx, &desired)
	settled := fixture.compare(c, ctx, &desired)

	c.Assert(fixture.updatableColumns(c, ctx, table), qt.DeepEquals, map[string]bool{
		"id": false, "state": true, "note": true, "owner_id": false,
	})
	c.Assert(settled.GrantsAdded, qt.HasLen, 0, qt.Commentf("grants added: %#v", settled.GrantsAdded))
	c.Assert(settled.GrantsRemoved, qt.HasLen, 0, qt.Commentf("grants removed: %#v", settled.GrantsRemoved))

	fixture.exec(c, ctx, "GRANT UPDATE (owner_id) ON "+table+" TO "+fixture.role)
	drifted := fixture.apply(c, ctx, &desired)

	c.Assert(drifted.GrantsRemoved, qt.HasLen, 1, qt.Commentf("grants removed: %#v", drifted.GrantsRemoved))
	c.Assert(drifted.GrantsRemoved[0].Column, qt.Equals, "owner_id")
	c.Assert(fixture.updatableColumns(c, ctx, table)["owner_id"], qt.IsFalse)
}

// updatableColumns answers, for each column of table, whether the fixture role
// can update it.
func (f routineGrantFixture) updatableColumns(c *qt.C, ctx context.Context, table string) map[string]bool {
	c.Helper()
	rows, err := f.conn.QueryContext(ctx, `
		SELECT a.attname, has_column_privilege($1, $2::regclass, a.attname, 'UPDATE')
		FROM pg_attribute a
		WHERE a.attrelid = $2::regclass AND a.attnum > 0 AND NOT a.attisdropped`, f.role, table)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	updatable := make(map[string]bool)
	for rows.Next() {
		var column string
		var held bool
		c.Assert(rows.Scan(&column, &held), qt.IsNil)
		updatable[column] = held
	}
	c.Assert(rows.Err(), qt.IsNil)
	return updatable
}
