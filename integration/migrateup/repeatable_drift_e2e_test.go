//go:build integration

package migrateup_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// These tests run `ptah migrations up --check-drift` against PostgreSQL after
// the directory's repeatable migration applied. A second versioned migration
// is pending, so a check that passes lets it apply and one that does not
// leaves it pending. The shadow database is a database of the test's own.

const repeatableDriftSecond = "CREATE TABLE account_notes (id INT PRIMARY KEY);\n"

// newRepeatableShadow is an empty PostgreSQL database for the replay, and a
// count of the relations its public schema holds.
func newRepeatableShadow(c *qt.C) (string, func(*qt.C) int) {
	c.Helper()
	address := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_rt_repeatable_shadow_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	url, err := atlasurl.WithDatabaseName(address, name)
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	relations := func(c *qt.C) int {
		c.Helper()
		var count int
		c.Assert(conn.QueryRowContext(c.Context(),
			`SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'`).Scan(&count), qt.IsNil)
		return count
	}
	return url, relations
}

// appliedForDrift applies the directory, then adds the second versioned
// migration and runs each statement of outOfBand on the target.
func appliedForDrift(c *qt.C, outOfBand ...string) *repeatableE2ETarget {
	c.Helper()
	r := newRepeatableE2ETarget(c)
	result, _ := r.up(c)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
	r.write(c, "0000000002_account_notes.up.sql", repeatableDriftSecond)
	r.write(c, "0000000002_account_notes.down.sql", "DROP TABLE account_notes;\n")
	for _, statement := range outOfBand {
		_, err := r.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	return r
}

// versions reads the versions the revision table records.
func (r *repeatableE2ETarget) versions(c *qt.C) []int64 {
	c.Helper()
	rows, err := r.conn.QueryContext(c.Context(), "SELECT version FROM schema_migrations ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	versions := make([]int64, 0)
	for rows.Next() {
		var version int64
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func TestRepeatableMigrationsDriftE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	r := appliedForDrift(c)
	shadow, relations := newRepeatableShadow(c)

	result, document := r.up(c, "--check-drift", "--shadow-db", shadow)

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
	c.Assert(document.DriftCheck, qt.DeepEquals, &migrator.DriftCheck{
		Outcome:             migrator.DriftClean,
		Replayed:            []int64{1},
		ReplayedKeys:        []string{"1"},
		ReplayedRepeatables: []string{"account_names"},
	})
	c.Assert(r.versions(c), qt.DeepEquals, []int64{1, 2})
	c.Assert(r.audit(c), qt.DeepEquals, []string{"v1"})
	c.Assert(relations(c), qt.Equals, 0)
}

func TestRepeatableMigrationsDriftE2E_FailurePath(t *testing.T) {
	t.Run("a view the repeatable migration created and the database lost is drift", func(t *testing.T) {
		c := qt.New(t)
		r := appliedForDrift(c, "DROP VIEW account_names")
		shadow, relations := newRepeatableShadow(c)

		result, document := r.up(c, "--check-drift", "--shadow-db", shadow)

		c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("%s", result.Stderr))
		c.Assert(document.DriftCheck.Outcome, qt.Equals, migrator.DriftDrifted)
		c.Assert(document.DriftCheck.ReplayedRepeatables, qt.DeepEquals, []string{"account_names"})
		c.Assert(document.DriftCheck.Findings, qt.HasLen, 1)
		c.Assert(document.DriftCheck.Findings[0].Kind, qt.Equals, "missing_view")
		c.Assert(r.versions(c), qt.DeepEquals, []int64{1})
		c.Assert(relations(c), qt.Equals, 0)
	})

	t.Run("a repeatable migration that changed after it applied cannot be replayed", func(t *testing.T) {
		c := qt.New(t)
		r := appliedForDrift(c)
		r.write(c, "R__account_names.sql", repeatableE2EView("v2", "name, id"))
		shadow, relations := newRepeatableShadow(c)

		result, document := r.up(c, "--check-drift", "--shadow-db", shadow)

		c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("%s", result.Stderr))
		c.Assert(document.DriftCheck, qt.DeepEquals, &migrator.DriftCheck{
			Outcome: migrator.DriftUnable,
			Stage:   "history",
			Reason:  migrator.UnreplayableChanged,
			Message: "the applied history cannot be replayed: repeatable migration account_names: " +
				"R__account_names.sql changed after it applied, and the content the database ran is not in the directory",
		})
		c.Assert(r.versions(c), qt.DeepEquals, []int64{1})
		c.Assert(r.audit(c), qt.DeepEquals, []string{"v1"})
		c.Assert(r.viewColumns(c), qt.DeepEquals, []string{"name"})
		c.Assert(relations(c), qt.Equals, 0)
	})
}
