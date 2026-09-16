//go:build integration

package integration_test

// What `ptah-compat schema apply` does with a plan PostgreSQL refuses inside a
// transaction.
//
// The compatibility surface carried its own check, a text search for
// CONCURRENTLY. It saw one of the two shapes the engine refuses and let the
// other one through: a value added to an existing enum type and then used in
// the same plan reached the server, which answered with a bare 55P04 naming
// neither the statement nor the flag (stokaro/ptah#3326). Native
// `ptah schema apply` had the same gap and closed it in #3283 by asking
// internal/txrequire; this asks the same rule.
//
// The concurrent-index row is the control in both directions. It held under the
// text search, and it is the one finding the surface still answers in Atlas's
// terms, naming the diff policy that produced the statement rather than only
// the flag.

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for the dev database below

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// TestAtlasSchemaApplyTransactionPreflightE2E_FailurePath drives the shipped
// compatibility binary at a plan the engine cannot run inside a transaction.
func TestAtlasSchemaApplyTransactionPreflightE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		setup       []string
		desired     string
		wantNamed   string
		witness     string
		witnessName string
	}{
		{
			name:        "an enum value added and used in one plan",
			setup:       txPreflightEnumSetup,
			desired:     txPreflightEnumDesired,
			wantNamed:   "it uses 'archived'",
			witness:     txPreflightArchivedLabel,
			witnessName: "probe_status",
		},
		{
			name:  "a concurrent index build names its diff policy",
			setup: []string{"CREATE TABLE widgets (id integer NOT NULL, a integer)"},
			desired: "CREATE TABLE widgets (id integer NOT NULL, a integer);\n" +
				"CREATE INDEX CONCURRENTLY idx_widgets_a ON widgets (a);\n",
			wantNamed:   "atlas.hcl diff.concurrent_index.create requires --tx-mode none",
			witness:     "SELECT count(*) FROM pg_indexes WHERE indexname = $1",
			witnessName: "idx_widgets_a",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			target := newTxPreflightTarget(c, ctx, test.setup)
			devURL := newAtlasPreflightDevURL(c, ctx)
			work := writeTxPreflightWorkdir(c, test.desired, "{}\n")

			applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: work},
				"schema", "apply",
				"--url", target.url,
				"--to", "file://"+filepath.Join(work, "desired.sql"),
				"--dev-url", devURL,
				"--auto-approve")

			comment := txPreflightCommentf(applied)
			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, comment)
			c.Assert(applied.Stderr, qt.Contains, test.wantNamed, comment)
			// The server was never asked. Its own refusals carry a SQLSTATE.
			c.Assert(applied.Stderr, qt.Not(qt.Contains), "SQLSTATE", comment)
			c.Assert(txPreflightCount(c, ctx, target, test.witness, test.witnessName), qt.Equals, 0, comment)
		})
	}
}

// TestAtlasSchemaApplyTransactionPreflightE2E_HappyPath is the way through the
// refusal, and the plan the refusal must not claim.
func TestAtlasSchemaApplyTransactionPreflightE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	target := newTxPreflightTarget(c, ctx, txPreflightEnumSetup)
	devURL := newAtlasPreflightDevURL(c, ctx)
	work := writeTxPreflightWorkdir(c, txPreflightEnumDesired, "{}\n")

	applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: work},
		"schema", "apply",
		"--url", target.url,
		"--to", "file://"+filepath.Join(work, "desired.sql"),
		"--dev-url", devURL,
		"--tx-mode", "none",
		"--auto-approve")

	comment := txPreflightCommentf(applied)
	c.Assert(applied.ExitCode, qt.Equals, 0, comment)
	c.Assert(txPreflightCount(c, ctx, target, txPreflightArchivedLabel, "probe_status"), qt.Equals, 1, comment)
}

// newAtlasPreflightDevURL is an empty database the compatibility surface
// normalizes the desired file through. It is not the target, and nothing in
// these rows reads it back.
func newAtlasPreflightDevURL(c *qt.C, ctx context.Context) string {
	c.Helper()

	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	database := fmt.Sprintf("ptah_atlaspre3326_dev_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, database)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, database) })
	return replaceDatabaseName(c, adminURL, database)
}
