package capability_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
)

// rollbackRow is one CockroachDB banner and whether a migration wrapped in a
// transaction rolls back as a unit on the server that printed it.
//
// wantRollsBack is not a preference: it is what a rolled-back CREATE TABLE left
// behind when measured through Ptah's own driver against each release.
type rollbackRow struct {
	name          string
	banner        string
	wantRollsBack bool
}

// TestCockroachTransactionalDDLFollowsTheMeasuredReleases pins the boundary
// where CockroachDB stopped rolling schema changes back.
//
// The setting `autocommit_before_ddl` arrived in v24 defaulted off and became
// on in v25, and with it on a schema statement inside a transaction commits
// that transaction before running. The two lines below that boundary keep the
// capability; the ones above it do not (stokaro/ptah#1849).
func TestCockroachTransactionalDDLFollowsTheMeasuredReleases(t *testing.T) {
	rows := []rollbackRow{{
		name:          "v23.2 has no such setting and rolls back",
		banner:        "CockroachDB CCL v23.2.30 (x86_64-pc-linux-gnu)",
		wantRollsBack: true,
	}, {
		name:          "v24.3 has the setting defaulted off and rolls back",
		banner:        "CockroachDB CCL v24.3.20 (x86_64-pc-linux-gnu)",
		wantRollsBack: true,
	}, {
		name:          "v25.4 is where the default flipped",
		banner:        "CockroachDB CCL v25.4.5 (x86_64-pc-linux-gnu)",
		wantRollsBack: false,
	}, {
		name:          "v26.2 inherits the flipped default",
		banner:        "CockroachDB CCL v26.2.5 (x86_64-pc-linux-gnu)",
		wantRollsBack: false,
	}, {
		name:          "v26.3 inherits it too",
		banner:        "CockroachDB CCL v26.3.0 (x86_64-pc-linux-gnu)",
		wantRollsBack: false,
	}}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			resolved := capability.ResolveServerVersion("cockroachdb", row.banner)
			c.Assert(resolved.Capabilities.Has(capability.TransactionalDDL), qt.Equals, row.wantRollsBack,
				qt.Commentf("banner=%q", row.banner))

			// The scope control. DDLInsideTransaction is a different question
			// -- whether the server takes the statement inside a transaction at
			// all -- and CockroachDB takes it on every line here. Answering the
			// rollback question must not quietly answer this one too.
			c.Assert(resolved.Capabilities.Has(capability.DDLInsideTransaction), qt.IsTrue,
				qt.Commentf("banner=%q", row.banner))
		})
	}
}

// TestPostgresKeepsTransactionalDDL is the family control: the boundary above
// belongs to CockroachDB, and moving it must not take PostgreSQL with it.
func TestPostgresKeepsTransactionalDDL(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		banner  string
	}{{
		name:    "postgresql",
		dialect: "postgres",
		banner:  "PostgreSQL 18.4 on x86_64-pc-linux-gnu",
	}, {
		name:    "yugabytedb",
		dialect: "yugabytedb",
		banner:  "PostgreSQL 15.2-YB-2026.1.1.0-b0 on x86_64-pc-linux-gnu",
	}}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			resolved := capability.ResolveServerVersion(row.dialect, row.banner)
			c.Assert(resolved.Capabilities.Has(capability.TransactionalDDL), qt.IsTrue,
				qt.Commentf("banner=%q", row.banner))
		})
	}
}
