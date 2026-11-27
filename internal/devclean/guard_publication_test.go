package devclean_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devclean/replay"
)

// publicationStatements are the publication statements a rehearsal of a
// desired schema runs, in each spelling the renderer writes.
var publicationStatements = []string{
	`CREATE PUBLICATION "Orders Feed" FOR TABLE ONLY "public"."orders" WITH (publish = 'insert, update')`,
	`CREATE PUBLICATION "everything" FOR ALL TABLES`,
	`ALTER PUBLICATION "Orders Feed" ADD TABLE ONLY "public"."items"`,
	`ALTER PUBLICATION "Orders Feed" SET (publish = 'insert', publish_via_partition_root = false)`,
	`ALTER PUBLICATION "Orders Feed" OWNER TO "replicator"`,
	`DROP PUBLICATION IF EXISTS "Orders Feed"`,
}

// claimedPublications is the scope of a claim that listed the dev database's
// objects and found Supabase's publication, one with a quoted name, and one
// whose unquoted name starts with a letter PostgreSQL does not fold.
func claimedPublications() replay.PublicationScope {
	return replay.PublicationScope{Dropped: true, Kept: []string{"supabase_realtime", "Kept Feed", "Étoile"}}
}

// TestDevReplayGuard_PostgreSQLPublication_HappyPath pins that a PostgreSQL
// replay runs a publication's DDL where every cleanup of the dev database drops
// a publication its claim did not find, so long as the statement names none
// the claim found.
func TestDevReplayGuard_PostgreSQLPublication_HappyPath(t *testing.T) {
	info := catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: "postgres://dev@pg:5432/ptah_dev"}
	statements := append(slices.Clone(publicationStatements),
		`DROP PUBLICATION IF EXISTS "Orders Feed", "everything" CASCADE`,
		`ALTER PUBLICATION "kept feed" ADD TABLE ONLY "public"."items"`,
		`ALTER PUBLICATION Supabase_Realtime_Copy OWNER TO "replicator"`,
	)
	for _, statement := range statements {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			guard := devclean.NewDevReplayGuard(info).WithPublications(claimedPublications)
			c.Assert(guard.ValidateStatement(statement), qt.IsNil)
		})
	}
}

// TestDevReplayGuard_PostgreSQLPublication_FailurePath pins the publication
// statements a replay still refuses: every one where no cleanup drops the
// run's publications, a change to or the drop of a publication the claim
// found, another engine of the family, and a subscription, which reaches past
// the dev database.
func TestDevReplayGuard_PostgreSQLPublication_FailurePath(t *testing.T) {
	yugabyte := catalog.ServerInfo{Dialect: platform.YugabyteDB, Schema: "public", URL: "postgres://dev@yb:5433/ptah_dev"}
	postgres := catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: "postgres://dev@pg:5432/ptah_dev"}
	unclaimed := func() replay.PublicationScope { return replay.PublicationScope{Kept: []string{"supabase_realtime"}} }
	tests := []struct {
		name         string
		info         catalog.ServerInfo
		publications func() replay.PublicationScope
		statement    string
		wantErr      string
	}{
		{"a claim that listed nothing", postgres, unclaimed, `CREATE PUBLICATION p FOR TABLE ONLY "public"."orders"`,
			`postgres migration replay rejects CREATE PUBLICATION because its effects cannot be confined to the disposable database realm.*`},
		{"no claim at all", postgres, nil, `DROP PUBLICATION IF EXISTS p`,
			`postgres migration replay rejects DROP PUBLICATION because its effects cannot be confined to the disposable database realm.*`},
		{"a member added to a kept publication", postgres, claimedPublications,
			`ALTER PUBLICATION supabase_realtime ADD TABLE ONLY "public"."orders"`,
			`postgres migration replay rejects ALTER PUBLICATION of the dev database's own publication "supabase_realtime" because its effects cannot be confined to the disposable database realm.*`},
		{"a kept publication renamed", postgres, claimedPublications, `ALTER PUBLICATION Supabase_Realtime RENAME TO feed`,
			`postgres migration replay rejects ALTER PUBLICATION of the dev database's own publication "supabase_realtime" because its effects cannot be confined to the disposable database realm.*`},
		{"a kept publication dropped", postgres, claimedPublications, `DROP PUBLICATION supabase_realtime`,
			`postgres migration replay rejects DROP PUBLICATION of the dev database's own publication "supabase_realtime" because its effects cannot be confined to the disposable database realm.*`},
		{"a kept publication dropped beside the run's", postgres, claimedPublications,
			`DROP PUBLICATION IF EXISTS run_feed, "Kept Feed" CASCADE`,
			`postgres migration replay rejects DROP PUBLICATION of the dev database's own publication "Kept Feed" because its effects cannot be confined to the disposable database realm.*`},
		{"a kept publication whose name folds only in ASCII", postgres, claimedPublications, `DROP PUBLICATION Étoile`,
			`postgres migration replay rejects DROP PUBLICATION of the dev database's own publication "Étoile" because .*`},
		{"a kept publication named with a Unicode escape", postgres, claimedPublications, `DROP PUBLICATION U&"supabase\005frealtime"`,
			`postgres migration replay rejects DROP PUBLICATION of the dev database's own publication "supabase_realtime" because .*`},
		{"YugabyteDB publication", yugabyte, claimedPublications, `CREATE PUBLICATION p FOR TABLE ONLY "public"."orders"`,
			`yugabytedb migration replay rejects CREATE PUBLICATION because its effects cannot be confined to the disposable database realm.*`},
		{"YugabyteDB publication change", yugabyte, claimedPublications, `ALTER PUBLICATION p ADD TABLE ONLY "public"."items"`,
			`yugabytedb migration replay rejects ALTER PUBLICATION because its effects cannot be confined to the disposable database realm.*`},
		{"PostgreSQL subscription", postgres, claimedPublications, `CREATE SUBSCRIPTION s CONNECTION 'host=elsewhere' PUBLICATION p`,
			`postgres migration replay rejects CREATE SUBSCRIPTION because its effects cannot be confined to the disposable database realm.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			guard := devclean.NewDevReplayGuard(test.info).WithPublications(test.publications)
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}
