package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/ydbstream"
)

const streamBody = "INSERT INTO sink SELECT * FROM source;"

func TestReadYQLStreamingQuery(t *testing.T) {
	for _, pool := range []string{"default", "`default`", "'default'"} {
		t.Run(pool, func(t *testing.T) {
			c := qt.New(t)
			body := "$f = ($x) -> { RETURN $x + 1; }; /* keep ; */ INSERT INTO sink SELECT $f(id) FROM source;"
			database, _, err := sqlschema.Read([]byte("CREATE STREAMING QUERY `jobs/copy.v1` WITH (RUN=FALSE, RESOURCE_POOL="+pool+") AS DO BEGIN\n"+body+"\nEND DO; CREATE TOPIC source;"), "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(database.StreamingQueries, qt.HasLen, 1)
			query := database.StreamingQueries[0]
			c.Assert(query.Name, qt.Equals, "copy.v1")
			c.Assert(query.Schema, qt.Equals, "jobs")
			c.Assert(query.Spec, qt.DeepEquals, ast.StreamingQuerySpec{Text: body, Run: new(false), ResourcePool: "default"})
			c.Assert(query.AllowStateReset, qt.IsFalse)
			c.Assert(database.Topics, qt.HasLen, 1)
		})
	}
}

func TestReadYQLStreamingQueryGuardsAcrossFiles(t *testing.T) {
	for _, test := range []struct {
		name, prefix, wantBody string
		reset                  bool
	}{
		{name: "guard keeps first", prefix: "CREATE STREAMING QUERY IF NOT EXISTS", wantBody: streamBody},
		{name: "replace updates", prefix: "CREATE OR REPLACE STREAMING QUERY", wantBody: "SELECT 2;", reset: true},
		{name: "replace with guard", prefix: "CREATE OR REPLACE STREAMING QUERY IF NOT EXISTS", wantBody: streamBody},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte("CREATE STREAMING QUERY `jobs.copy` AS DO BEGIN "+streamBody+" END DO; CREATE STREAMING QUERY `jobs/copy` AS DO BEGIN SELECT 1; END DO;"), "ydb")
			c.Assert(err, qt.IsNil)
			added, _, err := sqlschema.ReadOnto([]byte(test.prefix+" `jobs.copy` AS DO BEGIN SELECT 2; END DO;"), "ydb", sqlschema.NewDocument(&database))
			c.Assert(err, qt.IsNil)
			c.Assert(added.StreamingQueries, qt.HasLen, 0)
			c.Assert(database.StreamingQueries[0].Spec.Text, qt.Equals, test.wantBody)
			c.Assert(database.StreamingQueries[0].AllowStateReset, qt.Equals, test.reset)
			c.Assert(database.StreamingQueries[1].Spec.Text, qt.Equals, "SELECT 1;")
		})
	}
}

func TestReadYQLStreamingQueryRoundTrip(t *testing.T) {
	for _, prefix := range []string{"CREATE", "CREATE OR REPLACE"} {
		t.Run(prefix, func(t *testing.T) {
			c := qt.New(t)
			source := prefix + " STREAMING QUERY `jobs/copy` WITH (RUN=FALSE) AS DO BEGIN DEFINE ACTION $copy() AS INSERT INTO sink SELECT * FROM source; END DEFINE; DO $copy(); END DO;"
			database, _, err := sqlschema.Read([]byte(source), "ydb")
			c.Assert(err, qt.IsNil)
			caps := capability.YDB262().With(capability.StreamingQueries, true)
			rendered, err := builtin.GetOrderedCreateStatementsWithCapabilities(&database, "ydb", caps)
			c.Assert(err, qt.IsNil)
			again, _, err := sqlschema.Read([]byte(strings.Join(rendered, "\n")), "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(ydbstream.Equal(database.StreamingQueries[0].Spec, again.StreamingQueries[0].Spec), qt.IsTrue)
			c.Assert(again.StreamingQueries[0].AllowStateReset, qt.Equals, database.StreamingQueries[0].AllowStateReset)
		})
	}
}

func TestReadYQLStreamingQueryRefusals(t *testing.T) {
	for _, source := range []string{
		"CREATE STREAMING QUERY q AS SELECT 1;",
		"CREATE STREAMING QUERY q AS DO BEGIN END DO;",
		"CREATE STREAMING QUERY q AS DO BEGIN DROP TABLE events; END DO;",
		"CREATE STREAMING QUERY q AS DO BEGIN SELECT 1; END DO; DROP TABLE events;",
		"CREATE STREAMING QUERY q AS DO BEGIN SELECT 1; END;",
		"CREATE STREAMING QUERY q WITH (run='false') AS DO BEGIN SELECT 1; END DO;",
		"CREATE STREAMING QUERY q WITH (run=TRUE, run=FALSE) AS DO BEGIN SELECT 1; END DO;",
		"CREATE STREAMING QUERY q WITH (resource_pool=$pool) AS DO BEGIN SELECT 1; END DO;",
		"CREATE STREAMING QUERY q WITH (resource_pool=pool + suffix) AS DO BEGIN SELECT 1; END DO;",
		"CREATE STREAMING QUERY q WITH (force=TRUE) AS DO BEGIN SELECT 1; END DO;",
		"CREATE STREAMING QUERY `/local/q` AS DO BEGIN SELECT 1; END DO;",
		"CREATE STREAMING QUERY q AS DO BEGIN SELECT 1; END DO; CREATE STREAMING QUERY q AS DO BEGIN SELECT 2; END DO;",
	} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(source), "ydb")
			c.Assert(err, qt.IsNotNil)
			c.Assert(statements, qt.IsNil)
			c.Assert(database.StreamingQueries, qt.HasLen, 0)
		})
	}
}

func TestReadYQLStreamingQueryGuardDoesNotAuthorizeReset(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE OR REPLACE STREAMING QUERY IF NOT EXISTS copy AS DO BEGIN SELECT 1; END DO;"), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.StreamingQueries[0].AllowStateReset, qt.IsFalse)
}
