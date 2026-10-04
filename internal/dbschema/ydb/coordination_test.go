package ydb_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"google.golang.org/protobuf/proto"

	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/internal/ydbcoordination"
)

// fakeCoordination is a coordination service holding nodes by absolute path,
// and the log of the calls that change them.
type fakeCoordination struct {
	nodes map[string]*Ydb_Coordination.Config
	calls []string
	sent  []*Ydb_Coordination.Config
}

func (f *fakeCoordination) DescribeNode(_ context.Context, absolute string) (*Ydb_Coordination.DescribeNodeResult, error) {
	config, ok := f.nodes[absolute]
	if !ok {
		return nil, fmt.Errorf("describe YDB coordination node %s: %w: SCHEME_ERROR", absolute,
			ydbschema.ErrNoCoordinationNode)
	}
	return &Ydb_Coordination.DescribeNodeResult{Config: proto.Clone(config).(*Ydb_Coordination.Config)}, nil
}

func (f *fakeCoordination) CreateNode(_ context.Context, absolute string, config *Ydb_Coordination.Config) error {
	f.calls = append(f.calls, "create "+absolute)
	f.sent = append(f.sent, config)
	f.nodes[absolute] = config
	return nil
}

func (f *fakeCoordination) AlterNode(_ context.Context, absolute string, config *Ydb_Coordination.Config) error {
	f.calls = append(f.calls, "alter "+absolute)
	f.sent = append(f.sent, config)
	return nil
}

func (f *fakeCoordination) DropNode(_ context.Context, absolute string) error {
	f.calls = append(f.calls, "drop "+absolute)
	delete(f.nodes, absolute)
	return nil
}

// recognized reads one of Ptah's statements as the connection reads it.
func recognized(c *qt.C, text string) ydbcoordination.Query {
	c.Helper()
	query, ok, err := ydbcoordination.Recognize(text)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	return query
}

// A statement reaches the service with the path the connection resolves and
// the settings it names: a creation with only the settings it declares, a
// change with only the ones that change, and a drop by path.
func TestRunCoordinationStatement_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		root      string
		statement string
		wantCalls []string
		wantSent  []*Ydb_Coordination.Config
	}{
		{
			name:      "a creation at the root",
			root:      "/local",
			statement: "CREATE COORDINATION NODE `fresh` WITH (self_check_period = Interval('PT2S'))",
			wantCalls: []string{"create /local/fresh"},
			wantSent:  []*Ydb_Coordination.Config{{SelfCheckPeriodMillis: 2000}},
		},
		{
			name:      "a creation in a dev realm, under the realm's prefix",
			root:      "/local/ptah_dev/r1",
			statement: "PRAGMA TablePathPrefix(\"/local/ptah_dev/r1\");\nCREATE COORDINATION NODE `app/fresh`",
			wantCalls: []string{"create /local/ptah_dev/r1/app/fresh"},
			wantSent:  []*Ydb_Coordination.Config{{}},
		},
		{
			name: "Ptah's lock node name at a realm's root, which is not the database's",
			root: "/local/ptah_dev/r1",
			statement: "PRAGMA TablePathPrefix(\"/local/ptah_dev/r1\");\n" +
				"CREATE COORDINATION NODE ptah_locks WITH (attach_consistency_mode = 'relaxed')",
			wantCalls: []string{"create /local/ptah_dev/r1/ptah_locks"},
			wantSent: []*Ydb_Coordination.Config{{
				AttachConsistencyMode: Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_RELAXED,
			}},
		},
		{
			name: "a change of the settings it names",
			root: "/local",
			statement: "ALTER COORDINATION NODE `app/limits` SET (read_consistency_mode = 'strict', " +
				"rate_limiter_counters_mode = 'detailed')",
			wantCalls: []string{"alter /local/app/limits"},
			wantSent: []*Ydb_Coordination.Config{{
				ReadConsistencyMode:     Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT,
				RateLimiterCountersMode: Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_DETAILED,
			}},
		},
		{
			name: "a change whose result is checked with the node's own settings",
			root: "/local",
			// The node holds a self-check of 2.5 s, so a grace period of
			// 3.5 s is the shortest it runs with as written.
			statement: "ALTER COORDINATION NODE `app/limits` SET (session_grace_period = Interval('PT3.5S'))",
			wantCalls: []string{"alter /local/app/limits"},
			wantSent:  []*Ydb_Coordination.Config{{SessionGracePeriodMillis: 3500}},
		},
		{
			name:      "a drop",
			root:      "/local",
			statement: "DROP COORDINATION NODE `app/limits`",
			wantCalls: []string{"drop /local/app/limits"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := &fakeCoordination{nodes: map[string]*Ydb_Coordination.Config{
				"/local/app/limits": {SelfCheckPeriodMillis: 2500},
			}}

			err := ydbschema.RunCoordinationStatement(context.Background(), service, "/local", test.root,
				recognized(c, test.statement))

			c.Assert(err, qt.IsNil)
			c.Assert(service.calls, qt.DeepEquals, test.wantCalls)
			c.Assert(service.sent, qt.HasLen, len(test.wantSent))
			for i, want := range test.wantSent {
				c.Assert(proto.Equal(service.sent[i], want), qt.IsTrue, qt.Commentf("sent %v, want %v", service.sent[i], want))
			}
		})
	}
}

// A statement Ptah must not run is refused before the service changes
// anything: one that would touch Ptah's lock node or the server's own paths,
// one that leaves the root the connection treats as its database, a creation
// of a node that exists, which the service would answer with success and
// leave as it was, and a change that leaves a node with settings it would not
// run with.
func TestRunCoordinationStatement_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		root      string
		statement string
		wantErr   string
	}{
		{
			name:      "Ptah's lock node",
			root:      "/local",
			statement: "DROP COORDINATION NODE ptah_locks",
			wantErr:   `invalid coordination node statement: coordination node ptah_locks at the database root holds Ptah's own locks, .*`,
		},
		{
			name:      "Ptah's lock node by its absolute path",
			root:      "/local",
			statement: "DROP COORDINATION NODE `/local/ptah_locks`",
			wantErr:   `invalid coordination node statement: coordination node ptah_locks at the database root .*`,
		},
		{
			name:      "Ptah's lock node from a realm, by its absolute path",
			root:      "/local/ptah_dev/r1",
			statement: "PRAGMA TablePathPrefix(\"/local/ptah_dev/r1\");\nDROP COORDINATION NODE `/local/ptah_locks`",
			wantErr:   `invalid coordination node statement: coordination node /local/ptah_locks is not in /local/ptah_dev/r1`,
		},
		{
			name:      "a path that climbs out of a realm",
			root:      "/local/ptah_dev/r1",
			statement: "PRAGMA TablePathPrefix(\"/local/ptah_dev/r1\");\nCREATE COORDINATION NODE `../r2/n`",
			wantErr:   `invalid coordination node statement: coordination node /local/ptah_dev/r2/n is not in /local/ptah_dev/r1`,
		},
		{
			name:      "a path outside the database",
			root:      "/local",
			statement: "CREATE COORDINATION NODE `/other/n`",
			wantErr:   `invalid coordination node statement: coordination node /other/n is not in /local`,
		},
		{
			name:      "a server directory",
			root:      "/local",
			statement: "CREATE COORDINATION NODE `.sys/n`",
			wantErr:   `invalid coordination node statement: coordination node .sys/n has the path segment ".sys"; .*`,
		},
		{
			name:      "a relative prefix",
			root:      "/local",
			statement: "PRAGMA TablePathPrefix(\"app\");\nCREATE COORDINATION NODE n",
			wantErr:   `invalid coordination node statement: path app/n is not in database /local: .*`,
		},
		{
			name:      "a creation of a node that exists",
			root:      "/local",
			statement: "CREATE COORDINATION NODE `app/limits` WITH (self_check_period = Interval('PT3S'))",
			wantErr:   `create YDB coordination node /local/app/limits: the node already exists`,
		},
		{
			name:      "a change the node would not run with",
			root:      "/local",
			statement: "ALTER COORDINATION NODE `app/limits` SET (session_grace_period = Interval('PT3S'))",
			wantErr: `invalid coordination node statement: change YDB coordination node /local/app/limits: ` +
				`session_grace_period PT3S: .*`,
		},
		{
			name:      "a change of a node that does not exist",
			root:      "/local",
			statement: "ALTER COORDINATION NODE `missing` SET (read_consistency_mode = 'strict')",
			wantErr: `change YDB coordination node /local/missing: describe YDB coordination node /local/missing: ` +
				`no coordination node at this path: SCHEME_ERROR`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := &fakeCoordination{nodes: map[string]*Ydb_Coordination.Config{
				"/local/app/limits": {SelfCheckPeriodMillis: 2500},
				"/local/ptah_locks": {},
			}}

			err := ydbschema.RunCoordinationStatement(context.Background(), service, "/local", test.root,
				recognized(c, test.statement))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(service.calls, qt.HasLen, 0)
		})
	}
}
