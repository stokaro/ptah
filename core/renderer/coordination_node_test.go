package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// The YDB renderer writes Ptah's own statement for a coordination node, with
// the node's directory as a path and the settings it sets, and nothing for
// the settings left to YDB's defaults.
func TestRenderSQL_YDBWritesCoordinationNodeStatements(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "a creation in a directory",
			node: &ast.CreateCoordinationNodeNode{Name: "app.locks", Spec: ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 1500, AttachConsistencyMode: "relaxed",
			}},
			want: "CREATE COORDINATION NODE `app/locks` WITH (self_check_period = Interval('PT1.5S'), " +
				"attach_consistency_mode = 'relaxed');\n",
		},
		{
			name: "a creation with the defaults, under a quoted dotted name",
			node: &ast.CreateCoordinationNodeNode{Name: `"app.locks"`},
			want: "CREATE COORDINATION NODE `app.locks`;\n",
		},
		{
			name: "a change",
			node: &ast.AlterCoordinationNodeNode{Name: "locks", Spec: ast.CoordinationNodeSpec{
				SessionGracePeriodMillis: 20000, RateLimiterCountersMode: "detailed",
			}},
			want: "ALTER COORDINATION NODE `locks` SET (session_grace_period = Interval('PT20S'), " +
				"rate_limiter_counters_mode = 'detailed');\n",
		},
		{
			name: "a drop",
			node: &ast.DropCoordinationNodeNode{Name: "app.locks"},
			want: "DROP COORDINATION NODE `app/locks`;\n",
		},
		{
			name: "Ptah's lock node name in a directory",
			node: &ast.DropCoordinationNodeNode{Name: "app.ptah_locks"},
			want: "DROP COORDINATION NODE `app/ptah_locks`;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered, err := renderer.RenderSQL(platform.YDB, test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}

// The YDB renderer refuses a node it must not write: Ptah's lock node, a
// server's dot path, a creation the node would not run with as written, and a
// change of nothing.
func TestRenderSQL_YDBRefusesCoordinationNodes(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantErr string
		wantIs  error
	}{
		{
			name:    "Ptah's lock node",
			caps:    capability.YDB262(),
			node:    &ast.DropCoordinationNodeNode{Name: "ptah_locks"},
			wantErr: `coordination node ptah_locks: coordination node ptah_locks at the database root holds Ptah's own locks, .*`,
			wantIs:  ptaherr.ErrUnsupportedFeature,
		},
		{
			name:    "a server path",
			caps:    capability.YDB262(),
			node:    &ast.CreateCoordinationNodeNode{Name: `".sys".locks`},
			wantErr: `coordination node ".sys".locks: coordination node .sys/locks has the path segment ".sys"; .*`,
			wantIs:  ptaherr.ErrUnsupportedFeature,
		},
		{
			name: "a creation the node would not run with",
			caps: capability.YDB262(),
			node: &ast.CreateCoordinationNodeNode{Name: "locks", Spec: ast.CoordinationNodeSpec{
				ReadConsistencyMode: "eventual",
			}},
			wantErr: `coordination node locks: read_consistency_mode "eventual": the mode is "strict" or "relaxed"`,
			wantIs:  ptaherr.ErrUnsupportedFeature,
		},
		{
			name:    "a change of nothing",
			caps:    capability.YDB262(),
			node:    &ast.AlterCoordinationNodeNode{Name: "locks"},
			wantErr: `.*ALTER COORDINATION NODE locks names no setting to change`,
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered, err := renderer.RenderSQLWithCapabilities(platform.YDB, test.caps, test.node)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(rendered, qt.Equals, "")
		})
	}
}

// A YDB target without the key refuses a node through RenderSQLWithCapabilities
// and through a visit alike: a visit runs the renderer's central check as
// RenderSQL does, so there is no way past it through the renderer. The YDB
// renderer's own check of the key is covered in its package.
func TestRenderSQL_YDBWithoutTheKeyRefusesCoordinationNodes(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262().With(capability.CoordinationNodes, false)
	const want = `coordination node locks, which requires target capability coordination_nodes, unavailable on this ydb target`
	node := &ast.CreateCoordinationNodeNode{Name: "locks"}
	visitor, err := renderer.NewRendererWithCapabilities(platform.YDB, caps)
	c.Assert(err, qt.IsNil)

	rendered, centralErr := renderer.RenderSQLWithCapabilities(platform.YDB, caps, node)
	visitErr := node.Accept(visitor)

	c.Assert(centralErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(centralErr, qt.ErrorMatches, want)
	c.Assert(rendered, qt.Equals, "")
	c.Assert(visitErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(visitErr, qt.ErrorMatches, want)
	c.Assert(visitor.Output(), qt.Equals, "")
}

// coordinationNodeStatements are the three statements, as nodes.
var coordinationNodeStatements = []ast.Node{
	&ast.CreateCoordinationNodeNode{Name: "locks"},
	&ast.AlterCoordinationNodeNode{Name: "locks", Spec: ast.CoordinationNodeSpec{ReadConsistencyMode: "strict"}},
	&ast.DropCoordinationNodeNode{Name: "locks"},
}

// Every other target refuses a coordination node by its capability key, on
// two layers: the renderer's central check, and each dialect's own dispatcher,
// which a capability set claiming the key reaches.
func TestRenderSQL_OtherTargetsRefuseCoordinationNodes(t *testing.T) {
	for _, dialect := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer,
		platform.ClickHouse, platform.Oracle, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	} {
		for _, node := range coordinationNodeStatements {
			t.Run(dialect, func(t *testing.T) {
				c := qt.New(t)
				centrally, centralErr := renderer.RenderSQL(dialect, node)
				claimed := capability.ForDialect(dialect).With(capability.CoordinationNodes, true)
				dispatched, dispatchErr := renderer.RenderSQLWithCapabilities(dialect, claimed, node)

				c.Assert(centralErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(centralErr, qt.ErrorMatches, `coordination node locks, which requires target capability `+
					`coordination_nodes, unavailable on this \w+ target`)
				c.Assert(centrally, qt.Equals, "")
				c.Assert(dispatchErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(dispatchErr, qt.ErrorMatches, `.*coordination node locks, which requires target capability `+
					`coordination_nodes, unavailable on this \w+ target: a coordination node is a YDB object`)
				c.Assert(dispatched, qt.Equals, "")
			})
		}
	}
}

// A schema that declares a coordination node validates on YDB and is refused
// on every other target, before anything is rendered.
func TestValidateSchema_CoordinationNodes(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{CoordinationNodes: []schemamodel.CoordinationNode{{Schema: "app", Name: "locks"}}}

	c.Assert(renderer.ValidateSchema(declared, platform.YDB), qt.IsNil)
	err := renderer.ValidateSchema(declared, platform.Postgres)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `coordination node app.locks, which requires target capability coordination_nodes, `+
		`unavailable on this postgres target: a coordination node is a YDB object`)
}

// A declared node YDB would not run as written is refused when the schema is
// validated, whatever renders it.
func TestValidateSchema_YDBRefusesACoordinationNodeItWouldNotRun(t *testing.T) {
	tests := []struct {
		name    string
		node    schemamodel.CoordinationNode
		wantErr string
	}{
		{
			name:    "Ptah's lock node",
			node:    schemamodel.CoordinationNode{Name: "ptah_locks"},
			wantErr: `.*coordination node ptah_locks at the database root holds Ptah's own locks, .*`,
		},
		{
			name: "a grace period below the self-check period",
			node: schemamodel.CoordinationNode{Name: "locks", Spec: ast.CoordinationNodeSpec{
				SelfCheckPeriodMillis: 5000, SessionGracePeriodMillis: 5000,
			}},
			wantErr: `.*coordination node locks: session_grace_period PT5S: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := renderer.ValidateSchema(&schemamodel.Database{
				CoordinationNodes: []schemamodel.CoordinationNode{test.node},
			}, platform.YDB)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
