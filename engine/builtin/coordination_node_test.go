package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/engine/builtin"
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
			node: &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "app", Name: "locks", Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{
				SelfCheckPeriodMillis: 1500, AttachConsistencyMode: "relaxed",
			}}}}},
			want: "CREATE COORDINATION NODE `app/locks` WITH (self_check_period = Interval('PT1.5S'), " +
				"attach_consistency_mode = 'relaxed');\n",
		},
		{
			name: "a creation with the defaults, under a quoted dotted name",
			node: &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "app.locks", Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{}}}}},
			want: "CREATE COORDINATION NODE `app.locks`;\n",
		},
		{
			name: "a change",
			node: &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "locks", Change: ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}, After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{
				SessionGracePeriodMillis: 20000, RateLimiterCountersMode: "detailed",
			}}}}},
			want: "ALTER COORDINATION NODE `locks` SET (session_grace_period = Interval('PT20S'), " +
				"rate_limiter_counters_mode = 'detailed');\n",
		},
		{
			name: "a drop",
			node: &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "app", Name: "locks", Change: ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}}},
			want: "DROP COORDINATION NODE `app/locks`;\n",
		},
		{
			name: "Ptah's lock node name in a directory",
			node: &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "app", Name: "ptah_locks", Change: ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}}},
			want: "DROP COORDINATION NODE `app/ptah_locks`;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered, err := builtin.RenderSQL(platform.YDB, test.node)
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
			node:    &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "ptah_locks", Change: ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}}},
			wantErr: `invalid feature value: coordination node ptah_locks at the database root holds Ptah's own locks, .*`,
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
		},
		{
			name:    "a server path",
			caps:    capability.YDB262(),
			node:    &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: ".sys", Name: "locks", Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{}}}}},
			wantErr: `invalid feature value: coordination node .sys/locks has the path segment ".sys"; .*`,
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
		},
		{
			name: "a creation the node would not run with",
			caps: capability.YDB262(),
			node: &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "locks", Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{
				ReadConsistencyMode: "eventual",
			}}}}},
			wantErr: `invalid feature value: read_consistency_mode "eventual": the mode is "strict" or "relaxed"`,
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
		},
		{
			name:    "a change of nothing",
			caps:    capability.YDB262(),
			node:    &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "locks", Change: ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}, After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{}}}}},
			wantErr: `invalid feature value: coordination operands contain no change`,
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered, err := builtin.RenderSQLWithCapabilities(platform.YDB, test.caps, test.node)
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
	const want = `coordination nodes require target capability coordination_nodes on YDB`
	node := &ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "locks", Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{}}}}}
	visitor, err := builtin.NewRendererWithCapabilities(platform.YDB, caps)
	c.Assert(err, qt.IsNil)

	rendered, centralErr := builtin.RenderSQLWithCapabilities(platform.YDB, caps, node)
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
	&ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "locks", Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{}}}}},
	&ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "locks", Change: ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}, After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}}}},
	&ast.ExtensionStatement{Payload: &ydbast.CoordinationNode{Schema: "", Name: "locks", Change: ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}}},
}

// Other targets have no coordination handler. Claiming the capability cannot
// grant a target permission to render another engine's operation.
func TestRenderSQL_OtherTargetsRefuseCoordinationNodes(t *testing.T) {
	for _, dialect := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer,
		platform.ClickHouse, platform.Oracle, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	} {
		for _, node := range coordinationNodeStatements {
			t.Run(dialect, func(t *testing.T) {
				c := qt.New(t)
				centrally, centralErr := builtin.RenderSQL(dialect, node)
				claimed := capability.ForDialect(dialect).With(capability.CoordinationNodes, true)
				dispatched, dispatchErr := builtin.RenderSQLWithCapabilities(dialect, claimed, node)

				c.Assert(centralErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(centralErr, qt.ErrorMatches, `target "\w+" does not support extension "ptah.run/ydb/coordination-node-operation" in role "statement"`)
				c.Assert(centrally, qt.Equals, "")
				c.Assert(dispatchErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(dispatchErr, qt.ErrorMatches, `target "\w+" does not support extension "ptah.run/ydb/coordination-node-operation" in role "statement"`)
				c.Assert(dispatched, qt.Equals, "")
			})
		}
	}
}

// A schema that declares a coordination node validates on YDB and is refused
// on every other target, before anything is rendered.
func TestValidateSchema_CoordinationNodes(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbcoordination.DesiredObject("app", "locks", "", ydbcoordination.Spec{}),
	)),
	}

	c.Assert(builtin.ValidateSchema(declared, platform.YDB), qt.IsNil)
	err := builtin.ValidateSchema(declared, platform.Postgres)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `unsupported feature: feature objects are not registered for target "postgres"`)
}

// A declared node YDB would not run as written is refused when the schema is
// validated, whatever renders it.
func TestValidateSchema_YDBRefusesACoordinationNodeItWouldNotRun(t *testing.T) {
	tests := []struct {
		name string
		node schemaext.Object

		wantErr string
		wantIs  error
	}{
		{
			name: "Ptah's lock node", wantIs: ptaherr.ErrInvalidSchemaDiff,
			node: ydbcoordination.DesiredObject("", "ptah_locks", "", ydbcoordination.Spec{}),

			wantErr: `.*coordination node ptah_locks at the database root holds Ptah's own locks, .*`,
		},
		{
			name: "a grace period below the self-check period", wantIs: schemaext.ErrInvalidValue,
			node: ydbcoordination.DesiredObject("", "locks", "", ydbcoordination.Spec{SelfCheckPeriodMillis: 5000, SessionGracePeriodMillis: 5000}),

			wantErr: `.*session_grace_period PT5S: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := builtin.ValidateSchema(&schemamodel.Database{
				FeatureObjects: must.Must(schemaext.NewObjects(
					test.node)),
			}, platform.YDB)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
