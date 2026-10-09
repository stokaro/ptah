package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// The public planner refuses the entire batch before returning any operations.
// Complete operands carry the settings an ALTER keeps as well as those it changes.
func TestGenerateMigrationAST_CoordinationNodes_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		node    string
		caps    capability.Capabilities
		change  *ydbdiff.CoordinationNode
		want    error
		message string
	}{
		{name: "creation without capability", node: "locks", caps: capability.YDB262().With(capability.CoordinationNodes, false), change: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}, want: ptaherr.ErrUnsupportedFeature, message: "coordination nodes"},
		{name: "alteration without capability", node: "locks", caps: capability.YDB262().With(capability.CoordinationNodes, false), change: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}, After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}}, want: ptaherr.ErrUnsupportedFeature, message: "coordination nodes"},
		{name: "drop without capability", node: "locks", caps: capability.YDB262().With(capability.CoordinationNodes, false), change: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}, want: ptaherr.ErrUnsupportedFeature, message: "coordination nodes"},
		{name: "reserved creation", node: "ptah_locks", caps: capability.YDB262(), change: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}, want: ptaherr.ErrInvalidSchemaDiff, message: "holds Ptah's own locks"},
		{name: "reserved drop", node: "ptah_locks", caps: capability.YDB262(), change: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}, want: ptaherr.ErrInvalidSchemaDiff, message: "holds Ptah's own locks"},
		{name: "reserved alteration", node: "ptah_locks", caps: capability.YDB262(), change: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}, After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}}, want: ptaherr.ErrInvalidSchemaDiff, message: "holds Ptah's own locks"},
		{name: "invalid creation period", node: "locks", caps: capability.YDB262(), change: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{SelfCheckPeriodMillis: 100}}}, want: schemaext.ErrInvalidValue, message: "self_check_period PT0.1S"},
		{name: "invalid complete alteration", node: "locks", caps: capability.YDB262(), change: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{Spec: ydbcoordination.Spec{SelfCheckPeriodMillis: 5000}}, After: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{SelfCheckPeriodMillis: 5000, SessionGracePeriodMillis: 5000}}}, want: schemaext.ErrInvalidValue, message: "session_grace_period PT5S"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbcoordination.Ref("", "valid"), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}}, {Subject: ydbcoordination.Ref("", test.node), Value: test.change}}}
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(t.Context(), must.Must(builtin.New()), diff)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(err.Error(), qt.Contains, test.message)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
