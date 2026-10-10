package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/engine/builtin"
	"ptah.run/internal/devclean"
)

// TestBaselineGuard_RefusesOwnedReplications renders a declared async
// replication and a declared transfer through the owner and holds the
// rehearsal baseline to refusing each statement: neither is confined to the
// dev database realm, since a replication pulls from whatever its connection
// names and the realm prefix moves neither a replication's source nor a
// transfer's topic.
func TestBaselineGuard_RefusesOwnedReplications(t *testing.T) {
	tests := []struct {
		name    string
		object  schemaext.Object
		wantErr string
	}{
		{
			name: "an async replication",
			object: ydbreplication.DesiredReplicationObject("", "r", "", ydbreplication.ReplicationSpec{
				Connection: ydbreplication.Connection{ConnectionString: "grpcs://prod.example:2135/?database=/prod"},
				Items:      []ydbreplication.Item{{Source: "items", Target: "copy"}},
			}),
			wantErr: `ydb rehearsal baseline refuses async replication because its effects cannot be confined to the dev database realm`,
		},
		{
			name: "a transfer",
			object: ydbreplication.DesiredTransferObject("", "ingest", "", ydbreplication.TransferSpec{
				Source: "events", Target: "copy", Lambda: "($m) -> { return []; }",
			}),
			wantErr: `ydb rehearsal baseline refuses a transfer because its effects cannot be confined to the dev database realm`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(test.object))}
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(declared, platform.YDB, capability.YDB262())
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)

			err = devclean.NewBaselineGuard(baselineRealm).ValidateStatement(statements[0])

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
