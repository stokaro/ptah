package ydbplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbschema"
)

func TestParentOperationsRefuseReplicationBindingsWithoutChildChanges(t *testing.T) {
	for _, action := range []featureplan.ParentAction{featureplan.DropTable, featureplan.RebuildTable} {
		t.Run(string(action), func(t *testing.T) {
			c := qt.New(t)
			request := planningRequest(c)
			request.Changes = nil
			request.ParentKinds = []schemaext.Kind{ydbschema.ChangefeedKind}
			request.Tables[0].Action = action
			declarations := map[featureplan.ParentAction]schemacapture.TableDeclaration{
				featureplan.RebuildTable: request.Tables[0].Desired,
			}
			request.Tables[0].Desired = declarations[action]
			observed := &ydbschema.ObservedChangefeed{Spec: stream("replication", "UPDATES"),
				Replication: &ydbschema.ReplicationBinding{DestinationPath: "/remote/replica", ItemID: "1"}}
			objects, err := schemaext.NewObjects(schemaext.Object{Ref: ydbschema.ChangefeedRef("", "items", "replication"), Value: observed})
			c.Assert(err, qt.IsNil)
			request.Tables[0].Current.OwnedObjects = objects
			result, err := (ydbplan.Service{}).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(result.Err(request), qt.ErrorMatches, "(?s).*controller must release it.*")
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Parents, qt.HasLen, 0)
		})
	}
}

func TestPlannerRefusesAnIndependentReplicationStreamChange(t *testing.T) {
	c := qt.New(t)
	request := planningRequest(c)
	request.Changes = request.Changes[1:2]
	change := request.Changes[0].Value.(*ydbdiff.Changefeed)
	change.Before.Replication = &ydbschema.ReplicationBinding{DestinationPath: "/remote/replica", ItemID: "1"}
	result, err := (ydbplan.Service{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result.Err(request), qt.ErrorMatches, "(?s).*independently of its controller.*")
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Parents, qt.HasLen, 0)
}
