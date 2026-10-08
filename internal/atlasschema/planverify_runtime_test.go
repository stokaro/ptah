package atlasschema_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/migration/migrator"
	"ptah.run/migration/schemadiff"
)

type incompleteVerificationRuntime struct {
	*engine.Runtime
	calls int
	after int
}

func (r *incompleteVerificationRuntime) CompareFeatures(ctx context.Context, request schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	r.calls++
	result, err := r.Runtime.CompareFeatures(ctx, request)
	if err == nil && r.calls >= r.after {
		result.Undecided = append(result.Undecided, schemaext.UndecidedChange{
			Kind: "example.org/uninspected", Subject: objectidentity.ID{Kind: "example.org/uninspected", Name: objectidentity.Part{Source: "unknown", Normalized: "unknown"}},
			Reason: "inspection did not establish the requested state",
		})
	}
	return result, err
}

func TestPlanVerification_RejectsIncompleteEvidenceEvenWithoutChanges(t *testing.T) {
	c := qt.New(t)
	target := connectSQLite(c, filepath.Join(c.TempDir(), "target.db"))
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	runtime := &incompleteVerificationRuntime{Runtime: inspectFeatureRuntime(c), after: 1}
	err := atlasschema.VerifyAppliedPlanState(t.Context(), target, &schemamodel.Database{}, nil, runtime)
	c.Assert(err, qt.ErrorIs, schemadiff.ErrIncompleteComparison)
	c.Assert(err, qt.ErrorMatches, `.*applied successfully.*incomplete evidence.*`)
	c.Assert(atlasschema.IsPlanDesiredStateFailure(err), qt.IsFalse)
	c.Assert(runtime.calls, qt.Equals, 1)
}

func TestPlanRehearsal_RequiresCompleteEndStateAndCleansDevDatabase(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	targetPath := filepath.Join(dir, "target.db")
	devPath := filepath.Join(dir, "dev.db")
	target := connectSQLite(c, targetPath)
	c.Cleanup(func() { dbschema.CloseAndWarn(target) })
	runtime := &incompleteVerificationRuntime{Runtime: inspectFeatureRuntime(c), after: 2}
	err := atlasschema.RehearsePlanStatements(t.Context(), target,
		[]string{"CREATE TABLE transient (id INTEGER)", "DROP TABLE transient"}, &schemamodel.Database{},
		atlasschema.PlanRehearsalOptions{Runtime: runtime, DevURL: atlasurl.SQLiteURLFromPath(devPath), TargetURL: atlasurl.SQLiteURLFromPath(targetPath), TxMode: migrator.MigrationTxModeAll})
	c.Assert(err, qt.ErrorIs, schemadiff.ErrIncompleteComparison)
	c.Assert(runtime.calls, qt.Equals, 2)
	c.Assert(sqliteObjectCount(c, devPath, "transient"), qt.Equals, 0)
	c.Assert(sqliteObjectCount(c, targetPath, "transient"), qt.Equals, 0)
}
