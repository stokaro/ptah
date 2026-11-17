package migratedown

// White-box testing required: the report a run makes once its migration lock is
// released is what carries a lock lost or not released after the rollback
// committed, and that loss cannot be provoked through Execute. It has to land
// between the last commit and the release, which no server can be made to do
// on cue, and SQLite, the engine of the unit contour, takes no session lock.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/cliobs"
	"ptah.run/internal/cli/internal/migraterun"
	"ptah.run/migration/migrator"
)

// errLockReleaseFailed stands for the outcome of a hold whose lock could not
// be released after the run inside it succeeded.
var errLockReleaseFailed = errors.New("failed to release migration lock for migrate down: connection reset")

// reportedDocument is the part of the run document these tests read.
type reportedDocument struct {
	Outcome string `json:"outcome"`
	Error   *struct {
		Message string `json:"message"`
	} `json:"error"`
}

type reportFixture struct {
	text     bytes.Buffer
	document bytes.Buffer
	mig      *migrator.Migrator
	status   *migrator.MigrationStatus
}

func newReportFixture(c *qt.C) *reportFixture {
	c.Helper()
	f := &reportFixture{mig: newAppliedInterruptContextMigrator(c)}
	status, err := f.mig.GetMigrationStatus(context.Background())
	c.Assert(err, qt.IsNil)
	f.status = status
	return f
}

func (f *reportFixture) runDocument() *migraterun.Document {
	return &migraterun.Document{
		Enabled: true, Out: &f.document, Command: "migrations down",
		Direction: migrator.MigrationDirectionDown, Migrator: f.mig,
	}
}

func (f *reportFixture) rollbackReport() downReport {
	report := &rollbackReport{
		run: rollbackRun{
			opts:     &options{},
			migrator: f.mig,
			emit:     cliobs.NewEmitter(&f.text, nil),
			document: f.runDocument(),
			selected: []int64{1},
		},
		plan:        &migrator.MigrationPlan{Direction: migrator.MigrationDirectionDown, Versions: []int64{1}},
		finalStatus: f.status,
	}
	return report.finish
}

func (f *reportFixture) noopReport(c *qt.C) downReport {
	c.Helper()
	report, err := finishDownAtOrBelowTarget(context.Background(), downNoopInputs{
		migrator: f.mig, emit: cliobs.NewEmitter(&f.text, nil), document: f.runDocument(),
		dialect: "sqlite", status: f.status, targetVersion: 1,
	})
	c.Assert(err, qt.IsNil)
	return report
}

func decodeReportedDocument(c *qt.C, raw []byte) reportedDocument {
	c.Helper()
	var document reportedDocument
	c.Assert(json.Unmarshal(raw, &document), qt.IsNil, qt.Commentf("document:\n%s", raw))
	return document
}

func TestDownReportAfterTheLock_HappyPath(t *testing.T) {
	t.Run("a rollback whose lock was released reports success", func(t *testing.T) {
		c := qt.New(t)
		f := newReportFixture(c)

		err := f.rollbackReport()(nil)

		c.Assert(err, qt.IsNil)
		c.Assert(f.text.String(), qt.Contains, "Migration rollback completed successfully!")
		c.Assert(decodeReportedDocument(c, f.document.Bytes()).Error, qt.IsNil)
	})

	t.Run("a run already at its target whose lock was released reports success", func(t *testing.T) {
		c := qt.New(t)
		f := newReportFixture(c)

		err := f.noopReport(c)(nil)

		c.Assert(err, qt.IsNil)
		c.Assert(f.text.String(), qt.Contains, "Database is already at or below target version 1!")
		c.Assert(decodeReportedDocument(c, f.document.Bytes()), qt.DeepEquals, reportedDocument{Outcome: "up-to-date"})
	})
}

func TestDownReportAfterTheLock_FailurePath(t *testing.T) {
	t.Run("a rollback whose lock was not released reports the lock's failure", func(t *testing.T) {
		c := qt.New(t)
		f := newReportFixture(c)

		err := f.rollbackReport()(errLockReleaseFailed)

		c.Assert(err, qt.ErrorIs, errLockReleaseFailed)
		c.Assert(f.text.String(), qt.Not(qt.Contains), "completed successfully")
		document := decodeReportedDocument(c, f.document.Bytes())
		c.Assert(document.Error, qt.IsNotNil)
		c.Assert(document.Error.Message, qt.Equals, errLockReleaseFailed.Error())
	})

	t.Run("a run already at its target whose lock was not released reports nothing of its own", func(t *testing.T) {
		c := qt.New(t)
		f := newReportFixture(c)

		err := f.noopReport(c)(errLockReleaseFailed)

		c.Assert(err, qt.ErrorIs, errLockReleaseFailed)
		c.Assert(f.text.String(), qt.Equals, "")
		c.Assert(f.document.String(), qt.Equals, "")
	})
}
