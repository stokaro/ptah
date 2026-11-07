// Package migraterun writes the run document `ptah migrations up --json` and
// `ptah migrations down --json` print: one JSON document per run, on success
// and on failure, built from the migrator's own evidence by
// [migrator.NewRunResult]. Both commands share it so the two directions write
// one contract.
package migraterun

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"ptah.run/migration/migrator"
)

// Document is the one document a --json run prints on standard output. It is
// written at most once, so a failure that follows the run's document adds no
// second one. The zero value of Enabled writes nothing.
type Document struct {
	// Enabled reports that the run was asked for a document.
	Enabled bool
	// Out is where the document goes: standard output.
	Out io.Writer
	// Command names the command in the document, such as "migrations up".
	Command string
	// Direction is the direction the command runs.
	Direction migrator.MigrationDirection
	// Migrator is the run's migrator once one exists. It is the only witness
	// to a log entry the table did not get, so every document that follows it
	// asks it, whichever path the run took to get there.
	Migrator *migrator.Migrator

	written bool
}

// Write writes the run's evidence as its document, and does nothing without
// --json or once a document was written. The command and the direction are
// the document's own.
func (d *Document) Write(evidence migrator.RunEvidence) error {
	if !d.Enabled || d.written {
		return nil
	}
	d.written = true
	evidence.Command = d.Command
	evidence.Direction = d.Direction
	if d.Migrator != nil && !evidence.NotStarted {
		evidence.MigrationLog = d.Migrator.MigrationLogReport()
	}
	encoder := json.NewEncoder(d.Out)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(migrator.NewRunResult(evidence)); err != nil {
		return fmt.Errorf("error writing migration run result: %w", err)
	}
	return nil
}

// FailBeforeRun writes the document for a run that failed with err before it
// wrote one, and returns err.
//
// Every failure that reaches here without a document stopped before the
// migrator selected anything: the run's own document is written as soon as the
// migrator returns, and nothing before it changes the database. So the run
// sent no migration statement, and the document says failed, with nothing
// planned and no status.
func (d *Document) FailBeforeRun(err error, dryRun bool) error {
	writeErr := d.Write(migrator.RunEvidence{Err: err, DryRun: dryRun, NotStarted: true})
	if writeErr != nil {
		return errors.Join(err, writeErr)
	}
	return err
}
