// Package migraterun writes the run document `ptah migrations up --json` and
// `ptah migrations down --json` print: one JSON document per run, on success
// and on failure, built from the migrator's own evidence by
// [migrator.NewRunResult]. Both commands share it so the two directions write
// one contract.
//
// It also owns --server-details, the run's choice to show what the database
// server said about the statement or the commit the run stopped at. The
// details are added to the error line on standard error, and to the document
// as server_details; neither output carries them without the flag.
package migraterun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/pflag"

	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/migrationfailure"
	"ptah.run/migration/migrator"
)

// ServerDetailsFlag is the flag both commands register with
// [RegisterServerDetailsFlag].
const ServerDetailsFlag = "server-details"

// RegisterServerDetailsFlag registers --server-details on flags.
func RegisterServerDetailsFlag(flags *pflag.FlagSet, target *bool) {
	flags.BoolVar(target, ServerDetailsFlag, false,
		"When a statement or its commit fails, also show what the server said beyond its code (PostgreSQL protocol only: "+
			"DETAIL, HINT, error position and object names), after the error line and as server_details under --json; "+
			"server details may contain application data, such as the row values a constraint refused")
}

// ServerDetails is a run's --server-details choice, and what reading the
// details needs. The zero value adds nothing to any output.
type ServerDetails struct {
	// Enabled reports that the run was asked for --server-details.
	Enabled bool
	// Options is what the details may read besides the run's error: the
	// migration directory, once the run resolved it, and the command's
	// redaction.
	Options migrationfailure.Options
}

// NewServerDetails is the --server-details choice of a command whose context
// is ctx. The details it reads have the secrets of every database address the
// command opened under ctx taken out, as the command's error line has.
func NewServerDetails(ctx context.Context, enabled bool) ServerDetails {
	return ServerDetails{
		Enabled: enabled,
		Options: migrationfailure.Options{Redact: func(text string) string {
			return dburldisplay.RedactRecordedText(ctx, text)
		}},
	}
}

// details reads the details of err, and nil without --server-details or when
// err stopped at no statement or commit.
func (s ServerDetails) details(err error) *migrationfailure.Details {
	if !s.Enabled || err == nil {
		return nil
	}
	return migrationfailure.New(err, s.Options)
}

// Explain returns err with the server details of the statement or the commit
// it stopped at written after its message, for the error line a person reads.
// Without --server-details, and for an error that stopped at neither, it
// returns err as it is. The result wraps err, so errors.Is, errors.As and the
// exit status answer as they did.
func (s ServerDetails) Explain(err error) error {
	text := s.Text(err)
	if text == "" {
		return err
	}
	return explainedError{err: err, text: text}
}

// Text is the human-readable server details of the statement or the commit err
// stopped at, and empty without --server-details or when err stopped at
// neither.
func (s ServerDetails) Text(err error) string {
	details := s.details(err)
	if details == nil {
		return ""
	}
	return details.Text()
}

// explainedError is an error with the server details after its message.
type explainedError struct {
	err  error
	text string
}

func (e explainedError) Error() string { return e.err.Error() + "\n" + e.text }
func (e explainedError) Unwrap() error { return e.err }

// document is the run document: the run's result and, under
// --server-details, the details of its failure.
type document struct {
	migrator.RunResult
	ServerDetails *migrationfailure.Details `json:"server_details,omitempty"`
}

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
	// ServerDetails is the run's --server-details choice. The document carries
	// server_details only when it is enabled and the run stopped at a
	// statement or a commit.
	ServerDetails ServerDetails

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
	written := document{RunResult: migrator.NewRunResult(evidence), ServerDetails: d.ServerDetails.details(evidence.Err)}
	if err := encoder.Encode(written); err != nil {
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
