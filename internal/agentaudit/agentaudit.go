// Package agentaudit records what an AI-driven session asked for and what Ptah
// decided, so a change can be explained after the fact by something other than
// the transcript.
//
// # Why the transcript is not the record
//
// A conversation says what the model said it did. This says what Ptah did: the
// capability requested, the layer that answered, whether a human was asked, the
// digests before and after, and which gates ran. The two disagree exactly when
// it matters -- a model that reports a passing test it never ran leaves an audit
// record with no test in it.
//
// # Refusals are recorded, not only permissions
//
// #1487's hostile-repository scenario asks for an audit record showing the
// denied capability requests a malicious file provoked. A log written only on
// the success path would show a clean session for exactly the run worth looking
// at, so [Recorder.Record] is called for every decision and the outcome is a
// field rather than a filter.
//
// # What is deliberately absent
//
// No file content, no database URL, no provider credential, no conversation.
// The record carries digests and paths, which identify without disclosing: a
// digest says which bytes without carrying them, and a path inside a configured
// artifact scope names nothing the operator did not already point Ptah at.
// Redaction here is structural rather than a filter over free text, for the
// reason internal/deploymentreport gives about its own shape -- a filter is a
// list of things somebody remembered.
package agentaudit

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SchemaVersion identifies the record contract. A change to the shape of
// [Event] changes it.
const SchemaVersion = 1

// Surface names which product surface made the request. It is recorded because
// the two have different exposure: an MCP session is driven by a client Ptah
// does not control, and Ptah Assist is driven by Ptah.
type Surface string

const (
	// SurfaceMCP is the Model Context Protocol server.
	SurfaceMCP Surface = "mcp"
	// SurfaceAssist is the native Ptah Assist command.
	SurfaceAssist Surface = "assist"
)

// Outcome is what happened to the request.
type Outcome string

const (
	// OutcomePermitted reports an operation the policy allowed outright.
	OutcomePermitted Outcome = "permitted"
	// OutcomeApproved reports one a human approved.
	OutcomeApproved Outcome = "approved"
	// OutcomeDenied reports one the policy refused.
	OutcomeDenied Outcome = "denied"
	// OutcomeFailed reports one that was permitted and then did not succeed --
	// a rolled-back patch, a gate that found something. It is distinct from
	// denied because the authorization question and the execution question have
	// different answers and conflating them loses the more interesting one.
	OutcomeFailed Outcome = "failed"
)

// Event is one auditable decision.
//
// Every field is either Ptah's own text or an identifier. Nothing here is
// copied from the model's output except [Event.CallerSummary], which is labeled
// as the caller's words in both the field name and the documentation so a reader
// of the log cannot mistake it for Ptah's account of what happened.
type Event struct {
	SchemaVersion int       `json:"schema_version"`
	Timestamp     time.Time `json:"timestamp"`
	SessionID     string    `json:"session_id"`
	Surface       Surface   `json:"surface"`
	PtahVersion   string    `json:"ptah_version"`

	// Operation is the agent operation's name, such as apply_patch.
	Operation string `json:"operation"`
	// Capability is the scoped capability that was requested.
	Capability string `json:"capability,omitempty"`
	// Verdict is what the policy said: allow, ask or deny.
	Verdict string `json:"verdict,omitempty"`
	// DecidedBy names the policy layer that produced the verdict.
	DecidedBy string `json:"decided_by,omitempty"`
	// Approved reports that a human was asked and said yes, and ApprovalScope
	// how far that answer reached.
	Approved      bool   `json:"approved,omitempty"`
	ApprovalScope string `json:"approval_scope,omitempty"`

	Outcome Outcome `json:"outcome"`
	// Reason is Ptah's own sentence about the outcome, typically a refusal.
	Reason string `json:"reason,omitempty"`

	// Artifact, Paths and the digests identify what was touched, without
	// carrying any of it.
	Artifact     string   `json:"artifact,omitempty"`
	Paths        []string `json:"paths,omitempty"`
	PatchID      string   `json:"patch_id,omitempty"`
	BaseDigest   string   `json:"base_digest,omitempty"`
	ResultDigest string   `json:"result_digest,omitempty"`

	// Gates names the verification gates that ran, and GateFailures the ones
	// that found an error. A record with no gates on a write is itself the
	// finding.
	Gates        []string `json:"gates,omitempty"`
	GateFailures []string `json:"gate_failures,omitempty"`
	RolledBack   bool     `json:"rolled_back,omitempty"`

	// CallerSummary is the untrusted party's own description, kept because it is
	// useful context and labeled because it is not evidence.
	CallerSummary string `json:"caller_summary,omitempty"`
}

// validate refuses a record that would be misleading to read.
//
// The three required fields are the ones whose absence changes what the record
// means: without an operation there is nothing to explain, without an outcome
// the reader supplies one, and without a session the records cannot be grouped
// into the run that produced them.
func (e Event) validate() error {
	var errs []error
	if e.Operation == "" {
		errs = append(errs, errors.New("audit event names no operation"))
	}
	if e.Outcome == "" {
		errs = append(errs, errors.New("audit event states no outcome"))
	}
	if e.SessionID == "" {
		errs = append(errs, errors.New("audit event names no session"))
	}
	return errors.Join(errs...)
}

// Recorder receives events.
type Recorder interface {
	Record(event Event) error
}

// Discard is the recorder that keeps nothing.
//
// It exists so a caller always has one and never has to branch on whether
// auditing is configured -- a nil check at every call site is how one of them
// comes to be missing.
type Discard struct{}

// Record accepts and drops the event.
func (Discard) Record(Event) error { return nil }

// Writer appends events as JSON lines.
//
// One JSON object per line, appended and flushed: the format survives a
// truncated write at the end of the file, which a single JSON array does not,
// and the failure mode of an audit log is precisely a process that stopped
// unexpectedly.
type Writer struct {
	mu     sync.Mutex
	out    io.Writer
	closer io.Closer
	clock  func() time.Time

	sessionID   string
	surface     Surface
	ptahVersion string
}

// Options configures a writer.
type Options struct {
	// SessionID groups the events of one run. It is required.
	SessionID string
	// Surface names which product surface is recording.
	Surface Surface
	// PtahVersion is the build that made the decisions, because a policy
	// default that changed between releases is otherwise unexplainable from the
	// log.
	PtahVersion string
	// Clock is the time source, injectable so a test can pin timestamps.
	Clock func() time.Time
}

// NewWriter records to an already-open destination.
func NewWriter(out io.Writer, opts Options) (*Writer, error) {
	if opts.SessionID == "" {
		return nil, errors.New("audit writer requires a session id")
	}
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Writer{
		out:         out,
		clock:       clock,
		sessionID:   opts.SessionID,
		surface:     opts.Surface,
		ptahVersion: opts.PtahVersion,
	}, nil
}

// OpenFile appends to a log file, creating it and its directory if needed.
//
// The directory is 0o700 and the file 0o600, which is what every other
// Ptah-written state file in this tree uses. An audit log carries paths and
// digests rather than secrets, and it is still a record of what somebody did on
// their own machine.
func OpenFile(path string, opts Options) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create audit directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	writer, err := NewWriter(file, opts)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	writer.closer = file
	return writer, nil
}

// Record appends one event, filling in the fields the writer owns.
//
// The session, surface, version and timestamp are the writer's rather than the
// caller's, because a caller that could set them could also produce a record
// attributing its own decision to a different session.
func (w *Writer) Record(event Event) error {
	event.SchemaVersion = SchemaVersion
	event.SessionID = w.sessionID
	event.Surface = w.surface
	event.PtahVersion = w.ptahVersion
	event.Timestamp = w.clock().UTC()
	if err := event.validate(); err != nil {
		return err
	}

	line, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode audit event: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.out.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write audit event: %w", err)
	}
	return nil
}

// Close releases the file when the writer opened one. Closing twice is a no-op.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closer == nil {
		return nil
	}
	closer := w.closer
	w.closer = nil
	return closer.Close()
}

// DefaultPath is where a workspace keeps its agent audit log.
//
// Under the project's own `.ptah` directory, beside the approval keys that are
// already there: the log describes changes to this repository, so it belongs
// with the repository rather than in a home directory this tree has never
// written to.
func DefaultPath(root string) string {
	return filepath.Join(root, ".ptah", "agent-audit.jsonl")
}
