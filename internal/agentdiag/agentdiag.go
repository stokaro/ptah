// Package agentdiag is the error taxonomy both AI surfaces answer with.
//
// An agent that gets prose back has to guess. It guesses by retrying, because
// retrying is the only recovery a model can attempt without understanding what
// went wrong, and a retry is the wrong move for most of the refusals this
// contract issues: a denied capability, a spent preview token and a path that
// leaves the workspace all fail again identically. So every failure that leaves
// the agent contract carries a [Code] -- a short, stable string a program can
// branch on -- and the [Diagnostic] built from it says who acts next and
// whether the same call could ever succeed unchanged.
//
// # Where a code is assigned
//
// At the sentinel, with [Sentinel], so every site that returns or wraps it
// carries the code without remembering to. Errors that arrive from outside the
// agent packages -- a schema that will not load, a database that will not dial
// -- are coded where they cross into the contract, which is internal/agentapi.
//
// This package deliberately imports none of them. It is the leaf every agent
// package can reach, which is what lets a sentinel name its own code instead of
// being classified from a distance by a switch that has to be kept in step with
// packages it cannot see.
//
// # What is not here
//
// The remedy. A code says what went wrong; the sentence naming the flag that
// would clear it belongs to the surface, because the operator of an MCP server
// and the operator of Ptah Assist do not start the same process.
package agentdiag

import (
	"errors"
	"fmt"
)

// Code is one member of the closed taxonomy.
//
// The set is closed on purpose: a caller writing a branch per code needs the
// list to be finite and needs an unrecognized value to mean "a Ptah newer than
// the one I was written against", not "some other spelling of a code I already
// handle". Codes are added to the end of the list and never renamed; renaming
// one is a contract break and changes [ptah.run/internal/agentapi.Version].
type Code string

// The taxonomy. Every code names a condition a caller can tell apart from every
// other one -- two conditions that would lead an agent to do the same thing
// share a code rather than splitting into a pair nobody can act on differently.
const (
	// CodeInvalidRequest is an argument that is missing, malformed, or
	// contradicts another argument. The call cannot succeed until the caller
	// changes it.
	CodeInvalidRequest Code = "invalid_request"
	// CodeSchemaSourceUnreadable is a named schema source that did not load:
	// no such directory, a file that does not parse, an annotation the loader
	// refuses.
	CodeSchemaSourceUnreadable Code = "schema_source_unreadable"
	// CodeRenderFailed is a schema that loaded and does not render for the
	// requested dialect, which is a property of the declaration rather than of
	// the request around it.
	CodeRenderFailed Code = "render_failed"
	// CodeDatabaseUnreachable is a connection that could not be opened. A URL
	// the driver rejects lands here too: the driver reports "I cannot dial
	// this" for both, and inventing a distinction Ptah cannot measure would be
	// a code that lies half the time.
	CodeDatabaseUnreachable Code = "database_unreachable"
	// CodeDatabaseReadFailed is a connection that opened and a catalog read
	// that did not finish -- most often a role without the privilege the read
	// needs.
	CodeDatabaseReadFailed Code = "database_read_failed"
	// CodeNoWorkspace is an artifact operation on a session started without a
	// workspace, which is what a read-only server is.
	CodeNoWorkspace Code = "no_workspace"
	// CodeNoDatabaseTarget is a database operation on a session the operator
	// configured no live database for. It is separate from
	// CodeInvalidRequest because no name the caller could send would work.
	CodeNoDatabaseTarget Code = "no_database_target"
	// CodeNoSourceScope is a declared-schema operation on a session the
	// operator configured no directory for, so there is nowhere a schema may
	// be read from.
	CodeNoSourceScope Code = "no_source_scope"
	// CodeArtifactClassNotConfigured is a workspace that has no directory for
	// the artifact class the call names.
	CodeArtifactClassNotConfigured Code = "artifact_class_not_configured"
	// CodeCapabilityDenied is the policy refusing the capability the operation
	// needs, whether by configuration or by a rule no configuration can lift.
	CodeCapabilityDenied Code = "capability_denied"
	// CodeApprovalUnavailable is an operation the policy says to ask a human
	// about, on a client that cannot be asked.
	CodeApprovalUnavailable Code = "approval_unavailable"
	// CodeApprovalRefused is a human who was asked and said no. It is separate
	// from CodeCapabilityDenied because a person's answer is not a
	// configuration mistake and re-reading the policy will not explain it.
	CodeApprovalRefused Code = "approval_refused"
	// CodeUnsafePath is a location this session will not open: a path that is
	// absolute, one that escapes its configured scope, one that is not in the
	// plain form the contract accepts, or a source that would be fetched over
	// a network rather than read from disk.
	CodeUnsafePath Code = "unsafe_path"
	// CodeArtifactTooLarge is a file, a patch or a directory over one of the
	// contract's stated limits.
	CodeArtifactTooLarge Code = "artifact_too_large"
	// CodeNotRegularFile is a path inside the workspace that names a directory,
	// a symlink or a device rather than a file.
	CodeNotRegularFile Code = "not_regular_file"
	// CodeUnknownPreview is a preview token that was never minted, has expired,
	// was already spent, or belongs to a different patch.
	CodeUnknownPreview Code = "unknown_preview"
	// CodeDigestMismatch is an artifact that changed between the preview and
	// the apply. The caller re-reads it and composes a new patch.
	CodeDigestMismatch Code = "digest_mismatch"
	// CodeInvalidPatch is a patch the contract will not accept: an unknown
	// operation, content that is not UTF-8, two changes to one path, a change
	// that changes nothing.
	CodeInvalidPatch Code = "invalid_patch"
	// CodeGateFailed is a patch that was written, failed Ptah's verification,
	// and was undone. The response carries what the gates said.
	CodeGateFailed Code = "gate_failed"
	// CodeVerificationUnavailable is verification that could not run at all, so
	// nothing was written. It is not CodeGateFailed: no gate reached a verdict.
	CodeVerificationUnavailable Code = "verification_unavailable"
	// CodeWriteFailed is a filesystem write that did not complete. What the
	// operation managed to write is undone before the error is returned.
	CodeWriteFailed Code = "write_failed"
	// CodeInternal is a failure with no better code. A caller seeing it has
	// found a defect in Ptah rather than a mistake of its own.
	CodeInternal Code = "internal"
)

// Actor names who can do something about a failure.
//
// It exists so a caller can branch without a switch over every code: an agent
// asked to fix its own request behaves differently from one that must report
// the problem to the person watching and stop.
type Actor string

const (
	// ActorCaller means changing the request could succeed.
	ActorCaller Actor = "caller"
	// ActorOperator means the person who started Ptah must change how it was
	// started. No request the caller can compose will succeed until they do.
	ActorOperator Actor = "operator"
	// ActorPerson means a human was asked and answered. Nothing to fix.
	ActorPerson Actor = "person"
	// ActorEnvironment means something outside Ptah -- a database, a
	// filesystem -- did not do what was asked.
	ActorEnvironment Actor = "environment"
	// ActorPtah means Ptah refused its own write or failed in a way that is a
	// defect rather than a decision.
	ActorPtah Actor = "ptah"
)

// Diagnostic is one failure, described so a program can branch on it.
//
// Message is the human half and is never machine-read: it names files, limits
// and identifiers, and its wording is not part of the contract. Everything a
// caller may branch on is one of the other fields.
type Diagnostic struct {
	Code Code `json:"code"`
	// Actor is who can act on this failure.
	Actor Actor `json:"actor"`
	// Retryable reports that the same call, unchanged, could succeed later. It
	// is true only where waiting is genuinely the remedy -- a database that was
	// down, a write that failed -- so an agent that retries on it is not
	// looping on a refusal that will never change its answer.
	Retryable bool `json:"retryable"`
	// Message is what went wrong, in words, for a person or a model to read.
	Message string `json:"message"`
	// Hint is the action that would clear the failure on this surface, when the
	// surface has one to name. It is supplied by the surface rather than by
	// this package.
	Hint string `json:"hint,omitempty"`
}

// String renders the line a model reads: the code first, then the words.
//
// The code leads because a client that shows the model nothing but the text --
// which is most of them -- still puts the stable token in front of it.
func (d Diagnostic) String() string {
	line := string(d.Code) + ": " + d.Message
	if d.Hint == "" {
		return line
	}
	return line + ". " + d.Hint
}

// meaning is the closed taxonomy itself: what each code implies.
//
// Codes live here rather than in a method on Code so that a code with no entry
// is a build-time-visible omission a test can name, rather than a switch
// default that quietly answers "internal" forever.
var meaning = map[Code]struct {
	actor     Actor
	retryable bool
	// summary is the one-line meaning docs/agent-errors.md publishes.
	summary string
}{
	CodeInvalidRequest:             {ActorCaller, false, "An argument is missing, malformed, or contradicts another one."},
	CodeSchemaSourceUnreadable:     {ActorCaller, false, "The named schema source did not load."},
	CodeRenderFailed:               {ActorCaller, false, "The schema loaded but does not render for the requested dialect."},
	CodeDatabaseUnreachable:        {ActorEnvironment, true, "The database connection could not be opened."},
	CodeDatabaseReadFailed:         {ActorEnvironment, true, "The connection opened and the catalog read did not finish."},
	CodeNoWorkspace:                {ActorOperator, false, "The session was started without a workspace, so it has no artifact operations."},
	CodeNoDatabaseTarget:           {ActorOperator, false, "The session was started with no live database configured."},
	CodeNoSourceScope:              {ActorOperator, false, "The session was started with no directory a declared schema may be read from."},
	CodeArtifactClassNotConfigured: {ActorOperator, false, "The workspace has no directory for the artifact class the call names."},
	CodeCapabilityDenied:           {ActorOperator, false, "The policy refuses the capability the operation needs."},
	CodeApprovalUnavailable:        {ActorOperator, false, "The operation needs a human approval this client cannot be asked for."},
	CodeApprovalRefused:            {ActorPerson, false, "A human was asked and refused."},
	CodeUnsafePath:                 {ActorCaller, false, "The location is absolute, leaves its configured scope, is not in plain form, or would be fetched rather than read."},
	CodeArtifactTooLarge:           {ActorCaller, false, "A file, patch or directory is over one of the contract's stated limits."},
	CodeNotRegularFile:             {ActorCaller, false, "The path names a directory, symlink or device rather than a file."},
	CodeUnknownPreview:             {ActorCaller, false, "The preview token is unknown, expired, spent, or for another patch."},
	CodeDigestMismatch:             {ActorCaller, false, "The artifact changed between the preview and the apply."},
	CodeInvalidPatch:               {ActorCaller, false, "The patch is not one this contract accepts."},
	CodeGateFailed:                 {ActorCaller, false, "The patch was written, failed verification, and was undone."},
	CodeVerificationUnavailable:    {ActorOperator, false, "Verification could not run, so nothing was written."},
	CodeWriteFailed:                {ActorEnvironment, true, "A filesystem write did not complete; what was written was undone."},
	CodeInternal:                   {ActorPtah, false, "A failure with no better code, which is a defect rather than a decision."},
}

// Codes returns the taxonomy in declaration order, for a document or a test
// that has to enumerate it.
func Codes() []Code {
	return []Code{
		CodeInvalidRequest,
		CodeSchemaSourceUnreadable,
		CodeRenderFailed,
		CodeDatabaseUnreachable,
		CodeDatabaseReadFailed,
		CodeNoWorkspace,
		CodeNoDatabaseTarget,
		CodeNoSourceScope,
		CodeArtifactClassNotConfigured,
		CodeCapabilityDenied,
		CodeApprovalUnavailable,
		CodeApprovalRefused,
		CodeUnsafePath,
		CodeArtifactTooLarge,
		CodeNotRegularFile,
		CodeUnknownPreview,
		CodeDigestMismatch,
		CodeInvalidPatch,
		CodeGateFailed,
		CodeVerificationUnavailable,
		CodeWriteFailed,
		CodeInternal,
	}
}

// Summary is the published one-line meaning of a code, empty for a code the
// taxonomy does not have.
func Summary(code Code) string {
	return meaning[code].summary
}

// Coded is an error that names its own place in the taxonomy.
//
// It is an interface rather than a concrete type so a package with an error
// type of its own -- one carrying the decision behind a refusal, say -- can
// join the taxonomy by adding a method instead of by being wrapped.
type Coded interface {
	error
	// DiagnosticCode is this error's taxonomy code.
	DiagnosticCode() Code
}

// Error is an error with a code attached.
type Error struct {
	code Code
	err  error
}

// Error is the underlying message. The code is deliberately absent from it: the
// surface decides where a machine-readable token belongs in what it renders,
// and an error that printed its own code would put one in every log line that
// already says the same thing in words.
func (e *Error) Error() string { return e.err.Error() }

// Unwrap keeps errors.Is and errors.As working through the code.
func (e *Error) Unwrap() error { return e.err }

// DiagnosticCode implements [Coded].
func (e *Error) DiagnosticCode() Code { return e.code }

// Sentinel builds a package-level sentinel that carries its code.
//
// Every site that returns it, or wraps it with %w, reports the code without
// naming it again, which is what keeps the taxonomy from drifting away from the
// errors it describes.
func Sentinel(code Code, text string) error {
	return &Error{code: code, err: errors.New(text)}
}

// Errorf builds a coded error the way fmt.Errorf builds an uncoded one,
// including %w.
func Errorf(code Code, format string, args ...any) error {
	return &Error{code: code, err: fmt.Errorf(format, args...)}
}

// Wrap attaches a code to an error from somewhere that has none.
//
// It returns nil for a nil error, so a caller can wrap unconditionally.
func Wrap(code Code, err error) error {
	if err == nil {
		return nil
	}
	return &Error{code: code, err: err}
}

// CodeOf reports the code an error carries, and whether it carries one at all.
//
// The second return value is the distinction [Of] cannot make: an error that
// says CodeInternal because somebody classified it that way and one that says
// CodeInternal because nobody classified it are different facts, and a caller
// deciding whether an error came from a layer that codes its failures needs the
// second one.
func CodeOf(err error) (Code, bool) {
	coded, ok := errors.AsType[Coded](err)
	if !ok {
		return CodeInternal, false
	}
	return coded.DiagnosticCode(), true
}

// WithFallback attaches a code to an error that has none, and returns an error
// that already carries one unchanged.
func WithFallback(code Code, err error) error {
	if err == nil {
		return nil
	}
	if _, coded := CodeOf(err); coded {
		return err
	}
	return Wrap(code, err)
}

// Of classifies an error.
//
// An error with no code is [CodeInternal] rather than an omission: a surface
// that reported nothing for the errors nobody has classified would hide exactly
// the ones worth finding.
func Of(err error) Diagnostic {
	if err == nil {
		return Diagnostic{}
	}
	code, _ := CodeOf(err)
	entry, known := meaning[code]
	if !known {
		// A code outside the taxonomy is a bug in Ptah, and answering with it
		// would publish a token no client can look up.
		code, entry = CodeInternal, meaning[CodeInternal]
	}
	return Diagnostic{
		Code:      code,
		Actor:     entry.actor,
		Retryable: entry.retryable,
		Message:   err.Error(),
	}
}
