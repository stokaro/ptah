package atlasschema

import (
	"errors"
	"slices"

	digest "github.com/opencontainers/go-digest"

	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/errorkind"
	"ptah.run/internal/planrefusal"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/servercode"
	"ptah.run/internal/txrequire"
	"ptah.run/migration/migrator"
	"ptah.run/migration/schemadiff"
)

// PlanReportContractVersion is the version of the document `ptah schema plan
// --json` prints, and ApplyReportContractVersion the version of the one
// `ptah schema apply --json` prints.
//
// A version rises when a field changes meaning or leaves, and when an outcome
// gains a value, because a reader cannot act on an outcome it does not know. A
// new field a reader may ignore is not a new version. Neither is a new refusal
// code: a refusal of any code means that nothing reached the target database,
// which a reader that does not know the code can still act on. Nor is a new
// error kind, which a reader that does not know it reads as no kind. A
// consumer that does not know the version it reads refuses the document rather
// than reading the fields it recognizes out of a shape that means something
// else.
//
// Plan version 3 and apply version 4 carry error as an object with a kind and
// a message, where the versions before carried the message alone.
const (
	PlanReportContractVersion  = 3
	ApplyReportContractVersion = 4
)

// PlanReportCommand and ApplyReportCommand are the commands the two documents
// name in their command field.
const (
	PlanReportCommand  = "schema plan"
	ApplyReportCommand = "schema apply"
)

// PlanOutcome is how one `schema plan` run ended.
type PlanOutcome string

const (
	// PlanOutcomeChanges means the plan holds statements, and the report
	// carries the plan document.
	PlanOutcomeChanges PlanOutcome = "changes"
	// PlanOutcomeNoChanges means the comparison planned no statement. No plan
	// document exists, and none was saved. The database matches the desired
	// schema unless the report's Undecided names objects the read could not
	// check.
	PlanOutcomeNoChanges PlanOutcome = "no-changes"
	// PlanOutcomeRefused means planning refused for a reason the report's
	// [Refusal] names.
	PlanOutcomeRefused PlanOutcome = "refused"
	// PlanOutcomeFailed means the plan could not be computed or saved, for a
	// reason that has no refusal code. The report's error says why.
	PlanOutcomeFailed PlanOutcome = "failed"
)

// ApplyOutcome is how one `schema apply` run ended.
//
// The split that matters to a caller is between the outcomes that say nothing
// reached the target database -- no-changes, dry-run, canceled, refused and
// failed -- and the two that do not: applied, and unknown.
type ApplyOutcome string

const (
	// ApplyOutcomeApplied means every statement ran and the run returned
	// without an error.
	ApplyOutcomeApplied ApplyOutcome = "applied"
	// ApplyOutcomeNoChanges means the comparison planned no statement, so
	// there was nothing to run. The database matches the desired schema unless
	// the report's Undecided names objects the read could not check.
	ApplyOutcomeNoChanges ApplyOutcome = "no-changes"
	// ApplyOutcomeDryRun means the run was asked to change nothing, and did
	// not. For a plan file the source fingerprint was verified first, so a
	// dry run also says the plan was not stale when it was read.
	ApplyOutcomeDryRun ApplyOutcome = "dry-run"
	// ApplyOutcomeCanceled means the confirmation prompt was declined.
	ApplyOutcomeCanceled ApplyOutcome = "canceled"
	// ApplyOutcomeRefused means the run stopped before any statement reached
	// the target database, for a reason the report's [Refusal] names.
	ApplyOutcomeRefused ApplyOutcome = "refused"
	// ApplyOutcomeFailed means the run stopped before any statement reached
	// the target database, for a reason that has no refusal code. The report's
	// error says why.
	ApplyOutcomeFailed ApplyOutcome = "failed"
	// ApplyOutcomeUnknown means the statements were sent to the database and
	// the run returned an error. The report's [ApplyFailure] says where the run
	// stopped, and nothing in the report says what the database holds: with a
	// transaction it may have rolled them all back, without one it may hold some
	// of them, and a lost connection hides even that. Read the database before
	// deciding anything.
	ApplyOutcomeUnknown ApplyOutcome = "unknown"
)

// RefusalCode names why a plan or an apply refused.
type RefusalCode string

const (
	// RefusalStalePlan means the database no longer matches the state a plan
	// file was computed against. Re-plan against the database as it is now.
	RefusalStalePlan RefusalCode = "stale-plan"
	// RefusalProtectedTable means the plan would change a table the caller
	// fenced off with --protected-table. There is no override.
	RefusalProtectedTable RefusalCode = "protected-table"
	// RefusalLockTimeout means another session held the schema apply lock
	// longer than --lock-timeout allowed.
	RefusalLockTimeout RefusalCode = "lock-timeout"
	// RefusalTransactionPreflight means a statement conflicts with the
	// requested transaction mode.
	RefusalTransactionPreflight RefusalCode = "transaction-preflight"
	// RefusalTransactionUnavailable means the requested transaction mode needs
	// a transaction the target cannot open. --tx-mode none runs the plan
	// without one.
	RefusalTransactionUnavailable RefusalCode = "transaction-unavailable"
	// RefusalSimulationFailed means the plan failed its rehearsal on the
	// --dev-url database, and the command did not send it to the target.
	RefusalSimulationFailed RefusalCode = "simulation-failed"
	// RefusalViewSecurityOption means the plan would take a security option from
	// a view because the declaration does not state it. State the option to keep
	// it, or state it as off to remove it.
	RefusalViewSecurityOption RefusalCode = "view-security-option"
	// RefusalDependentObject means the plan drops an object something else
	// depends on, where the drop would stop on the dependent or take it with
	// nothing to create it again. Change the dependent first.
	RefusalDependentObject RefusalCode = "dependent-object"
	// RefusalDeclarationRefused means the comparison refused what the schema
	// states, because this target cannot host it or plan it. The error message
	// names the declaration. Change it or target another engine.
	RefusalDeclarationRefused RefusalCode = "declaration-refused"
)

// Refusal is the reason a plan or an apply refused, with the details a caller
// acts on. Only the fields the code describes are set.
type Refusal struct {
	// Code is one of the RefusalCode constants.
	Code RefusalCode `json:"code"`
	// Tables are the fenced tables a protected-table refusal names, sorted.
	Tables []string `json:"tables,omitempty"`
	// Changed is what moved under a stale-plan refusal: "schema" for the
	// structure the source fingerprint describes, "rows" for the declared
	// rows the rows fingerprint describes.
	Changed string `json:"changed,omitempty"`
	// PlanFingerprint and DatabaseFingerprint are the two fingerprints a
	// stale-plan refusal compared: the one the plan recorded and the one the
	// database has now, of the kind Changed names.
	PlanFingerprint     string `json:"plan_fingerprint,omitempty"`
	DatabaseFingerprint string `json:"database_fingerprint,omitempty"`
	// ViewOptions are the security options a view-security-option refusal names,
	// one entry per view and option, in the order the comparison lists the views
	// and each view's options in name order.
	ViewOptions []planrefusal.ViewOption `json:"view_options,omitempty"`
}

// PlanReport is the machine-readable record of one `schema plan` run.
type PlanReport struct {
	// ContractVersion is [PlanReportContractVersion].
	ContractVersion int `json:"contract_version"`
	// Command is [PlanReportCommand].
	Command string `json:"command"`
	// Outcome is how the run ended.
	Outcome PlanOutcome `json:"outcome"`
	// PlanDigest is the SHA-256 of the plan document's canonical bytes, in
	// sha256:<hex> form: the file --save and --output write and the document
	// --dry-run prints without --json. It is present with the plan.
	//
	// Those bytes are not in this report. Plan is the same document decoded
	// and encoded again inside the report, so its bytes do not hash to
	// PlanDigest, and no encoder a caller has is promised to reproduce the
	// canonical form. A caller that needs the bytes -- to digest, sign or pass
	// to `schema apply --plan` -- runs with --output and reads the file.
	PlanDigest string `json:"plan_digest,omitempty"`
	// PlanPath is where --save or --output wrote the plan document, and empty
	// under --dry-run.
	PlanPath string `json:"plan_path,omitempty"`
	// Plan is the document content, present when the run has operations or notes.
	// See PlanDigest for why it is not the document's bytes.
	Plan *PlanFile `json:"plan,omitempty"`
	// Undecided are the declared objects the comparison withheld, and absent
	// when it withheld none.
	//
	// A declared object is undecided when the read of the database did not
	// describe its kind -- the server refused the catalog, the selection left
	// the kind out, or the target cannot report it -- so nothing checked
	// whether the object exists. No statement is planned for it, because a
	// creation could fail or diverge against an object that is already there.
	// Common-object limits retain the kind, name, reason, and provenance.
	// Feature limits retain the model kind, structured subject, and reason,
	// including limits on a whole namespace. The command explains each limit
	// on standard error. Version 2 uses this structured report in place of the
	// version 1 array of common-object limits.
	//
	// The field is independent of the outcome. Next to changes it names what
	// the plan does not cover; next to no-changes it means the database is not
	// shown to match. A caller that decides "in sync" from the outcome reads
	// this field too.
	Undecided schemadiff.Diagnostics `json:"undecided,omitzero"`
	// Refusal is present when Outcome is refused.
	Refusal *Refusal `json:"refusal,omitempty"`
	// Error is the run's error, present when Outcome is refused or failed:
	// its kind, when the failure is one [errorkind] names, and the sentence
	// the command prints on standard error.
	Error *errorkind.Report `json:"error,omitempty"`
	// ProviderDiagnostic preserves structured provider setup and trust failures.
	ProviderDiagnostic *providerdiagnostic.Diagnostic `json:"provider_diagnostic,omitempty"`
	// SchemaDiagnostics are every diagnostic a provider reported when it
	// refused the SQL schema documents it read, with their positions. Error
	// names the first error among them.
	SchemaDiagnostics []providerdiagnostic.SchemaDiagnostic `json:"schema_diagnostics,omitempty"`
}

// PlanEvidence is what a caller saw of one `schema plan` run.
type PlanEvidence struct {
	// Plan is the computed plan, and nil when planning did not finish.
	Plan *PlanFile
	// Document is the plan's canonical bytes, [MarshalPlanFile] of Plan.
	Document []byte
	// Path is where the document was saved, and empty when it was not.
	Path string
	// Undecided are the declared objects the comparison withheld, from
	// [PreparePlanFileReportingUndecided].
	Undecided schemadiff.Diagnostics
	// Err is the run's error, and nil when it returned cleanly.
	Err error
}

// NewPlanReport classifies one plan run from its evidence.
func NewPlanReport(evidence PlanEvidence) PlanReport {
	report := PlanReport{
		ContractVersion: PlanReportContractVersion,
		Command:         PlanReportCommand,
		Undecided:       evidence.Undecided.Clone(),
	}
	if evidence.Err != nil {
		report.Error = errorkind.NewReport(evidence.Err)
		report.ProviderDiagnostic, _ = errors.AsType[*providerdiagnostic.Diagnostic](evidence.Err)
		report.SchemaDiagnostics = schemaDiagnostics(evidence.Err)
		report.Refusal = refusalFor(evidence.Err)
		report.Outcome = PlanOutcomeFailed
		if report.Refusal != nil {
			report.Outcome = PlanOutcomeRefused
		}
		return report
	}
	if evidence.Plan == nil || !evidence.Plan.HasChanges() {
		report.Outcome = PlanOutcomeNoChanges
		if evidence.Plan != nil && len(evidence.Plan.Annotations) != 0 {
			report.Plan = evidence.Plan
			if len(evidence.Document) != 0 {
				report.PlanDigest = digest.FromBytes(evidence.Document).String()
				report.PlanPath = evidence.Path
			}
		}
		return report
	}
	report.Outcome = PlanOutcomeChanges
	report.Plan = evidence.Plan
	report.PlanDigest = digest.FromBytes(evidence.Document).String()
	report.PlanPath = evidence.Path
	return report
}

// ApplyReport is the machine-readable record of one `schema apply` run.
type ApplyReport struct {
	// ContractVersion is [ApplyReportContractVersion].
	ContractVersion int `json:"contract_version"`
	// Command is [ApplyReportCommand].
	Command string `json:"command"`
	// Outcome is how the run ended.
	Outcome ApplyOutcome `json:"outcome"`
	// PlanName and PlanDigest identify the plan file --plan read: the name it
	// records, and the SHA-256 of the bytes it was decoded from, in
	// sha256:<hex> form. The file is read once, so those are also the bytes
	// --require-approval verified and the statements that ran. Both are empty
	// for a plan computed from a desired schema, and for a run that stopped
	// before it decoded the file.
	PlanName   string `json:"plan_name,omitempty"`
	PlanDigest string `json:"plan_digest,omitempty"`
	// Units are the operations the run listed as its planned changes, in
	// order, and absent when it stopped before it had any. The outcome says
	// what became of them: under applied they are what ran.
	Units []ApplyUnitReport `json:"units,omitempty"`
	// Annotations are non-executable notes positioned among Units.
	Annotations []renderer.Annotation `json:"annotations,omitempty"`
	// Undecided is [PlanReport.Undecided] for this run: next to applied it
	// names what the run did not create, and next to no-changes it means the
	// database is not shown to match. It is absent when nothing was withheld,
	// and for a run that read --plan: a plan file records statements, and what
	// was withheld when it was computed is in the report of the `schema plan`
	// run that computed it.
	Undecided schemadiff.Diagnostics `json:"undecided,omitzero"`
	// Refusal is present when Outcome is refused.
	Refusal *Refusal `json:"refusal,omitempty"`
	// Error is the run's error, present when Outcome is refused, failed or
	// unknown: its kind, when the failure is one [errorkind] names, and the
	// sentence the command prints on standard error.
	Error *errorkind.Report `json:"error,omitempty"`
	// Failure names the unit or the commit the run stopped at and the code the
	// database answered with. It is present only when Outcome is unknown and a
	// statement or the commit failed. A run that stopped before any statement
	// left has none, and neither does one that stopped at something else, such as
	// a lock lost after every statement succeeded.
	Failure *ApplyFailure `json:"failure,omitempty"`
	// ProviderDiagnostic preserves structured provider setup and trust failures.
	ProviderDiagnostic *providerdiagnostic.Diagnostic `json:"provider_diagnostic,omitempty"`
	// SchemaDiagnostics is [PlanReport.SchemaDiagnostics] for this run.
	SchemaDiagnostics []providerdiagnostic.SchemaDiagnostic `json:"schema_diagnostics,omitempty"`
}

// ApplyFailure identifies the unit or the commit a dispatched apply stopped at,
// and the code the database answered with.
//
// It carries no text. The message of [ApplyReport.Error] quotes what the database
// said, and that can include a row of data or the statement itself; a caller that
// must not publish data reads this instead. It says where the run stopped, which
// is not what the database holds: see [ApplyOutcomeUnknown].
type ApplyFailure struct {
	// Statement is the 1-based position in [ApplyReport.Units] of the unit the run
	// stopped at, and Statements is how many entries Units holds. Each entry is
	// one request to the database, however many changes the planner merged into it
	// or split out of it, so Units[Statement-1] is the one that failed.
	//
	// An executable provider reports a confirmed prefix rather than a failed unit,
	// so there Statement is the first unit it did not confirm: the provider
	// stopped there or later. Statement is zero when no unit is to blame: the
	// commit failed, or the provider confirmed every unit and its answer was still
	// not a success.
	Statement  int `json:"statement"`
	Statements int `json:"statements"`
	// Commit reports that every unit ran and the commit that ends them failed, as
	// a deferred constraint does.
	Commit bool `json:"commit,omitempty"`
	// SQLState is the SQLSTATE the server answered with, read from a
	// PostgreSQL-protocol or MySQL-protocol driver error. It is empty when the
	// server gave no answer, such as after a lost connection or a canceled
	// statement, on an engine with no SQLSTATE, such as SQL Server, and from an
	// executable provider, whose protocol has no field for one.
	SQLState string `json:"sqlstate,omitempty"`
	// ErrorNumber is the server's error number, such as 1062 for a duplicate key
	// on MySQL and 2627 on SQL Server. It is read from a MySQL-protocol, SQL
	// Server, Oracle or ClickHouse driver error, means something only on that
	// engine, and is zero everywhere else.
	ErrorNumber int `json:"error_number,omitempty"`
}

// ApplyEvidence is what a caller saw of one `schema apply` run.
type ApplyEvidence struct {
	// PlanName and PlanDigest identify the plan file the run read, and are
	// empty for a plan computed from a desired schema.
	PlanName   string
	PlanDigest string
	// Units are the operations the run listed as its planned changes.
	Units []ApplyUnitReport
	// Annotations are the non-executable notes shown with Units.
	Annotations []renderer.Annotation
	// Undecided are the declared objects the comparison withheld, from
	// [ApplyRuntimePlan.Undecided].
	Undecided schemadiff.Diagnostics
	// Dispatched reports that a statement left for the database. It is set as
	// the first one leaves, so a run that died sending it still reports it, and
	// it stays set, so a failure later in the run does not read as nothing having
	// reached the database. Reading the plan again, checking it, and beginning a
	// transaction send no statement and do not set it.
	Dispatched bool
	// Completed is how a run that returned without an error ended: applied,
	// no-changes, dry-run or canceled. It is not read when Err is set.
	Completed ApplyOutcome
	// Err is the run's error, and nil when it returned cleanly.
	Err error
}

// NewApplyReport classifies one apply run from its evidence.
//
// An error after dispatch is unknown whatever its type: the statements were
// sent, and no error the run returns afterwards says which of them committed.
// A lock session lost on release is the case in point. Its error arrives after
// every statement succeeded, and the command reports a failure rather than let
// a lost session read as success.
//
// An error before dispatch is refused or failed, and that includes a transaction
// that could not begin, however it failed. A connection lost during BEGIN leaves
// a session whose state nobody knows, but no statement of the plan was in it:
// the request that begins a transaction carries none.
func NewApplyReport(evidence ApplyEvidence) ApplyReport {
	report := ApplyReport{
		ContractVersion: ApplyReportContractVersion,
		Command:         ApplyReportCommand,
		PlanName:        evidence.PlanName,
		PlanDigest:      evidence.PlanDigest,
		Units:           evidence.Units,
		Annotations:     slices.Clone(evidence.Annotations),
		Undecided:       evidence.Undecided.Clone(),
		Outcome:         evidence.Completed,
	}
	if evidence.Err == nil {
		return report
	}
	report.Error = errorkind.NewReport(evidence.Err)
	report.ProviderDiagnostic, _ = errors.AsType[*providerdiagnostic.Diagnostic](evidence.Err)
	report.SchemaDiagnostics = schemaDiagnostics(evidence.Err)
	if evidence.Dispatched {
		report.Outcome = ApplyOutcomeUnknown
		report.Failure = applyFailure(evidence.Err)
		return report
	}
	report.Refusal = refusalFor(evidence.Err)
	report.Outcome = ApplyOutcomeFailed
	if report.Refusal != nil {
		report.Outcome = ApplyOutcomeRefused
	}
	return report
}

// applyFailure reads where a dispatched run stopped out of its error, and nil
// when no statement and no commit failed.
//
// It is asked only after dispatch. Before dispatch an error can name a statement
// too, a transaction preflight or a failed rehearsal on the dev database, and the
// target never saw it.
//
// The codes are read off the failing step's own error, because a rollback's
// failure joined beside it is not the answer to that step.
func applyFailure(err error) *ApplyFailure {
	failure := applyFailureAt(err)
	if failure == nil {
		return nil
	}
	if listed, ok := errors.AsType[*listedStatementsError](err); ok {
		failure.Statement = listed.position(failure.Statement)
		failure.Statements = listed.total
	}
	return failure
}

// applyFailureAt is the failure err names, in positions among the units the run
// executed.
func applyFailureAt(err error) *ApplyFailure {
	if commit, ok := errors.AsType[*failedCommitError](err); ok {
		failure := &ApplyFailure{Statements: commit.statements, Commit: true}
		failure.SQLState, failure.ErrorNumber = servercode.Read(commit.err)
		return failure
	}
	if statement, ok := errors.AsType[*migrator.MigrationExecutionError](err); ok {
		failure := &ApplyFailure{Statement: statement.StatementIndex, Statements: statement.Total}
		failure.SQLState, failure.ErrorNumber = servercode.Read(statement.Err)
		return failure
	}
	if batch, ok := errors.AsType[*dbschema.UnitExecutionError](err); ok {
		failure := &ApplyFailure{Statements: batch.Total}
		if batch.Applied < batch.Total {
			failure.Statement = batch.Applied + 1
		}
		return failure
	}
	return nil
}

// schemaDiagnostics returns every diagnostic of a provider's refusal of the SQL
// schema documents it read, and nil for any other error.
func schemaDiagnostics(err error) []providerdiagnostic.SchemaDiagnostic {
	refused, ok := errors.AsType[*providerdiagnostic.SchemaRefusal](err)
	if !ok {
		return nil
	}
	return slices.Clone(refused.Diagnostics)
}

// refusalFor names the refusal err carries, and nil when it carries none.
//
// Every error recognized here is raised before a statement is sent to the
// target. That is what lets a refusal promise that nothing reached the target
// database, and it is why [NewApplyReport] asks only once it knows the
// statements were not dispatched. A failed rehearsal is no exception, and its
// promise is as narrow as the one [SimulationError] makes: the command did not
// send the plan to the target. The statements did run on the dev database, and
// what they did there, or through it, is outside what the error can speak for.
func refusalFor(err error) *Refusal {
	var stale *StalePlanError
	var fenced *ProtectedTableError
	var preflight *TransactionPreflightError
	var unavailable *TransactionUnavailableError
	var ownerRequirement *txrequire.OwnerRequirementError
	var simulation *SimulationError
	var lostViewSecurity *planrefusal.LostViewSecurityError
	var dependentObject *planrefusal.DependentObjectError
	var declaration *planrefusal.DeclarationError
	switch {
	case errors.As(err, &stale):
		changed := "schema"
		if stale.Rows {
			changed = "rows"
		}
		return &Refusal{
			Code:                RefusalStalePlan,
			Changed:             changed,
			PlanFingerprint:     stale.PlanFingerprint,
			DatabaseFingerprint: stale.DatabaseFingerprint,
		}
	case errors.As(err, &fenced):
		return &Refusal{Code: RefusalProtectedTable, Tables: fenced.Tables}
	case IsLockTimeout(err):
		return &Refusal{Code: RefusalLockTimeout}
	case errors.As(err, &preflight), errors.As(err, &ownerRequirement):
		return &Refusal{Code: RefusalTransactionPreflight}
	case errors.As(err, &unavailable):
		return &Refusal{Code: RefusalTransactionUnavailable}
	case errors.As(err, &simulation):
		return &Refusal{Code: RefusalSimulationFailed}
	case errors.As(err, &lostViewSecurity):
		return &Refusal{Code: RefusalViewSecurityOption, ViewOptions: lostViewSecurity.Options}
	case errors.As(err, &dependentObject):
		return &Refusal{Code: RefusalDependentObject}
	case errors.As(err, &declaration):
		return &Refusal{Code: RefusalDeclarationRefused}
	default:
		return nil
	}
}
