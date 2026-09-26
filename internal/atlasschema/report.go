package atlasschema

import (
	"errors"

	digest "github.com/opencontainers/go-digest"
)

// PlanReportContractVersion is the version of the document `ptah schema plan
// --json` prints, and ApplyReportContractVersion the version of the one
// `ptah schema apply --json` prints.
//
// A version rises when a field changes meaning or leaves, and when an outcome
// gains a value, because a reader cannot act on an outcome it does not know. A
// new field a reader may ignore is not a new version. Neither is a new refusal
// code: a refusal of any code means that nothing reached the target database,
// which a reader that does not know the code can still act on. A consumer that
// does not know the version it reads refuses the document rather than reading
// the fields it recognizes out of a shape that means something else.
const (
	PlanReportContractVersion  = 1
	ApplyReportContractVersion = 1
)

// PlanOutcome is how one `schema plan` run ended.
type PlanOutcome string

const (
	// PlanOutcomeChanges means the plan holds statements, and the report
	// carries the plan document.
	PlanOutcomeChanges PlanOutcome = "changes"
	// PlanOutcomeNoChanges means the database already matches the desired
	// schema. No plan document exists, and none was saved.
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
	// ApplyOutcomeNoChanges means the database already matches the desired
	// schema, so there was nothing to run.
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
	// the run returned an error. Nothing in the report says how far they got:
	// with a transaction the database may have rolled them all back, without
	// one it may hold some of them, and a lost connection hides even that.
	// Read the database before deciding anything.
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
	// RefusalTransactionPreflight means a statement cannot run inside the
	// transaction the transaction mode asks for.
	RefusalTransactionPreflight RefusalCode = "transaction-preflight"
	// RefusalSimulationFailed means the plan failed its rehearsal on the
	// --dev-url database, before the target was touched.
	RefusalSimulationFailed RefusalCode = "simulation-failed"
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
}

// PlanReport is the machine-readable record of one `schema plan` run.
type PlanReport struct {
	// ContractVersion is [PlanReportContractVersion].
	ContractVersion int `json:"contract_version"`
	// Outcome is how the run ended.
	Outcome PlanOutcome `json:"outcome"`
	// PlanDigest is the SHA-256 of the plan document's canonical bytes, in
	// sha256:<hex> form: the file --save and --output write and the document
	// --dry-run prints without --json. It is present with the plan.
	PlanDigest string `json:"plan_digest,omitempty"`
	// PlanPath is where --save or --output wrote the plan document, and empty
	// under --dry-run.
	PlanPath string `json:"plan_path,omitempty"`
	// Plan is the plan document, present when Outcome is changes.
	Plan *PlanFile `json:"plan,omitempty"`
	// Refusal is present when Outcome is refused.
	Refusal *Refusal `json:"refusal,omitempty"`
	// Error is the run's error message, present when Outcome is refused or
	// failed. It is the sentence the command prints on standard error.
	Error string `json:"error,omitempty"`
}

// PlanEvidence is what a caller saw of one `schema plan` run.
type PlanEvidence struct {
	// Plan is the computed plan, and nil when planning did not finish.
	Plan *PlanFile
	// Document is the plan's canonical bytes, [MarshalPlanFile] of Plan.
	Document []byte
	// Path is where the document was saved, and empty when it was not.
	Path string
	// Err is the run's error, and nil when it returned cleanly.
	Err error
}

// NewPlanReport classifies one plan run from its evidence.
func NewPlanReport(evidence PlanEvidence) PlanReport {
	report := PlanReport{ContractVersion: PlanReportContractVersion}
	if evidence.Err != nil {
		report.Error = evidence.Err.Error()
		report.Refusal = refusalFor(evidence.Err)
		report.Outcome = PlanOutcomeFailed
		if report.Refusal != nil {
			report.Outcome = PlanOutcomeRefused
		}
		return report
	}
	if evidence.Plan == nil || !evidence.Plan.HasChanges() {
		report.Outcome = PlanOutcomeNoChanges
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
	// Outcome is how the run ended.
	Outcome ApplyOutcome `json:"outcome"`
	// PlanName and PlanDigest identify the plan file --plan read: the name it
	// records, and the SHA-256 of the bytes it was decoded from, in
	// sha256:<hex> form. Both are empty for a plan computed from a desired
	// schema.
	PlanName   string `json:"plan_name,omitempty"`
	PlanDigest string `json:"plan_digest,omitempty"`
	// Statements are the statements the run listed as its planned changes, in
	// order, and absent when it stopped before it had any. The outcome says
	// what became of them: under applied they are what ran.
	Statements []string `json:"statements,omitempty"`
	// Refusal is present when Outcome is refused.
	Refusal *Refusal `json:"refusal,omitempty"`
	// Error is the run's error message, present when Outcome is refused,
	// failed or unknown. It is the sentence the command prints on standard
	// error.
	Error string `json:"error,omitempty"`
}

// ApplyEvidence is what a caller saw of one `schema apply` run.
type ApplyEvidence struct {
	// PlanName and PlanDigest identify the plan file the run read, and are
	// empty for a plan computed from a desired schema.
	PlanName   string
	PlanDigest string
	// Statements are the statements the run listed as its planned changes.
	Statements []string
	// Dispatched reports that the statements were handed to the database. It
	// is set before execution starts, so a run that died inside it still
	// reports it.
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
func NewApplyReport(evidence ApplyEvidence) ApplyReport {
	report := ApplyReport{
		ContractVersion: ApplyReportContractVersion,
		PlanName:        evidence.PlanName,
		PlanDigest:      evidence.PlanDigest,
		Statements:      evidence.Statements,
		Outcome:         evidence.Completed,
	}
	if evidence.Err == nil {
		return report
	}
	report.Error = evidence.Err.Error()
	if evidence.Dispatched {
		report.Outcome = ApplyOutcomeUnknown
		return report
	}
	report.Refusal = refusalFor(evidence.Err)
	report.Outcome = ApplyOutcomeFailed
	if report.Refusal != nil {
		report.Outcome = ApplyOutcomeRefused
	}
	return report
}

// refusalFor names the refusal err carries, and nil when it carries none.
//
// Every error recognized here is raised before a statement is sent to the
// target. That is what lets a refusal promise that nothing reached the target
// database, and it is why [NewApplyReport] asks only once it knows the
// statements were not dispatched. A failed rehearsal is no exception: its
// statements ran on the dev database and never on the target.
func refusalFor(err error) *Refusal {
	var stale *StalePlanError
	var fenced *ProtectedTableError
	var preflight *TransactionPreflightError
	var simulation *SimulationError
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
	case errors.As(err, &preflight):
		return &Refusal{Code: RefusalTransactionPreflight}
	case errors.As(err, &simulation):
		return &Refusal{Code: RefusalSimulationFailed}
	default:
		return nil
	}
}
