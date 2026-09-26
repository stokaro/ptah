package atlasschema_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	digest "github.com/opencontainers/go-digest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dblock"
	"ptah.run/migration/migrator"
	"ptah.run/migration/safety"
)

// reportErrors are the errors the report rows classify: one of each refusal
// the contract names, wrapped the way the command's own path wraps some of
// them, and one error that names none.
type reportErrors struct {
	staleSchema error
	staleRows   error
	fenced      error
	lockTimeout error
	preflight   error
	simulation  error
	plain       error
}

func newReportErrors(c *qt.C) reportErrors {
	c.Helper()
	preflight := atlasschema.PreflightApplyTransaction(
		platform.Postgres, capability.Postgres16(), migrator.MigrationTxModeFile,
		[]string{`CREATE INDEX CONCURRENTLY "idx_a" ON "widgets" ("a")`},
	)
	var preflightErr *atlasschema.TransactionPreflightError
	c.Assert(preflight, qt.ErrorAs, &preflightErr)
	return reportErrors{
		staleSchema: &atlasschema.StalePlanError{
			PlanFingerprint:     "sha256:" + fmt.Sprintf("%064x", 1),
			DatabaseFingerprint: "sha256:" + fmt.Sprintf("%064x", 2),
		},
		staleRows: &atlasschema.StalePlanError{
			PlanFingerprint:     "sha256:" + fmt.Sprintf("%064x", 3),
			DatabaseFingerprint: "sha256:" + fmt.Sprintf("%064x", 4),
			Rows:                true,
		},
		fenced: &atlasschema.ProtectedTableError{Tables: []string{"public.regions", "regions"}},
		lockTimeout: fmt.Errorf("acquire schema apply lock: %w", &dblock.TimeoutError{
			Dialect: platform.Postgres, Name: atlasschema.ApplyLockName, Timeout: 5 * time.Second,
		}),
		preflight:  preflight,
		simulation: &atlasschema.SimulationError{Stage: "plan", Err: errors.New("no such table: users")},
		plain:      errors.New("connect to --db-url: connection refused"),
	}
}

// TestNewApplyReport_ClassifiesFromTheEvidence pins the rule a caller decides
// from: which outcomes say nothing reached the database, and which refusal a
// typed error becomes.
//
// The dispatched rows are the ones the rule exists for. A refusal's own type
// is not evidence once the statements were sent, so an error of any type after
// dispatch is unknown -- including one that would have been a refusal a moment
// earlier.
func TestNewApplyReport_ClassifiesFromTheEvidence(t *testing.T) {
	c := qt.New(t)
	errs := newReportErrors(c)
	statements := []string{`CREATE TABLE "orders" ("id" integer)`}

	tests := []struct {
		name     string
		evidence atlasschema.ApplyEvidence
		want     atlasschema.ApplyReport
	}{
		{
			name: "applied",
			evidence: atlasschema.ApplyEvidence{
				PlanName: "add_orders", PlanDigest: "sha256:" + fmt.Sprintf("%064x", 9),
				Statements: statements, Dispatched: true, Completed: atlasschema.ApplyOutcomeApplied,
			},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeApplied,
				PlanName:        "add_orders",
				PlanDigest:      "sha256:" + fmt.Sprintf("%064x", 9),
				Statements:      statements,
			},
		},
		{
			name:     "nothing to change",
			evidence: atlasschema.ApplyEvidence{Completed: atlasschema.ApplyOutcomeNoChanges},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeNoChanges,
			},
		},
		{
			name:     "dry run",
			evidence: atlasschema.ApplyEvidence{Statements: statements, Completed: atlasschema.ApplyOutcomeDryRun},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeDryRun,
				Statements:      statements,
			},
		},
		{
			name:     "confirmation declined",
			evidence: atlasschema.ApplyEvidence{Statements: statements, Completed: atlasschema.ApplyOutcomeCanceled},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeCanceled,
				Statements:      statements,
			},
		},
		{
			name:     "stale schema",
			evidence: atlasschema.ApplyEvidence{Err: errs.staleSchema},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeRefused,
				Refusal: &atlasschema.Refusal{
					Code:                atlasschema.RefusalStalePlan,
					Changed:             "schema",
					PlanFingerprint:     "sha256:" + fmt.Sprintf("%064x", 1),
					DatabaseFingerprint: "sha256:" + fmt.Sprintf("%064x", 2),
				},
				Error: errs.staleSchema.Error(),
			},
		},
		{
			name:     "stale rows",
			evidence: atlasschema.ApplyEvidence{Err: errs.staleRows},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeRefused,
				Refusal: &atlasschema.Refusal{
					Code:                atlasschema.RefusalStalePlan,
					Changed:             "rows",
					PlanFingerprint:     "sha256:" + fmt.Sprintf("%064x", 3),
					DatabaseFingerprint: "sha256:" + fmt.Sprintf("%064x", 4),
				},
				Error: errs.staleRows.Error(),
			},
		},
		{
			name:     "protected table",
			evidence: atlasschema.ApplyEvidence{Err: errs.fenced},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeRefused,
				Refusal: &atlasschema.Refusal{
					Code:   atlasschema.RefusalProtectedTable,
					Tables: []string{"public.regions", "regions"},
				},
				Error: errs.fenced.Error(),
			},
		},
		{
			name:     "lock timeout",
			evidence: atlasschema.ApplyEvidence{Err: errs.lockTimeout},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeRefused,
				Refusal:         &atlasschema.Refusal{Code: atlasschema.RefusalLockTimeout},
				Error:           errs.lockTimeout.Error(),
			},
		},
		{
			name:     "transaction preflight",
			evidence: atlasschema.ApplyEvidence{Statements: statements, Err: errs.preflight},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeRefused,
				Statements:      statements,
				Refusal:         &atlasschema.Refusal{Code: atlasschema.RefusalTransactionPreflight},
				Error:           errs.preflight.Error(),
			},
		},
		{
			name:     "dev database rehearsal",
			evidence: atlasschema.ApplyEvidence{Statements: statements, Err: errs.simulation},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeRefused,
				Statements:      statements,
				Refusal:         &atlasschema.Refusal{Code: atlasschema.RefusalSimulationFailed},
				Error:           errs.simulation.Error(),
			},
		},
		{
			name:     "an error that names no refusal",
			evidence: atlasschema.ApplyEvidence{Err: errs.plain},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeFailed,
				Error:           errs.plain.Error(),
			},
		},
		{
			name:     "an error after dispatch",
			evidence: atlasschema.ApplyEvidence{Statements: statements, Dispatched: true, Err: errs.plain},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeUnknown,
				Statements:      statements,
				Error:           errs.plain.Error(),
			},
		},
		{
			name: "a refusal's type after dispatch",
			evidence: atlasschema.ApplyEvidence{
				Statements: statements, Dispatched: true, Completed: atlasschema.ApplyOutcomeApplied, Err: errs.lockTimeout,
			},
			want: atlasschema.ApplyReport{
				ContractVersion: atlasschema.ApplyReportContractVersion,
				Outcome:         atlasschema.ApplyOutcomeUnknown,
				Statements:      statements,
				Error:           errs.lockTimeout.Error(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasschema.NewApplyReport(test.evidence)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestNewPlanReport_ClassifiesFromTheEvidence pins what a plan run reports:
// the document and its digest only with changes, and a refusal only for an
// error that names one.
func TestNewPlanReport_ClassifiesFromTheEvidence(t *testing.T) {
	c := qt.New(t)
	errs := newReportErrors(c)
	plan := atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "add_orders",
		Dialect:         platform.SQLite,
		FromFingerprint: "sha256:" + fmt.Sprintf("%064x", 5),
		ToFingerprint:   "sha256:" + fmt.Sprintf("%064x", 6),
		Statements: []atlasschema.PlanStatement{
			{SQL: `CREATE TABLE "orders" ("id" integer)`, Severity: safety.Safe, Reason: "does not remove data or tighten constraints"},
		},
	}
	document, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	synced := atlasschema.PlanFile{FormatVersion: atlasschema.PlanFormatVersion, Dialect: platform.SQLite}

	tests := []struct {
		name     string
		evidence atlasschema.PlanEvidence
		want     atlasschema.PlanReport
	}{
		{
			name:     "changes saved to a file",
			evidence: atlasschema.PlanEvidence{Plan: &plan, Document: document, Path: "add_orders.plan.json"},
			want: atlasschema.PlanReport{
				ContractVersion: atlasschema.PlanReportContractVersion,
				Outcome:         atlasschema.PlanOutcomeChanges,
				PlanDigest:      digest.FromBytes(document).String(),
				PlanPath:        "add_orders.plan.json",
				Plan:            &plan,
			},
		},
		{
			name:     "changes previewed",
			evidence: atlasschema.PlanEvidence{Plan: &plan, Document: document},
			want: atlasschema.PlanReport{
				ContractVersion: atlasschema.PlanReportContractVersion,
				Outcome:         atlasschema.PlanOutcomeChanges,
				PlanDigest:      digest.FromBytes(document).String(),
				Plan:            &plan,
			},
		},
		{
			name:     "nothing to change",
			evidence: atlasschema.PlanEvidence{Plan: &synced},
			want: atlasschema.PlanReport{
				ContractVersion: atlasschema.PlanReportContractVersion,
				Outcome:         atlasschema.PlanOutcomeNoChanges,
			},
		},
		{
			name:     "protected table",
			evidence: atlasschema.PlanEvidence{Err: errs.fenced},
			want: atlasschema.PlanReport{
				ContractVersion: atlasschema.PlanReportContractVersion,
				Outcome:         atlasschema.PlanOutcomeRefused,
				Refusal: &atlasschema.Refusal{
					Code:   atlasschema.RefusalProtectedTable,
					Tables: []string{"public.regions", "regions"},
				},
				Error: errs.fenced.Error(),
			},
		},
		{
			name:     "an error that names no refusal",
			evidence: atlasschema.PlanEvidence{Err: errs.plain},
			want: atlasschema.PlanReport{
				ContractVersion: atlasschema.PlanReportContractVersion,
				Outcome:         atlasschema.PlanOutcomeFailed,
				Error:           errs.plain.Error(),
			},
		},
		{
			name:     "a failed save keeps no plan",
			evidence: atlasschema.PlanEvidence{Plan: &plan, Document: document, Path: "add_orders.plan.json", Err: errs.plain},
			want: atlasschema.PlanReport{
				ContractVersion: atlasschema.PlanReportContractVersion,
				Outcome:         atlasschema.PlanOutcomeFailed,
				Error:           errs.plain.Error(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasschema.NewPlanReport(test.evidence)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestReports_WireNames pins the field names a consumer parses. They are the
// contract the version number stands for, and a Go rename that changed one
// would pass every test that reads the report back through the same type.
func TestReports_WireNames(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "p",
		Dialect:         platform.SQLite,
		FromFingerprint: "sha256:f",
		ToFingerprint:   "sha256:t",
		Statements:      []atlasschema.PlanStatement{{SQL: "S", Severity: safety.Destructive, Reason: "R"}},
		Destructive:     true,
	}

	planReport, err := json.Marshal(atlasschema.PlanReport{
		ContractVersion: 1,
		Outcome:         atlasschema.PlanOutcomeChanges,
		PlanDigest:      "sha256:d",
		PlanPath:        "p.plan.json",
		Plan:            &plan,
		Refusal: &atlasschema.Refusal{
			Code: atlasschema.RefusalStalePlan, Tables: []string{"t"}, Changed: "rows",
			PlanFingerprint: "sha256:a", DatabaseFingerprint: "sha256:b",
		},
		Error: "e",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(planReport), qt.Equals, `{"contract_version":1,"outcome":"changes",`+
		`"plan_digest":"sha256:d","plan_path":"p.plan.json",`+
		`"plan":{"format_version":1,"name":"p","dialect":"sqlite","from_fingerprint":"sha256:f",`+
		`"to_fingerprint":"sha256:t","destructive":true,"statements":[{"sql":"S","severity":"destructive","reason":"R"}]},`+
		`"refusal":{"code":"stale-plan","tables":["t"],"changed":"rows",`+
		`"plan_fingerprint":"sha256:a","database_fingerprint":"sha256:b"},"error":"e"}`)

	applyReport, err := json.Marshal(atlasschema.ApplyReport{
		ContractVersion: 1,
		Outcome:         atlasschema.ApplyOutcomeUnknown,
		PlanName:        "p",
		PlanDigest:      "sha256:d",
		Statements:      []string{"S"},
		Refusal:         &atlasschema.Refusal{Code: atlasschema.RefusalLockTimeout},
		Error:           "e",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(applyReport), qt.Equals, `{"contract_version":1,"outcome":"unknown",`+
		`"plan_name":"p","plan_digest":"sha256:d","statements":["S"],`+
		`"refusal":{"code":"lock-timeout"},"error":"e"}`)
}

// TestReports_OutcomeAndRefusalValues pins every value a consumer switches on.
// A new outcome raises the contract version and a new refusal code does not,
// so the two lists are asserted whole: adding to either is a decision this row
// makes visible.
func TestReports_OutcomeAndRefusalValues(t *testing.T) {
	c := qt.New(t)

	c.Assert([]atlasschema.PlanOutcome{
		atlasschema.PlanOutcomeChanges,
		atlasschema.PlanOutcomeNoChanges,
		atlasschema.PlanOutcomeRefused,
		atlasschema.PlanOutcomeFailed,
	}, qt.DeepEquals, []atlasschema.PlanOutcome{"changes", "no-changes", "refused", "failed"})
	c.Assert([]atlasschema.ApplyOutcome{
		atlasschema.ApplyOutcomeApplied,
		atlasschema.ApplyOutcomeNoChanges,
		atlasschema.ApplyOutcomeDryRun,
		atlasschema.ApplyOutcomeCanceled,
		atlasschema.ApplyOutcomeRefused,
		atlasschema.ApplyOutcomeFailed,
		atlasschema.ApplyOutcomeUnknown,
	}, qt.DeepEquals, []atlasschema.ApplyOutcome{
		"applied", "no-changes", "dry-run", "canceled", "refused", "failed", "unknown",
	})
	c.Assert([]atlasschema.RefusalCode{
		atlasschema.RefusalStalePlan,
		atlasschema.RefusalProtectedTable,
		atlasschema.RefusalLockTimeout,
		atlasschema.RefusalTransactionPreflight,
		atlasschema.RefusalSimulationFailed,
	}, qt.DeepEquals, []atlasschema.RefusalCode{
		"stale-plan", "protected-table", "lock-timeout", "transaction-preflight", "simulation-failed",
	})
	c.Assert(atlasschema.PlanReportContractVersion, qt.Equals, 1)
	c.Assert(atlasschema.ApplyReportContractVersion, qt.Equals, 1)
}

func TestReadPlanFileDigest_HappyPath(t *testing.T) {
	c := qt.New(t)
	plan := atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "add_orders",
		Dialect:         platform.SQLite,
		FromFingerprint: "sha256:" + fmt.Sprintf("%064x", 7),
		ToFingerprint:   "sha256:" + fmt.Sprintf("%064x", 8),
		Statements:      []atlasschema.PlanStatement{{SQL: `CREATE TABLE "orders" ("id" integer)`, Severity: safety.Safe}},
	}
	document, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	// A reformatted document is the same plan and different bytes, so the
	// digest is of what was read rather than of a re-encoding.
	reformatted := append([]byte("\n"), document...)
	path := filepath.Join(c.TempDir(), "add_orders.plan.json")
	c.Assert(os.WriteFile(path, reformatted, 0o600), qt.IsNil)

	got, gotDigest, err := atlasschema.ReadPlanFileDigest(path)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, plan)
	c.Assert(gotDigest, qt.Equals, digest.FromBytes(reformatted).String())
	c.Assert(gotDigest, qt.Not(qt.Equals), digest.FromBytes(document).String())
}

func TestReadPlanFileDigest_FailurePath(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "empty.plan.json")
	c.Assert(os.WriteFile(path, []byte(`{"format_version": 1, "statements": []}`), 0o600), qt.IsNil)

	got, gotDigest, err := atlasschema.ReadPlanFileDigest(path)

	c.Assert(err, qt.ErrorMatches, `invalid plan file .*: plan dialect is required`)
	c.Assert(got, qt.DeepEquals, atlasschema.PlanFile{})
	c.Assert(gotDigest, qt.Equals, "")
}
