//go:build integration

package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// planDirectiveConcurrentEnv makes the planner emit CREATE INDEX CONCURRENTLY,
// the statement PostgreSQL refuses inside a transaction block.
const planDirectiveConcurrentEnv = `env "local" {
  diff {
    concurrent_index {
      create = true
    }
  }
}
`

// planDirectivePlainEnv leaves the planner on an ordinary CREATE INDEX, which
// any transaction mode can apply.
const planDirectivePlainEnv = `env "local" {
}
`

// planDirectiveNativeRow is one native `ptah schema apply --plan` run over a
// plan written by `ptah-compat schema plan`.
type planDirectiveNativeRow struct {
	name        string
	env         string
	directive   []string
	applyTxMode []string
}

// planDirectiveNativeBinaries builds the native binary under test and the
// compatibility binary that writes the plan file.
//
// The plan comes from `ptah-compat schema plan --directive` because that is the
// command that writes a `-- atlas:txmode` header into a native JSON plan; the
// native `ptah schema plan` has no such flag.
func planDirectiveNativeBinaries(c *qt.C, ctx context.Context, repoRoot string) (native, compat string) {
	c.Helper()
	dir := c.TempDir()
	native = filepath.Join(dir, "ptah")
	compat = filepath.Join(dir, "ptah-compat")
	buildPtah(c, ctx, repoRoot, native)
	buildPtahCompat(c, ctx, repoRoot, compat)
	return native, compat
}

// applyNativePlanDirectiveRow seeds a throwaway database, writes the row's plan
// with the compatibility binary, applies it with the native binary, and returns
// the native output, whether the index now exists, and the native error.
func applyNativePlanDirectiveRow(
	c *qt.C,
	ctx context.Context,
	adminURL, native, compat string,
	row planDirectiveNativeRow,
) (applyOut string, indexExists bool, applyErr error) {
	c.Helper()
	work := c.TempDir()
	const table = "plan_directive"
	const index = "plan_directive_email_idx"

	// A throwaway database per row: the plan is fingerprinted against the
	// whole database, and the shared one holds other tests' tables.
	rowURL := newPlanDirectiveDatabase(c, ctx, adminURL)
	db := openPlanDirectiveDB(c, rowURL)

	desiredPath := filepath.Join(work, "desired.sql")
	c.Assert(os.WriteFile(desiredPath, []byte(
		"CREATE TABLE "+table+" (id INTEGER PRIMARY KEY, email TEXT);\n"+
			"CREATE INDEX "+index+" ON "+table+" (email);\n"), 0o600), qt.IsNil)
	// The seed leaves the index as the only difference, so the plan is one
	// statement.
	runPlanDirectiveSQL(c, ctx, db,
		"CREATE TABLE "+table+" (id INTEGER PRIMARY KEY, email TEXT)")
	c.Assert(os.WriteFile(filepath.Join(work, "atlas.hcl"), []byte(row.env), 0o600), qt.IsNil)

	planPath := filepath.Join(work, "directive.plan.json")
	planArgs := append([]string{
		"schema", "plan",
		"--env", "local",
		"--from", rowURL,
		"--to", "file://" + desiredPath,
		"--output", planPath,
	}, row.directive...)
	planCmd := exec.CommandContext(ctx, compat, planArgs...)
	planCmd.Dir = work
	planOut, planErr := planCmd.CombinedOutput()
	c.Assert(planErr, qt.IsNil, qt.Commentf("plan:\n%s", planOut))

	applyArgs := append([]string{
		"schema", "apply",
		"--db-url", rowURL,
		"--plan", planPath,
		"--auto-approve",
	}, row.applyTxMode...)
	applyCmd := exec.CommandContext(ctx, native, applyArgs...)
	applyCmd.Dir = work
	out, err := applyCmd.CombinedOutput()
	return string(out), planDirectiveE2EIndexExists(c, ctx, db, index), err
}

// TestSchemaApplyPlanDirectiveTxModeE2E_HappyPath is stokaro/ptah#3284: the
// native `ptah schema apply --plan` executes a plan in the transaction mode its
// `-- atlas:txmode` header selects.
//
// PostgreSQL is the target that can tell the difference from the outside: it
// refuses CREATE INDEX CONCURRENTLY inside a transaction block. The catalog is
// the observable, not the exit code, because an apply that wrapped the
// statement and reported anything at all would still leave no index.
//
// The directive row is the finding. The flag row is the control that fails if
// the plan had become unappliable for a reason unrelated to the transaction
// mode: the same concurrent plan, without the header, applies when the
// operator types the mode the header would have supplied.
func TestSchemaApplyPlanDirectiveTxModeE2E_HappyPath(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	native, compat := planDirectiveNativeBinaries(c, ctx, e2eRepoRoot(t))

	tests := []planDirectiveNativeRow{
		{
			name:      "the plan's directive supplies the transaction mode",
			env:       planDirectiveConcurrentEnv,
			directive: []string{"-d", "atlas:txmode none"},
		},
		{
			name:        "the operator can supply the same mode by flag",
			env:         planDirectiveConcurrentEnv,
			applyTxMode: []string{"--tx-mode", "none"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			out, indexExists, err := applyNativePlanDirectiveRow(c, ctx, adminURL, native, compat, test)

			c.Assert(err, qt.IsNil, qt.Commentf("apply:\n%s", out))
			c.Assert(out, qt.Contains, "Schema apply completed successfully.")
			c.Assert(indexExists, qt.IsTrue, qt.Commentf("apply:\n%s", out))
		})
	}
}

// TestSchemaApplyPlanDirectiveTxModeE2E_FailurePath pins the refusals on
// either side of the directive.
//
// The --tx-mode all row is the finding: a plan whose header selects none is
// refused under a global all, as ptah-compat refuses the same file, instead of
// being applied in one transaction and reported as a success. Its plan is an
// ordinary CREATE INDEX, so only the refusal can leave the index absent.
//
// The concurrent row is the control for the happy path's directive row: with
// neither a header nor a flag, the transaction preflight refuses the concurrent
// index before anything runs, so that row cannot pass because CONCURRENTLY was
// appliable under the default mode.
func TestSchemaApplyPlanDirectiveTxModeE2E_FailurePath(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	native, compat := planDirectiveNativeBinaries(c, ctx, e2eRepoRoot(t))

	tests := []struct {
		row     planDirectiveNativeRow
		wantOut string
	}{
		{
			row: planDirectiveNativeRow{
				name:        "tx-mode all refuses a plan whose directive selects none",
				env:         planDirectivePlainEnv,
				directive:   []string{"-d", "atlas:txmode none"},
				applyTxMode: []string{"--tx-mode", "all"},
			},
			wantOut: `(?s).*cannot set txmode directive to "none" in ".*directive\.plan\.json" when txmode "all" is set globally.*`,
		},
		{
			row: planDirectiveNativeRow{
				name: "without a directive or a flag the concurrent index is refused",
				env:  planDirectiveConcurrentEnv,
			},
			wantOut: `(?s).*the planned changes cannot run inside a transaction: ` +
				`statement 1 of 1: CREATE or DROP INDEX CONCURRENTLY is refused inside a ` +
				`transaction block; rerun with --tx-mode none.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.row.name, func(t *testing.T) {
			c := qt.New(t)

			out, indexExists, err := applyNativePlanDirectiveRow(c, ctx, adminURL, native, compat, test.row)

			c.Assert(err, qt.IsNotNil, qt.Commentf("apply:\n%s", out))
			c.Assert(out, qt.Matches, test.wantOut)
			c.Assert(out, qt.Not(qt.Contains), "Schema apply completed successfully.")
			c.Assert(indexExists, qt.IsFalse, qt.Commentf("apply:\n%s", out))
		})
	}
}
