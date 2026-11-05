//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/rolepassword"
)

// postgresRoleLogin is why role cannot connect to target with password, or
// nil when it can.
func postgresRoleLogin(c *qt.C, target, role, password string) error {
	c.Helper()
	parsed, err := url.Parse(target)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(role, password)
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, parsed.String())
	if err != nil {
		return err
	}
	return conn.Close(ctx)
}

// postgresRoleSchema declares role, which logs in with password.
func postgresRoleSchema(c *qt.C, role, password string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.yaml")
	body := "roles:\n  " + role + ":\n    login: true\n    password: " + strconv.Quote(password) + "\n"
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// TestPostgresBinary_KeepsARolesPasswordOutOfEveryOutput drives the native
// commands through every way a declared role password reaches PostgreSQL: a
// saved plan schema apply --plan runs, a plan schema apply computes and runs
// in one go, a dry run, and a generated migration migrations up runs. Each
// role logs in with the password the schema declares, which holds a quote and
// a backslash, and not with another. Then it searches everything the runs
// wrote, the saved plan and the migration files for the passwords: they are
// nowhere, and the statements name each role's variable instead (ADR 0071).
// A saved plan and a migration file read the value from that variable, which
// the test exports as an operator would; the computed apply reads the
// declaration and has no variable set.
func TestPostgresBinary_KeepsARolesPasswordOutOfEveryOutput(t *testing.T) {
	c := qt.New(t)
	target, admin := scratchReplayDatabase(c)
	suffix := time.Now().UnixNano()
	planned, applied, migrated := fmt.Sprintf("ptah_pw_plan_%d", suffix), fmt.Sprintf("ptah_pw_apply_%d", suffix), fmt.Sprintf("ptah_pw_migrate_%d", suffix)
	passwords := map[string]string{planned: `Plan'SENTINEL\1`, applied: `Apply'SENTINEL\2`, migrated: `Migrate'SENTINEL\3`}
	c.Cleanup(func() {
		for role := range passwords {
			_, _ = admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+role+`"`)
		}
	})
	planFile := filepath.Join(c.TempDir(), "plan.json")
	migrations := c.TempDir()
	appliedSchema := postgresRoleSchema(c, applied, passwords[applied])

	plannedOut, planErr := runPtahNativeWithError("schema", "plan", "--schema-file", postgresRoleSchema(c, planned, passwords[planned]),
		"--db-url", target, "--output", planFile)
	c.Setenv(rolepassword.Variable(planned), passwords[planned])
	fromPlanOut, fromPlanErr := runPtahNativeWithError("schema", "apply", "--plan", planFile, "--db-url", target, "--auto-approve")
	previewOut, previewErr := runPtahNativeWithError("schema", "apply", "--schema-file", appliedSchema, "--db-url", target, "--dry-run")
	appliedOut, applyErr := runPtahNativeWithError("schema", "apply", "--schema-file", appliedSchema, "--db-url", target, "--auto-approve")
	generatedOut, generateErr := runPtahNativeWithError("migrations", "generate", "--schema-file", postgresRoleSchema(c, migrated, passwords[migrated]),
		"--db-url", target, "--migrations-dir", migrations, "--name", "role")
	c.Setenv(rolepassword.Variable(migrated), passwords[migrated])
	upOut, upErr := runPtahNativeWithError("migrations", "up", "--db-url", target, "--migrations-dir", migrations)

	c.Assert(planErr, qt.IsNil, qt.Commentf("schema plan:\n%s", plannedOut))
	c.Assert(plannedOut, qt.Contains, `CREATE ROLE "`+planned+`" WITH LOGIN PASSWORD $`+rolepassword.Variable(planned)+" ")
	c.Assert(fromPlanErr, qt.IsNil, qt.Commentf("schema apply --plan:\n%s", fromPlanOut))
	c.Assert(previewErr, qt.IsNil, qt.Commentf("schema apply --dry-run:\n%s", previewOut))
	c.Assert(previewOut, qt.Contains, `CREATE ROLE "`+applied+`" WITH LOGIN PASSWORD $`+rolepassword.Variable(applied)+" ")
	c.Assert(applyErr, qt.IsNil, qt.Commentf("schema apply:\n%s", appliedOut))
	c.Assert(generateErr, qt.IsNil, qt.Commentf("migrations generate:\n%s", generatedOut))
	c.Assert(upErr, qt.IsNil, qt.Commentf("migrations up:\n%s", upOut))
	for _, role := range []string{planned, applied, migrated} {
		c.Assert(postgresRoleLogin(c, target, role, passwords[role]), qt.IsNil, qt.Commentf("role %s", role))
		c.Assert(postgresRoleLogin(c, target, role, "Other1password"), qt.IsNotNil, qt.Commentf("role %s", role))
	}

	plan, err := os.ReadFile(planFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(plan), qt.Contains, "PASSWORD $"+rolepassword.Variable(planned))
	upFiles, err := filepath.Glob(filepath.Join(migrations, "*role*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(upFiles, qt.HasLen, 1)
	upSQL, err := os.ReadFile(upFiles[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, "PASSWORD $"+rolepassword.Variable(migrated))
	written := []string{plannedOut, fromPlanOut, previewOut, appliedOut, generatedOut, upOut, string(plan)}
	files, err := os.ReadDir(migrations)
	c.Assert(err, qt.IsNil)
	for _, file := range files {
		body, err := os.ReadFile(filepath.Join(migrations, file.Name()))
		c.Assert(err, qt.IsNil)
		written = append(written, string(body))
	}
	for _, text := range written {
		c.Assert(text, qt.Not(qt.Contains), "SENTINEL")
	}
	c.Assert(written, qt.HasLen, 7+len(files))
	c.Assert(len(files) >= 2, qt.IsTrue, qt.Commentf("migrations generate wrote %d files", len(files)))
}
