//go:build integration

package migrationsdrift_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/migrationsdrift"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/errorkind"
	"ptah.run/migration/migrator"
)

// These tests run `ptah migrations drift` over the targets of an atlas.hcl
// env with for_each. Each target is its own database on the server, with its
// own migration lock and catalog, and each report is read back from the
// databases it describes.

// targetsRun writes an atlas.hcl whose env "tenants" expands to one target
// per url, replayed on shadow, runs the report over it with the migrations of
// dir, and returns the document and the run.
func targetsRun(c *qt.C, dir, shadow string, urls ...string) (migrationsdrift.TargetsDocument, clirun.Result) {
	c.Helper()
	quoted := make([]string, len(urls))
	for i, url := range urls {
		quoted[i] = strconv.Quote(url)
	}
	project := filepath.Join(c.TempDir(), "atlas.hcl")
	c.Assert(os.WriteFile(project, []byte("env {\n"+
		"  for_each = ["+strings.Join(quoted, ", ")+"]\n"+
		"  name     = atlas.env\n"+
		"  url      = each.value\n"+
		"  dev      = "+strconv.Quote(shadow)+"\n"+
		"}\n"), 0o600), qt.IsNil)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute},
		"migrations", "drift", "--json", "--config", project, "--env", "tenants", "--migrations-dir", dir)
	var got migrationsdrift.TargetsDocument
	c.Assert(json.Unmarshal([]byte(result.Stdout), &got), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return got, result
}

// targetReport is the report a target's entry carries.
func targetReport(c *qt.C, target migrationsdrift.Target) document {
	c.Helper()
	var got document
	c.Assert(json.Unmarshal(target.Report, &got), qt.IsNil, qt.Commentf("%s", target.Report))
	return got
}

// missingDatabase is the address of a database the engine's server does not
// hold, reached with the same account as the other targets.
func missingDatabase(c *qt.C, engine driftEngine) (url, name string) {
	c.Helper()
	name = fmt.Sprintf("ptah_mdrift_missing_%d", time.Now().UnixNano())
	url, err := atlasurl.WithDatabaseName(dbtarget.URL(c, engine.admin), name)
	c.Assert(err, qt.IsNil)
	return url, name
}

func TestMigrationsDriftE2E_HappyPathReportsEveryTargetOfAForEachEnv(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			first, second := newDriftRun(c, engine, "1"), newDriftRun(c, engine, "2")

			got, result := targetsRun(c, first.dir, first.shadow, first.target, second.target)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
			c.Assert(got.Targets, qt.HasLen, 2)
			c.Assert(got.Targets[0].Instance, qt.Equals, 1)
			c.Assert(got.Targets[0].ExitCode, qt.Equals, 0)
			c.Assert(targetReport(c, got.Targets[0]).Check.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(targetReport(c, got.Targets[0]).Pending, qt.DeepEquals, []string{"2"})
			c.Assert(got.Targets[1].Instance, qt.Equals, 2)
			c.Assert(got.Targets[1].ExitCode, qt.Equals, 0)
			c.Assert(targetReport(c, got.Targets[1]).Check.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(targetReport(c, got.Targets[1]).Check.Replayed, qt.DeepEquals, []int64{1, 2})
			c.Assert(first.appliedVersion(c), qt.Equals, 1)
			c.Assert(second.appliedVersion(c), qt.Equals, 2)
			c.Assert(first.scalar(c, first.shadow, engine.tables), qt.Equals, 0)
		})
	}
}

// A target that drifted exits 1 and the run goes on to the target after it.
func TestMigrationsDriftE2E_FailurePathReportsADriftedTargetAndGoesOn(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			drifted := newDriftRun(c, engine, "1", "ALTER TABLE drift_users ADD COLUMN nickname VARCHAR(40)")
			clean := newDriftRun(c, engine, "1")

			got, result := targetsRun(c, clean.dir, clean.shadow, drifted.target, clean.target)

			c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("%s", result.Stderr))
			c.Assert(got.Targets, qt.HasLen, 2)
			c.Assert(got.Targets[0].ExitCode, qt.Equals, 1)
			c.Assert(targetReport(c, got.Targets[0]).Check.Findings, qt.DeepEquals, []migrator.DriftFinding{{
				Kind: "extra_column", Object: "drift_users.nickname", Table: "drift_users", Column: "nickname",
				Message: "extra column drift_users.nickname",
			}})
			c.Assert(got.Targets[1].ExitCode, qt.Equals, 0)
			c.Assert(targetReport(c, got.Targets[1]).Check.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(drifted.nicknameColumns(c), qt.Equals, 1)
			c.Assert(clean.nicknameColumns(c), qt.Equals, 0)
		})
	}
}

// A target the run cannot open exits 2, beside one that drifted, and the run
// goes past both to the last target. The statements the drifted target's
// report plans bring it back to its history, which the next run reads.
func TestMigrationsDriftE2E_FailurePathReportsATargetItCannotOpenAndGoesOn(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			drifted := newDriftRun(c, engine, "1", "ALTER TABLE drift_users ADD COLUMN nickname VARCHAR(40)")
			clean := newDriftRun(c, engine, "1")
			missing, missingName := missingDatabase(c, engine)

			got, result := targetsRun(c, clean.dir, clean.shadow, drifted.target, missing, clean.target)

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("%s", result.Stderr))
			c.Assert(result.Stderr, qt.Equals, "")
			c.Assert(got.Targets, qt.HasLen, 3)
			c.Assert(got.Targets[0].ExitCode, qt.Equals, 1)
			c.Assert(targetReport(c, got.Targets[0]).Check.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(got.Targets[1].Instance, qt.Equals, 2)
			c.Assert(got.Targets[1].URL, qt.Equals, dburldisplay.Format(missing))
			c.Assert(got.Targets[1].ExitCode, qt.Equals, 2)
			c.Assert(got.Targets[1].Report, qt.IsNil)
			c.Assert(got.Targets[1].Error, qt.IsNotNil)
			c.Assert(got.Targets[1].Error.Kind, qt.Equals, errorkind.DatabaseNotFound)
			c.Assert(got.Targets[1].Error.Message, qt.Matches, `error connecting to database: .*`+missingName+`.*`)
			c.Assert(got.Targets[2].ExitCode, qt.Equals, 0)
			c.Assert(targetReport(c, got.Targets[2]).Check.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(drifted.nicknameColumns(c), qt.Equals, 1)
			c.Assert(drifted.appliedVersion(c), qt.Equals, 1)
			c.Assert(clean.scalar(c, clean.shadow, engine.tables), qt.Equals, 0)
			c.Assert(clean.scalar(c, dbtarget.URL(c, engine.admin), fmt.Sprintf(engine.databases, missingName)), qt.Equals, 0)

			drifted.exec(c, targetReport(c, got.Targets[0]).Statements...)
			again, result := targetsRun(c, clean.dir, clean.shadow, drifted.target, missing, clean.target)

			c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("%s", result.Stderr))
			c.Assert(again.Targets[0].ExitCode, qt.Equals, 0)
			c.Assert(targetReport(c, again.Targets[0]).Check.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(drifted.nicknameColumns(c), qt.Equals, 0)
		})
	}
}
