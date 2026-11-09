//go:build integration

package mysqlaccount_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// comparisonVerb is one verb that reads a database and reports how it differs from
// a desired schema, with what its answer says apart from the address it read.
type comparisonVerb struct {
	name   string
	run    func(c *qt.C, address, file string, flags ...string) clirun.Result
	answer func(c *qt.C, got clirun.Result) string
}

// comparisonVerbs are `schema drift` and `schema compare`, which read through the
// same check `schema diff` does and take its selectors.
func comparisonVerbs() []comparisonVerb {
	return []comparisonVerb{
		{
			name: "schema drift",
			run: func(c *qt.C, address, file string, flags ...string) clirun.Result {
				c.Helper()
				return drift(c, address, file, append(flags, "--format", "json", "--exit-code=false")...)
			},
			answer: driftAnswer,
		},
		{
			name: "schema compare",
			run: func(c *qt.C, address, file string, flags ...string) clirun.Result {
				c.Helper()
				return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					append([]string{"schema", "compare", "--db-url", address, "--schema-file", file}, flags...)...)
			},
			answer: compareAnswer,
		},
	}
}

// driftAnswer is a document without the address, which names the account.
func driftAnswer(c *qt.C, got clirun.Result) string {
	c.Helper()
	var document map[string]any
	c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	delete(document, "database_url")
	answer, err := json.Marshal(document)
	c.Assert(err, qt.IsNil)
	return string(answer)
}

// compareAnswer is what the verb prints under its heading, which names the account.
func compareAnswer(c *qt.C, got clirun.Result) string {
	c.Helper()
	_, answer, found := strings.Cut(got.Stdout, "=== SCHEMA COMPARISON ===\n")
	c.Assert(found, qt.IsTrue, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	return answer
}

// sharedObjectsFile writes what both fixture databases hold as a desired schema
// that names no database, so a run can compare the left one with it.
func sharedObjectsFile(c *qt.C) string {
	c.Helper()
	file := filepath.Join(c.TempDir(), "desired.sql")
	c.Assert(os.WriteFile(file, []byte(strings.Join(sharedObjects, ";\n")+";\n"), 0o600), qt.IsNil)
	return file
}

// TestAComparisonVerbThatLeavesAKindOutDoesNotNeedItsPrivilegeE2E covers a read
// through `schema drift` and `schema compare` that depends on nothing the account
// cannot see. Each kind-level selector reaches the check that refuses a read of what
// an account cannot see, so the account gets what the administrator's connection
// gets from the same selector. Without the selector the same account is refused.
// Each selector leaves events out too, because a report names them for an account
// holding no EVENT and the administrator holds it.
func TestAComparisonVerbThatLeavesAKindOutDoesNotNeedItsPrivilegeE2E(t *testing.T) {
	rows := []struct {
		name   string
		grants func(server) []string
		flags  []string
	}{
		{
			name: "no TRIGGER, every trigger excluded",
			grants: func(s server) []string {
				return []string{"GRANT SELECT, SHOW VIEW ON {db}.* TO {account}", s.routineGrant}
			},
			flags: []string{"--exclude", "*[type=trigger|event]"},
		},
		{
			name: "no SHOW VIEW, every view excluded",
			grants: func(s server) []string {
				return []string{"GRANT SELECT, INSERT, TRIGGER ON {db}.* TO {account}", s.routineGrant}
			},
			flags: []string{"--exclude", "*[type=view|event]"},
		},
		{
			name: "no routine privilege, every routine excluded",
			grants: func(server) []string {
				return []string{"GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}"}
			},
			flags: []string{"--exclude", "*[type=function|event]"},
		},
		{
			name: "no SHOW VIEW and no routine privilege, tables included",
			grants: func(server) []string {
				return []string{"GRANT SELECT, INSERT, TRIGGER ON {db}.* TO {account}"}
			},
			flags: []string{"--include", "*[type=table]"},
		},
	}

	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			file := sharedObjectsFile(c)
			administrator := f.url("", f.left)

			for i, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					c := qt.New(t)
					account := f.newAccount(c, fmt.Sprintf("v%d", i), row.grants(s)...)
					address := f.url(account, f.left)

					for _, verb := range comparisonVerbs() {
						t.Run(verb.name, func(t *testing.T) {
							c := qt.New(t)

							got := verb.run(c, address, file, row.flags...)
							want := verb.run(c, administrator, file, row.flags...)

							comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr)
							c.Assert(got.ExitCode, qt.Equals, 0, comment)
							c.Assert(want.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", want.Stderr))
							c.Assert(verb.answer(c, got), qt.Equals, verb.answer(c, want))

							refused := verb.run(c, address, file)
							c.Assert(refused.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
							c.Assert(refused.Stderr, qt.Contains, "cannot see everything")
						})
					}
				})
			}
		})
	}
}

// TestAComparisonVerbTakesAnAccountHoldingOnlySelectE2E covers the account that needs
// every selector at once. MariaDB hides constraint rows from this account, but
// its visible primary index still gives the same answer as the administrator.
func TestAComparisonVerbTakesAnAccountHoldingOnlySelectE2E(t *testing.T) {
	const everyKind = "*[type=trigger|view|function|constraint|event]"
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			file := sharedObjectsFile(c)
			address := f.url(f.newAccount(c, "sel", "GRANT SELECT ON {db}.* TO {account}"), f.left)
			administrator := f.url("", f.left)

			for _, verb := range comparisonVerbs() {
				t.Run(verb.name, func(t *testing.T) {
					c := qt.New(t)

					got := verb.run(c, address, file, "--exclude", everyKind)
					want := verb.run(c, administrator, file, "--exclude", everyKind)

					c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(want.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", want.Stdout, want.Stderr))
					c.Assert(verb.answer(c, got), qt.Equals, verb.answer(c, want))
				})
			}
		})
	}
}

// TestAComparisonVerbSelectorNamingOneObjectVouchesForNoOtherE2E covers the limit of
// the relief. A read that cannot see a trigger cannot say what it is called, so a
// selector naming one is refused as if it were absent.
func TestAComparisonVerbSelectorNamingOneObjectVouchesForNoOtherE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s, leftProcedure...)
			file := sharedObjectsFile(c)
			account := f.newAccount(c, "named", "GRANT SELECT, SHOW VIEW ON {db}.* TO {account}", s.routineGrant)

			for _, verb := range comparisonVerbs() {
				t.Run(verb.name, func(t *testing.T) {
					c := qt.New(t)
					got := verb.run(c, f.url(account, f.left), file, "--exclude", "child_trg*[type=trigger]")
					assertRefused(c, got, 2, named(f.left, lineTriggers)...)
				})
			}
		})
	}
}

// TestAComparisonVerbSelectorOnEventsE2E covers what each selector does to the part
// of a report that names an object nothing compares. A selector leaving out every
// event leaves the read with nothing to report, for an account that cannot list
// them too. A selector naming one event matches it, so nothing warns, and it stays
// named, as on every verb: the database still holds it and no selection changes that.
func TestAComparisonVerbSelectorOnEventsE2E(t *testing.T) {
	const reads = "GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}"
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			file := declaredAsItIs(c, f, f.right)
			f.execIn(c, f.right, createEvent)
			address := f.url("", f.right)
			account := f.url(f.newAccount(c, "noevent", reads, s.routineGrant), f.right)

			t.Run("every event left out", func(t *testing.T) {
				c := qt.New(t)

				for _, reader := range []string{address, account} {
					document := drift(c, reader, file, "--format", "json", "--exclude", "*[type=event]")
					c.Assert(document.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", document.Stdout, document.Stderr))
					c.Assert(document.Stderr, qt.Not(qt.Contains), "matched no objects")
					c.Assert(document.Stdout, qt.Not(qt.Contains), "not_compared")

					text := drift(c, reader, file, "--exclude", "*[type=event]")
					c.Assert(text.Stdout, qt.Equals, "No schema drift detected.\n")
					c.Assert(text.Stderr, qt.Not(qt.Contains), "matched no objects")

					compared := comparisonVerbs()[1].run(c, reader, file, "--exclude", "*[type=event]")
					c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
					c.Assert(compared.Stderr, qt.Not(qt.Contains), "matched no objects")
					c.Assert(compared.Stdout, qt.Contains, "No schema differences detected.\n")
					c.Assert(compared.Stdout, qt.Not(qt.Contains), "Not compared")
					c.Assert(compared.Stdout, qt.Not(qt.Contains), "not compared")
				}
			})

			t.Run("one event named", func(t *testing.T) {
				c := qt.New(t)

				got := drift(c, address, file, "--format", "json", "--exclude", "e_tick[type=event]")

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Not(qt.Contains), "matched no objects")
				var document driftDocument
				c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil)
				c.Assert(document.Drift, qt.IsFalse)
				c.Assert(notCompared(document), qt.DeepEquals, []string{"event  e_tick"})

				compared := comparisonVerbs()[1].run(c, address, file, "--exclude", "e_tick[type=event]")
				c.Assert(compared.ExitCode, qt.Equals, 0)
				c.Assert(compared.Stderr, qt.Not(qt.Contains), "matched no objects")
				c.Assert(compared.Stdout, qt.Contains, "event \"e_tick\": Ptah does not model events")
			})
		})
	}
}
