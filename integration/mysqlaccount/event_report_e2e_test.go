//go:build integration

package mysqlaccount_test

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/clirun"
)

// createEvent is an event of the administrator's, which runs nothing a test can
// see: its first run is a day away.
const createEvent = "CREATE EVENT e_tick ON SCHEDULE EVERY 1 DAY DO SELECT 1"

// driftDocument is what these tests read of `schema drift --format json`.
type driftDocument struct {
	Drift    bool `json:"drift"`
	Failed   bool `json:"failed"`
	Findings []struct {
		Category string `json:"category"`
	} `json:"findings"`
	NotCompared []catalog.UnmodeledObject `json:"not_compared"`
}

// declaredAsItIs writes the desired schema of a database as it stands, which
// declares no event, since no schema source can.
func declaredAsItIs(c *qt.C, f fixture, database string) string {
	c.Helper()
	inspected := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"schema", "inspect", "--db-url", f.url("", database), "--format", "hcl")
	c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
	file := filepath.Join(c.TempDir(), "desired.hcl")
	c.Assert(os.WriteFile(file, []byte(inspected.Stdout), 0o600), qt.IsNil)
	return file
}

func drift(c *qt.C, address, file string, flags ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		append([]string{"schema", "drift", "--db-url", address, "--schema-file", file}, flags...)...)
}

func driftAsDocument(c *qt.C, address, file string) (driftDocument, clirun.Result) {
	c.Helper()
	got := drift(c, address, file, "--format", "json")
	var document driftDocument
	c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	return document, got
}

// notCompared spells a document's entries by what the server decides, the
// kind, the database and the name: an entry without a name is the whole kind.
func notCompared(document driftDocument) []string {
	spelled := make([]string, 0, len(document.NotCompared))
	for _, object := range document.NotCompared {
		spelled = append(spelled, strings.TrimRight(object.Kind+" "+object.Schema+" "+object.Name, " "))
	}
	return spelled
}

// TestSchemaDriftNamesAnEventE2E is the report the issue measured: a database that
// matches its desired schema, and an event made by hand. A trigger made the same
// way is a difference, and the event is not, because nothing can declare it.
func TestSchemaDriftNamesAnEventE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			file := declaredAsItIs(c, f, f.right)
			address := f.url("", f.right)

			clean, got := driftAsDocument(c, address, file)
			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(clean.Drift, qt.IsFalse)
			c.Assert(notCompared(clean), qt.HasLen, 0)
			c.Assert(got.Stdout, qt.Not(qt.Contains), "not_compared")
			c.Assert(drift(c, address, file).Stdout, qt.Equals, "No schema drift detected.\n")

			f.execIn(c, f.right, createEvent)

			document, got := driftAsDocument(c, address, file)
			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(document.Drift, qt.IsFalse)
			c.Assert(document.Failed, qt.IsFalse)
			c.Assert(document.Findings, qt.HasLen, 0)
			c.Assert(notCompared(document), qt.DeepEquals, []string{"event  e_tick"})
			c.Assert(document.NotCompared[0].Reason, qt.Contains, "Ptah does not model events")

			text := drift(c, address, file)
			c.Assert(text.ExitCode, qt.Equals, 0)
			c.Assert(text.Stdout, qt.Contains, "No schema drift found, but 1 event was not compared.\n")
			c.Assert(text.Stdout, qt.Contains, "\nNot compared:\n- event \"e_tick\": Ptah does not model events")

			annotated := drift(c, address, file, "--format", "github-actions")
			c.Assert(annotated.ExitCode, qt.Equals, 0)
			c.Assert(annotated.Stdout, qt.Contains, "::warning title=Ptah object not compared::event \"e_tick\": ")
		})
	}
}

// TestAnAccountWithoutEventIsToldE2E covers the account a read cannot prove has
// seen them. information_schema.EVENTS answers it with no rows and no error, so its
// empty answer must not read as a database without events, and nothing a comparison
// does depends on the answer, so it must not be refused either.
func TestAnAccountWithoutEventIsToldE2E(t *testing.T) {
	const reads = "GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}"
	rows := []struct {
		name   string
		grants func(server) []string
		want   []string
	}{
		{
			name:   "no EVENT",
			grants: func(s server) []string { return []string{reads, s.routineGrant} },
			want:   []string{"event"},
		},
		{
			name:   "EVENT on the database",
			grants: func(s server) []string { return []string{reads, s.routineGrant, "GRANT EVENT ON {db}.* TO {account}"} },
			want:   []string{"event  e_tick"},
		},
		{
			// A pattern that matches more of the name outranks the fixture's
			// unescaped one, so it carries every privilege the read needs.
			name: "EVENT on a pattern",
			grants: func(s server) []string {
				return []string{
					"GRANT SELECT, SHOW VIEW, TRIGGER, EVENT ON " + everyDatabase + ".* TO {account}",
					strings.ReplaceAll(s.routineGrant, "{db}", everyDatabase),
				}
			},
			want: []string{"event  e_tick"},
		},
		{
			name:   "EVENT globally",
			grants: func(s server) []string { return []string{reads, s.routineGrant, "GRANT EVENT ON *.* TO {account}"} },
			want:   []string{"event  e_tick"},
		},
		{
			name: "ALL PRIVILEGES on the database",
			grants: func(s server) []string {
				return []string{"GRANT ALL PRIVILEGES ON {db}.* TO {account}", s.routineGrant}
			},
			want: []string{"event  e_tick"},
		},
	}

	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			file := declaredAsItIs(c, f, f.right)
			f.execIn(c, f.right, createEvent)

			for i, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					c := qt.New(t)
					account := f.newAccount(c, fmt.Sprintf("e%d", i), row.grants(s)...)

					document, got := driftAsDocument(c, f.url(account, f.right), file)

					c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(document.Drift, qt.IsFalse)
					c.Assert(document.Failed, qt.IsFalse)
					c.Assert(notCompared(document), qt.DeepEquals, row.want)
				})
			}

			t.Run("what it is told", func(t *testing.T) {
				c := qt.New(t)
				account := f.newAccount(c, "told", reads, s.routineGrant)

				got := drift(c, f.url(account, f.right), file)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "No schema drift found, but events were not compared.\n")
				c.Assert(got.Stdout, qt.Contains, "holds no EVENT privilege on `"+f.right+"`.*")
				c.Assert(got.Stdout, qt.Contains, account)
			})
		})
	}
}

// TestAWholeServerDriftSaysWhichDatabaseE2E is a read of every database an account
// can see, which may list events in one and not in another.
func TestAWholeServerDriftSaysWhichDatabaseE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			file := declaredAsItIs(c, f, f.right)
			f.execIn(c, f.left, createEvent)
			f.execIn(c, f.right, createEvent)
			account := f.newAccount(c, "server", "GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}", s.routineGrant)
			f.exec(c, "GRANT EVENT ON `"+f.left+"`.* TO '"+account+"'@'%'")

			document, got := driftAsDocument(c, f.url(account, ""), file)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(notCompared(document), qt.DeepEquals, []string{"event " + f.left + " e_tick", "event " + f.right})
		})
	}
}

// TestApplyLeavesAnEventWhereItIsE2E is the direction a report cannot show. No plan
// removes an event, since no document can keep one: a removal would drop every
// event of a database on its first apply, whatever the schema says. The server says
// what is left. The trigger beside it is the control: that one the plan removes.
func TestApplyLeavesAnEventWhereItIsE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			file := declaredAsItIs(c, f, f.right)
			address := f.url("", f.right)
			f.execIn(c, f.right, createEvent, "CREATE TRIGGER hand_trg BEFORE INSERT ON child FOR EACH ROW SET NEW.amount = 1")

			document, got := driftAsDocument(c, address, file)
			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(document.Drift, qt.IsTrue)
			c.Assert(document.Findings[0].Category, qt.Equals, "triggers_removed")
			c.Assert(notCompared(document), qt.DeepEquals, []string{"event  e_tick"})

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
				"schema", "apply", "--db-url", address, "--schema-file", file, "--auto-approve")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))

			c.Assert(f.count(c, "information_schema.EVENTS", "EVENT_SCHEMA", f.right), qt.Equals, 1)
			c.Assert(f.count(c, "information_schema.TRIGGERS", "TRIGGER_SCHEMA", f.right), qt.Equals, 0)

			document, got = driftAsDocument(c, address, file)
			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(document.Drift, qt.IsFalse)
			c.Assert(notCompared(document), qt.DeepEquals, []string{"event  e_tick"})
		})
	}
}

// count reads how many rows a catalog view holds for database.
func (f fixture) count(c *qt.C, view, column, database string) int {
	c.Helper()
	var count int
	query := "SELECT COUNT(*) FROM " + view + " WHERE " + column + " = ?"
	c.Assert(f.admin.QueryRowContext(c.Context(), query, database).Scan(&count), qt.IsNil)
	return count
}

// TestEverySurfaceThatReadsNamesAnEventE2E holds the other verbs that read through
// the same path to the same rule, on the stream each has room on: `schema compare`
// reports on standard output, and a verb whose output is a document or a plan says
// so on standard error. The stream the answer goes to stays what it was.
func TestEverySurfaceThatReadsNamesAnEventE2E(t *testing.T) {
	surfaces := append([]surface{{
		name:   "schema compare",
		binary: clirun.Ptah,
		args: func(u, _, file string) []string {
			return []string{"schema", "compare", "--db-url", u, "--schema-file", file}
		},
		wantStdout: "No schema differences detected, but 1 event was not compared:\n  event \"e_tick\": Ptah does not model events",
	}}, surfacesThatWarn()...)

	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			file := declaredAsItIs(c, f, f.right)
			f.execIn(c, f.right, createEvent)
			address, atlas := f.url("", f.right), atlasURL(c, f, "", f.right)

			for _, surface := range surfaces {
				t.Run(surface.name, func(t *testing.T) {
					c := qt.New(t)

					got := clirun.Run(c, surface.binary, clirun.Options{Dir: c.TempDir()}, surface.args(address, atlas, file)...)

					comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr)
					c.Assert(got.ExitCode, qt.Equals, 0, comment)
					c.Assert(got.Stdout, qt.Contains, surface.wantStdout, comment)
					c.Assert(got.Stderr, qt.Contains, surface.wantStderr, comment)
				})
			}

			t.Run("a run that leaves events out", func(t *testing.T) {
				c := qt.New(t)

				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					"schema", "inspect", "--db-url", address, "--format", "sql", "--exclude", "*[type=event]")

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
				c.Assert(got.Stderr, qt.Not(qt.Contains), "left out")
			})
		})
	}
}

// surface is one verb that reads a database through the path that lists events.
type surface struct {
	name              string
	binary            clirun.Target
	args              func(url, atlas, file string) []string
	wantStdout        string
	wantLimitedStdout string
	wantStderr        string
}

// surfacesThatWarn are the verbs whose answer has no place for an event, each with
// what it says over a database that holds one, to an account that can see it.
func surfacesThatWarn() []surface {
	const leftOut = "event \"e_tick\" in the database is left out: Ptah does not model events"
	const limitedRoles = "No changes found in the visible scope. Comparison limits:\n- --from roles and grants: the source did not describe this kind\n- --to roles and grants: the source did not describe this kind"
	return []surface{
		{
			name:       "schema inspect",
			binary:     clirun.Ptah,
			args:       func(u, _, _ string) []string { return []string{"schema", "inspect", "--db-url", u, "--format", "sql"} },
			wantStdout: "CREATE TABLE",
			wantStderr: leftOut,
		},
		{
			name:       "db read",
			binary:     clirun.Ptah,
			args:       func(u, _, _ string) []string { return []string{"db", "read", "--db-url", u} },
			wantStdout: "CREATE TABLE",
			wantStderr: leftOut,
		},
		{
			name:              "schema diff of a database with itself",
			binary:            clirun.Ptah,
			args:              func(u, _, _ string) []string { return []string{"schema", "diff", "--from", u, "--to", u} },
			wantStdout:        "Schemas are synced",
			wantLimitedStdout: limitedRoles,
			wantStderr:        "2 objects were left out: event \"e_tick\" in --from, event \"e_tick\" in --to (Ptah does not model events",
		},
		{
			name:   "schema apply",
			binary: clirun.Ptah,
			args: func(u, _, file string) []string {
				return []string{"schema", "apply", "--db-url", u, "--schema-file", file, "--dry-run"}
			},
			wantStdout: "synced",
			wantStderr: leftOut,
		},
		{
			name:   "schema plan",
			binary: clirun.Ptah,
			args: func(u, _, file string) []string {
				return []string{"schema", "plan", "--db-url", u, "--schema-file", file, "--dry-run"}
			},
			wantStdout: "",
			wantStderr: leftOut,
		},
		{
			name:   "migrations plan",
			binary: clirun.Ptah,
			args: func(u, _, file string) []string {
				return []string{"migrations", "plan", "--db-url", u, "--schema-file", file}
			},
			wantStdout: "",
			wantStderr: leftOut,
		},
		{
			name:       "ptah-compat schema inspect",
			binary:     clirun.Compat,
			args:       func(_, a, _ string) []string { return []string{"schema", "inspect", "-u", a} },
			wantStdout: "table \"child\"",
			wantStderr: leftOut,
		},
		{
			name:   "ptah-compat schema apply",
			binary: clirun.Compat,
			args: func(_, a, _ string) []string {
				return []string{"schema", "apply", "-u", a, "--to", a, "--dry-run"}
			},
			wantStdout: "synced",
			wantStderr: leftOut,
		},
		{
			name:              "ptah-compat schema diff",
			binary:            clirun.Compat,
			args:              func(_, a, _ string) []string { return []string{"schema", "diff", "--from", a, "--to", a} },
			wantStdout:        "Schemas are synced",
			wantLimitedStdout: limitedRoles,
			wantStderr:        "2 objects were left out: event \"e_tick\" in --from, event \"e_tick\" in --to (Ptah does not model events",
		},
	}
}

// TestAnAccountWithoutEventHearsItOnlyFromAReportE2E is a least-privilege account
// on the verbs that claim nothing about events. A warning there would be the same
// sentence on every run, and it would change nothing the account could do. The
// reports that claim to have compared still say what they did not look at; the
// statement of `schema drift` is held by [TestAnAccountWithoutEventIsToldE2E].
func TestAnAccountWithoutEventHearsItOnlyFromAReportE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			file := declaredAsItIs(c, f, f.right)
			f.execIn(c, f.right, createEvent)
			account := f.newAccount(c, "quiet", "GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}", s.routineGrant)
			address, atlas := f.url(account, f.right), atlasURL(c, f, account, f.right)

			for _, surface := range surfacesThatWarn() {
				t.Run(surface.name, func(t *testing.T) {
					c := qt.New(t)

					got := clirun.Run(c, surface.binary, clirun.Options{Dir: c.TempDir()}, surface.args(address, atlas, file)...)

					comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr)
					c.Assert(got.ExitCode, qt.Equals, 0, comment)
					c.Assert(got.Stdout, qt.Contains, cmp.Or(surface.wantLimitedStdout, surface.wantStdout), comment)
					c.Assert(got.Stderr, qt.Not(qt.Contains), "left out", comment)
					c.Assert(got.Stderr, qt.Not(qt.Contains), "EVENT", comment)
				})
			}

			t.Run("schema compare", func(t *testing.T) {
				c := qt.New(t)

				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					"schema", "compare", "--db-url", address, "--schema-file", file)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "No schema differences detected, but events were not compared:\n")
				c.Assert(got.Stdout, qt.Contains, "holds no EVENT privilege")
			})
		})
	}
}

// TestAWholeServerReadWarnsAboutWhatItSawE2E is the read that may list events in
// one database and not in another. The verb names the one it saw, and the database
// it could not list leaves no line.
func TestAWholeServerReadWarnsAboutWhatItSawE2E(t *testing.T) {
	for _, s := range servers() {
		t.Run(s.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFixture(c, s)
			f.execIn(c, f.left, createEvent)
			f.execIn(c, f.right, createEvent)
			account := f.newAccount(c, "seen", "GRANT SELECT, SHOW VIEW, TRIGGER ON {db}.* TO {account}", s.routineGrant)
			f.exec(c, "GRANT EVENT ON `"+f.left+"`.* TO '"+account+"'@'%'")

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
				"schema", "inspect", "--db-url", f.url(account, ""), "--format", "sql")

			comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr)
			c.Assert(got.ExitCode, qt.Equals, 0, comment)
			c.Assert(got.Stderr, qt.Contains, "event \""+f.left+".e_tick\" in the database is left out: Ptah does not model events", comment)
			c.Assert(got.Stderr, qt.Not(qt.Contains), f.right, comment)
		})
	}
}
