package atlas_test

import (
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These rows cover stokaro/ptah#1098: a converted Flyway directory is ORDERED
// numerically and made LINEAR textually, and `migrate apply` used to answer both
// questions with the numeric key.
//
// Measured on the pinned community binary, which reports
// `atlas community version v1.3.0`, sqlite, `--dir file://m?format=flyway`,
// atlas.sum written by each tool for its own run, exit codes captured
// immediately. Every row's expectation is that binary's answer unless the
// comment says otherwise:
//
//	applied      added            pinned v1.3.0                       here
//	V2           V10              1, "was added out of order"         refused
//	V9           V10              1, "was added out of order"         refused
//	V2           V100             1, "was added out of order"         refused
//	V2           R__r             1, "was added out of order"         refused
//	V2           R9__r            1, "was added out of order"         refused
//	V2, V10      V15              1, "was added out of order"         refused
//	V2, V10      V3               1, "was added out of order"         refused
//	V2           V3               0, executed                         executed
//	V2           V20              0, executed                         executed
//	V2, V10      V20              0, executed                         executed
//	V10          V11              0, executed                         executed
//	V01          V2               0, executed                         executed
//	V2, V10      (none)           0, no pending files                 no-op
//
// Every refusing row above exited 0 here before the change and executed the
// file — that is the defect, and it is what those rows print if the change is
// reverted. The executing rows are green either way: their job is to fail if
// the new comparison starts refusing ordinary sequences.

type flywaySourceOrderOutput struct {
	stdout string
	stderr string
	err    error
}

func (o flywaySourceOrderOutput) text() string {
	return errorText(o.err) + o.stdout + o.stderr
}

type flywaySourceOrderRow struct {
	name string
	// applied are written, hashed and applied first; the row's second apply
	// runs against the history they leave behind. Empty means no history,
	// which is the shape with nothing for the source comparison to compare
	// against.
	applied []string
	// added are written next, and the directory is re-hashed before the second
	// apply.
	added []string
	// args are appended to the second `migrate apply` invocation.
	args []string
	// assert is the per-row wiring: refusing rows and executing rows do not
	// share an assertion, and branching inside one body would hide which of the
	// two a row is.
	assert func(c *qt.C, dbPath string, out flywaySourceOrderOutput)
}

func TestCompatMigrateApplyFlywaySourceVersionOrder(t *testing.T) {
	// refused asserts the refusal names both the mark it compared against and
	// the migration it refused, by SOURCE version, and that the listed tables
	// are exactly the ones the first apply left — nothing ran, because the
	// refusal is raised while the migration set is selected.
	refused := func(tables []string, sourceVersions ...string) func(*qt.C, string, flywaySourceOrderOutput) {
		return func(c *qt.C, dbPath string, out flywaySourceOrderOutput) {
			c.Assert(out.err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", out.stdout, out.stderr))
			c.Assert(out.text(), qt.Contains, "out-of-order pending migrations for current version")
			for _, source := range sourceVersions {
				c.Assert(out.text(), qt.Contains, `(source version "`+source+`")`)
			}
			c.Assert(userTables(c, dbPath), qt.DeepEquals, tables)
		}
	}
	executed := func(tables ...string) func(*qt.C, string, flywaySourceOrderOutput) {
		return func(c *qt.C, dbPath string, out flywaySourceOrderOutput) {
			c.Assert(out.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", out.stdout, out.stderr))
			c.Assert(userTables(c, dbPath), qt.DeepEquals, tables)
		}
	}

	tests := []flywaySourceOrderRow{
		{
			// The issue's shape. Reverted: exit 0, "Migrating to version
			// 4611686018427836747 from 1 pending migrations.", tables sq2 and
			// sq10 — the migration the oracle refuses, executed.
			name:    "a tenth migration added under an applied second",
			applied: []string{"V2__sq2.sql"},
			added:   []string{"V10__sq10.sql"},
			assert:  refused([]string{"sq2"}, "2", "10"),
		},
		{
			// Reverted: exit 0, tables sq2 and sq10. Separates "two digits" from
			// "numerically far above the mark": 10 > 9 numerically, and "10" <
			// "9" textually, so only the textual comparison refuses it.
			name:    "a tenth migration added under an applied ninth",
			applied: []string{"V9__sq2.sql"},
			added:   []string{"V10__sq10.sql"},
			assert:  refused([]string{"sq2"}, "9", "10"),
		},
		{
			// Reverted: exit 0, tables sq2 and sq100. Three digits reach the
			// same rule, so the refusal is not a two-digit special case.
			name:    "a hundredth migration added under an applied second",
			applied: []string{"V2__sq2.sql"},
			added:   []string{"V100__sq100.sql"},
			assert:  refused([]string{"sq2"}, "2", "100"),
		},
		{
			// Atlas CE gives every repeatable the empty version string, which
			// sorts below every token, so it refuses a repeatable added to a
			// database with any history. Reverted: exit 0, tables sq2 and sqr —
			// the repeatable runs, because its reserved int64 slot is the
			// highest there is and the numeric comparison sees nothing wrong.
			name:    "a repeatable added to recorded history",
			applied: []string{"V2__sq2.sql"},
			added:   []string{"R__sqr.sql"},
			assert:  refused([]string{"sq2"}, "2", ""),
		},
		{
			// The token is Atlas CE's version STRING for the file, not the
			// file's own token. Every repeatable is version "" to CE whatever it
			// is called, and CE refuses all of R__r.sql, R1__r.sql, R9__r.sql
			// and Rfoo__r.sql added over an applied V2 — measured. Reading the
			// file's own token instead would let this one through, because "9"
			// sorts above the applied "2". Reverted: exit 0, tables sq2 and sqr.
			name:    "a repeatable whose own token sorts high is still refused",
			applied: []string{"V2__sq2.sql"},
			added:   []string{"R9__sqr.sql"},
			assert:  refused([]string{"sq2"}, "2", ""),
		},
		{
			// The mark is the highest applied TOKEN, not the token of the
			// highest applied version, and this is the input that separates
			// them: the applied tokens are "2" and "10", whose maxima disagree.
			// "15" sorts below "2" and above "10". Comparing against "10" would
			// pass it, and the numeric comparison passes it too (15 > 10), so
			// only the right operand refuses it. Reverted: exit 0, tables sq10,
			// sq15 and sq2.
			name:    "a fifteenth migration under applied second and tenth",
			applied: []string{"V2__sq2.sql", "V10__sq10.sql"},
			added:   []string{"V15__sq15.sql"},
			assert:  refused([]string{"sq10", "sq2"}, "10", "15"),
		},
		{
			// The control for the row above: same applied pair, and "20" sorts
			// above both applied tokens, so it runs. Without it, "refuse
			// everything two-digit added to this history" would pass too.
			name:    "a twentieth migration still applies over applied second and tenth",
			applied: []string{"V2__sq2.sql", "V10__sq10.sql"},
			added:   []string{"V20__sq20.sql"},
			assert:  executed("sq10", "sq2", "sq20"),
		},
		{
			// The union's other half. "3" sorts ABOVE the highest applied token
			// "2", so the source comparison passes it; the numeric one refuses
			// it because it lands under the applied V10. Reverted to a source-
			// only comparison: exit 0, tables sq2, sq3 and sq10.
			name:    "an ordinary migration under an applied two-digit version",
			applied: []string{"V2__sq2.sql", "V10__sq10.sql"},
			added:   []string{"V3__sq3.sql"},
			assert: func(c *qt.C, dbPath string, out flywaySourceOrderOutput) {
				c.Assert(out.err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", out.stdout, out.stderr))
				c.Assert(out.text(), qt.Contains, `(source version "3")`)
				c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"sq10", "sq2"})
			},
		},
		{
			// Control. Reverted: green, unchanged — its job is to fail if the
			// new comparison refuses an ordinary sequence.
			name:    "the next single-digit migration still applies",
			applied: []string{"V2__sq2.sql"},
			added:   []string{"V3__sq3.sql"},
			assert:  executed("sq2", "sq3"),
		},
		{
			// Control, and the one that pins the comparison as textual rather
			// than "refuse anything with more digits": "20" sorts above "2".
			name:    "a twentieth migration still applies over an applied second",
			applied: []string{"V2__sq2.sql"},
			added:   []string{"V20__sq20.sql"},
			assert:  executed("sq2", "sq20"),
		},
		{
			// Control: two two-digit versions in their own order.
			name:    "an eleventh migration still applies over an applied tenth",
			applied: []string{"V10__sq2.sql"},
			added:   []string{"V11__sq11.sql"},
			assert:  executed("sq11", "sq2"),
		},
		{
			// Control: zero padding, standard Flyway practice. "2" sorts above
			// "01", and the pinned binary applies it, printing
			// `Migrating to version 2 from 01`.
			name:    "an unpadded second migration still applies over an applied zero-padded first",
			applied: []string{"V01__sq2.sql"},
			added:   []string{"V2__sq3.sql"},
			assert:  executed("sq2", "sq3"),
		},
		{
			// Two controls in one row. The first apply runs against no history
			// at all, so the source comparison has no applied token to compare
			// against and must not manufacture one — it goes red if the
			// highest-applied-token lookup falls back to a zero value instead
			// of reporting "none". The second apply then runs against history
			// holding both, where V10's own token "10" sorts below the highest
			// applied token "2": it must not be flagged, because it is applied,
			// not pending. Feeding the comparison the full version list instead
			// of the pending one turns this row red.
			name:    "a settled directory holding both versions is not refused afterwards",
			applied: []string{"V2__sq2.sql", "V10__sq10.sql"},
			added:   nil,
			assert:  executed("sq10", "sq2"),
		},
		{
			// linear-skip leaves out-of-order migrations unapplied rather than
			// refusing, and the pinned binary answers this exact input with
			// "No migration files to execute" and an unchanged database.
			// Reverted: exit 0 as well, but tables sq2 AND sq10 — the skip
			// silently executed what it claims to skip.
			name:    "linear-skip leaves the tenth migration unapplied",
			applied: []string{"V2__sq2.sql"},
			added:   []string{"V10__sq10.sql"},
			args:    []string{"--exec-order=linear-skip"},
			assert: func(c *qt.C, dbPath string, out flywaySourceOrderOutput) {
				c.Assert(out.err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", out.stdout, out.stderr))
				c.Assert(out.stdout, qt.Contains, "No migration files to execute")
				c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"sq2"})
			},
		},
		{
			// The escape hatch the refusal names has to work, or the refusal is
			// a dead end. The pinned binary runs it too.
			name:    "non-linear runs the tenth migration the refusal names",
			applied: []string{"V2__sq2.sql"},
			added:   []string{"V10__sq10.sql"},
			args:    []string{"--exec-order=non-linear"},
			assert:  executed("sq10", "sq2"),
		},
		{
			// A preview must not report work a real apply refuses. The pinned
			// binary refuses the dry run too. Reverted: exit 0 and
			// "Would have applied 1 migrations."
			name:    "a dry run refuses rather than previewing the refused migration",
			applied: []string{"V2__sq2.sql"},
			added:   []string{"V10__sq10.sql"},
			args:    []string{"--dry-run"},
			assert:  refused([]string{"sq2"}, "2", "10"),
		},
		{
			// NON-INTERFERENCE with the surviving-baseline exemption. B10's
			// token sorts below the applied "2" exactly like V10's does, so a
			// source comparison applied outside the exemption would refuse a
			// baseline this build runs. This row is green before and after the
			// change; it goes red if the exemption is applied to only one half
			// of the union. It is NOT a claim about the pinned binary, which
			// skips this baseline silently — that divergence is stokaro/ptah#1003
			// and is not decided here.
			name:    "a surviving baseline below the mark stays exempt",
			applied: []string{"V2__sq2.sql"},
			added:   []string{"B10__sqbase.sql"},
			assert:  executed("sq2", "sqbase"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			dir := filepath.Join(root, "migrations")
			dbPath := filepath.Join(root, "order.db")

			for _, name := range tt.applied {
				writeAtlasApplyProjectMigration(c, dir, name, flywaySourceOrderSQL(name))
			}
			hashConvertedApplyDir(c, dir, "flyway")
			_, _, err := runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir+"?format=flyway")
			c.Assert(err, qt.IsNil)

			for _, name := range tt.added {
				writeAtlasApplyProjectMigration(c, dir, name, flywaySourceOrderSQL(name))
			}
			hashConvertedApplyDir(c, dir, "flyway")
			args := append([]string{
				"migrate", "apply",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + dir + "?format=flyway",
			}, tt.args...)
			stdout, stderr, err := runCompat(args...)

			tt.assert(c, dbPath, flywaySourceOrderOutput{stdout: stdout, stderr: stderr, err: err})
		})
	}
}

// flywaySourceOrderSQL derives each fixture's table from its own description,
// so a row's expected table list names the files that ran rather than a
// separately maintained mapping.
func flywaySourceOrderSQL(name string) string {
	base := strings.TrimSuffix(name, ".sql")
	_, description, _ := strings.Cut(base, "__")
	return "CREATE TABLE " + description + " (id INTEGER PRIMARY KEY);"
}
