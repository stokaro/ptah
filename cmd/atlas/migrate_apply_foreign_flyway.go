package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// foreignFlywayRevision is one migration this database has already run, but
// recorded under the source tool's version token instead of the Atlas version
// this build executes it under.
type foreignFlywayRevision struct {
	// source is the Flyway file name, relative to the migration directory.
	source string
	// recorded is the version in the revision table: the source token, read
	// the same way the revision reader reads it.
	recorded int64
	// current is the Atlas version this build would execute the file under.
	current int64
}

// checkForeignFlywayRevisions refuses an apply that would re-run migrations
// another Atlas implementation has already applied through the same converted
// directory (stokaro/ptah#1100).
//
// A `?format=flyway` directory is converted to Atlas single-file migrations
// before it is executed, and the two implementations do not agree on the
// version the converted migration is then recorded under. Atlas CE identifies a
// Flyway migration by its opaque version STRING and records that string
// verbatim; Ptah's migrator identifies a migration by an int64, so converting
// projects the token onto the band-and-slot encoding documented above
// flywayComponentSlot. Measured on the pinned community binary v1.3.0 with
// V1__a.sql and V2__b.sql: it records `1` and `2`, this build records
// `4611686018427469511` and `4611686018427510315`.
//
// The projection is not a different spelling of one version, it makes the two
// revision tables mutually unreadable, and in one direction that means
// re-execution. Reading a table this build wrote is benign — the community
// binary sees versions above everything it has and executes nothing — but
// reading a table the community binary wrote, every converted file matches no
// row and the whole directory reads as pending. Measured, both halves:
//
//	CREATE TABLE t1 (id int)                 -> `table t1 already exists`, exit 1,
//	                                            and a dirty revision left behind
//	CREATE TABLE IF NOT EXISTS + INSERT       -> exit 0, and the seed row inserted twice
//
// The second is why this is a refusal and not a diagnostic. Nothing in the exit
// code, stdout or `migrate status` said the data had been duplicated.
//
// WHY REFUSE RATHER THAN MATCH. The community binary exits 0 on that database
// having executed nothing, so refusing exits 1 where it exits 0. That is the
// deliberate half (b) choice: the alternatives are to reproduce its version
// space (which changes migration identity in the migrator, far past this issue)
// or to record the token alongside the version and match on it (which is the
// same change seen from the other end, since the revision table would still be
// keyed on the int64). Refusing removes the data-safety edge without touching
// the projection, and the parity rule allows being stricter, never looser.
//
// THE RULE, and what separates it from the plausible looser one. "A recorded
// version no converted file produces" would refuse an ordinary baseline squash,
// which retires applied migrations by design. The rule is narrower: a file's
// SOURCE TOKEN is recorded AND the version that file converts to is not. A
// database this build wrote fails the first clause; a database an operator has
// already migrated forward fails the second.
//
// The token is compared as an int64 read by strconv.ParseInt, because that is
// exactly what the revision reader does with the recorded string
// (parseAtlasRevisionVersion), so "01" and "1" meet in the same place they meet
// there. A token the reader cannot parse — "1.5", or a repeatable's empty
// string — is skipped, and needs no branch here: such a row makes the read
// itself fail first, with `Atlas revision version "1.5" is not a numeric Ptah
// migration version`, which is already a refusal before anything executes.
//
// ONLY FLYWAY, measured rather than assumed. Of the five converted layouts,
// only Flyway re-executes. Applying the same two-migration directory with each
// implementation into its own database and reading each with the other: goose
// records `00001`/`00002` against this build's `1`/`2`, which meet under
// ParseInt, so nothing is pending; golang-migrate, dbmate and liquibase record
// identical versions on both. Those three still refuse cross-tool, with
// `checksum mismatch: stored <base64>, current <hex>` — the two implementations
// also disagree about the checksum ENCODING — but that is a refusal, not a
// re-execution, and it is not this issue. Extending this guard to them would be
// unreachable code, since their tokens ARE the versions they convert to.
//
// There is exactly ONE layout gate, and it is inside
// [atlasmigrateimport.FlywayCoveredSourceVersions], which reports nothing for
// any other layout. Repeating it here would make each copy individually
// unkillable — either alone still suppresses the check — and a rule two
// redundant guards hold is a rule no test can hold.
func checkForeignFlywayRevisions(
	captured fs.FS,
	format atlasmigrateimport.Format,
	plan atlasmigrate.ApplyPlan,
) error {
	if plan.Status == nil {
		return nil
	}
	applied := plan.Status.AppliedMigrations
	if len(applied) == 0 {
		return nil
	}
	covered, err := atlasmigrateimport.FlywayCoveredSourceVersions(captured, format)
	if err != nil {
		// The conversion error this would report is the one the caller already
		// surfaces on its own path; nothing to add here.
		return nil //nolint:nilerr // conversion failures are reported by the apply path itself.
	}
	stale := foreignFlywayRevisions(covered, applied)
	if len(stale) == 0 {
		return nil
	}
	return errors.New(foreignFlywayRefusal(stale, applied))
}

// foreignFlywayRevisions selects the migrations this database recorded under
// the source token.
//
// The third clause is the one that keeps the detector from inventing a
// refusal. A converted version and a source token live in the same int64 space,
// and a baseline converts into the LOW band — B10__base.sql becomes 448844 —
// so a directory that also holds V448844__x.sql has a token equal to a version
// this very directory produces. Recording it says nothing about which
// implementation wrote the row, so that pairing is dropped rather than read as
// foreign history. Measured without it, `migrate apply 1` on exactly that
// directory (which runs the baseline alone, since the baseline band sorts
// first) makes the next apply refuse a database this build had just written
// itself, blaming another implementation for a row it did not write.
//
// The cost is stated rather than hidden: on that same pathological directory
// the clause also declines to flag the genuine cross-tool case, because the
// evidence really is ambiguous. That input then behaves exactly as it did
// before this check existed, so it is a detection this does not add rather than
// one it takes away.
func foreignFlywayRevisions(
	covered []atlasmigrateimport.FlywayCoveredSourceVersion,
	applied []int64,
) []foreignFlywayRevision {
	ours := make([]int64, 0, len(covered))
	for _, migration := range covered {
		ours = append(ours, migration.Version)
	}

	var stale []foreignFlywayRevision
	for _, migration := range covered {
		recorded, err := strconv.ParseInt(strings.TrimSpace(migration.Token), 10, 64)
		if err != nil {
			continue
		}
		if !slices.Contains(applied, recorded) || slices.Contains(applied, migration.Version) {
			continue
		}
		if slices.Contains(ours, recorded) {
			continue
		}
		stale = append(stale, foreignFlywayRevision{
			source:   migration.Source,
			recorded: recorded,
			current:  migration.Version,
		})
	}
	return stale
}

// foreignFlywayRefusal renders the refusal and the two routes measured to work.
//
// The hand-written UPDATE the pre-#982 refusal prints is deliberately NOT
// offered here, and the last paragraph says why. That repair moves a row
// between two encodings of ONE implementation, so the recorded hash still
// covers the same converted body. Across implementations it does not: measured,
// rewriting `1` to `4611686018427469511` by hand on a community-binary-written
// table gets `migration 4611686018427469511 checksum mismatch: stored
// 9mRxDpig5tZbhHslnEoWdaeiT7fFPnkxapig7dheO1w=, current
// ca5446be32fd97a6194819f463b3321c799f9acd0fb138f62dca0cc48164dfa8` on the next
// apply — the two implementations record the checksum in different encodings.
// `migrate set` writes whole revision rows, so it settles both at once.
//
// Both routes are measured on the fixture in the reproduction, not inferred:
// `migrate set <head>` reports `(2 set)` and removes nothing, the next
// `migrate apply` prints `No migration files to execute.` with the seed row
// still inserted once, and the community binary afterwards prints
// `No migration files to execute` at exit 0 — so adopting this build's versions
// does not take the database away from the implementation that wrote it.
func foreignFlywayRefusal(stale []foreignFlywayRevision, applied []int64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "this database records converted Flyway migrations under their SOURCE version token, "+
		"which is how another Atlas implementation identifies them; Ptah's migrator identifies a migration "+
		"by the int64 that token projects to, so %d already-applied migration(s) read as pending here and "+
		"would run a second time. Nothing has been applied", len(stale))

	b.WriteString("\n\nrecorded version -> version this build uses:\n")
	head := int64(0)
	for _, revision := range stale {
		fmt.Fprintf(&b, "  %-20d -> %-20d %s\n", revision.recorded, revision.current, revision.source)
		head = max(head, revision.current)
	}

	b.WriteString("\nways forward:\n")
	b.WriteString("  - keep applying this database with the implementation that wrote the table: it reads " +
		"its own versions and is unaffected\n")
	b.WriteString(foreignFlywaySetRoute(head, applied))

	b.WriteString("\nrewriting the version column by hand is not enough: the two implementations also record " +
		"the migration checksum differently, so an apply after a bare version rewrite refuses with a " +
		"checksum mismatch. `migrate set` writes the whole revision row.\n")
	return b.String()
}

// foreignFlywaySetRoute renders the second way forward, or says there is none.
//
// `migrate set V` moves the database to EXACTLY V: it records everything up to
// V as applied and REMOVES every revision above it. Offering it unconditionally
// would print a command that deletes real history on one measurable shape.
// Converted baselines sit in the low band — B2__base.sql becomes 122412 — and a
// baseline squashes files whose token sorts at or below its own AS A STRING, so
// `V1000000__z.sql` (token "1000000" < "2", value 1000000) is squashed out of
// the covered set while its revision row sits above the head. Recommending
// `migrate set 122412` there would retire the migration the database actually
// ran.
//
// So the route is offered only when it removes nothing, and named as unsafe
// with the rows it would delete otherwise — the same choice
// [flywayBaselineRefusal] makes for the same verb, and for the same reason: a
// refusal that prints a command which quietly makes it worse is not a way
// forward.
func foreignFlywaySetRoute(head int64, applied []int64) string {
	var removed []int64
	for _, version := range applied {
		if version > head {
			removed = append(removed, version)
		}
	}
	if len(removed) == 0 {
		return fmt.Sprintf("  - adopt the versions this build uses: `migrate set %d`, with the same --dir "+
			"and --url, records every migration up to and including that version as applied under those "+
			"versions. The other implementation still reads the result as up to date.\n", head)
	}

	labels := make([]string, 0, len(removed))
	for _, version := range removed {
		labels = append(labels, strconv.FormatInt(version, 10))
	}
	return fmt.Sprintf("\nadopting the versions this build uses has no safe route here: `migrate set %d` "+
		"would move the database to exactly that version and remove the revision(s) above it — %s — which "+
		"is history this database really ran.\n", head, strings.Join(labels, ", "))
}
