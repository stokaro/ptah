package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/migratevalidate"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// This file holds the atlas.sum integrity gate for NATIVE Atlas migration
// directories, shared by every compat verb that reads one.
//
// It is deliberately verb-neutral. The gate started life inside
// `migrate apply` (#955, #970, #972), and keeping it there is what produced
// #974: `migrate status` and `migrate set` read the same directories through
// the same capture step and reported normally on a directory Atlas refuses,
// because the rule was a private detail of one call site rather than one rule
// with several call sites. Measured against the pinned community binary
// v1.3.0, an unhashed one-migration directory is refused by `migrate status`,
// `migrate set`, `migrate apply` and `migrate validate` alike, so there is one
// predicate to express, not four.
//
// A verb that can also read a foreign layout keeps its own dispatcher, because
// the covered file set differs per layout; see verifyAtlasApplyChecksum in
// migrate_apply.go. `migrate status` and `migrate set` reject both spellings
// of a non-Atlas layout (`?format=` on --dir and a non-atlas --dir-format)
// before they reach the gate, so only the native branch is reachable there.
//
// The foreign-layout half lives here too, as verifyCoveredAtlasDirChecksum.
// It started inside `migrate apply` (#973), and keeping it there is what
// produced #1095 in the same shape #974 had: `migrate import` reads the same
// directories and the same atlas.sum through the same capture step, and
// converted a directory apply refuses — writing the conversion out under a
// fresh sum, so the tampering ended up laundered into a directory that
// verifies clean.

// atlasSumPolicy decides what an ABSENT atlas.sum means to the verb running the
// gate. A PRESENT one always has to verify, whichever policy applies.
//
// The split between them is measured against the pinned community binary
// v1.3.0, not a judgement call. On one Goose directory with its atlas.sum
// deleted, `migrate apply` exits 1 with `checksum file not found` while
// `migrate import` exits 0 and writes the conversion. That single row is the
// whole difference between the two constants, and it is the right way round:
// import exists to read a directory another tool wrote, which by construction
// has never been hashed, so requiring a sum there would refuse the verb's own
// purpose. Verifying one that IS present costs nothing and is what #1095 asks
// for.
type atlasSumPolicy int

const (
	// requireAtlasSum refuses a source that carries no atlas.sum whenever the
	// covered set is non-empty. It is the policy for every verb that executes,
	// reports on, or records the directory's migrations.
	requireAtlasSum atlasSumPolicy = iota
	// verifyAtlasSumWhenPresent accepts a source that carries no atlas.sum at
	// all, and verifies one that does. It is the policy for `migrate import`.
	verifyAtlasSumWhenPresent
)

// verifyCoveredAtlasDirChecksum verifies a directory laid out in a foreign
// tool's convention against the atlas.sum that directory carries, over exactly
// the file set Atlas CE covers for that layout.
//
// It is the same computation `ptah-compat migrate hash` writes and
// `migrate validate` checks (#984, #992), so a directory this gate refuses is
// one those two verbs also refuse, and one they call clean passes here.
//
// What this gate verifies is also what the caller consumes, for every layout.
// That holds structurally rather than by agreement: the importer selects the
// file set it converts with the same [atlasmigrateimport.SumFileNames] rule
// this gate hashes. It was not always true — until #982 the Flyway importer ran
// a wider selection than the checksum covered, so a superseded baseline and a
// lowercase-prefixed file executed on a directory both tools called clean.
//
// Under requireAtlasSum an empty covered set is exempt from the missing-sum
// refusal, and that predicate is measured rather than assumed. CE's refusal
// keys on the covered set being non-empty, NOT on the directory holding any
// *.sql: an unhashed golang-migrate directory holding only 1_init.down.sql, and
// an unhashed Flyway directory holding only U1__init.sql, both exit 0 with "No
// migration files to execute", while an unhashed Goose directory holding only
// foo.sql exits 1. SumFileNames returning an empty slice is exactly that
// predicate. The exemption is deliberately limited to the unhashed branch: a
// hashed directory whose covered files were all deleted is drift, and CE
// reports it as one — measured on `migrate import` too, where deleting the only
// covered file of a hashed Goose directory gives `L2: 1_init.sql was removed`
// rather than a nothing-to-import success.
func verifyCoveredAtlasDirChecksum(
	cmd *cobra.Command,
	fsys fs.FS,
	format atlasmigrateimport.Format,
	policy atlasSumPolicy,
) error {
	names, err := atlasmigrateimport.SumFileNames(fsys, format)
	if err != nil {
		return err
	}
	result, hashed, err := migratesum.VerifyAtlasFilesHashed(fsys, names)
	switch {
	case errors.Is(err, migratesum.ErrSumFileMalformed):
		return migratevalidate.FailAtlasChecksumMismatch(cmd, nil)
	case errors.Is(err, migratesum.ErrCoveredEntryUnreadable):
		// A covered entry that is a directory (#991). It reaches here on the
		// converted path too, because SumFileNames selects by name and the
		// captured snapshot now records such a directory instead of dropping it.
		return migratevalidate.FailAtlasChecksumUnreadableEntry(cmd, err)
	case err != nil:
		return err
	case !hashed && policy == verifyAtlasSumWhenPresent:
		return checkCoveredAtlasEntriesReadable(cmd, fsys, names)
	case !hashed && len(names) == 0:
		return nil
	case !hashed:
		return migratevalidate.FailAtlasChecksumFileNotFound(cmd)
	case !result.OK():
		return migratevalidate.FailAtlasChecksumMismatch(cmd, result.FirstMismatch())
	}
	return nil
}

// checkCoveredAtlasEntriesReadable refuses a directory holding a covered entry
// that cannot be read, on the path where there is no atlas.sum to verify
// against.
//
// Without it, verifyAtlasSumWhenPresent would return early on an unhashed
// source and never attempt the read, which is not what the community binary
// does: measured, `migrate import` on an UNHASHED Goose directory holding a
// DIRECTORY named `weird.sql` exits 1 there with `read file "weird.sql": is a
// directory`, exactly as it does on the hashed one. Membership of the covered
// set is decided by the name, so the entry is a member whether or not anything
// recorded a hash for it, and a gate that skipped the read would exit 0 and
// convert — the direction parity must never take.
//
// It reuses [migratesum.ComputeAtlasFiles], the same computation the verify
// path runs, so "which entries are read, and in what order" keeps one
// definition. Flyway is unaffected: Atlas walks that tree instead of globbing
// it, so a directory there is a node it descends into and never a covered
// entry.
func checkCoveredAtlasEntriesReadable(cmd *cobra.Command, fsys fs.FS, names []string) error {
	_, err := migratesum.ComputeAtlasFiles(fsys, names)
	if errors.Is(err, migratesum.ErrCoveredEntryUnreadable) {
		return migratevalidate.FailAtlasChecksumUnreadableEntry(cmd, err)
	}
	return err
}

// verifyAtlasWriteDirChecksum runs the native Atlas integrity gate over the
// directory a WRITING verb is about to write into, before it writes anything.
//
// `migrate new` and `migrate diff` are the two verbs that create a migration
// file and a fresh atlas.sum, and until stokaro/ptah#1086 they were the two
// that never checked the one they were about to overwrite. That is the worst
// place to leave the gate out: every other verb reports on a drifted directory,
// while these two rewrite the checksum over it, so the drift stops being
// visible to `migrate validate` afterwards. Measured against the pinned
// community binary v1.3.0, an unhashed one-migration directory and a
// hashed-then-edited one are both refused by `migrate new` and `migrate diff`
// with the same stdout guidance block and the same `Error: checksum file not
// found` / `Error: checksum mismatch` on stderr that `migrate apply` prints.
//
// The refusal has to precede the write rather than accompany it, which is why
// this is a preflight on the compat surface instead of a check inside the
// writer: a gate that fires once the file exists has already left the mess it
// was there to prevent.
//
// A directory that does not exist yet is not an integrity error. Both verbs
// create their directory, and the community binary exits 0 on `migrate new
// --dir file://does-not-exist` and on `migrate diff` into one, so the gate has
// nothing to verify and says so by returning nil. The remaining exemptions —
// an empty directory, one holding no top-level *.sql — belong to
// [failUnhashedAtlasDir] and are shared with every other verb.
// The format parameter is the layout the run SELECTED, and the gate runs over
// that layout's covered set rather than the Atlas one. `migrate diff` can be
// pointed at a foreign layout since stokaro/ptah#1013; verifying a
// golang-migrate directory as if it were an Atlas one would refuse the pair it
// legitimately holds — atlas.sum covers the `.up.sql` halves alone there — and
// then rewrite the sum over the wrong set.
func verifyAtlasWriteDirChecksum(
	cmd *cobra.Command,
	project atlasProject,
	dir atlasargs.LocalDir,
	format atlasmigrateimport.Format,
) error {
	source, err := project.captureLocal(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if atlasmigrate.ReadsNativeAtlasDir(format) {
		return verifyNativeAtlasDirChecksum(cmd, source.FileSystem)
	}
	return verifyCoveredAtlasDirChecksum(cmd, source.FileSystem, format, requireAtlasSum)
}

// checkAtlasWriteDirChecksum is the refusal half of the writing gate for an
// already-captured filesystem, dispatched on the selected layout.
//
// It is what a writing verb re-checks the LOCKED snapshot with, so the
// predicate that refused before the lock and the predicate that refuses under
// it are one definition rather than two that have to agree.
func checkAtlasWriteDirChecksum(
	cmd *cobra.Command,
	fsys fs.FS,
	format atlasmigrateimport.Format,
) error {
	if atlasmigrate.ReadsNativeAtlasDir(format) {
		return checkNativeAtlasDirChecksum(cmd, fsys)
	}
	return verifyCoveredAtlasDirChecksum(cmd, fsys, format, requireAtlasSum)
}

// verifyNativeAtlasDirChecksum enforces the atlas.sum integrity gate on a
// captured NATIVE Atlas migration filesystem: a missing atlas.sum and a failed
// verification both refuse the command, with output byte-identical to
// `migrate validate` on the same directory.
//
// The refusal is returned as-is, never wrapped through cmdutil.Fail or
// failAtlasCommand: the migratevalidate helpers already wrote the guidance
// block to stdout and return an exitcode.New(1, ...) error whose message the
// root command prints as `Error: checksum file not found`. Re-wrapping would
// prepend `error: ` and move the text to the other stream, losing the byte
// parity that is the point of routing through those helpers.
//
// Call it immediately after the directory snapshot and before connecting to
// the database. That ordering is measured, not stylistic: the community binary
// prints the checksum refusal even when --url is unreachable, and on
// `migrate set` even when the positional arity is wrong.
//
// A directory the gate ACCEPTS also gets its declined SQL files named on
// stderr, which is the one place this surface deliberately prints where the
// community binary is silent; see [warnDeclinedAtlasFiles].
func verifyNativeAtlasDirChecksum(cmd *cobra.Command, fsys fs.FS) error {
	if err := checkNativeAtlasDirChecksum(cmd, fsys); err != nil {
		return err
	}
	warnDeclinedAtlasFiles(cmd, fsys)
	return nil
}

// checkNativeAtlasDirChecksum is the refusal half of the gate, split out so the
// declined-file notice covers every accepting branch rather than only the
// verified one. The unhashed-but-exempt branch is exactly where a directory
// whose only SQL sits in a subdirectory lands, so leaving it uncovered would
// keep the silence for the shape stokaro/ptah#976 is about.
func checkNativeAtlasDirChecksum(cmd *cobra.Command, fsys fs.FS) error {
	result, hashed, err := migratesum.VerifyHashed(fsys, migrator.MigrationDirFormatAtlas)
	switch {
	case errors.Is(err, migratesum.ErrSumFileMalformed):
		// A malformed atlas.sum has no entry-level mismatch to point at; the
		// validate surface reports it as a plain checksum mismatch.
		return migratevalidate.FailAtlasChecksumMismatch(cmd, nil)
	case errors.Is(err, migratesum.ErrCoveredEntryUnreadable):
		// A covered entry that is a directory (#991). The community binary
		// prints the checksum preamble here, not a bare error, so routing it
		// anywhere else would lose the stream layout this gate exists to match.
		return migratevalidate.FailAtlasChecksumUnreadableEntry(cmd, err)
	case err != nil:
		return err
	case !hashed:
		return failUnhashedAtlasDir(cmd, fsys)
	case !result.OK():
		return migratevalidate.FailAtlasChecksumMismatch(cmd, result.FirstMismatch())
	}
	return nil
}

// warnDeclinedAtlasFiles names on stderr every SQL file the directory holds
// that atlas.sum cannot cover, and that the command is therefore about to skip.
//
// This is the one place ptah-compat deliberately prints where the community
// binary prints nothing, and the reason is stated rather than assumed. On a
// directory whose only migration is `sub/2_b.sql`, the community binary writes
// an atlas.sum covering zero files, `validate` exits 0 with both streams empty,
// and `apply` reports `No migration files to execute` at exit 0: every gate
// green, database empty, the migration the author committed never ran and
// nothing ever said so. The same silence swallows `2_c.SQL`, which on a
// case-insensitive filesystem is a typo rather than a decision. Selecting
// exactly the covered set is what makes atlas.sum mean something
// (stokaro/ptah#976); saying which files that left out is what keeps the fix
// from replacing one silent drop with another.
//
// Exit codes and stdout stay byte-identical to the community binary, so the
// cost is stderr bytes on directories that hold an uncovered SQL file at all.
//
// It runs only once the directory has been accepted. A refusal has already told
// the operator the directory is not runnable, and appending a note about which
// files would not have run is noise on top of it.
//
// The declined set is derived by subtracting the covered set from every *.sql
// the tree holds, and the covered set comes from
// [atlasmigrateimport.SumFileNames] — the same function `migrate hash` writes
// from and `migrate validate` verifies against. Restating the top-level,
// case-sensitive rule locally would recreate exactly the drift between two
// views of one directory that #976 was.
func warnDeclinedAtlasFiles(cmd *cobra.Command, fsys fs.FS) {
	declined, err := declinedAtlasFiles(fsys)
	if err != nil || len(declined) == 0 {
		// A directory that cannot be walked has nothing trustworthy to report,
		// and the verbs sharing this gate all read it again immediately.
		return
	}
	out := cmd.ErrOrStderr()
	for _, name := range declined {
		fmt.Fprintf(out, "warning: %s is not covered by atlas.sum and will not run; "+
			"Atlas migrations are top-level files named *.sql\n", name)
	}
}

// declinedAtlasFiles returns, in walk order, the SQL files fsys holds that the
// Atlas file selection leaves out: anything below the top level, and any
// top-level name whose extension is spelled other than ".sql".
func declinedAtlasFiles(fsys fs.FS) ([]string, error) {
	names, err := atlasmigrateimport.SumFileNames(fsys, atlasmigrateimport.FormatAtlas)
	if err != nil {
		return nil, err
	}
	covered := make(map[string]struct{}, len(names))
	for _, name := range names {
		covered[name] = struct{}{}
	}

	var declined []string
	err = fs.WalkDir(fsys, ".", func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.EqualFold(path.Ext(p), ".sql") {
			return nil
		}
		if _, ok := covered[p]; ok {
			return nil
		}
		declined = append(declined, p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return declined, nil
}

// failUnhashedAtlasDir refuses a NATIVE Atlas directory that carries no
// atlas.sum, unless it holds no SQL file anywhere in its tree.
//
// The exemption exists because a directory with nothing to execute is not a
// checksum error: the community binary reports success and exits 0 on an empty
// directory and on one holding only non-SQL files, so a CI bootstrap that
// creates an empty migrations directory keeps working (#970). The gate fires on
// the presence of a SQL file, not on parseable versioned migrations — an
// unhashed directory holding only `foo.sql` is refused there, and so it is
// here.
//
// The scan reads the top level only, matching both the community binary's view
// and — since #976 — Ptah's own. It used to recurse, and the justification was
// that Ptah's registrar executed nested files, so keying the exemption on the
// shallower view would have let unhashed migrations run unverified. That
// premise is gone: the registrar now selects exactly the set atlas.sum covers
// (migrator.retainAtlasSumCovered), so a nested file is not a migration on
// either tool and refusing the directory it sits in protects nothing. Measured
// with only the loader change applied, an unhashed directory whose sole `.sql`
// is one level down still exited 1 here against the community binary's 0 —
// over-refusal with nothing left to justify it. The two changes therefore land
// together; the loader change alone leaves this, and this alone would reopen
// the unhashed nested hole.
//
// The suffix test stays case-sensitive for the same reason it always was: the
// covered set is. A directory holding only `1_a.SQL` has an empty covered set,
// so it is nothing-to-execute rather than an unhashed history, on both tools.
//
// A converted directory still uses a different predicate. That is now about
// the covered set alone rather than about depth: Flyway's atlas.sum genuinely
// reaches into subdirectories, so an unhashed Flyway directory whose only
// migration sits one level down is refused on both tools, while the same shape
// read as goose is exempt on both.
func failUnhashedAtlasDir(cmd *cobra.Command, fsys fs.FS) error {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return fmt.Errorf("scan migration directory for SQL files: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		return migratevalidate.FailAtlasChecksumFileNotFound(cmd)
	}
	return nil
}
