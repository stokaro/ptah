package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/migratevalidate"
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
