#!/bin/bash
#
# Regenerates the Atlas CE integrity-file corpus in this directory.
#
# Every case directory holds source files plus the atlas.sum that Atlas CE
# itself wrote for them, so TestSumFileNamesMatchesAtlasCE can assert that
# Ptah's per-format file set reproduces the oracle byte for byte instead of
# trusting a rule transcribed by hand.
#
# The oracle is pinned. Bumping it is a deliberate act: update the repository
# lock, re-run, and review the resulting diff, because any change in the sums is
# a change in what Atlas considers a migration.
#
#   oracle:  ptah-atlas-conformance/bin/atlas
#   version: atlas community version v1.3.0
#   date:    2026-08-03
#
# A system-wide `atlas` on PATH is frequently a different build, so the
# oracle is invoked by absolute path and
# its version is checked before anything is written.
#
# Usage:
#   ./regenerate.sh [path-to-atlas]
#
# After running, `git status` must be clean. A diff means either the oracle
# changed or this script drifted from the committed corpus.

set -euo pipefail

ATLAS="${1:-$HOME/Work/denis/ptah-atlas-conformance/bin/atlas}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(git -C "$HERE" rev-parse --show-toplevel)"

# shellcheck source=scripts/lib/atlas-ce-oracle.sh
source "$ROOT/scripts/lib/atlas-ce-oracle.sh"
atlas_ce_load_lock "$ROOT/scripts/atlas-ce-oracle.lock"

if [ ! -x "$ATLAS" ]; then
	echo "regenerate: oracle not found or not executable: $ATLAS" >&2
	echo "regenerate: pass the path to the pinned Atlas CE binary as \$1" >&2
	exit 1
fi

actual_version="$(atlas_ce_verify_binary "$ATLAS")"

# Migration bodies. Content is irrelevant to the file-set rule under test but
# must stay byte-stable, because every recorded hash covers it.
SQL_PLAIN='CREATE TABLE widgets (id INTEGER PRIMARY KEY);
'
SQL_SECOND='CREATE TABLE gadgets (id INTEGER PRIMARY KEY);
'
SQL_DOWN='DROP TABLE widgets;
'
SQL_GOOSE='-- +goose Up
CREATE TABLE widgets (id INTEGER PRIMARY KEY);
-- +goose Down
DROP TABLE widgets;
'
SQL_DBMATE='-- migrate:up
CREATE TABLE widgets (id INTEGER PRIMARY KEY);
-- migrate:down
DROP TABLE widgets;
'
SQL_LIQUIBASE='--liquibase formatted sql
--changeset ptah:1
CREATE TABLE widgets (id INTEGER PRIMARY KEY);
'

case_dir=""

new_case() {
	case_dir="$HERE/$1"
	rm -rf "$case_dir"
	mkdir -p "$case_dir"
}

put() {
	local name="$1" body="$2"
	mkdir -p "$(dirname "$case_dir/$name")"
	printf '%s' "$body" >"$case_dir/$name"
}

seal() {
	local format="$1"
	"$ATLAS" migrate hash --dir "file://$case_dir?format=$format"
	printf '  %-52s %s\n' "${case_dir#"$HERE"/}" "$(head -1 "$case_dir/atlas.sum")"
}

# seal_refused records a shape the oracle DECLINES to hash.
#
# seal() cannot express one. `set -e` aborts the whole regeneration on a
# non-zero oracle exit, and the corpus format -- a committed atlas.sum per case
# -- has no way to say "no sum exists for this shape". That gap is why the
# directory-named-*.sql family (stokaro/ptah#991) went unrecorded: the only
# outcome the corpus could hold was agreement.
#
# The refusal is stored as atlas.refused holding the oracle's own message with
# the machine-specific case path replaced by a placeholder. That file is inert
# for every format under test: it carries no .sql suffix, so no glob matches it
# and the Flyway walk does not parse it.
seal_refused() {
	local format="$1" out ec
	set +e
	out="$("$ATLAS" migrate hash --dir "file://$case_dir?format=$format" 2>&1)"
	ec=$?
	set -e
	if [ "$ec" -eq 0 ]; then
		echo "regenerate: expected a refusal for ${case_dir#"$HERE"/}, oracle exited 0" >&2
		exit 1
	fi
	if [ -e "$case_dir/atlas.sum" ]; then
		echo "regenerate: oracle refused ${case_dir#"$HERE"/} but still wrote atlas.sum" >&2
		exit 1
	fi
	# The community binary can append generic installation advice to a refusal.
	# It is not part of the format-specific verdict and varies with invocation
	# context, so exclude that exact footer from the deterministic corpus.
	printf '%s\n' "$out" |
		sed -e "s|$case_dir|<case-dir>|g" \
			-e "/^You're running the community build of Atlas, which differs from the official version\\.$/,\$d" \
		>"$case_dir/atlas.refused"
	printf '  %-52s %s\n' "${case_dir#"$HERE"/}" "REFUSED (exit $ec)"
}

echo "regenerating Atlas CE corpus with $actual_version"

# --- atlas (native) -------------------------------------------------------
new_case atlas/basic
put 1_init.sql "$SQL_PLAIN"
put 2_more.sql "$SQL_SECOND"
seal atlas

new_case atlas/subdirectory
put 1_top.sql "$SQL_PLAIN"
put sub/2_nested.sql "$SQL_SECOND"
seal atlas

# --- goose ----------------------------------------------------------------
new_case goose/basic
put 1_init.sql "$SQL_GOOSE"
seal goose

new_case goose/extra-files
put 1_init.sql "$SQL_GOOSE"
put 2_seed.go 'package migrations
'
put foo.sql "$SQL_GOOSE"
seal goose

new_case goose/subdirectory
put 1_init.sql "$SQL_GOOSE"
put sub/2_nested.sql "$SQL_GOOSE"
seal goose

new_case goose/uppercase-extension
put 1_init.SQL "$SQL_GOOSE"
seal goose

new_case goose/no-sql
put README.md 'not a migration
'
seal goose

new_case goose/non-sql-content
put 1_init.sql '-- +goose Up
CREATE TABLE widgets (id INTEGER PRIMARY KEY);
'
put notes.sql 'not sql at all
'
seal goose

new_case goose/sum-ignore-directive
put 1_ignored.sql '-- atlas:sum ignore
-- +goose Up
CREATE TABLE widgets (id INTEGER PRIMARY KEY);
'
put 2_kept.sql '-- +goose Up
CREATE TABLE gadgets (id INTEGER PRIMARY KEY);
'
seal goose

# --- dbmate ---------------------------------------------------------------
new_case dbmate/basic
put 1_init.sql "$SQL_DBMATE"
seal dbmate

new_case dbmate/non-numbered
put 1_init.sql "$SQL_DBMATE"
put foo.sql "$SQL_DBMATE"
seal dbmate

# --- liquibase ------------------------------------------------------------
new_case liquibase/basic
put 1_init.sql "$SQL_LIQUIBASE"
seal liquibase

new_case liquibase/xml-changelog
put 1_init.sql "$SQL_LIQUIBASE"
put changelog.xml '<databaseChangeLog></databaseChangeLog>
'
seal liquibase

# --- golang-migrate -------------------------------------------------------
new_case golang-migrate/basic
put 1_init.up.sql "$SQL_PLAIN"
put 1_init.down.sql "$SQL_DOWN"
seal golang-migrate

new_case golang-migrate/mixed-up-down-bare
put 1_init.up.sql "$SQL_PLAIN"
put 1_init.down.sql "$SQL_DOWN"
put 2_bare.sql "$SQL_SECOND"
seal golang-migrate

new_case golang-migrate/odd-names
put 1_init.up.sql "$SQL_PLAIN"
put foo.up.sql "$SQL_SECOND"
put 2_x.UP.sql "$SQL_SECOND"
seal golang-migrate

new_case golang-migrate/down-only
put 1_init.down.sql "$SQL_DOWN"
seal golang-migrate

new_case golang-migrate/doubled-suffix
put 1_init.up.sql "$SQL_PLAIN"
put 2_x.up.up.sql "$SQL_SECOND"
seal golang-migrate

# --- flyway ---------------------------------------------------------------
new_case flyway/basic
put V1__init.sql "$SQL_PLAIN"
seal flyway

new_case flyway/undo-baseline-versioned
put V1__init.sql "$SQL_PLAIN"
put U1__init.sql "$SQL_DOWN"
put B2__baseline.sql "$SQL_PLAIN"
put V3__third.sql "$SQL_SECOND"
seal flyway

new_case flyway/repeatable
put V1__init.sql "$SQL_PLAIN"
put R__views.sql "$SQL_SECOND"
seal flyway

new_case flyway/undo-only
put U1__init.sql "$SQL_DOWN"
seal flyway

new_case flyway/non-flyway-sql
put V1__init.sql "$SQL_PLAIN"
put plain.sql "$SQL_SECOND"
seal flyway

new_case flyway/numeric-ordering
put V1.5__a.sql "$SQL_PLAIN"
put V2__b.sql "$SQL_SECOND"
put V10__c.sql "$SQL_PLAIN"
seal flyway

new_case flyway/minor-version-ordering
put V1__a.sql "$SQL_PLAIN"
put V1.5__b.sql "$SQL_SECOND"
put V2__c.sql "$SQL_PLAIN"
seal flyway

new_case flyway/two-repeatables
put V1__init.sql "$SQL_PLAIN"
put R__alpha.sql "$SQL_SECOND"
put R__zeta.sql "$SQL_PLAIN"
seal flyway

new_case flyway/versioned-repeatable
put V1__b.sql "$SQL_PLAIN"
put R1__a.sql "$SQL_SECOND"
seal flyway

new_case flyway/two-baselines
put B1__one.sql "$SQL_PLAIN"
put B2__two.sql "$SQL_SECOND"
put V3__three.sql "$SQL_PLAIN"
seal flyway

new_case flyway/duplicate-baseline-version
put B2__a.sql "$SQL_PLAIN"
put B2__b.sql "$SQL_SECOND"
put V3__c.sql "$SQL_PLAIN"
seal flyway

new_case flyway/baseline-repeatable-undo
put V1__one.sql "$SQL_PLAIN"
put B2__base.sql "$SQL_SECOND"
put R__view.sql "$SQL_PLAIN"
put U3__undo.sql "$SQL_DOWN"
put V4__four.sql "$SQL_SECOND"
seal flyway

new_case flyway/baseline-string-cut
put B2__base.sql "$SQL_PLAIN"
put V10__ten.sql "$SQL_SECOND"
seal flyway

new_case flyway/baseline-dotted
put B1.5__base.sql "$SQL_PLAIN"
put V1__a.sql "$SQL_SECOND"
put V2__b.sql "$SQL_PLAIN"
seal flyway

new_case flyway/baseline-equal-version
put B2__base.sql "$SQL_PLAIN"
put V2__same.sql "$SQL_SECOND"
put V3__later.sql "$SQL_PLAIN"
seal flyway

new_case flyway/baseline-unparseable
put Bx__y.sql "$SQL_PLAIN"
put V1__a.sql "$SQL_SECOND"
put V2__b.sql "$SQL_PLAIN"
seal flyway

new_case flyway/baseline-two-unparseable
put B2__base.sql "$SQL_PLAIN"
put V5__five.sql "$SQL_SECOND"
put Va__a.sql "$SQL_PLAIN"
put Vq__q.sql "$SQL_SECOND"
seal flyway

new_case flyway/lowercase-prefix
put v1__one.sql "$SQL_PLAIN"
put V2__two.sql "$SQL_SECOND"
seal flyway

new_case flyway/underscore-version
put V1_5__a.sql "$SQL_PLAIN"
put V2__b.sql "$SQL_SECOND"
seal flyway

new_case flyway/uppercase-extension
put V1__x.SQL "$SQL_PLAIN"
seal flyway

new_case flyway/leading-zero
put V01__a.sql "$SQL_PLAIN"
put V2__b.sql "$SQL_SECOND"
seal flyway

# Both leading components exceed MaxInt32, so a build that scored them with
# strconv.Atoi (platform int) would clamp both to the same ceiling on a 32-bit
# target and swap them. The oracle orders 9e9 before 1e10.
new_case flyway/wide-components
put V9000000000__a.sql "$SQL_PLAIN"
put V10000000000__b.sql "$SQL_SECOND"
seal flyway

new_case flyway/no-version
put V__x.sql "$SQL_PLAIN"
put V1__ok.sql "$SQL_SECOND"
seal flyway

new_case flyway/non-numeric-version
put Vx__y.sql "$SQL_PLAIN"
put V1__ok.sql "$SQL_SECOND"
seal flyway

new_case flyway/unparseable-tie
put V__z.sql "$SQL_PLAIN"
put Vx__y.sql "$SQL_SECOND"
put V1__ok.sql "$SQL_PLAIN"
seal flyway

new_case flyway/unparseable-with-baseline
put B2__base.sql "$SQL_PLAIN"
put V3__three.sql "$SQL_SECOND"
put Vx__y.sql "$SQL_PLAIN"
seal flyway

new_case flyway/double-separator
put V0__z.sql "$SQL_PLAIN"
put V1__a__b.sql "$SQL_SECOND"
seal flyway

new_case flyway/empty-description
put V1__.sql "$SQL_PLAIN"
put V2__ok.sql "$SQL_SECOND"
seal flyway

new_case flyway/no-separator
put V1.sql "$SQL_PLAIN"
put V2__ok.sql "$SQL_SECOND"
seal flyway

# An ordinary word starting with V/B/R/U is a Flyway file to Atlas CE. Backup.sql
# in particular parses as a baseline and squashes every versioned migration.
new_case flyway/word-versioned
put Video.sql "$SQL_PLAIN"
put V1__ok.sql "$SQL_SECOND"
seal flyway

new_case flyway/word-baseline
put Backup.sql "$SQL_PLAIN"
put V1__ok.sql "$SQL_SECOND"
put V2__ok2.sql "$SQL_PLAIN"
seal flyway

new_case flyway/word-undo
put Users.sql "$SQL_PLAIN"
put V1__ok.sql "$SQL_SECOND"
seal flyway

new_case flyway/word-repeatable
put Reports.sql "$SQL_PLAIN"
put V1__ok.sql "$SQL_SECOND"
seal flyway

new_case flyway/bare-prefix
put V.sql "$SQL_PLAIN"
put V1__ok.sql "$SQL_SECOND"
seal flyway

new_case flyway/lowercase-word
put video.sql "$SQL_PLAIN"
put V1__ok.sql "$SQL_SECOND"
seal flyway

new_case flyway/negative-version
put V-1__a.sql "$SQL_PLAIN"
put V2__b.sql "$SQL_SECOND"
seal flyway

# Flyway is the one format Atlas CE recurses into. Nested files are covered
# under their slash path and ordered by version alongside the top-level ones.
new_case flyway/subdirectory
put V1__top.sql "$SQL_PLAIN"
put sub/V2__nested.sql "$SQL_SECOND"
seal flyway

new_case flyway/deep-subdirectory
put V1__top.sql "$SQL_PLAIN"
put a/V2__mid.sql "$SQL_SECOND"
put a/b/V3__deep.sql "$SQL_PLAIN"
seal flyway

new_case flyway/subdirectory-version-order
put V9__top.sql "$SQL_PLAIN"
put zzz/V1__nested.sql "$SQL_SECOND"
seal flyway

new_case flyway/subdirectory-repeatable
put V1__one.sql "$SQL_PLAIN"
put sub/R__view.sql "$SQL_SECOND"
seal flyway

new_case flyway/subdirectory-undo
put V1__one.sql "$SQL_PLAIN"
put sub/U1__one.sql "$SQL_DOWN"
seal flyway

# Controls proving the other formats do NOT recurse.
new_case dbmate/subdirectory
put 1_top.sql "$SQL_DBMATE"
put sub/2_nested.sql "$SQL_DBMATE"
seal dbmate

new_case liquibase/subdirectory
put 1_top.sql "$SQL_LIQUIBASE"
put sub/2_nested.sql "$SQL_LIQUIBASE"
seal liquibase

new_case golang-migrate/subdirectory
put 1_top.up.sql "$SQL_PLAIN"
put sub/2_nested.up.sql "$SQL_SECOND"
seal golang-migrate

# --- version-token axis --------------------------------------------------
# A curated corpus of single-integer versions cannot separate the candidate
# comparators. These pairs differ only in trailing zero components or in how a
# token parses, which is exactly where a zero-extending comparator inverts the
# oracle's order.
new_case flyway/version-trailing-zero
put V1__seed.sql "$SQL_PLAIN"
put V1.0__seed.sql "$SQL_SECOND"
seal flyway

new_case flyway/version-two-trailing-zeros
put V2__b.sql "$SQL_PLAIN"
put V2.0.0__a.sql "$SQL_SECOND"
seal flyway

new_case flyway/version-trailing-separator
put V1__b.sql "$SQL_PLAIN"
put V1.__a.sql "$SQL_SECOND"
seal flyway

new_case flyway/version-leading-separator
put V.1__a.sql "$SQL_PLAIN"
put V0.5__b.sql "$SQL_SECOND"
seal flyway

new_case flyway/version-non-numeric-component
put Vx.5__a.sql "$SQL_PLAIN"
put V0.3__b.sql "$SQL_SECOND"
seal flyway

new_case flyway/version-non-numeric-ties-zero
put V1.x__a.sql "$SQL_PLAIN"
put V1.0__b.sql "$SQL_SECOND"
put V1.5__c.sql "$SQL_PLAIN"
seal flyway

new_case flyway/version-negative
put V-5__a.sql "$SQL_PLAIN"
put V-1__b.sql "$SQL_SECOND"
seal flyway

new_case flyway/version-overflow
put V20240101120000000000__a.sql "$SQL_PLAIN"
put V2__b.sql "$SQL_SECOND"
seal flyway

# --- hidden directories --------------------------------------------------
new_case flyway/hidden-subdirectory
put .archive/V1__old.sql "$SQL_PLAIN"
put V2__new.sql "$SQL_SECOND"
seal flyway

# --- baseline reach across the walk --------------------------------------
new_case flyway/baseline-squashes-nested-after
put B2__base.sql "$SQL_PLAIN"
put V3__three.sql "$SQL_SECOND"
put sub/V1__one.sql "$SQL_PLAIN"
seal flyway

new_case flyway/baseline-nested-spares-earlier
put V1__one.sql "$SQL_PLAIN"
put V2__two.sql "$SQL_SECOND"
put sub/B9__base.sql "$SQL_PLAIN"
seal flyway

new_case flyway/baseline-superseded-spares-survivor
put B2__a.sql "$SQL_PLAIN"
put V3__b.sql "$SQL_SECOND"
put sub/B5__c.sql "$SQL_PLAIN"
seal flyway

# The backwards reach: a baseline drops files already accepted, comparing each
# survivor's full PATH against the baseline's version token as strings. The
# nested file outranks the baseline numerically in every one of these, so a
# numeric reading predicts the same answer for all of them and is wrong.
new_case flyway/backwards-reach-squashed
put 4dir/V9__old.sql "$SQL_PLAIN"
put B5__base.sql "$SQL_SECOND"
seal flyway

new_case flyway/backwards-reach-kept
put 6dir/V9__old.sql "$SQL_PLAIN"
put B5__base.sql "$SQL_SECOND"
seal flyway

# "5d" and "5e" are the same version number on opposite sides of the boundary.
new_case flyway/backwards-reach-token-spares
put 5dir/V9__old.sql "$SQL_PLAIN"
put B5d__base.sql "$SQL_SECOND"
seal flyway

new_case flyway/backwards-reach-token-squashes
put 5dir/V9__old.sql "$SQL_PLAIN"
put B5e__base.sql "$SQL_SECOND"
seal flyway

new_case flyway/backwards-reach-spares-repeatable
put 0dir/R__x.sql "$SQL_PLAIN"
put B5__base.sql "$SQL_SECOND"
seal flyway

# Numeric token, so no rule keyed on non-numeric tokens could cover it.
new_case flyway/backwards-reach-huge-token
put 0archive/V5__old.sql "$SQL_PLAIN"
put B99999999999999999999__base.sql "$SQL_SECOND"
seal flyway

# Reached AFTER the baseline, so the forward test applies: version token, not
# path. zdir/ would win a path comparison and still loses.
new_case flyway/forward-squash-uses-token
put B5__base.sql "$SQL_PLAIN"
put zdir/V1__old.sql "$SQL_SECOND"
seal flyway

# Supersede compares tokens as strings: "2" >= "10", so B2 replaces B10. Under
# a numeric reading B10 would stay and swallow V3.
new_case flyway/baseline-supersede-string-order
put B10__a.sql "$SQL_PLAIN"
put B2__b.sql "$SQL_SECOND"
put V3__c.sql "$SQL_PLAIN"
seal flyway

# Operand boundaries. Each layout discriminates which operands one comparison
# uses, or how it breaks a tie; without them the choice is arbitrary and a
# plausible edit passes every other test.

# Supersede compares TOKENS: name "B5__base.sql" outranks "9" while token "5"
# does not, so a name-based supersede would install B5 here.
new_case flyway/supersede-compares-tokens
put B5__base.sql "$SQL_PLAIN"
put 1dir/B9__base.sql "$SQL_SECOND"
seal flyway

# The backward reach squashes an EXACTLY equal path: the baseline's token is
# literally "V1.sql" and the survivor is named "V1.sql". A strict < would spare it.
new_case flyway/backward-reach-is-inclusive
put V1.sql "$SQL_PLAIN"
put sub/BV1.sql.sql "$SQL_SECOND"
seal flyway

# The backward reach belongs to INSTALLATION, not to encountering a baseline:
# zz/B1 is skipped, and must not reach back over a survivor the FORWARD test
# admitted despite its path sorting below "1".
new_case flyway/skipped-baseline-does-not-reach
put 0a/B5__base.sql "$SQL_PLAIN"
put 0b/V9__x.sql "$SQL_SECOND"
put zz/B1__base.sql "$SQL_PLAIN"
seal flyway

# Ties resolve oppositely, which is why supersede and the forward squash cannot
# be collapsed into one comparison even though their operand types match.
new_case flyway/tie-equal-baseline-wins
put B2__a.sql "$SQL_PLAIN"
put zdir/B2__b.sql "$SQL_SECOND"
seal flyway

new_case flyway/tie-equal-file-loses
put B2__base.sql "$SQL_PLAIN"
put zdir/V2__same.sql "$SQL_SECOND"
seal flyway

# A directory named B3 and a file named B3.sql: sorting the paths puts B3.sql
# first ('.' below '/'), while a walk descends B3 first. The two orders select
# different files, so this is the case that proves selection follows the WALK.
new_case flyway/walk-order-vs-path-sort
put B3/V2__a.sql "$SQL_PLAIN"
put B3.sql "$SQL_SECOND"
seal flyway

new_case flyway/baseline-ordinary-project
put B1__baseline.sql "$SQL_PLAIN"
put V2__init.sql "$SQL_SECOND"
put views/V3__view.sql "$SQL_PLAIN"
seal flyway

# --- a DIRECTORY whose name the layout's glob matches (#991) ---------------
# The four globbing formats select covered entries by name, so a directory
# called weird.sql is a member and the read that follows fails: the oracle
# refuses the whole directory and writes nothing. Every case carries a real file
# inside the directory because git does not track empty ones -- and note.txt
# also proves the refusal keys on the directory itself rather than on anything
# it holds.
new_case atlas/directory-named-sql
put 1_init.sql "$SQL_PLAIN"
put weird.sql/note.txt 'not a migration
'
seal_refused atlas

new_case goose/directory-named-sql
put 1_init.sql "$SQL_GOOSE"
put weird.sql/note.txt 'not a migration
'
seal_refused goose

new_case dbmate/directory-named-sql
put 1_init.sql "$SQL_DBMATE"
put weird.sql/note.txt 'not a migration
'
seal_refused dbmate

new_case liquibase/directory-named-sql
put 1_init.sql "$SQL_LIQUIBASE"
put weird.sql/note.txt 'not a migration
'
seal_refused liquibase

# golang-migrate globs *.up.sql, so the pair below separates the SUFFIX FILTER
# from the read: the same directory name is ignored under one suffix and refused
# under the other. A fix expressed as "reject any .sql directory" reddens the
# first of these.
new_case golang-migrate/directory-named-sql
put 1_init.up.sql "$SQL_PLAIN"
put weird.sql/note.txt 'not a migration
'
seal golang-migrate

new_case golang-migrate/directory-named-up-sql
put 1_init.up.sql "$SQL_PLAIN"
put weird.up.sql/note.txt 'not a migration
'
seal_refused golang-migrate

# Flyway is exempt in BOTH tools, and not by special-casing: the oracle WALKS a
# Flyway tree rather than globbing it, so a directory is a node it descends into
# and never reads. The nested case pins that the recursion still covers what is
# inside such a directory.
new_case flyway/directory-named-sql
put V1__init.sql "$SQL_PLAIN"
put weird.sql/note.txt 'not a migration
'
seal flyway

new_case flyway/directory-named-sql-nested
put V1__init.sql "$SQL_PLAIN"
put weird.sql/V2__nested.sql "$SQL_SECOND"
seal flyway

echo "done"
