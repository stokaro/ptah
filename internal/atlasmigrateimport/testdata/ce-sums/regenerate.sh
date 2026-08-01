#!/bin/bash
#
# Regenerates the Atlas CE integrity-file corpus in this directory.
#
# Every case directory holds source files plus the atlas.sum that Atlas CE
# itself wrote for them, so TestSumFileNamesMatchesAtlasCE can assert that
# Ptah's per-format file set reproduces the oracle byte for byte instead of
# trusting a rule transcribed by hand.
#
# The oracle is pinned. Bumping it is a deliberate act: change ORACLE_VERSION
# below, re-run, and review the resulting diff, because any change in the sums
# is a change in what Atlas considers a migration.
#
#   oracle:  ptah-atlas-conformance/bin/atlas
#   version: atlas community version v1.2.0
#   date:    2026-08-01
#
# A system-wide `atlas` on PATH is frequently a different build (a v1.2.4
# canary at the time of writing), so the oracle is invoked by absolute path and
# its version is checked before anything is written.
#
# Usage:
#   ./regenerate.sh [path-to-atlas]
#
# After running, `git status` must be clean. A diff means either the oracle
# changed or this script drifted from the committed corpus.

set -euo pipefail

ORACLE_VERSION="atlas community version v1.2.0"
ATLAS="${1:-$HOME/Work/denis/ptah-atlas-conformance/bin/atlas}"
HERE="$(cd "$(dirname "$0")" && pwd)"

if [ ! -x "$ATLAS" ]; then
	echo "regenerate: oracle not found or not executable: $ATLAS" >&2
	echo "regenerate: pass the path to the pinned Atlas CE binary as \$1" >&2
	exit 1
fi

actual_version="$("$ATLAS" version | head -1)"
if [ "$actual_version" != "$ORACLE_VERSION" ]; then
	echo "regenerate: oracle version mismatch" >&2
	echo "  want: $ORACLE_VERSION" >&2
	echo "  got:  $actual_version" >&2
	echo "regenerate: update ORACLE_VERSION deliberately and review the corpus diff" >&2
	exit 1
fi

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

echo "regenerating Atlas CE corpus with $actual_version"

# --- atlas (native) -------------------------------------------------------
new_case atlas/basic
put 1_init.sql "$SQL_PLAIN"
put 2_more.sql "$SQL_SECOND"
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

echo "done"
