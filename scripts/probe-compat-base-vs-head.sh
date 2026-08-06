#!/bin/bash
#
# Base-vs-head sweep for a change that claims a surface is UNCHANGED.
#
# "I only added a branch, the old path is untouched" is a claim about behavior,
# and reading a diff does not establish it — a shared helper, a capture rule or
# a resolver can move underneath an untouched-looking call site. This runs two
# ptah-compat binaries over the same inputs and compares exit code, stdout,
# stderr and the resulting directory tree byte for byte.
#
# Sections:
#   A. native atlas `migrate apply` (no ?format=) — the surface stokaro/ptah#973
#      claims to leave alone.
#   B. `migrate import` — shares atlasmigrateimport.CaptureFS, which #973
#      changed, so "import is unaffected" is a claim needing the same evidence.
#   C. native `migrate hash` / `validate` / `status`.
#
# WHAT SECTION B CAN AND CANNOT SEE. Loaders skip inputs they do not recognize,
# so `migrate import` detects a capture that NARROWED and is blind to one that
# WIDENED — and #973's capture change is a widening. A mutation that stops
# capturing atlas.sum entirely, undoing half of that change, leaves this sweep
# at 57/57 SAME while scripts/probe-atlas-apply-gate.sh reddens 30 rows. Section
# B bounds the blast radius of the change on import; it does not bound the
# change itself. Sections A and C are two-sided: their commands read what the
# capture produces, so both directions surface there.
#
# Usage:
#   go build -o bin/ptah-compat-base ./cmd/ptah-compat   # from the base commit
#   go build -o bin/ptah-compat-head ./cmd/ptah-compat   # from the head commit
#   scripts/probe-compat-base-vs-head.sh
#
#   PTAH_COMPAT_BASE=/path/to/base PTAH_COMPAT_HEAD=/path/to/head \
#     scripts/probe-compat-base-vs-head.sh
#
# No Atlas oracle is involved: this compares Ptah against Ptah. Parity with
# Atlas CE is scripts/probe-atlas-apply-gate.sh's job.
#
# Exits non-zero if any row differs. Scratch directories are created under the
# system temp directory and removed on exit.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${PTAH_COMPAT_BASE:-$ROOT/bin/ptah-compat-base}"
HEAD="${PTAH_COMPAT_HEAD:-$ROOT/bin/ptah-compat-head}"
for bin in "$BASE" "$HEAD"; do
	if [ ! -x "$bin" ]; then
		echo "probe: not found or not executable: $bin" >&2
		echo "probe: build both binaries first (see the usage note in this file)" >&2
		exit 1
	fi
done
# Resolved AFTER the check and BEFORE the cd below: a relative override would
# otherwise pass the check and then make every row exit 127 from the scratch
# directory, which reads as 57 matching failures rather than as a bad path.
BASE="$(cd "$(dirname "$BASE")" && pwd)/$(basename "$BASE")"
HEAD="$(cd "$(dirname "$HEAD")" && pwd)/$(basename "$HEAD")"
if [ "$(shasum -a 256 "$BASE" | cut -d" " -f1)" = "$(shasum -a 256 "$HEAD" | cut -d" " -f1)" ]; then
	echo "probe: base and head are the same binary; every row would trivially match" >&2
	exit 1
fi
W="$(mktemp -d "${TMPDIR:-/tmp}/ptah-base-head-probe.XXXXXX")"
trap 'rm -rf "$W"' EXIT
cd "$W" || exit 1

DEAD='postgres://u:p@127.0.0.1:1/db?sslmode=disable'
fail=0
rows=0

# fixture <dir> <shape> — build one source tree.
fixture() {
	local d="$1" shape="$2"
	rm -rf "$d"; mkdir -p "$d"
	case "$shape" in
	plain|unhashed|clean|tampered|added|removed|handedited|malformed|bothsums|dryrun|conn|format|amount)
		printf 'CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' >"$d/1_init.sql" ;;
	empty) : ;;
	nosql) printf 'hi\n' >"$d/README.md"; printf '' >"$d/.gitkeep" ;;
	subonly) mkdir -p "$d/sub"; printf 'CREATE TABLE n (id int);\n' >"$d/sub/1_init.sql" ;;
	foo) printf 'CREATE TABLE foo (id int);\n' >"$d/foo.sql" ;;
	nested)
		printf 'CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' >"$d/1_init.sql"
		mkdir -p "$d/sub"; printf 'CREATE TABLE n (id int);\n' >"$d/sub/2_more.sql" ;;
	gosibling)
		printf 'CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' >"$d/1_init.sql"
		printf 'package migrations\n' >"$d/2_seed.go" ;;
	weirddir)
		printf 'CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' >"$d/1_init.sql"
		mkdir -p "$d/weird.sql" ;;
	checkpoint)
		printf 'CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' >"$d/1_init.sql"
		printf -- '-- atlas:checkpoint\n\nCREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' >"$d/2_cp.sql" ;;
	goose) printf -- '-- +goose Up\nCREATE TABLE t1 (id int);\n-- +goose Down\nDROP TABLE t1;\n' >"$d/1_init.sql" ;;
	goosesum)
		printf -- '-- +goose Up\nCREATE TABLE t1 (id int);\n' >"$d/1_init.sql"
		printf 'h1:stale=\n1_init.sql h1:stale=\n' >"$d/atlas.sum" ;;
	flywaynested)
		printf 'CREATE TABLE t1 (id int);\n' >"$d/V1__init.sql"
		mkdir -p "$d/sub"; printf 'CREATE TABLE n (id int);\n' >"$d/sub/V2__nested.sql" ;;
	gmpair)
		printf 'CREATE TABLE t1 (id int);\n' >"$d/1_init.up.sql"
		printf 'DROP TABLE t1;\n' >"$d/1_init.down.sql" ;;
	liquibasexml)
		printf -- '--liquibase formatted sql\n--changeset a:1\nCREATE TABLE t1 (id int);\n' >"$d/1_init.sql"
		printf -- '<databaseChangeLog></databaseChangeLog>\n' >"$d/changelog.xml" ;;
	esac
}

# postfix <dir> <shape> <bin> — apply the post-hash mutation, hashing with <bin>.
postfix() {
	local d="$1" shape="$2" bin="$3"
	case "$shape" in
	clean|tampered|added|removed|handedited|bothsums|nested|gosibling|weirddir|checkpoint|conn|format|amount|dryrun)
		"$bin" migrate hash --dir "file://$d" >/dev/null 2>&1 ;;
	malformed) printf 'not a sum file\n' >"$d/atlas.sum" ;;
	esac
	case "$shape" in
	tampered) printf -- '\n-- tampered\n' >>"$d/1_init.sql" ;;
	added) printf 'CREATE TABLE t2 (id int);\n' >"$d/2_extra.sql" ;;
	removed) rm -f "$d/1_init.sql" ;;
	handedited) python3 -c "
import sys
p=sys.argv[1]
l=open(p).read().splitlines()
l[0]='h1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='
open(p,'w').write('\n'.join(l)+'\n')
" "$d/atlas.sum" ;;
	bothsums) "$bin" migrations hash --dir "$d" --dir-format ptah >/dev/null 2>&1
		# The native binary is the same file; ptah.sum has no compat verb.
		;;
	esac
}

# normalize_run_output — read stdin, write the comparison form on stdout.
#
# --format reports embed wall-clock Start/End stamps and elapsed durations. Base
# compared against BASE differs in exactly those fields, so they are normalized
# rather than treated as a change. Nothing else is normalized.
#
# The duration rule used to end in `\b`, which is not portable: GNU sed reads it
# as a word boundary and the BSD sed macOS ships reads it as the literal letter
# `b`. Measured on the input lines `took 12.5ms to run` and `took 12.5msb to
# run`, with `s/[0-9.]+(ms|ns|s)\b/DUR/g`:
#
#   macOS sed  -> rewrites only `12.5msb`
#   GNU sed    -> rewrites only `12.5ms `
#
# Exactly inverted, so on macOS no duration was normalized and every --format
# row compared two different elapsed times and reported a spurious CHANGED. The
# boundary is now spelled with an explicit character class plus an end-of-line
# case, which both engines agree on. See stokaro/ptah#1139.
normalize_run_output() {
	sed -E \
		-e 's/[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+(\+[0-9:]+|Z)/TS/g' \
		-e 's/[0-9.]+(ms|µs|ns|s)([^a-zA-Z0-9_])/DUR\2/g' \
		-e 's/[0-9.]+(ms|µs|ns|s)$/DUR/'
}

# run2 <label> <shape> <argtemplate...> — build the fixture twice, run both
# binaries, compare exit + stdout + stderr + resulting tree.
# The literal DIR in the argument template is replaced with the tree path.
run2() {
	local label="$1" shape="$2"; shift 2
	local slug b h
	slug=$(echo "$label" | tr -c 'a-zA-Z0-9' '-')
	b="b-$slug"
	h="h-$slug"
	fixture "$b" "$shape"; postfix "$b" "$shape" "$BASE"
	fixture "$h" "$shape"; postfix "$h" "$shape" "$HEAD"
	# A hashing step that silently failed would collapse every "hashed" row onto
	# the unhashed one and compare nothing. Assert the file exists where the
	# shape says it should.
	case "$shape" in
	clean|tampered|added|removed|handedited|bothsums|nested|gosibling|checkpoint|conn|format|amount|dryrun|malformed)
		if [ ! -f "$b/atlas.sum" ] || [ ! -f "$h/atlas.sum" ]; then
			fail=1
			printf '  %-52s FIXTURE-BROKEN (no atlas.sum written)\n' "$label"
			return
		fi ;;
	esac

	local bargs=() hargs=()
	for a in "$@"; do bargs+=("${a//DIR/$b}"); hargs+=("${a//DIR/$h}"); done

	local bout brc hout hrc
	bout=$("$BASE" "${bargs[@]}" 2>&1); brc=$?
	hout=$("$HEAD" "${hargs[@]}" 2>&1); hrc=$?
	# Normalize the directory name out of both outputs so only real differences
	# survive the comparison.
	bout=${bout//$b/DIR}; hout=${hout//$h/DIR}
	bout=$(printf '%s' "$bout" | normalize_run_output)
	hout=$(printf '%s' "$hout" | normalize_run_output)
	local btree htree
	btree=$(cd "$b" 2>/dev/null && find . -type f | sort | while read -r f; do printf '%s %s\n' "$f" "$(shasum -a 256 "$f" | cut -c1-16)"; done)
	htree=$(cd "$h" 2>/dev/null && find . -type f | sort | while read -r f; do printf '%s %s\n' "$f" "$(shasum -a 256 "$f" | cut -c1-16)"; done)

	rows=$((rows + 1))
	if [ "$brc" = "$hrc" ] && [ "$bout" = "$hout" ] && [ "$btree" = "$htree" ]; then
		printf '  %-52s SAME  exit=%s %s\n' "$label" "$brc" "$(echo "$bout" | tr '\n' '|' | cut -c1-64)"
	else
		fail=1
		printf '  %-52s CHANGED\n' "$label"
		printf '       base exit=%s [%s]\n' "$brc" "$(echo "$bout" | tr '\n' '|' | cut -c1-200)"
		printf '       head exit=%s [%s]\n' "$hrc" "$(echo "$hout" | tr '\n' '|' | cut -c1-200)"
		[ "$btree" != "$htree" ] && { echo "       base tree: $(echo "$btree" | tr '\n' ' ')"; echo "       head tree: $(echo "$htree" | tr '\n' ' ')"; }
	fi
}

echo "===== A. native atlas migrate apply (no ?format=)"
for shape in unhashed clean tampered added removed empty nosql subonly foo malformed handedited bothsums nested gosibling weirddir checkpoint; do
	run2 "apply $shape" "$shape" migrate apply --url "sqlite://DIR.db" --dir "file://DIR"
done
run2 "apply dry-run unhashed" plain    migrate apply --url "sqlite://DIR.db" --dir "file://DIR" --dry-run
run2 "apply dry-run clean"    dryrun   migrate apply --url "sqlite://DIR.db" --dir "file://DIR" --dry-run
run2 "apply unreachable url unhashed" plain migrate apply --url "$DEAD" --dir "file://DIR"
run2 "apply unreachable url clean"    conn  migrate apply --url "$DEAD" --dir "file://DIR"
run2 "apply --format unhashed" plain  migrate apply --url "sqlite://DIR.db" --dir "file://DIR" --format '{{ json . }}'
run2 "apply --format clean"    format migrate apply --url "sqlite://DIR.db" --dir "file://DIR" --format '{{ json . }}'
run2 "apply amount 1"          amount migrate apply 1 --url "sqlite://DIR.db" --dir "file://DIR"
run2 "apply ?format=atlas unhashed" plain migrate apply --url "sqlite://DIR.db" --dir "file://DIR?format=atlas"
run2 "apply ?format=atlas clean"    clean migrate apply --url "sqlite://DIR.db" --dir "file://DIR?format=atlas"
run2 "apply ?format= (empty) unhashed" plain migrate apply --url "sqlite://DIR.db" --dir "file://DIR?format="
run2 "apply missing directory"  empty migrate apply --url "sqlite://DIR.db" --dir "file://DIR/absent"
run2 "apply unknown format"     plain migrate apply --url "sqlite://DIR.db" --dir "file://DIR?format=sqitch"

echo
echo "===== B. migrate import (shares the changed CaptureFS)"
for shape in goose goosesum flywaynested gmpair liquibasexml nested subonly empty nosql weirddir; do
	case "$shape" in
	flywaynested) f=flyway ;;
	gmpair) f=golang-migrate ;;
	liquibasexml) f=liquibase ;;
	*) f=goose ;;
	esac
	run2 "import $shape as $f" "$shape" migrate import --from "file://DIR" --to "file://DIR-out" --dir-format "$f"
done

echo
echo "===== C. native migrate hash / validate / status"
for shape in unhashed clean tampered empty nosql subonly nested weirddir gosibling; do
	run2 "hash $shape"     "$shape" migrate hash --dir "file://DIR"
	run2 "validate $shape" "$shape" migrate validate --dir "file://DIR"
done
run2 "status clean" clean migrate status --url "sqlite://DIR.db" --dir "file://DIR"

echo
echo "$rows rows compared"
if [ "$fail" -ne 0 ]; then echo "SWEEP FAILED"; exit 1; fi
echo "SWEEP: base and head are byte-identical on every row"
