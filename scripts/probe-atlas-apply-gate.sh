#!/bin/bash
#
# Differential check of the `ptah-compat migrate apply` integrity gate against
# the pinned Atlas CE binary, for migration directories read in a foreign
# tool's layout through `?format=`.
#
# Atlas CE verifies atlas.sum before it parses the source layout and before it
# opens the target database. It refuses an unhashed directory with
# `checksum file not found` and a tampered one with `checksum mismatch`, naming
# the SOURCE file. This compares both tools row by row on that behavior, and on
# the rows where CE exits 0 — the ones that stop the gate from over-refusing.
#
# Section 9a is the one to read before trusting the gate: for Flyway it asserts
# that what EXECUTES is exactly what atlas.sum COVERS. It used to assert the
# opposite — the importer ran a wider set than the checksum covered, so SQL
# executed that nothing protected on a directory both tools called clean. That
# was stokaro/ptah#982, and it is closed.
#
# The oracle is pinned. A different Atlas build may have changed the very rules
# under test, so the version is checked before anything is compared.
#
#   oracle:  ptah-atlas-conformance/bin/atlas
#   version: atlas community version v1.2.0
#
# A system-wide `atlas` on PATH is frequently a different build (a v1.2.4
# canary at the time of writing), so the oracle is invoked by absolute path.
#
# Usage:
#   scripts/probe-atlas-apply-gate.sh [path-to-atlas]
#   PTAH_ATLAS_ORACLE=/path/to/atlas scripts/probe-atlas-apply-gate.sh
#
# Exits non-zero if any comparison diverges. Scratch directories are created
# under the system temp directory and removed on exit.
#
# Refs stokaro/ptah#973, #976, #980, #982, #984, #992, #994.
set -u

ORACLE_VERSION="atlas community version v1.2.0"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ATLAS="${1:-${PTAH_ATLAS_ORACLE:-$HOME/Work/denis/ptah-atlas-conformance/bin/atlas}}"
COMPAT="$ROOT/bin/ptah-compat"

if [ ! -x "$ATLAS" ]; then
	echo "probe: oracle not found or not executable: $ATLAS" >&2
	echo "probe: pass the path to the pinned Atlas CE binary as \$1" >&2
	exit 1
fi
actual_version="$("$ATLAS" version | head -1)"
if [ "$actual_version" != "$ORACLE_VERSION" ]; then
	echo "probe: oracle version mismatch" >&2
	echo "  want: $ORACLE_VERSION" >&2
	echo "  got:  $actual_version" >&2
	exit 1
fi

if [ ! -x "$COMPAT" ]; then
	echo "probe: building ptah-compat" >&2
	go build -o "$COMPAT" "$ROOT/cmd/ptah-compat" || exit 1
fi

# Section 9a compares the ORDER migrations executed in, which is read back out
# of the sqlite target databases. Without sqlite3 that section would silently
# degrade to comparing counts, which is exactly the weakness it exists to close.
if ! command -v sqlite3 >/dev/null 2>&1; then
	echo "probe: sqlite3 is required (section 9a compares execution order)" >&2
	exit 1
fi

BASE="$(mktemp -d "${TMPDIR:-/tmp}/ptah-apply-gate-probe.XXXXXX")"
trap 'rm -rf "$BASE"' EXIT
cd "$BASE" || exit 1

# A database URL that cannot be reached. It proves the gate precedes the
# connection rather than merely appearing to: CE emits the checksum refusal
# INSTEAD of the connection error.
DEAD_URL='postgres://u:p@127.0.0.1:1/db?sslmode=disable'

fail=0
# open980 counts exempt rows where ptah-compat still exits 1 after the gate
# passed, so the residual divergence has a number rather than a vibe.
open980=0

# seed <dir> <format> — one migration in the named layout.
seed() {
	local d="$1" f="$2"
	rm -rf "$d"
	mkdir -p "$d"
	case "$f" in
	goose) printf -- '-- +goose Up\nCREATE TABLE t1 (id INTEGER PRIMARY KEY);\n-- +goose Down\nDROP TABLE t1;\n' >"$d/1_init.sql" ;;
	dbmate) printf -- '-- migrate:up\nCREATE TABLE t1 (id INTEGER PRIMARY KEY);\n-- migrate:down\nDROP TABLE t1;\n' >"$d/1_init.sql" ;;
	liquibase) printf -- '--liquibase formatted sql\n--changeset app:1\nCREATE TABLE t1 (id INTEGER PRIMARY KEY);\n--rollback DROP TABLE t1;\n' >"$d/1_init.sql" ;;
	flyway) printf -- 'CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' >"$d/V1__init.sql" ;;
	golang-migrate)
		printf -- 'CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' >"$d/1_init.up.sql"
		printf -- 'DROP TABLE t1;\n' >"$d/1_init.down.sql"
		;;
	esac
}

# covered <format> — the source file Atlas CE hashes for that layout.
covered() {
	case "$1" in
	goose | dbmate | liquibase) echo 1_init.sql ;;
	flyway) echo V1__init.sql ;;
	golang-migrate) echo 1_init.up.sql ;;
	esac
}

# signature <output> — the refusal shape, or "-" when there is none. Success
# text differs between the two tools by design (Atlas prints a per-statement
# log, ptah-compat prints its own summary), so only refusals are compared
# verbatim.
signature() {
	echo "$1" | grep -E '^(Error: |	L[0-9]+: |You have a checksum error)' | tr '\n' '|'
}

# compare <label> <ce-dir> <ptah-dir> <format> [extra apply args...]
compare() {
	local label="$1" ce="$2" pt="$3" f="$4"
	shift 4
	local ceout cerc ptout ptrc cesig ptsig cedb ptdb
	ceout=$("$ATLAS" migrate apply --url "sqlite://$ce.db" --dir "file://$ce?format=$f" "$@" 2>&1)
	cerc=$?
	ptout=$("$COMPAT" migrate apply --url "sqlite://$pt.db" --dir "file://$pt?format=$f" "$@" 2>&1)
	ptrc=$?
	cesig=$(signature "$ceout")
	ptsig=$(signature "$ptout")
	cedb=no; [ -f "$ce.db" ] && cedb=yes
	ptdb=no; [ -f "$pt.db" ] && ptdb=yes
	if [ "$cerc" = "$ptrc" ] && [ "$cesig" = "$ptsig" ] && [ "$cedb" = "$ptdb" ]; then
		printf '  %-46s MATCH   exit=%s db=%-3s %s\n' "$label" "$cerc" "$cedb" "${cesig:--}"
	else
		fail=1
		printf '  %-46s DIFFER\n' "$label"
		printf '       CE   exit=%s db=%-3s [%s]\n' "$cerc" "$cedb" "$(echo "$ceout" | tr '\n' '|' | cut -c1-160)"
		printf '       ptah exit=%s db=%-3s [%s]\n' "$ptrc" "$ptdb" "$(echo "$ptout" | tr '\n' '|' | cut -c1-160)"
	fi
}

# hash_both <ce-dir> <ptah-dir> <format> — hash with the ORACLE in both trees,
# so what ptah-compat verifies against is a sum Atlas CE actually wrote.
hash_both() {
	"$ATLAS" migrate hash --dir "file://$1?format=$3" >/dev/null 2>&1
	"$ATLAS" migrate hash --dir "file://$2?format=$3" >/dev/null 2>&1
}

# exempt <label> <ce-dir> <ptah-dir> <format> — a row the gate must NOT refuse.
#
# The assertion is one-sided on purpose. The gate's obligation is that it emits
# no checksum refusal where CE exits 0; what happens after it is a different
# question, and on several of these rows ptah-compat still exits 1 because its
# converter reports "no importable migration files found" where CE reports
# "No migration files to execute" (stokaro/ptah#980). That divergence predates
# this gate and is reached only after the gate passes, so it is reported rather
# than treated as a gate failure — but it is printed on every row it affects,
# because a gate that silently started refusing them would otherwise look the
# same as one that did not.
exempt() {
	local label="$1" ce="$2" pt="$3" f="$4"
	local ceout cerc ptout ptrc note
	ceout=$("$ATLAS" migrate apply --url "sqlite://$ce.db" --dir "file://$ce?format=$f" 2>&1)
	cerc=$?
	ptout=$("$COMPAT" migrate apply --url "sqlite://$pt.db" --dir "file://$pt?format=$f" 2>&1)
	ptrc=$?
	if echo "$ptout" | grep -q 'checksum'; then
		fail=1
		printf '  %-46s OVER-REFUSED by the gate\n' "$label"
		printf '       CE   exit=%s [%s]\n' "$cerc" "$(echo "$ceout" | tr '\n' '|' | cut -c1-160)"
		printf '       ptah exit=%s [%s]\n' "$ptrc" "$(echo "$ptout" | sed "s|$BASE|.|g" | tr '\n' '|' | cut -c1-160)"
		return
	fi
	note="-"
	if [ "$cerc" != "$ptrc" ]; then
		note="#980: ptah $(echo "$ptout" | sed "s|$BASE|.|g" | tr '\n' '|' | cut -c1-72)"
		open980=$((open980 + 1))
	fi
	printf '  %-46s NOT-REFUSED  CE exit=%s ptah exit=%s  %s\n' "$label" "$cerc" "$ptrc" "$note"
}

echo "===== 1. five formats x five integrity states"
for f in goose dbmate liquibase flyway golang-migrate; do
	c=$(covered "$f")
	case "$f" in
	goose | dbmate | liquibase) x=2_extra.sql ;;
	flyway) x=V2__extra.sql ;;
	golang-migrate) x=2_extra.up.sql ;;
	esac
	for state in unhashed clean tampered added removed; do
		ce="ce-$state-$f"; pt="pt-$state-$f"
		seed "$ce" "$f"; seed "$pt" "$f"
		[ "$state" != unhashed ] && hash_both "$ce" "$pt" "$f"
		case "$state" in
		tampered)
			printf -- '\n-- tampered\n' >>"$ce/$c"
			printf -- '\n-- tampered\n' >>"$pt/$c"
			;;
		added)
			for d in "$ce" "$pt"; do
				case "$f" in
				goose) printf -- '-- +goose Up\nCREATE TABLE t2 (id int);\n' >"$d/$x" ;;
				dbmate) printf -- '-- migrate:up\nCREATE TABLE t2 (id int);\n' >"$d/$x" ;;
				liquibase) printf -- '--liquibase formatted sql\n--changeset app:2\nCREATE TABLE t2 (id int);\n' >"$d/$x" ;;
				*) printf -- 'CREATE TABLE t2 (id int);\n' >"$d/$x" ;;
				esac
			done
			;;
		removed)
			rm -f "$ce/$c" "$pt/$c"
			;;
		esac
		compare "$f $state" "$ce" "$pt" "$f"
	done
done

echo
echo "===== 2. negative rows: CE exits 0, so the gate must not refuse"
# An empty covered set is nothing-to-execute for CE, not a checksum error.
# These are the rows that separate "the covered set is non-empty" from the
# plausible alternative "the directory holds any *.sql".
for pair in "golang-migrate:1_init.down.sql" "flyway:U1__undo.sql" "flyway:plain.sql"; do
	f="${pair%%:*}"; name="${pair##*:}"
	ce="ce-empty-$f-$name"; pt="pt-empty-$f-$name"
	rm -rf "$ce" "$pt"; mkdir -p "$ce" "$pt"
	printf -- 'DROP TABLE t1;\n' >"$ce/$name"
	printf -- 'DROP TABLE t1;\n' >"$pt/$name"
	exempt "$f, only $name (empty covered set)" "$ce" "$pt" "$f"
done
for f in goose dbmate liquibase golang-migrate; do
	ce="ce-empty-dir-$f"; pt="pt-empty-dir-$f"
	rm -rf "$ce" "$pt"; mkdir -p "$ce" "$pt"
	exempt "$f, empty directory" "$ce" "$pt" "$f"
	rm -rf "$ce" "$pt"; mkdir -p "$ce" "$pt"
	printf 'migrations live here\n' >"$ce/README.md"
	printf 'migrations live here\n' >"$pt/README.md"
	exempt "$f, no SQL files" "$ce" "$pt" "$f"
done
# The .sql suffix match is case-sensitive on both tools, so an uppercase name
# leaves the covered set empty rather than making it unverifiable.
ce="ce-upper"; pt="pt-upper"
rm -rf "$ce" "$pt"; mkdir -p "$ce" "$pt"
printf -- '-- +goose Up\nCREATE TABLE t1 (id int);\n' >"$ce/1_INIT.SQL"
printf -- '-- +goose Up\nCREATE TABLE t1 (id int);\n' >"$pt/1_INIT.SQL"
exempt "goose, only 1_INIT.SQL" "$ce" "$pt" goose
# An empty covered set that WAS hashed: the sum is the single-line empty-set
# digest, and verifying it must come out clean rather than reading as drift.
for pair in "golang-migrate:1_init.down.sql" "flyway:U1__undo.sql"; do
	f="${pair%%:*}"; name="${pair##*:}"
	ce="ce-hashed-empty-$f"; pt="pt-hashed-empty-$f"
	rm -rf "$ce" "$pt"; mkdir -p "$ce" "$pt"
	printf -- 'DROP TABLE t1;\n' >"$ce/$name"
	printf -- 'DROP TABLE t1;\n' >"$pt/$name"
	hash_both "$ce" "$pt" "$f"
	exempt "$f, empty covered set, hashed" "$ce" "$pt" "$f"
done
# Editing a file the covered set excludes is invisible to CE, so it must stay
# invisible here: the golang-migrate down file, the Flyway undo file, and a
# versioned Flyway file squashed away by a higher baseline.
ce="ce-neg-gm"; pt="pt-neg-gm"
seed "$ce" golang-migrate; seed "$pt" golang-migrate
hash_both "$ce" "$pt" golang-migrate
printf -- '\n-- tampered down\n' >>"$ce/1_init.down.sql"
printf -- '\n-- tampered down\n' >>"$pt/1_init.down.sql"
compare "golang-migrate, down file edited" "$ce" "$pt" golang-migrate
ce="ce-neg-fw"; pt="pt-neg-fw"
for d in "$ce" "$pt"; do
	rm -rf "$d"; mkdir -p "$d"
	printf 'CREATE TABLE t1 (id int);\n' >"$d/V1__init.sql"
	printf 'DROP TABLE t1;\n' >"$d/U1__undo.sql"
done
hash_both "$ce" "$pt" flyway
printf -- '\n-- tampered undo\n' >>"$ce/U1__undo.sql"
printf -- '\n-- tampered undo\n' >>"$pt/U1__undo.sql"
compare "flyway, undo file edited" "$ce" "$pt" flyway
ce="ce-neg-base"; pt="pt-neg-base"
for d in "$ce" "$pt"; do
	rm -rf "$d"; mkdir -p "$d"
	printf 'CREATE TABLE a (id int);\n' >"$d/V1__one.sql"
	printf 'CREATE TABLE b (id int);\n' >"$d/B2__base.sql"
done
hash_both "$ce" "$pt" flyway
printf -- '\n-- tampered squashed\n' >>"$ce/V1__one.sql"
printf -- '\n-- tampered squashed\n' >>"$pt/V1__one.sql"
compare "flyway, baseline-squashed file edited" "$ce" "$pt" flyway

echo
echo "===== 3. subdirectories: shallow for four formats, recursive for flyway"
for f in goose dbmate liquibase golang-migrate; do
	ce="ce-sub-$f"; pt="pt-sub-$f"
	rm -rf "$ce" "$pt"; mkdir -p "$ce/sub" "$pt/sub"
	printf -- '-- +goose Up\nCREATE TABLE n (id int);\n' >"$ce/sub/1_init.sql"
	printf -- '-- +goose Up\nCREATE TABLE n (id int);\n' >"$pt/sub/1_init.sql"
	exempt "$f, migration only in sub/" "$ce" "$pt" "$f"
done
# Flyway is the one layout whose covered set reaches below the top level, so
# the same shape is a checksum refusal there. A capture that stopped at the top
# level would make the verifier hash a smaller set than CE recorded.
ce="ce-sub-flyway"; pt="pt-sub-flyway"
rm -rf "$ce" "$pt"; mkdir -p "$ce/sub" "$pt/sub"
printf 'CREATE TABLE n (id int);\n' >"$ce/sub/V2__nested.sql"
printf 'CREATE TABLE n (id int);\n' >"$pt/sub/V2__nested.sql"
compare "flyway, migration only in sub/ (refused)" "$ce" "$pt" flyway
ce="ce-nested"; pt="pt-nested"
for d in "$ce" "$pt"; do
	rm -rf "$d"; mkdir -p "$d/sub"
	printf 'CREATE TABLE t1 (id int);\n' >"$d/V1__init.sql"
	printf 'CREATE TABLE n (id int);\n' >"$d/sub/V2__nested.sql"
done
hash_both "$ce" "$pt" flyway
compare "flyway, nested file hashed clean" "$ce" "$pt" flyway
printf -- '\n-- tampered\n' >>"$ce/sub/V2__nested.sql"
printf -- '\n-- tampered\n' >>"$pt/sub/V2__nested.sql"
compare "flyway, nested file tampered" "$ce" "$pt" flyway

echo
echo "===== 4. the gate precedes the source-format parse"
ce="ce-nodirective"; pt="pt-nodirective"
rm -rf "$ce" "$pt"; mkdir -p "$ce" "$pt"
printf 'CREATE TABLE nd (id int);\n' >"$ce/1_init.sql"
printf 'CREATE TABLE nd (id int);\n' >"$pt/1_init.sql"
compare "goose, unhashed and unparseable" "$ce" "$pt" goose
ce="ce-unparseable"; pt="pt-unparseable"
seed "$ce" goose; seed "$pt" goose
hash_both "$ce" "$pt" goose
printf 'CREATE TABLE t1 (id int);\n' >"$ce/1_init.sql"
printf 'CREATE TABLE t1 (id int);\n' >"$pt/1_init.sql"
compare "goose, tampered until unparseable" "$ce" "$pt" goose

echo
echo "===== 5. the gate precedes the database connection"
for state in unhashed clean; do
	ce="ce-conn-$state"; pt="pt-conn-$state"
	seed "$ce" goose; seed "$pt" goose
	[ "$state" = clean ] && hash_both "$ce" "$pt" goose
	ceout=$("$ATLAS" migrate apply --url "$DEAD_URL" --dir "file://$ce?format=goose" 2>&1)
	cerc=$?
	ptout=$("$COMPAT" migrate apply --url "$DEAD_URL" --dir "file://$pt?format=goose" 2>&1)
	ptrc=$?
	cegate=no; echo "$ceout" | grep -q 'checksum' && cegate=yes
	ptgate=no; echo "$ptout" | grep -q 'checksum' && ptgate=yes
	if [ "$cerc" = "$ptrc" ] && [ "$cegate" = "$ptgate" ]; then
		printf '  %-46s MATCH   exit=%s checksum-refusal=%s\n' "unreachable --url, $state" "$cerc" "$cegate"
	else
		fail=1
		printf '  %-46s DIFFER\n' "unreachable --url, $state"
		printf '       CE   exit=%s [%s]\n' "$cerc" "$(echo "$ceout" | tr '\n' '|' | cut -c1-160)"
		printf '       ptah exit=%s [%s]\n' "$ptrc" "$(echo "$ptout" | tr '\n' '|' | cut -c1-160)"
	fi
done

echo
echo "===== 6. --dry-run"
for state in unhashed clean; do
	ce="ce-dry-$state"; pt="pt-dry-$state"
	seed "$ce" goose; seed "$pt" goose
	[ "$state" = clean ] && hash_both "$ce" "$pt" goose
	compare "--dry-run, $state" "$ce" "$pt" goose --dry-run
done

echo
echo "===== 7. --env local, atlas.hcl migration { format = goose }"
for state in unhashed clean tampered; do
	for tool in ce ptah; do
		p="$BASE/env-$tool-$state"
		rm -rf "$p"; mkdir -p "$p/migrations"
		printf -- '-- +goose Up\nCREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' >"$p/migrations/1_init.sql"
		cat >"$p/atlas.hcl" <<EOF
env "local" {
  url = "sqlite://env.db"
  migration {
    dir    = "file://migrations"
    format = goose
  }
}
EOF
		[ "$state" != unhashed ] && "$ATLAS" migrate hash --dir "file://$p/migrations?format=goose" >/dev/null 2>&1
		[ "$state" = tampered ] && printf -- '\n-- tampered\n' >>"$p/migrations/1_init.sql"
	done
	cd "$BASE/env-ce-$state" || exit 1
	ceout=$("$ATLAS" migrate apply --env local 2>&1); cerc=$?
	cd "$BASE/env-ptah-$state" || exit 1
	ptout=$("$COMPAT" migrate apply --env local 2>&1); ptrc=$?
	cd "$BASE" || exit 1
	cesig=$(signature "$ceout"); ptsig=$(signature "$ptout")
	if [ "$cerc" = "$ptrc" ] && [ "$cesig" = "$ptsig" ]; then
		printf '  %-46s MATCH   exit=%s %s\n' "--env local, $state" "$cerc" "${cesig:--}"
	else
		fail=1
		printf '  %-46s DIFFER\n' "--env local, $state"
		printf '       CE   exit=%s [%s]\n' "$cerc" "$(echo "$ceout" | tr '\n' '|' | cut -c1-160)"
		printf '       ptah exit=%s [%s]\n' "$ptrc" "$(echo "$ptout" | tr '\n' '|' | cut -c1-160)"
	fi
done

echo
echo "===== 8. the native atlas layout is unmoved"
for state in unhashed clean tampered; do
	ce="ce-native-$state"; pt="pt-native-$state"
	for d in "$ce" "$pt"; do
		rm -rf "$d"; mkdir -p "$d"
		printf 'CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' >"$d/1_init.sql"
	done
	if [ "$state" != unhashed ]; then
		"$ATLAS" migrate hash --dir "file://$ce" >/dev/null 2>&1
		"$ATLAS" migrate hash --dir "file://$pt" >/dev/null 2>&1
	fi
	if [ "$state" = tampered ]; then
		printf -- '\n-- tampered\n' >>"$ce/1_init.sql"
		printf -- '\n-- tampered\n' >>"$pt/1_init.sql"
	fi
	ceout=$("$ATLAS" migrate apply --url "sqlite://$ce.db" --dir "file://$ce" 2>&1); cerc=$?
	ptout=$("$COMPAT" migrate apply --url "sqlite://$pt.db" --dir "file://$pt" 2>&1); ptrc=$?
	cesig=$(signature "$ceout"); ptsig=$(signature "$ptout")
	if [ "$cerc" = "$ptrc" ] && [ "$cesig" = "$ptsig" ]; then
		printf '  %-46s MATCH   exit=%s %s\n' "native atlas, $state" "$cerc" "${cesig:--}"
	else
		fail=1
		printf '  %-46s DIFFER\n' "native atlas, $state"
		printf '       CE   exit=%s [%s]\n' "$cerc" "$(echo "$ceout" | tr '\n' '|' | cut -c1-160)"
		printf '       ptah exit=%s [%s]\n' "$ptrc" "$(echo "$ptout" | tr '\n' '|' | cut -c1-160)"
	fi
done

echo
echo "===== 9a. Flyway: what EXECUTES is what atlas.sum COVERS (stokaro/ptah#982)"
# This section used to assert a DIVERGENCE. Before #982 the importer selected a
# WIDER set than atlas.sum covers, so a file no checksum protected executed on a
# directory both tools called clean, and the gate could not see it. #982 made the
# importer and the hasher share one selection rule, so the section now asserts
# PARITY and fails if either shape stops matching.
#
# The count is compared against the covered set itself, not merely between the
# two tools: a change that made both run the same WRONG number would otherwise
# read as green. atlas.sum carries one directory-hash line plus one line per
# covered file, so the covered count is its line count minus one.
flyway_parity() { # flyway_parity <label> <builder> <slug> [file-to-tamper]
	local label="$1" build="$2" slug="$3" tamper="${4:-}"
	local ce="ce-parity-$slug" pt="pt-parity-$slug"
	rm -rf "$ce" "$pt"; mkdir -p "$ce" "$pt"
	$build "$ce"; $build "$pt"
	"$ATLAS" migrate hash --dir "file://$ce?format=flyway" >/dev/null 2>&1
	"$ATLAS" migrate hash --dir "file://$pt?format=flyway" >/dev/null 2>&1
	local covered
	covered=$(($(wc -l <"$ce/atlas.sum") - 1))
	# When a tamper target is named it lands on a file the oracle's own sum does
	# NOT cover, which is what made both shapes silent: validate stays clean on
	# both tools, so only the executed set can tell the two behaviors apart.
	if [ -n "$tamper" ]; then
		printf 'CREATE TABLE pwned (id int);\n' >>"$ce/$tamper"
		printf 'CREATE TABLE pwned (id int);\n' >>"$pt/$tamper"
	fi
	local cev ptv ceout ptout cerc ptrc cerun ptrun
	cev=$("$ATLAS" migrate validate --dir "file://$ce?format=flyway" >/dev/null 2>&1; echo $?)
	ptv=$("$COMPAT" migrate validate --dir "file://$pt?format=flyway" >/dev/null 2>&1; echo $?)
	ceout=$("$ATLAS" migrate apply --url "sqlite://$ce.db" --dir "file://$ce?format=flyway" 2>&1); cerc=$?
	ptout=$("$COMPAT" migrate apply --url "sqlite://$pt.db" --dir "file://$pt?format=flyway" 2>&1); ptrc=$?
	# What each tool actually executed, in the order it executed it. Comparing
	# COUNTS here would call a mutant that runs the surviving baseline second a
	# MATCH; the tables are created in execution order, so rowid order separates
	# them. The migration descriptions are what both tools have in common — the
	# converted Atlas versions are Ptah's own projection and differ by design.
	cerun=$(migration_order "$ce.db")
	ptrun=$(migration_order "$pt.db")
	local ceran
	ceran=$(printf '%s' "$cerun" | tr ',' '\n' | grep -c .)
	if [ "$cev" = "$ptv" ] && [ "$cerc" = "$ptrc" ] && [ "$cerun" = "$ptrun" ] && [ "$ceran" = "$covered" ]; then
		printf '  %-46s MATCH   validate=%s apply=%s  covered=%s ran=[%s]\n' \
			"$label" "$cev" "$cerc" "$covered" "$cerun"
	else
		fail=1
		printf '  %-46s DIFFER  covered=%s\n' "$label" "$covered"
		printf '       CE   validate=%s apply=%s ran=[%s] [%s]\n' "$cev" "$cerc" "$cerun" "$(echo "$ceout" | tr '\n' '|' | cut -c1-120)"
		printf '       ptah validate=%s apply=%s ran=[%s] [%s]\n' "$ptv" "$ptrc" "$ptrun" "$(echo "$ptout" | tr '\n' '|' | cut -c1-120)"
	fi
}

# migration_order lists the descriptions of the migrations a target database
# recorded, in the order they were applied. Both tools write the Flyway
# description into the revision row, so it is the one identifier that survives
# Ptah's version projection.
migration_order() {
	[ -f "$1" ] || { printf ''; return; }
	sqlite3 "$1" "SELECT group_concat(description, ',') FROM (SELECT description FROM atlas_schema_revisions ORDER BY rowid);" 2>/dev/null
}
build_superseded_baseline() {
	printf 'CREATE TABLE one (id int);\n'   >"$1/B1__one.sql"
	printf 'CREATE TABLE two (id int);\n'   >"$1/B2__two.sql"
	printf 'CREATE TABLE three (id int);\n' >"$1/V3__three.sql"
}
build_lowercase_prefix() {
	printf 'CREATE TABLE init (id int);\n' >"$1/V1__init.sql"
	printf 'CREATE TABLE evil (id int);\n' >"$1/v2__evil.sql"
}
build_repeatable() {
	printf 'CREATE TABLE init (id int);\n' >"$1/V1__init.sql"
	printf 'CREATE VIEW v AS SELECT 1;\n'  >"$1/R__views.sql"
}
build_nested() {
	printf 'CREATE TABLE init (id int);\n'   >"$1/V1__init.sql"
	mkdir -p "$1/sub"
	printf 'CREATE TABLE nested (id int);\n' >"$1/sub/V2__nested.sql"
}
build_baseline_outranks() {
	printf 'CREATE TABLE base (id int);\n' >"$1/B10__base.sql"
	printf 'CREATE TABLE x (id int);\n'    >"$1/V2__x.sql"
}
# The two shapes #982 opened with: the tampered file is OUTSIDE the covered set,
# so both tools must leave it unexecuted.
flyway_parity "superseded baseline B1 stays unexecuted" build_superseded_baseline base B1__one.sql
flyway_parity "lowercase v2__ prefix stays unexecuted" build_lowercase_prefix lower v2__evil.sql
# The loud half of #982, an over-refusal rather than a silent execution: CE
# hashes AND executes a repeatable, and the importer used to refuse it.
flyway_parity "repeatable is covered and executes" build_repeatable repeat
# Flyway is the one layout whose covered set reaches below the top level, and
# the importer used to read only the top level.
flyway_parity "nested migration is covered and executes" build_nested nested
# A surviving baseline executes FIRST even when its version outranks the
# survivor numerically, which is what the converted version bands carry.
flyway_parity "baseline outranking its survivor executes" build_baseline_outranks band

echo
echo "===== 9b. a DIRECTORY named *.sql appears after hashing (stokaro/ptah#991)"
# CE's shallow glob matches the directory and then hard-errors; SumFileNames
# skips it, so the recomputed sum still verifies. Nothing unverified executes —
# a directory holds no SQL — so this is loss of tamper DETECTION, not of
# execution safety. It is still CE refusing where we apply.
ce="ce-evildir"; pt="pt-evildir"
seed "$ce" goose; seed "$pt" goose
hash_both "$ce" "$pt" goose
mkdir -p "$ce/2_evil.sql" "$pt/2_evil.sql"
ceout=$("$ATLAS" migrate apply --url "sqlite://$ce.db" --dir "file://$ce?format=goose" 2>&1); cerc=$?
ptout=$("$COMPAT" migrate apply --url "sqlite://$pt.db" --dir "file://$pt?format=goose" 2>&1); ptrc=$?
printf '  %-46s ce exit=%s | ptah exit=%s [%s]\n' "goose + 2_evil.sql/ after hashing" "$cerc" "$ptrc" \
	"$(echo "$ptout" | tr '\n' '|' | cut -c1-70)"

echo
echo "===== 9c. an unreadable source file (dangling symlink under sub/)"
# Both refuse; only the wording differs. Ptah reports the capture failure where
# CE reports its checksum block, because Ptah cannot read the file at capture
# time and CE cannot read it at hash time. Safe direction, pinned so a future
# Atlas-shaped message is a visible change.
ce="ce-symlink"; pt="pt-symlink"
for d in "$ce" "$pt"; do
	rm -rf "$d"; mkdir -p "$d/sub"
	printf 'CREATE TABLE a (id int);\n' >"$d/V1__init.sql"
	ln -s /nonexistent/target "$d/sub/V2__dangling.sql"
done
ceout=$("$ATLAS" migrate apply --url "sqlite://$ce.db" --dir "file://$ce?format=flyway" 2>&1); cerc=$?
ptout=$("$COMPAT" migrate apply --url "sqlite://$pt.db" --dir "file://$pt?format=flyway" 2>&1); ptrc=$?
printf '  %-46s ce exit=%s | ptah exit=%s [%s]\n' "flyway, dangling nested symlink" "$cerc" "$ptrc" \
	"$(echo "$ptout" | sed "s|$BASE|.|g" | tr '\n' '|' | cut -c1-70)"
if [ "$cerc" != "$ptrc" ]; then
	fail=1
	echo "       UNEXPECTED: the two tools no longer agree on the exit code"
fi

echo
echo "===== 9. divergences the gate passes through (stokaro/ptah#980, #981)"
# Both are exit-1 refusals where CE exits 0, both predate this change, and both
# are reached only AFTER the gate passes. The second is why #980 is not fixed
# here: its covered set is NOT empty and Atlas CE really does execute foo.sql
# (as version "foo"), so reporting "No migration files to execute" there would
# replace a loud refusal with a silent no-op. A correct #980 fix has to tell
# "the covered set is empty" apart from "the covered set is non-empty and Ptah's
# importer refused it", which is a decision for that issue.
#
# A third row lived here until #982: a hashed Flyway R__ file, which CE executes
# and Ptah refused to import. It is now parity and moved to section 9a.
for spec in "golang-migrate:1_init.down.sql:down-only, empty covered set" "goose:foo.sql:foo.sql hashed, non-empty covered set"; do
	f="${spec%%:*}"; rest="${spec#*:}"; name="${rest%%:*}"; label="${rest##*:}"
	ce="ce-open-$f-$name"; pt="pt-open-$f-$name"
	rm -rf "$ce" "$pt"; mkdir -p "$ce" "$pt"
	printf 'CREATE TABLE t1 (id int);\n' >"$ce/$name"
	printf 'CREATE TABLE t1 (id int);\n' >"$pt/$name"
	[ "$name" = foo.sql ] && hash_both "$ce" "$pt" "$f"
	ceout=$("$ATLAS" migrate apply --url "sqlite://$ce.db" --dir "file://$ce?format=$f" 2>&1); cerc=$?
	ptout=$("$COMPAT" migrate apply --url "sqlite://$pt.db" --dir "file://$pt?format=$f" 2>&1); ptrc=$?
	printf '  %-46s CE exit=%s | ptah exit=%s [%s]\n' "$f, $label" "$cerc" "$ptrc" \
		"$(echo "$ptout" | sed "s|$BASE|.|g" | tr '\n' '|' | cut -c1-100)"
	if echo "$ptout" | grep -q 'checksum'; then
		fail=1
		echo "       UNEXPECTED: the gate refused a row it must exempt"
	fi
done

echo
if [ "$fail" -ne 0 ]; then
	echo "probe: FAILED"
	exit 1
fi
echo "probe: every gated row matches Atlas CE."
echo "       $open980 exempt rows still exit 1 after the gate passes (stokaro/ptah#980)."
echo "       Flyway executes exactly its covered set (section 9a), so for all five"
echo "       layouts everything apply executes was covered by the checksum it verified."
