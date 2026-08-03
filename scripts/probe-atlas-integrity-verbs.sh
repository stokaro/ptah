#!/bin/bash
#
# Differential check of `ptah-compat migrate hash` and `migrate validate`
# against the pinned Atlas CE binary.
#
# For every source layout and both spellings Atlas accepts, the two tools must
# write a byte-identical atlas.sum and agree on validate's exit code and both
# output streams. It also re-measures the divergences this surface documents
# rather than fixes, so a future change that quietly closes or widens one shows
# up here.
#
# The oracle is pinned. A different Atlas build may have changed the very rules
# under test, so the version is checked before anything is compared.
#
#   oracle:  ptah-atlas-conformance/bin/atlas
#   version: atlas community version v1.3.0
#
# A system-wide `atlas` on PATH is frequently a different build, so the
# oracle is invoked by absolute path.
#
# Usage:
#   scripts/probe-atlas-integrity-verbs.sh [path-to-atlas]
#   PTAH_ATLAS_ORACLE=/path/to/atlas scripts/probe-atlas-integrity-verbs.sh
#
# Exits non-zero if any comparison diverges. Scratch directories are created
# under the system temp directory and removed on exit.
#
# Refs stokaro/ptah#973, #974, #983, #990, #991, #1095.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ATLAS="${1:-${PTAH_ATLAS_ORACLE:-$HOME/Work/denis/ptah-atlas-conformance/bin/atlas}}"
COMPAT="$ROOT/bin/ptah-compat"

# shellcheck source=scripts/lib/atlas-ce-oracle.sh
source "$ROOT/scripts/lib/atlas-ce-oracle.sh"
atlas_ce_load_lock "$ROOT/scripts/atlas-ce-oracle.lock"

if [ ! -x "$ATLAS" ]; then
	echo "probe: oracle not found or not executable: $ATLAS" >&2
	echo "probe: pass the path to the pinned Atlas CE binary as \$1" >&2
	exit 1
fi
atlas_ce_verify_binary "$ATLAS" >/dev/null || exit 1

if [ ! -x "$COMPAT" ]; then
	echo "probe: building ptah-compat" >&2
	go build -o "$COMPAT" "$ROOT/cmd/ptah-compat" || exit 1
fi

BASE="$(mktemp -d "${TMPDIR:-/tmp}/ptah-integrity-probe.XXXXXX")"
trap 'rm -rf "$BASE"' EXIT

fail=0

# seed <dir> — a layout every format reads differently.
seed() {
  local d="$BASE/$1"
  rm -rf "$d"; mkdir -p "$d/sub"
  printf -- '-- +goose Up\nCREATE TABLE a (id int);\n'      > "$d/1_init.sql"
  printf -- '-- +goose Up\nCREATE TABLE b (id int);\n'      > "$d/2_more.up.sql"
  printf -- 'DROP TABLE b;\n'                               > "$d/2_more.down.sql"
  printf -- 'CREATE TABLE v (id int);\n'                    > "$d/V1__x.sql"
  printf -- 'CREATE TABLE v2 (id int);\n'                   > "$d/V10__y.sql"
  printf -- 'DROP TABLE v;\n'                               > "$d/U1__undo.sql"
  printf -- 'CREATE VIEW r AS SELECT 1;\n'                  > "$d/R__view.sql"
  printf -- 'CREATE TABLE base (id int);\n'                 > "$d/B0__base.sql"
  printf -- 'CREATE TABLE nested (id int);\n'               > "$d/sub/V2__nested.sql"
  printf -- 'not sql at all\n'                              > "$d/notes.txt"
  echo "$d"
}

check_sum() { # check_sum <label> <format> <spelling>
  local label="$1" format="$2" spelling="$3"
  local ce ptah
  ce=$(seed "ce-$label"); ptah=$(seed "ptah-$label")
  if [ "$spelling" = query ]; then
    "$ATLAS"  migrate hash --dir "file://$ce?format=$format"   >/dev/null 2>&1
    "$COMPAT" migrate hash --dir "file://$ptah?format=$format" >/dev/null 2>&1
  else
    "$ATLAS"  migrate hash --dir "file://$ce"   --dir-format "$format" >/dev/null 2>&1
    "$COMPAT" migrate hash --dir "file://$ptah" --dir-format "$format" >/dev/null 2>&1
  fi
  if diff -q "$ce/atlas.sum" "$ptah/atlas.sum" >/dev/null 2>&1; then
    printf '  %-28s %-6s IDENTICAL  %s\n' "$format" "$spelling" "$(head -1 "$ce/atlas.sum")"
  else
    fail=1
    printf '  %-28s %-6s DIFFERENT\n' "$format" "$spelling"
    diff "$ce/atlas.sum" "$ptah/atlas.sum" 2>&1 | sed 's/^/       /'
  fi
}

echo "===== 1. hash: byte-identical atlas.sum, every format, both spellings"
for format in atlas goose dbmate liquibase flyway golang-migrate; do
  check_sum "$format-q" "$format" query
  check_sum "$format-f" "$format" flag
done

echo
echo "===== 2. hash: the two spellings agree with each other on ptah-compat"
for format in goose flyway golang-migrate; do
  q=$(seed "pq-$format"); f=$(seed "pf-$format")
  "$COMPAT" migrate hash --dir "file://$q?format=$format" >/dev/null 2>&1
  "$COMPAT" migrate hash --dir "file://$f" --dir-format "$format" >/dev/null 2>&1
  diff -q "$q/atlas.sum" "$f/atlas.sum" >/dev/null 2>&1 \
    && printf '  %-16s IDENTICAL\n' "$format" \
    || { fail=1; printf '  %-16s DIFFERENT\n' "$format"; }
done

echo
echo "===== 3. validate: unhashed / clean / tampered, CE vs ptah-compat"
for format in goose flyway golang-migrate dbmate liquibase; do
  for state in unhashed clean tampered; do
    ce=$(seed "vce-$format-$state"); ptah=$(seed "vpt-$format-$state")
    if [ "$state" != unhashed ]; then
      "$ATLAS" migrate hash --dir "file://$ce?format=$format"   >/dev/null 2>&1
      "$ATLAS" migrate hash --dir "file://$ptah?format=$format" >/dev/null 2>&1
    fi
    if [ "$state" = tampered ]; then
      printf -- '-- +goose Up\nCREATE TABLE pwned (id int);\n' > "$ce/1_init.sql"
      printf -- 'CREATE TABLE pwnedv (id int);\n'              > "$ce/V1__x.sql"
      printf -- '-- +goose Up\nCREATE TABLE pwned (id int);\n' > "$ptah/1_init.sql"
      printf -- 'CREATE TABLE pwnedv (id int);\n'              > "$ptah/V1__x.sql"
    fi
    ceout=$("$ATLAS"  migrate validate --dir "file://$ce?format=$format" 2>&1);   cerc=$?
    ptout=$("$COMPAT" migrate validate --dir "file://$ptah?format=$format" 2>&1); ptrc=$?
    if [ "$cerc" = "$ptrc" ] && [ "$ceout" = "$ptout" ]; then
      printf '  %-16s %-9s MATCH   exit=%s %s\n' "$format" "$state" "$cerc" "$(echo "$ceout" | tr '\n' '|' | cut -c1-70)"
    else
      fail=1
      printf '  %-16s %-9s DIFFER\n' "$format" "$state"
      printf '       CE  (%s): %s\n' "$cerc" "$(echo "$ceout" | tr '\n' '|')"
      printf '       ptah(%s): %s\n' "$ptrc" "$(echo "$ptout" | tr '\n' '|')"
    fi
  done
done

echo
echo "===== 4. the atlas layout is unmoved: query, flag and plain agree"
p=$(seed "atlas-plain"); q=$(seed "atlas-query"); f=$(seed "atlas-flag")
"$COMPAT" migrate hash --dir "file://$p" > "$BASE/plain.out" 2>&1
"$COMPAT" migrate hash --dir "file://$q?format=atlas" > "$BASE/query.out" 2>&1
"$COMPAT" migrate hash --dir "file://$f" --dir-format atlas > "$BASE/flag.out" 2>&1
diff -q "$p/atlas.sum" "$q/atlas.sum" >/dev/null && diff -q "$p/atlas.sum" "$f/atlas.sum" >/dev/null \
  && echo "  sums IDENTICAL" || { fail=1; echo "  sums DIFFERENT"; }
echo "  plain stdout: $(sed "s|$p|DIR|" "$BASE/plain.out" | tr '\n' '|')"
echo "  query stdout: $(sed "s|$q|DIR|" "$BASE/query.out" | tr '\n' '|')"
echo "  flag  stdout: $(sed "s|$f|DIR|" "$BASE/flag.out"  | tr '\n' '|')"

echo
echo "===== 5. precedence: the query wins over the flag, both directions"
a=$(seed "prec-a"); b=$(seed "prec-b")
"$COMPAT" migrate hash --dir "file://$a?format=goose"  --dir-format flyway >/dev/null 2>&1
"$COMPAT" migrate hash --dir "file://$b?format=flyway" --dir-format goose  >/dev/null 2>&1
ce_a=$(seed "prec-ce-a"); ce_b=$(seed "prec-ce-b")
"$ATLAS" migrate hash --dir "file://$ce_a?format=goose"  --dir-format flyway >/dev/null 2>&1
"$ATLAS" migrate hash --dir "file://$ce_b?format=flyway" --dir-format goose  >/dev/null 2>&1
diff -q "$a/atlas.sum" "$ce_a/atlas.sum" >/dev/null && echo "  goose-query + flyway-flag  MATCHES CE" || { fail=1; echo "  goose-query + flyway-flag  DIFFERS"; }
diff -q "$b/atlas.sum" "$ce_b/atlas.sum" >/dev/null && echo "  flyway-query + goose-flag  MATCHES CE" || { fail=1; echo "  flyway-query + goose-flag  DIFFERS"; }
echo "  goose  entries: $(sed -n '2,$p' "$a/atlas.sum" | awk '{printf "%s ", $1}')"
echo "  flyway entries: $(sed -n '2,$p' "$b/atlas.sum" | awk '{printf "%s ", $1}')"

echo
echo "===== 6. --env: atlas.hcl migration.format reaches both verbs"
mkdir -p "$BASE/env-project"
seed "env-project/migrations" >/dev/null
cat > "$BASE/env-project/atlas.hcl" <<EOF
env "local" {
  migration {
    dir    = "file://migrations"
    format = flyway
  }
}
EOF
cd "$BASE/env-project" || exit 1
out=$("$COMPAT" migrate hash --env local 2>&1); rc=$?
printf '  hash --env local            exit=%s out=[%s]\n' "$rc" "$(echo "$out" | sed "s|$BASE/env-project|.|" | tr '\n' '|')"
echo "  entries: $(sed -n '2,$p' migrations/atlas.sum | awk '{printf "%s ", $1}')"
ceenv=$(seed "env-ce")
"$ATLAS" migrate hash --dir "file://$ceenv?format=flyway" >/dev/null 2>&1
diff <(sed -n '1p' migrations/atlas.sum) <(sed -n '1p' "$ceenv/atlas.sum") >/dev/null \
  && echo "  dir hash MATCHES CE flyway" || { fail=1; echo "  dir hash DIFFERS from CE flyway"; }
out=$("$COMPAT" migrate validate --env local 2>&1); rc=$?
printf '  validate --env local        exit=%s out=[%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$COMPAT" migrate hash --env local --dir-format goose 2>&1); rc=$?
printf '  hash --env local --dir-format goose exit=%s\n' "$rc"
echo "  entries: $(sed -n '2,$p' migrations/atlas.sum | awk '{printf "%s ", $1}')"
cd "$BASE" || exit 1

echo
echo "===== 7. refusals: unknown format, case, extra query keys, stray args"
# Every row here is a documented divergence (stokaro/ptah#990), so both tools
# are printed: a future change that quietly closes or widens one is visible.
d=$(seed refuse)
for spec in "--dir=file://$d?format=sqitch" "--dir=file://$d?format=GOOSE" "--dir=file://$d?format=goose&other=1" "--dir=file://$d?format=goose&format=flyway" "--dir=file://$d?format=flyway;x=1"; do
  out=$("$COMPAT" migrate hash "$spec" 2>&1); rc=$?
  ceout=$("$ATLAS" migrate hash "$spec" 2>&1); cerc=$?
  printf '  %-42s ptah exit=%s [%s]\n' "${spec#--dir=file://$d}" "$rc" "$(echo "$out" | tr '\n' '|')"
  printf '  %-42s CE   exit=%s [%s]\n' "" "$cerc" "$(echo "$ceout" | tr '\n' '|')"
done
out=$("$COMPAT" migrate hash --dir "file://$d" --dir-format GOOSE 2>&1); rc=$?
printf '  --dir-format GOOSE                         ptah exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$COMPAT" migrate hash --dir "file://$d" --dir-format goose stray 2>&1); rc=$?
printf '  stray positional                           ptah exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$ATLAS" migrate hash --dir "file://$d" --dir-format goose stray 2>&1); rc=$?
printf '  stray positional                           CE   exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$COMPAT" migrate hash --dir "file://$d" --dir-format goose --bogus 2>&1); rc=$?
printf '  unknown flag                               ptah exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$ATLAS" migrate hash --dir "file://$d" --dir-format goose --bogus 2>&1); rc=$?
printf '  unknown flag                               CE   exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
e1=$(seed term-named); e2=$(seed term-other)
rm -f "$e1/atlas.sum" "$e2/atlas.sum"
out=$("$COMPAT" migrate hash --dir "file://$e1?format=atlas" -- --dir "file://$e2" 2>&1); rc=$?
printf '  -- terminator                              ptah exit=%s [%s] named=%s other=%s\n' \
  "$rc" "$(echo "$out" | tr '\n' '|')" \
  "$([ -f "$e1/atlas.sum" ] && echo hashed || echo none)" \
  "$([ -f "$e2/atlas.sum" ] && echo HASHED-WRONG-DIR || echo none)"
rm -f "$e1/atlas.sum" "$e2/atlas.sum"
out=$("$ATLAS" migrate hash --dir "file://$e1?format=atlas" -- --dir "file://$e2" 2>&1); rc=$?
printf '  -- terminator                              CE   exit=%s [%s] named=%s other=%s\n' \
  "$rc" "$(echo "$out" | tr '\n' '|')" \
  "$([ -f "$e1/atlas.sum" ] && echo hashed || echo none)" \
  "$([ -f "$e2/atlas.sum" ] && echo HASHED-WRONG-DIR || echo none)"
if [ -f "$e2/atlas.sum" ]; then fail=1; echo "  FAIL: hashed a directory that was never named"; fi

echo
echo "===== 8. missing directory and empty directory"
out=$("$COMPAT" migrate hash --dir "file://$BASE/nope?format=goose" 2>&1); rc=$?
printf '  ptah missing dir  exit=%s [%s]\n' "$rc" "$(echo "$out" | sed "s|$BASE|B|" | tr '\n' '|')"
out=$("$ATLAS" migrate hash --dir "file://$BASE/nope?format=goose" 2>&1); rc=$?
printf '  CE   missing dir  exit=%s [%s]\n' "$rc" "$(echo "$out" | sed "s|$BASE|B|" | tr '\n' '|')"
mkdir -p "$BASE/empty-p" "$BASE/empty-c"
"$COMPAT" migrate hash --dir "file://$BASE/empty-p?format=goose" >/dev/null 2>&1
"$ATLAS"  migrate hash --dir "file://$BASE/empty-c?format=goose" >/dev/null 2>&1
diff -q "$BASE/empty-p/atlas.sum" "$BASE/empty-c/atlas.sum" >/dev/null \
  && echo "  empty dir sums IDENTICAL: $(cat "$BASE/empty-p/atlas.sum")" \
  || { fail=1; echo "  empty dir sums DIFFERENT"; }

echo
echo "===== 9. MINOR 7: a DIRECTORY named weird.sql"
mkdir -p "$BASE/w-ce/weird.sql" "$BASE/w-pt/weird.sql" "$BASE/w-atlas/weird.sql"
for d in w-ce w-pt w-atlas; do printf -- '-- +goose Up\nCREATE TABLE w (id int);\n' > "$BASE/$d/1_init.sql"; done
out=$("$ATLAS"  migrate hash --dir "file://$BASE/w-ce?format=goose" 2>&1); rc=$?
printf '  CE   converted  exit=%s [%s] sum=%s\n' "$rc" "$(echo "$out" | sed "s|$BASE|B|" | tr '\n' '|')" "$(head -1 "$BASE/w-ce/atlas.sum" 2>/dev/null || echo NONE)"
out=$("$COMPAT" migrate hash --dir "file://$BASE/w-pt?format=goose" 2>&1); rc=$?
printf '  ptah converted  exit=%s [%s] sum=%s\n' "$rc" "$(echo "$out" | sed "s|$BASE|B|" | tr '\n' '|')" "$(head -1 "$BASE/w-pt/atlas.sum" 2>/dev/null || echo NONE)"
out=$("$COMPAT" migrate hash --dir "file://$BASE/w-atlas" 2>&1); rc=$?
printf '  ptah atlas      exit=%s [%s] sum=%s\n' "$rc" "$(echo "$out" | sed "s|$BASE|B|" | tr '\n' '|')" "$(head -1 "$BASE/w-atlas/atlas.sum" 2>/dev/null || echo NONE)"
out=$("$ATLAS" migrate validate --dir "file://$BASE/w-pt?format=goose" 2>&1); rc=$?
printf '  CE validate the sum ptah wrote: exit=%s [%s]\n' "$rc" "$(echo "$out" | sed "s|$BASE|B|" | tr '\n' '|')"

echo
echo "===== 10. validate --dev-url on a converted directory"
mkdir -p "$BASE/dev-ok" "$BASE/dev-bad"
printf -- '-- +goose Up\nCREATE TABLE dev_ok (id int);\n' > "$BASE/dev-ok/1_init.sql"
printf -- '-- +goose Up\nTHIS IS NOT SQL;\n'              > "$BASE/dev-bad/1_init.sql"
for d in dev-ok dev-bad; do "$ATLAS" migrate hash --dir "file://$BASE/$d?format=goose" >/dev/null 2>&1; done
out=$("$COMPAT" migrate validate --dir "file://$BASE/dev-ok?format=goose" --dev-url "sqlite://$BASE/ok.db" 2>&1); rc=$?
printf '  ptah valid sql  exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$ATLAS" migrate validate --dir "file://$BASE/dev-ok?format=goose" --dev-url "sqlite://$BASE/ok2.db" 2>&1); rc=$?
printf '  CE   valid sql  exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$COMPAT" migrate validate --dir "file://$BASE/dev-bad?format=goose" --dev-url "sqlite://$BASE/bad.db" 2>&1); rc=$?
printf '  ptah bad sql    exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$ATLAS" migrate validate --dir "file://$BASE/dev-bad?format=goose" --dev-url "sqlite://$BASE/bad2.db" 2>&1); rc=$?
printf '  CE   bad sql    exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"

echo
echo "===== 11. #980 seam: LoadFS is never reached by the integrity path"
mkdir -p "$BASE/nosql" "$BASE/subonly/sub"
printf 'nope\n' > "$BASE/nosql/readme.txt"
printf -- '-- +goose Up\nCREATE TABLE s (id int);\n' > "$BASE/subonly/sub/1_init.sql"
for d in nosql subonly; do
  out=$("$COMPAT" migrate hash --dir "file://$BASE/$d?format=goose" 2>&1); rc=$?
  printf '  ptah hash %-8s exit=%s [%s]\n' "$d" "$rc" "$(echo "$out" | sed "s|$BASE|B|" | tr '\n' '|')"
  out=$("$ATLAS" migrate hash --dir "file://$BASE/$d?format=goose" 2>&1); rc=$?
  printf '  CE   hash %-8s exit=%s [%s]\n' "$d" "$rc" "$(echo "$out" | sed "s|$BASE|B|" | tr '\n' '|')"
  out=$("$COMPAT" migrate validate --dir "file://$BASE/$d?format=goose" 2>&1); rc=$?
  printf '  ptah val  %-8s exit=%s [%s]\n' "$d" "$rc" "$(echo "$out" | tr '\n' '|')"
  out=$("$ATLAS" migrate validate --dir "file://$BASE/$d?format=goose" 2>&1); rc=$?
  printf '  CE   val  %-8s exit=%s [%s]\n' "$d" "$rc" "$(echo "$out" | tr '\n' '|')"
done

echo
echo "===== 12. the integrity gate reaches status and set, not only hash/validate (#974)"
# Sections 1-11 only ever ask hash and validate about atlas.sum. That is exactly
# how #974 survived: `migrate status` and `migrate set` read the same directory
# through the same capture step and reported normally on one the community
# binary refuses. This section walks the five directory states plus the two
# exemptions across both verbs.
#
# Refusal rows compare the exit code AND both streams separately, because three
# of the interesting inputs already exit 1 for an unrelated reason and an
# exit-code-only comparison would coincide.
#
# Exempt rows compare the exit code only: on a directory both tools accept, the
# two report formats differ by design ("Migration Status: OK" against
# "=== MIGRATION STATUS ==="), which is a reporting divergence outside this
# gate's scope.

# native_seed <dir> <state> — one Atlas migration in the requested drift state.
native_seed() {
  local d="$BASE/$1" state="$2"
  rm -rf "$d"; mkdir -p "$d"
  printf 'CREATE TABLE t1 (id INTEGER PRIMARY KEY);\n' > "$d/20260101000000_init.sql"
  case "$state" in
    unhashed) : ;;
    clean)    "$ATLAS" migrate hash --dir "file://$d" >/dev/null 2>&1 ;;
    edited)   "$ATLAS" migrate hash --dir "file://$d" >/dev/null 2>&1
              printf '\n-- edited after hashing\n' >> "$d/20260101000000_init.sql" ;;
    added)    "$ATLAS" migrate hash --dir "file://$d" >/dev/null 2>&1
              printf 'CREATE TABLE t2 (id INTEGER PRIMARY KEY);\n' > "$d/20260102000000_two.sql" ;;
    removed)  "$ATLAS" migrate hash --dir "file://$d" >/dev/null 2>&1
              rm -f "$d/20260101000000_init.sql" ;;
    empty)    rm -f "$d/20260101000000_init.sql" ;;
    nonsql)   rm -f "$d/20260101000000_init.sql"
              printf 'migrations live here\n' > "$d/README.md" ;;
    nested)   rm -f "$d/20260101000000_init.sql"; mkdir -p "$d/sub"
              printf 'CREATE TABLE n (id INTEGER PRIMARY KEY);\n' > "$d/sub/20260101000000_init.sql" ;;
    # The three shapes #976 was about. `nested` above covers the unhashed half
    # that #974 already pinned; these cover the hashed half, which was open.
    nestedhash)
              mkdir -p "$d/sub"
              printf 'CREATE TABLE n (id INTEGER PRIMARY KEY);\n' > "$d/sub/20260102000000_nested.sql"
              "$ATLAS" migrate hash --dir "file://$d" >/dev/null 2>&1 ;;
    nestedtamper)
              mkdir -p "$d/sub"
              printf 'CREATE TABLE n (id INTEGER PRIMARY KEY);\n' > "$d/sub/20260102000000_nested.sql"
              "$ATLAS" migrate hash --dir "file://$d" >/dev/null 2>&1
              # Edited AFTER hashing, and no entry in atlas.sum covers it: the
              # file whose contents can change what runs without changing a hash.
              printf 'CREATE TABLE pwned (id INTEGER PRIMARY KEY);\n' >> "$d/sub/20260102000000_nested.sql" ;;
    uppercase)
              rm -f "$d/20260101000000_init.sql"
              printf 'CREATE TABLE u (id INTEGER PRIMARY KEY);\n' > "$d/20260101000000_init.SQL"
              "$ATLAS" migrate hash --dir "file://$d" >/dev/null 2>&1 ;;
    uppermixed)
              printf 'CREATE TABLE u (id INTEGER PRIMARY KEY);\n' > "$d/20260102000000_upper.SQL"
              "$ATLAS" migrate hash --dir "file://$d" >/dev/null 2>&1 ;;
  esac
  echo "$d"
}

# apply_tables <binary> <dir> <db> — apply and echo "<exit>|<non-atlas tables>".
# The executed table set is the property #976 is about; exit codes agree on the
# hashed shapes even when what ran does not, so comparing exit alone is blind
# to exactly this bug.
apply_tables() {
  local bin="$1" dir="$2" db="$3" rc
  "$bin" migrate apply --dir "file://$dir" --url "sqlite://$db" >/dev/null 2>&1
  rc=$?
  printf '%s|%s' "$rc" "$(sqlite3 "$db" \
    "select name from sqlite_master where type='table' and name not like 'atlas_%' order by name;" \
    2>/dev/null | tr '\n' ' ')"
}

# gate_row <verb> <state> <mode> [extra args...]
#   mode=streams  compare exit code and both streams (a refusal is expected)
#   mode=exit     compare exit code only (the state is exempt on both tools)
#   mode=report   print both, never fail (a recorded known divergence)
gate_row() {
  local verb="$1" state="$2" mode="$3"; shift 3
  local ce ptah ceout cerr cerc ptout pterr ptrc verdict
  ce=$(native_seed "g-ce-$verb-$state" "$state")
  ptah=$(native_seed "g-pt-$verb-$state" "$state")
  local ceargs=() ptargs=()
  case "$verb" in
    status) ceargs=(migrate status --url "sqlite://$ce.db"   --dir "file://$ce")
            ptargs=(migrate status --url "sqlite://$ptah.db" --dir "file://$ptah") ;;
    set)    ceargs=(migrate set 20260101000000 --url "sqlite://$ce.db"   --dir "file://$ce")
            ptargs=(migrate set 20260101000000 --url "sqlite://$ptah.db" --dir "file://$ptah") ;;
  esac
  ceout=$("$ATLAS"  "${ceargs[@]}" "$@" 2>"$BASE/g.err"); cerc=$?; cerr=$(cat "$BASE/g.err")
  ptout=$("$COMPAT" "${ptargs[@]}" "$@" 2>"$BASE/g.err"); ptrc=$?; pterr=$(cat "$BASE/g.err")
  case "$mode" in
    streams) [ "$cerc" = "$ptrc" ] && [ "$ceout" = "$ptout" ] && [ "$cerr" = "$pterr" ] \
               && verdict=MATCH || { verdict=DIFFER; fail=1; } ;;
    exit)    [ "$cerc" = "$ptrc" ] && verdict="MATCH(exit)" || { verdict=DIFFER; fail=1; } ;;
    report)  verdict=KNOWN-DIVERGENCE ;;
  esac
  printf '  %-6s %-8s %-16s CE exit=%s  ptah exit=%s\n' "$verb" "$state" "$verdict" "$cerc" "$ptrc"
  if [ "$verdict" != MATCH ] && [ "$verdict" != "MATCH(exit)" ]; then
    printf '       CE   out=[%s] err=[%s]\n' "$(echo "$ceout" | tr '\n' '|')" "$(echo "$cerr" | tr '\n' '|')"
    printf '       ptah out=[%s] err=[%s]\n' "$(echo "$ptout" | tr '\n' '|')" "$(echo "$pterr" | tr '\n' '|')"
  fi
}

for verb in status set; do
  for state in unhashed edited added removed; do
    gate_row "$verb" "$state" streams
  done
  # Since #976 both tools read the top level only, so every nested and
  # case-variant shape is exempt on both and compares by exit code — the same
  # mode the other agreed-on states use, no longer a recorded divergence.
  # ptah-compat additionally names the declined file on stderr, which is why
  # these are not streams rows; that line is asserted in the apply section below.
  for state in clean empty nonsql nested nestedhash nestedtamper uppercase uppermixed; do
    gate_row "$verb" "$state" exit
  done
done

echo
echo "  #976: what EXECUTES, not just what exits — the covered set is the run set"
for state in nestedtamper uppermixed clean; do
  ced=$(native_seed "a-ce-$state" "$state")
  ptd=$(native_seed "a-pt-$state" "$state")
  ceres=$(apply_tables "$ATLAS"  "$ced" "$ced.db")
  ptres=$(apply_tables "$COMPAT" "$ptd" "$ptd.db")
  if [ "$ceres" = "$ptres" ]; then
    verdict=MATCH
  else
    verdict=DIFFER; fail=1
  fi
  printf '  %-14s %-8s CE [%s]  ptah [%s]\n' "$state" "$verdict" "$ceres" "$ptres"
done
echo "  (a 'pwned' table under nestedtamper means SQL ran that no checksum covered)"

echo
echo "  #976: the declined file is named on stderr, which the community binary does not do"
decl=$(native_seed "a-decl" nestedtamper)
"$COMPAT" migrate apply --dir "file://$decl" --url "sqlite://$decl.decl.db" >/dev/null 2>"$BASE/d.err"
printf '    ptah stderr [%s]\n' "$(tr '\n' '|' < "$BASE/d.err")"
cedecl=$(native_seed "a-decl-ce" nestedtamper)
"$ATLAS" migrate apply --dir "file://$cedecl" --url "sqlite://$cedecl.decl.db" >/dev/null 2>"$BASE/d.err"
printf '    CE   stderr [%s]\n' "$(tr '\n' '|' < "$BASE/d.err")"

echo
echo "  ordering: the refusal precedes the connection and the arity check"
ordr=$(native_seed "g-order" unhashed)
out=$("$COMPAT" migrate status --url "postgres://u:p@127.0.0.1:1/db?sslmode=disable" --dir "file://$ordr" 2>&1); rc=$?
printf '    ptah status unreachable --url  exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$ATLAS" migrate status --url "postgres://u:p@127.0.0.1:1/db?sslmode=disable" --dir "file://$ordr" 2>&1); rc=$?
printf '    CE   status unreachable --url  exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$COMPAT" migrate set --url "sqlite://$ordr.arity.db" --dir "file://$ordr" 2>&1); rc=$?
printf '    ptah set zero positionals      exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"
out=$("$ATLAS" migrate set --url "sqlite://$ordr.arity.db" --dir "file://$ordr" 2>&1); rc=$?
printf '    CE   set zero positionals      exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|')"

echo
echo "  still ungated on both sides: migrate lint reads an unhashed directory"
lintd=$(native_seed "g-lint" unhashed)
out=$("$COMPAT" migrate lint --dir "file://$lintd" --dev-url "sqlite://$lintd.lint.db" --latest 1 2>&1); rc=$?
printf '    ptah lint  exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|' | cut -c1-90)"
out=$("$ATLAS" migrate lint --dir "file://$lintd" --dev-url "sqlite://$lintd.lint2.db" --latest 1 2>&1); rc=$?
printf '    CE   lint  exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|' | cut -c1-90)"

echo
echo "  STILL DIVERGENT, and no issue tracks it yet: migrate new and migrate diff"
echo "  write an atlas.sum over a directory nothing verified, turning drift into"
echo "  apparent cleanliness. Deliberately out of scope for #974 — gating them"
echo "  interacts with the empty-directory bootstrap flow and needs its own"
echo "  predicate measurement."
newd=$(native_seed "g-new" unhashed)
out=$("$COMPAT" migrate new addcol --dir "file://$newd" 2>&1); rc=$?
printf '    ptah new   exit=%s sum=%s\n' "$rc" "$([ -f "$newd/atlas.sum" ] && echo WRITTEN || echo none)"
cend=$(native_seed "g-new-ce" unhashed)
out=$("$ATLAS" migrate new addcol --dir "file://$cend" 2>&1); rc=$?
printf '    CE   new   exit=%s sum=%s [%s]\n' "$rc" "$([ -f "$cend/atlas.sum" ] && echo WRITTEN || echo none)" "$(echo "$out" | tr '\n' '|' | cut -c1-60)"

echo
echo "===== 13. migrate import verifies the SOURCE atlas.sum (#1095)"
# `migrate import` reads a migration directory and its atlas.sum through the
# same capture step as every verb above, and verified neither: a directory
# `migrate apply` refuses was converted, written out, and hashed over whatever
# the conversion produced — so the tampering came out the other side as a
# destination `migrate validate` calls clean.
#
# Import's missing-sum policy is its own, and measured: a source with atlas.sum
# DELETED imports at exit 0 on both tools, where the same directory is refused
# with `checksum file not found` by apply, status and set. Requiring a sum here
# would refuse a directory another tool wrote, which is the verb's whole
# purpose. The `nosum` row below is what holds that line.
#
# Every row also records whether a destination atlas.sum was written, because
# the exit code alone cannot tell a refusal from a refusal that wrote first.

# import_seed <dir> <format> <state> — a source in the requested drift state.
import_seed() {
  local d="$BASE/$1" format="$2" state="$3" first
  rm -rf "$d"; mkdir -p "$d"
  case "$format" in
    goose)          first="$d/1_init.sql"
                    printf -- '-- +goose Up\nCREATE TABLE a (id int);\n-- +goose Down\nDROP TABLE a;\n' > "$first" ;;
    dbmate)         first="$d/20240101000000_init.sql"
                    printf -- '-- migrate:up\nCREATE TABLE a (id int);\n-- migrate:down\nDROP TABLE a;\n' > "$first" ;;
    liquibase)      first="$d/1_init.sql"
                    printf -- '--liquibase formatted sql\n--changeset app:1\nCREATE TABLE a (id int);\n' > "$first" ;;
    flyway)         first="$d/V1__init.sql"
                    printf 'CREATE TABLE a (id int);\n' > "$first" ;;
    golang-migrate) first="$d/1_init.up.sql"
                    printf 'CREATE TABLE a (id int);\n' > "$first"
                    printf 'DROP TABLE a;\n' > "$d/1_init.down.sql" ;;
  esac
  "$ATLAS" migrate hash --dir "file://$d?format=$format" >/dev/null 2>&1
  case "$state" in
    clean)     : ;;
    nosum)     rm -f "$d/atlas.sum" ;;
    edited)    printf -- '\n-- edited after hashing\n' >> "$first" ;;
    added)     case "$format" in
                 goose)          printf -- '-- +goose Up\nCREATE TABLE b (id int);\n' > "$d/2_more.sql" ;;
                 dbmate)         printf -- '-- migrate:up\nCREATE TABLE b (id int);\n' > "$d/20240102000000_more.sql" ;;
                 liquibase)      printf -- '--liquibase formatted sql\n--changeset app:2\nCREATE TABLE b (id int);\n' > "$d/2_more.sql" ;;
                 flyway)         printf 'CREATE TABLE b (id int);\n' > "$d/V2__more.sql" ;;
                 golang-migrate) printf 'CREATE TABLE b (id int);\n' > "$d/2_more.up.sql" ;;
               esac ;;
    removed)   rm -f "$first" ;;
    malformed) printf 'not a sum file\n' > "$d/atlas.sum" ;;
    evildir)   mkdir -p "$d/weird.sql" ;;
    # The row that keeps the missing-sum exemption from swallowing the read: an
    # unreadable covered entry refuses on both tools with NO atlas.sum present.
    evildirnosum) rm -f "$d/atlas.sum"; mkdir -p "$d/weird.sql" ;;
  esac
  echo "$d"
}

# import_row <format> <state> <spelling> <mode>
#   spelling=query  the format rides the --from URL
#   spelling=flag   the format rides --dir-format
#   mode=streams    compare exit code and both streams (a refusal is expected)
#   mode=exit       compare exit code and whether a destination sum was written
import_row() {
  local format="$1" state="$2" spelling="$3" mode="$4"
  local ce ptah ceout cerr cerc ptout pterr ptrc verdict cesum ptsum
  ce=$(import_seed "i-ce-$format-$state-$spelling" "$format" "$state")
  ptah=$(import_seed "i-pt-$format-$state-$spelling" "$format" "$state")
  local ceargs=() ptargs=()
  if [ "$spelling" = query ]; then
    ceargs=(migrate import --from "file://$ce?format=$format"   --to "file://$ce.dst")
    ptargs=(migrate import --from "file://$ptah?format=$format" --to "file://$ptah.dst")
  else
    ceargs=(migrate import --from "file://$ce"   --to "file://$ce.dst"   --dir-format "$format")
    ptargs=(migrate import --from "file://$ptah" --to "file://$ptah.dst" --dir-format "$format")
  fi
  ceout=$("$ATLAS"  "${ceargs[@]}" 2>"$BASE/i.err"); cerc=$?; cerr=$(cat "$BASE/i.err")
  ptout=$("$COMPAT" "${ptargs[@]}" 2>"$BASE/i.err"); ptrc=$?; pterr=$(cat "$BASE/i.err")
  cesum=$([ -f "$ce.dst/atlas.sum" ] && echo WRITTEN || echo none)
  ptsum=$([ -f "$ptah.dst/atlas.sum" ] && echo WRITTEN || echo none)
  case "$mode" in
    streams) [ "$cerc" = "$ptrc" ] && [ "$ceout" = "$ptout" ] && [ "$cerr" = "$pterr" ] \
               && [ "$cesum" = "$ptsum" ] && verdict=MATCH || { verdict=DIFFER; fail=1; } ;;
    # The stderr wording of an unreadable covered entry is Ptah's own on every
    # verb (`... is a directory, not a migration file; rename it`), a recorded
    # pre-existing divergence shared with apply/status/set, so this mode
    # compares the exit code and what reached the destination.
    exit)    [ "$cerc" = "$ptrc" ] && [ "$cesum" = "$ptsum" ] \
               && verdict="MATCH(exit+dst)" || { verdict=DIFFER; fail=1; } ;;
  esac
  printf '  %-15s %-13s %-6s %-16s CE exit=%s dst=%-7s  ptah exit=%s dst=%s\n' \
    "$format" "$state" "$spelling" "$verdict" "$cerc" "$cesum" "$ptrc" "$ptsum"
  if [ "$verdict" = DIFFER ]; then
    printf '       CE   out=[%s] err=[%s]\n' "$(echo "$ceout" | tr '\n' '|')" "$(echo "$cerr" | tr '\n' '|')"
    printf '       ptah out=[%s] err=[%s]\n' "$(echo "$ptout" | tr '\n' '|')" "$(echo "$pterr" | tr '\n' '|')"
  fi
}

for format in goose dbmate liquibase flyway golang-migrate; do
  for spelling in query flag; do
    for state in edited added removed malformed; do
      import_row "$format" "$state" "$spelling" streams
    done
    for state in clean nosum; do
      import_row "$format" "$state" "$spelling" exit
    done
  done
done

echo
echo "  a covered entry that is a DIRECTORY refuses whether or not a sum exists"
# golang-migrate is the negative control and is expected to exit 0 on both:
# its covered set is *.up.sql, so a directory named weird.sql is not a member
# and there is nothing to fail reading. That is what shows the refusal keys on
# the covered set rather than on any directory whose name ends in .sql.
#
# flyway is deliberately absent: Atlas walks that tree instead of globbing it,
# so a directory there is a node it descends into and never a covered entry.
for format in goose dbmate liquibase golang-migrate; do
  import_row "$format" evildir query exit
  import_row "$format" evildirnosum query exit
done

echo
echo "  the checksum refusal outranks import's own destination rules"
# Both of these used to win here, because they ran before anything read the
# source. They are the last checks standing between a tampered source and a
# written directory, so either of them passing on an unverified source is the
# bug rather than a message difference.
ordce=$(import_seed "i-order-ce" goose edited)
ordpt=$(import_seed "i-order-pt" goose edited)
mkdir -p "$ordce.dst" "$ordpt.dst"
printf 'SELECT 1;\n' > "$ordce.dst/9_existing.sql"
printf 'SELECT 1;\n' > "$ordpt.dst/9_existing.sql"
out=$("$ATLAS"  migrate import --from "file://$ordce?format=goose" --to "file://$ordce.dst" 2>&1); rc=$?
printf '    CE   dst-not-empty  exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|' | cut -c1-80)"
out=$("$COMPAT" migrate import --from "file://$ordpt?format=goose" --to "file://$ordpt.dst" 2>&1); rc=$?
printf '    ptah dst-not-empty  exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|' | cut -c1-80)"
out=$("$ATLAS"  migrate import --from "file://$ordce?format=goose" --to "file://$ordce" 2>&1); rc=$?
printf '    CE   to-equals-from exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|' | cut -c1-80)"
out=$("$COMPAT" migrate import --from "file://$ordpt?format=goose" --to "file://$ordpt" 2>&1); rc=$?
printf '    ptah to-equals-from exit=%s [%s]\n' "$rc" "$(echo "$out" | tr '\n' '|' | cut -c1-80)"

echo
echo "  STILL DIVERGENT, out of scope for #1095: an empty source directory"
echo "  reports 'nothing to import' at exit 0 on the community binary and"
echo "  'no importable migration files found' at exit 1 here, hashed or not."
for state in hashed unhashed; do
  d="$BASE/i-empty-$state"; rm -rf "$d"; mkdir -p "$d"
  [ "$state" = hashed ] && "$ATLAS" migrate hash --dir "file://$d?format=goose" >/dev/null 2>&1
  out=$("$ATLAS"  migrate import --from "file://$d?format=goose" --to "file://$d.dst1" 2>&1); rc=$?
  printf '    CE   %-9s exit=%s [%s]\n' "$state" "$rc" "$(echo "$out" | tr '\n' '|' | cut -c1-70)"
  out=$("$COMPAT" migrate import --from "file://$d?format=goose" --to "file://$d.dst2" 2>&1); rc=$?
  printf '    ptah %-9s exit=%s [%s]\n' "$state" "$rc" "$(echo "$out" | tr '\n' '|' | cut -c1-70)"
done

echo
[ "$fail" = 0 ] && echo "ALL DIFFERENTIAL CHECKS MATCHED" || echo "SOME DIFFERENTIAL CHECKS FAILED"
exit "$fail"
