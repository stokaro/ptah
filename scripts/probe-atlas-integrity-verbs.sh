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
# Refs stokaro/ptah#973, #983, #990, #991.
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
  esac
  echo "$d"
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
  for state in clean empty nonsql; do
    gate_row "$verb" "$state" exit
  done
  # The community binary ignores subdirectories; Ptah's registrar recurses, so
  # the shared recursive predicate refuses here where that binary exits 0. A
  # deliberate, pinned divergence (#976), recorded rather than enforced.
  gate_row "$verb" nested report
done

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
echo "  STILL DIVERGENT, tracked separately: migrate new and migrate diff write"
echo "  an atlas.sum over a directory nothing verified, turning drift into"
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
[ "$fail" = 0 ] && echo "ALL DIFFERENTIAL CHECKS MATCHED" || echo "SOME DIFFERENTIAL CHECKS FAILED"
exit "$fail"
