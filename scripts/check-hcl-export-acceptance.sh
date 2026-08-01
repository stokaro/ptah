#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ptah_bin="${PTAH_BIN:-$repo_root/bin/ptah}"
workspace="$(mktemp -d "${TMPDIR:-/tmp}/ptah-hcl-export-acceptance.XXXXXX")"
trap 'rm -rf "$workspace"' EXIT

if [[ ! -x "$ptah_bin" ]]; then
	printf 'ptah binary is not executable: %s\n' "$ptah_bin" >&2
	exit 1
fi

fixture_root="$repo_root/internal/goannotationexport/testdata/parity"
cleanup_fixture_root="$repo_root/internal/goannotationexport/testdata/cleanup"
export_root="$workspace/export-models"
refusal_root="$workspace/refusal-models"
cleanup_root="$workspace/cleanup-models"
schema_file="$workspace/schema.hcl"

cp -R "$fixture_root" "$export_root"
"$ptah_bin" schema export \
	--from go \
	--to hcl \
	--root-dir "$export_root" \
	--out "$schema_file"

cp "$schema_file" "$workspace/schema.first.hcl"
"$ptah_bin" schema export \
	--from go \
	--to hcl \
	--root-dir "$export_root" \
	--out "$schema_file"
cmp "$workspace/schema.first.hcl" "$schema_file"

"$ptah_bin" schema render \
	--schema-file "$schema_file" \
	--dialect postgres \
	>"$workspace/schema.sql"

required_sql=(
	'CREATE TABLE "app"."users"'
	'CREATE SEQUENCE IF NOT EXISTS "app"."order_seq"'
	'CREATE DOMAIN "app"."email_address"'
	'CREATE TYPE "app"."postal_address"'
	'CREATE OR REPLACE FUNCTION "app"."lookup_user"'
	'CREATE MATERIALIZED VIEW "app"."user_stats"'
	'CREATE POLICY "users_policy"'
	'GRANT SELECT, INSERT ON TABLE "app"."users"'
	'CREATE TRIGGER "users_touch"'
)
for statement in "${required_sql[@]}"; do
	if ! grep -Fq "$statement" "$workspace/schema.sql"; then
		printf 'rendered HCL is missing expected SQL: %s\n' "$statement" >&2
		exit 1
	fi
done

cp -R "$fixture_root" "$refusal_root"
cp "$refusal_root/models.go" "$workspace/models.before-refusal.go"
printf 'previous schema\n' >"$workspace/refusal.hcl"
cp "$workspace/refusal.hcl" "$workspace/refusal.before.hcl"
if "$ptah_bin" schema export \
	--from go \
	--to hcl \
	--root-dir "$refusal_root" \
	--out "$workspace/refusal.hcl" \
	--cleanup-go-annotations \
	--cleanup-diff \
	>"$workspace/refusal.diff" \
	2>"$workspace/refusal.diff.err"; then
	printf 'cleanup diff unexpectedly accepted opaque SQL bodies\n' >&2
	exit 1
fi
grep -Fq 'refuse to clean Go annotations after a lossy HCL export' "$workspace/refusal.diff.err"

opaque_paths=(
	'functions.app.lookup_user'
	'materialized_views.app.user_stats'
	'triggers["app.users"]["users_touch"]'
	'views.app.active_users'
)
for path in "${opaque_paths[@]}"; do
	grep -Fq -- "- $path: raw SQL body is emitted as opaque HCL text" "$workspace/refusal.diff.err"
done
cmp "$workspace/models.before-refusal.go" "$refusal_root/models.go"
cmp "$workspace/refusal.before.hcl" "$workspace/refusal.hcl"

if "$ptah_bin" schema export \
	--from go \
	--to hcl \
	--root-dir "$refusal_root" \
	--out "$workspace/refusal.hcl" \
	--cleanup-go-annotations \
	>"$workspace/refusal.out" \
	2>"$workspace/refusal.err"; then
	printf 'destructive cleanup unexpectedly accepted opaque SQL bodies\n' >&2
	exit 1
fi
grep -Fq 'refuse to clean Go annotations after a lossy HCL export' "$workspace/refusal.err"
cmp "$workspace/models.before-refusal.go" "$refusal_root/models.go"
cmp "$workspace/refusal.before.hcl" "$workspace/refusal.hcl"

cp -R "$cleanup_fixture_root" "$cleanup_root"
cp "$cleanup_root/models.go" "$workspace/models.before.go"
"$ptah_bin" schema export \
	--from go \
	--to hcl \
	--root-dir "$cleanup_root" \
	--out "$workspace/cleanup.hcl" \
	--cleanup-go-annotations \
	--cleanup-diff \
	>"$workspace/cleanup.diff"
cmp "$workspace/models.before.go" "$cleanup_root/models.go"
grep -Fq -- '-//ptah:schema:table' "$workspace/cleanup.diff"
grep -Fq -- '-	//ptah:embedded' "$workspace/cleanup.diff"

"$ptah_bin" schema export \
	--from go \
	--to hcl \
	--root-dir "$cleanup_root" \
	--out "$workspace/cleanup.hcl" \
	--cleanup-go-annotations \
	>"$workspace/cleanup.out"

if grep -Rq --include='*.go' '//ptah:' "$cleanup_root"; then
	printf 'destructive cleanup left Ptah annotations in Go source\n' >&2
	exit 1
fi
grep -Fq 'package cleanup' "$cleanup_root/models.go"
grep -Fq '// User is the application user.' "$cleanup_root/models.go"
grep -Fq 'type User struct {' "$cleanup_root/models.go"
grep -Fq 'Email string' "$cleanup_root/models.go"
grep -Fq 'type Audit struct {' "$cleanup_root/models.go"

cp "$workspace/cleanup.hcl" "$workspace/cleanup.before-repeat.hcl"
cp "$cleanup_root/models.go" "$workspace/models.before-repeat.go"
if "$ptah_bin" schema export \
	--from go \
	--to hcl \
	--root-dir "$cleanup_root" \
	--out "$workspace/cleanup.hcl" \
	--cleanup-go-annotations \
	>"$workspace/repeat.out" \
	2>"$workspace/repeat.err"; then
	printf 'repeated destructive cleanup unexpectedly succeeded\n' >&2
	exit 1
fi
grep -Fq 'no removable Ptah Go annotations found for cleanup' "$workspace/repeat.err"
cmp "$workspace/cleanup.before-repeat.hcl" "$workspace/cleanup.hcl"
cmp "$workspace/models.before-repeat.go" "$cleanup_root/models.go"

printf 'Go annotation to HCL export acceptance: OK\n'
