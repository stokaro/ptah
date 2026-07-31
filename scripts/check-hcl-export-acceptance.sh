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
export_root="$workspace/export-models"
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

cp -R "$fixture_root" "$cleanup_root"
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
grep -Fq 'package parity' "$cleanup_root/models.go"
grep -Fq 'type User struct {' "$cleanup_root/models.go"
grep -Fq 'Email string' "$cleanup_root/models.go"
grep -Fq 'type UserData struct{}' "$cleanup_root/models.go"

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
grep -Fq 'no Ptah Go annotations found to export and clean' "$workspace/repeat.err"
cmp "$workspace/cleanup.before-repeat.hcl" "$workspace/cleanup.hcl"
cmp "$workspace/models.before-repeat.go" "$cleanup_root/models.go"

printf 'Go annotation to HCL export acceptance: OK\n'
