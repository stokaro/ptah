#!/usr/bin/env bash
# Execute the source-neutral migration and brownfield paths documented by the
# site. The fixture is the common representable subset; identical normalized
# inspection output is therefore required across transports.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
fixture="$repo_root/docs/site/fixtures/source-equivalence"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-source-workflows.XXXXXX")"
cleanup() {
	rm -rf "$work_dir"
}
trap cleanup EXIT

ptah_bin="${PTAH_BIN:-$work_dir/ptah}"
if [ -z "${PTAH_BIN:-}" ]; then
	(
		cd "$repo_root"
		go build -o "$ptah_bin" ./cmd/ptah
	)
fi

normalize_schema() {
	local schema_file="$1"
	local output="$2"
	local database="$3"
	"$ptah_bin" schema inspect \
		--schema-file "$schema_file" \
		--dev-url "sqlite://$database" \
		--format json >"$output"
}

normalize_schema "$fixture/schema.sql" "$work_dir/baseline.json" "$work_dir/baseline.db"

verify_generated_source() {
	local label="$1"
	local source_cwd="$2"
	shift 2
	local migrations="$work_dir/migrations-$label"
	local target="$work_dir/target-$label.db"
	local plan="$work_dir/plan-$label.json"
	local generated="$work_dir/generated-$label.json"

	(
		cd "$source_cwd"
		"$ptah_bin" migrations plan \
			"$@" \
			--db-url "sqlite://$target" \
			--report json >"$plan"
		"$ptah_bin" migrations generate \
			"$@" \
			--db-url "sqlite://$target" \
			--migrations-dir "$migrations" \
			--name init \
			--report json >/dev/null
	)

	if ! grep -Fq '"destructive": false' "$plan" || ! grep -Fq '"assessments": [' "$plan"; then
		echo "check-source-workflows: $label plan has no non-destructive structured verdict" >&2
		exit 1
	fi
	local up_file
	up_file="$(find "$migrations" -maxdepth 1 -name '*_init.up.sql' -print -quit)"
	local down_file
	down_file="$(find "$migrations" -maxdepth 1 -name '*_init.down.sql' -print -quit)"
	local report_file
	report_file="$(find "$migrations" -maxdepth 1 -name '*_init.safety.json' -print -quit)"
	for artifact in "$up_file" "$down_file" "$report_file"; do
		if [ -z "$artifact" ] || [ ! -s "$artifact" ]; then
			echo "check-source-workflows: $label omitted a migration artifact" >&2
			exit 1
		fi
	done

	normalize_schema "$up_file" "$generated" "$work_dir/generated-$label.db"
	if ! cmp -s "$work_dir/baseline.json" "$generated"; then
		echo "check-source-workflows: $label generated a different normalized schema" >&2
		diff -u "$work_dir/baseline.json" "$generated" >&2 || true
		exit 1
	fi
	"$ptah_bin" migrations hash --dir "$migrations" >/dev/null
	"$ptah_bin" migrations validate --dir "$migrations" >/dev/null
}

verify_generated_source sql "$repo_root" --schema-file "$fixture/schema.sql"
verify_generated_source yaml "$repo_root" --schema-file "$fixture/schema.yaml"
verify_generated_source hcl "$repo_root" --schema-file "$fixture/schema.hcl"
verify_generated_source dbml "$repo_root" --schema-file "$fixture/schema.dbml"
verify_generated_source go "$repo_root" --root-dir "$fixture/models"
verify_generated_source external "$repo_root" \
	--schema-cmd "$fixture/external-schema.sh" --schema-format sql
verify_generated_source configured "$fixture" \
	--config ptah.yaml --allow-external-schema
verify_generated_source composite "$repo_root" \
	--schema-file "$fixture/composite/schema.sql" \
	--schema-file "$fixture/composite/shared.yaml" \
	--schema-file "$fixture/composite/vendor.hcl"

adopt_source() {
	local label="$1"
	local source_file="$2"
	local next_flag="$3"
	local next_source="$4"
	shift 4
	local target="$work_dir/adopt-$label.db"
	local empty="$work_dir/adopt-empty-$label.db"
	local shadow="$work_dir/adopt-shadow-$label.db"
	local migrations="$work_dir/adopt-migrations-$label"

	"$ptah_bin" schema apply \
		--schema-file "$fixture/schema.sql" \
		--db-url "sqlite://$target" \
		--auto-approve >/dev/null
	"$ptah_bin" schema drift "$@" --db-url "sqlite://$target" >/dev/null
	"$ptah_bin" migrations generate \
		"$@" \
		--db-url "sqlite://$empty" \
		--migrations-dir "$migrations" \
		--name init >/dev/null
	"$ptah_bin" migrations hash --dir "$migrations" >/dev/null
	"$ptah_bin" migrations baseline \
		--db-url "sqlite://$target" \
		--migrations-dir "$migrations" \
		--shadow-db "sqlite://$shadow" >/dev/null
	"$ptah_bin" migrations status \
		--db-url "sqlite://$target" \
		--migrations-dir "$migrations" >"$work_dir/status-$label.txt"
	if ! grep -Fq 'Pending Migrations: 0' "$work_dir/status-$label.txt"; then
		echo "check-source-workflows: $label adoption left pending migrations" >&2
		exit 1
	fi
	if [ ! -s "$source_file" ]; then
		echo "check-source-workflows: $label adoption wrote no source" >&2
		exit 1
	fi

	"$ptah_bin" migrations generate \
		"$next_flag" "$next_source" \
		--db-url "sqlite://$target" \
		--migrations-dir "$migrations" \
		--name add_published_at >/dev/null
	"$ptah_bin" migrations hash --dir "$migrations" >/dev/null
	"$ptah_bin" migrations up \
		--db-url "sqlite://$target" \
		--migrations-dir "$migrations" \
		--verify-sum >/dev/null
	"$ptah_bin" schema drift \
		"$next_flag" "$next_source" \
		--db-url "sqlite://$target" >/dev/null
	"$ptah_bin" migrations status \
		--db-url "sqlite://$target" \
		--migrations-dir "$migrations" >"$work_dir/status-next-$label.txt"
	if ! grep -Fq 'Pending Migrations: 0' "$work_dir/status-next-$label.txt"; then
		echo "check-source-workflows: $label first subsequent change left pending migrations" >&2
		exit 1
	fi
}

sql_source="$work_dir/adopted.sql"
sql_database="$work_dir/read-sql.db"
"$ptah_bin" schema apply --schema-file "$fixture/schema.sql" --db-url "sqlite://$sql_database" --auto-approve >/dev/null
"$ptah_bin" db read --db-url "sqlite://$sql_database" >"$sql_source"
adopt_source sql "$sql_source" --schema-file "$fixture/next/schema.sql" --schema-file "$sql_source"

hcl_source="$work_dir/adopted.hcl"
hcl_database="$work_dir/read-hcl.db"
"$ptah_bin" schema apply --schema-file "$fixture/schema.sql" --db-url "sqlite://$hcl_database" --auto-approve >/dev/null
"$ptah_bin" schema inspect --db-url "sqlite://$hcl_database" >"$hcl_source"
adopt_source hcl "$hcl_source" --schema-file "$fixture/next/schema.hcl" --schema-file "$hcl_source"

dbml_source="$work_dir/adopted.dbml"
dbml_database="$work_dir/read-dbml.db"
"$ptah_bin" schema apply --schema-file "$fixture/schema.sql" --db-url "sqlite://$dbml_database" --auto-approve >/dev/null
"$ptah_bin" schema inspect --db-url "sqlite://$dbml_database" --format dbml >"$dbml_source"
adopt_source dbml "$dbml_source" --schema-file "$fixture/next/schema.dbml" --schema-file "$dbml_source"

go_source="$work_dir/models"
go_database="$work_dir/read-go.db"
"$ptah_bin" schema apply --schema-file "$fixture/schema.sql" --db-url "sqlite://$go_database" --auto-approve >/dev/null
"$ptah_bin" introspect --db-url "sqlite://$go_database" --out "$go_source" --package models >/dev/null
adopt_source go "$go_source" --root-dir "$fixture/next/models" --root-dir "$go_source"

proto_dir="$work_dir/proto/acme/inventory/v1"
mkdir -p "$proto_dir"
proto_file="$proto_dir/schema.proto"
"$ptah_bin" schema export \
	--to protobuf \
	--schema-file "$repo_root/docs/site/fixtures/protobuf-export/schema.yaml" \
	--out "$proto_file" \
	--proto-package acme.inventory.v1 \
	--go-package github.com/acme/inventory/gen/inventory/v1 >/dev/null
if ! grep -Fq 'enum ProductsStatus {' "$proto_file" ||
	! grep -Fq 'ptah:protobuf-export-version=2' "$proto_file"; then
	echo "check-source-workflows: static protobuf example omitted its enum or compatibility marker" >&2
	exit 1
fi
cp "$proto_file" "$work_dir/first.proto"
"$ptah_bin" schema export \
	--to protobuf \
	--schema-file "$repo_root/docs/site/fixtures/protobuf-export/schema.yaml" \
	--out "$proto_file" \
	--proto-package acme.inventory.v1 \
	--go-package github.com/acme/inventory/gen/inventory/v1 >/dev/null
if ! cmp -s "$work_dir/first.proto" "$proto_file"; then
	echo "check-source-workflows: static protobuf example changed on its second export" >&2
	exit 1
fi

echo "check-source-workflows: OK (8 migration source paths; 4 brownfield adoption paths; static Protobuf export)"
