#!/usr/bin/env bash
# Fails when a package the public API ledger declares stable carries an exported
# declaration with no doc comment.
#
# The package ledger and released-baseline API check do not assess doc comment
# coverage. An audit found ten exported declarations on the stable surface
# saying nothing about themselves -- including `goschema.ParseFile`, one of the
# seven parse entry points the library was pitched on (stokaro/ptah#2246 §8).
#
# The package set is read through `list-public-api-packages.sh`, the same
# command those gates run, so this cannot enforce a different surface than they
# do.
set -euo pipefail

export GOWORK=off

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

module_path="$(go list -m -f '{{.Path}}')"

# The ledger lists full import paths; the tool takes directories relative to the
# module root.
# Read with a loop rather than `mapfile`, which needs a bash this repository
# does not require: the macOS system bash is 3.2.
package_dirs=()
while IFS= read -r directory; do
	[[ -n "$directory" ]] && package_dirs+=("$directory")
done < <(
	"$script_dir/list-public-api-packages.sh" |
		sed "s|^${module_path}$|.|; s|^${module_path}/||" |
		sort -u
)

if [[ ${#package_dirs[@]} -eq 0 ]]; then
	printf '%s: found no packages in the ledger; refusing to report a vacuous pass\n' "$0" >&2
	exit 1
fi

go run ./internal/cmd/exporteddocs "$repo_root" "${package_dirs[@]}"
