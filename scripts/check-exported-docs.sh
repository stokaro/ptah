#!/usr/bin/env bash
# Fails when a package the public API ledger declares stable carries an exported
# declaration with no doc comment.
#
# The three Go-API gates measure the SHAPE of the public surface: which packages
# are in it, which symbols they export, and whether a change to those is
# compatible. None of them can see a doc comment. The snapshot in particular is
# byte-identical whether a method is documented or not, which is why an audit
# found ten exported declarations on the stable surface saying nothing about
# themselves -- including `goschema.ParseFile`, one of the seven parse entry
# points the library was pitched on (stokaro/ptah#2246 §8).
#
# The package set is read through `check-public-api-snapshot.sh
# --list-packages`, the same scrape those gates run, so this cannot enforce a
# different surface than they do.
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
	"$script_dir/check-public-api-snapshot.sh" --list-packages |
		sed "s|^${module_path}$|.|; s|^${module_path}/||" |
		sort -u
)

if [[ ${#package_dirs[@]} -eq 0 ]]; then
	printf '%s: found no packages in the ledger; refusing to report a vacuous pass\n' "$0" >&2
	exit 1
fi

go run ./internal/cmd/exporteddocs "$repo_root" "${package_dirs[@]}"
