#!/usr/bin/env bash
# Exercises .github/actions/ptah/install.sh against a fixture release served
# from a temporary directory over HTTP, the way the action meets a real one.
#
# The properties held here are the ones a workflow author cannot see: the
# archive is verified against checksums.txt before anything is unpacked, every
# way the check can fail to happen is a refusal (a wrong digest, no entry for
# the archive, no checksums.txt in the release), and a verified archive's
# providers directory lands beside the installed ptah. Nothing here reaches
# GitHub; PTAH_ACTION_API_URL aims the script at the fixture.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

script="${PTAH_ACTION_INSTALL_SCRIPT:-$repo_root/.github/actions/ptah/install.sh}"
action_path="$repo_root/.github/actions/ptah"

for tool in python3 node tar curl; do
	command -v "$tool" >/dev/null 2>&1 || {
		echo "check-action-install: need $tool" >&2
		exit 1
	}
done
if command -v sha256sum >/dev/null 2>&1; then
	hasher=(sha256sum)
else
	hasher=(shasum -a 256)
fi

case "$(uname -s)" in
Linux) os=linux ;;
Darwin) os=darwin ;;
*)
	echo "check-action-install: this suite runs on Linux and macOS" >&2
	exit 1
	;;
esac
case "$(uname -m)" in
x86_64 | amd64) arch=amd64 ;;
aarch64 | arm64) arch=arm64 ;;
*)
	echo "check-action-install: unsupported architecture $(uname -m)" >&2
	exit 1
	;;
esac

work="$(mktemp -d "${TMPDIR:-/tmp}/ptah-action-check.XXXXXX")"
server_pid=""
cleanup() {
	if [ -n "$server_pid" ]; then kill "$server_pid" 2>/dev/null || true; fi
	rm -rf "$work"
}
trap cleanup EXIT INT TERM

failures=0
checks=0
fail() {
	failures=$((failures + 1))
	printf '  FAIL  %s\n' "$*" >&2
}
pass() { printf '  ok    %s\n' "$*"; }

site="$work/site"
mkdir -p "$site"
asset="ptah_1.0.0_${os}_${arch}.tar.gz"
provider_dir="providers/github.com/stokaro/ptah-ydb-plugin/0.1.1"

stage="$work/stage"
mkdir -p "$stage/$provider_dir"
printf '#!/bin/sh\necho ptah\n' >"$stage/ptah"
printf '#!/bin/sh\necho provider\n' >"$stage/$provider_dir/ptah-plugin-ydb"
chmod 0755 "$stage/ptah" "$stage/$provider_dir/ptah-plugin-ydb"
tar -czf "$site/$asset" -C "$stage" ptah providers
good="$("${hasher[@]}" "$site/$asset" | awk '{print $1}')"

# An archive from a release that predates the bundle.
stage_old="$work/stage-old"
mkdir -p "$stage_old"
cp "$stage/ptah" "$stage_old/ptah"
tar -czf "$site/old_${os}_${arch}.tar.gz" -C "$stage_old" ptah
old_good="$("${hasher[@]}" "$site/old_${os}_${arch}.tar.gz" | awk '{print $1}')"

port_file="$work/port"
python3 - "$site" "$port_file" <<'PY' &
import http.server, os, socketserver, sys
os.chdir(sys.argv[1])
class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args): pass
with socketserver.TCPServer(("127.0.0.1", 0), Handler) as httpd:
    open(sys.argv[2], "w").write(str(httpd.server_address[1]))
    httpd.serve_forever()
PY
server_pid=$!
deadline=$((SECONDS + 30))
while [ ! -s "$port_file" ] && [ "$SECONDS" -lt "$deadline" ]; do sleep 0.05; done
[ -s "$port_file" ] || {
	echo "check-action-install: the fixture server did not start" >&2
	exit 1
}
base="http://127.0.0.1:$(cat "$port_file")"

# release_json <archive-name> <with-checksums: yes|no> -> writes $site/api/releases/tags/v1.0.0
release_json() {
	local name="$1" with_checksums="$2" assets
	assets="{\"name\":\"$name\",\"browser_download_url\":\"$base/$name\"}"
	if [ "$with_checksums" = yes ]; then
		assets="$assets,{\"name\":\"checksums.txt\",\"browser_download_url\":\"$base/checksums.txt\"}"
	fi
	mkdir -p "$site/api/releases/tags"
	printf '{"tag_name":"v1.0.0","assets":[%s]}\n' "$assets" >"$site/api/releases/tags/v1.0.0"
}

runner=""
bindir=""
status=0

# run_install leaves the exit status in $status and the output in $out.
run_install() {
	runner="$work/runner"
	rm -rf "$runner"
	mkdir -p "$runner"
	: >"$runner/path"
	: >"$runner/output"
	status=0
	env -u GITHUB_TOKEN \
		PTAH_VERSION=v1.0.0 PTAH_ACTION_API_URL="$base/api" \
		GITHUB_ACTION_PATH="$action_path" RUNNER_TEMP="$runner" \
		GITHUB_PATH="$runner/path" GITHUB_OUTPUT="$runner/output" \
		bash "$script" >"$work/out" 2>&1 || status=$?
	bindir="$runner/ptah-action-bin"
}

# expect runs the command after the description and fails the case when it
# fails. A negated check is written `expect "..." not test -e path`.
not() { ! "$@"; }
expect() {
	local description="$1"
	shift
	checks=$((checks + 1))
	if "$@"; then return 0; fi
	fail "$description"
	sed 's/^/      | /' "$work/out" >&2
	return 1
}

# A sound release installs ptah and its providers directory.
release_json "$asset" yes
printf '%s  %s\n' "0000000000000000000000000000000000000000000000000000000000000000" "$asset.sbom.json" >"$site/checksums.txt"
printf '%s  %s\n' "$good" "$asset" >>"$site/checksums.txt"
run_install
if expect "a verified archive installs (exit $status)" [ "$status" -eq 0 ] &&
	expect "ptah is installed and executable" test -x "$bindir/ptah" &&
	expect "the provider lands at the bundled path beside ptah" test -x "$bindir/$provider_dir/ptah-plugin-ydb" &&
	expect "ptah-bin is published" grep -qF "ptah-bin=$bindir/ptah" "$runner/output"; then
	pass "a verified archive installs ptah and the providers directory beside it"
fi

# An archive with no providers directory installs ptah alone.
release_json "old_${os}_${arch}.tar.gz" yes
printf '%s  %s\n' "$old_good" "old_${os}_${arch}.tar.gz" >"$site/checksums.txt"
run_install
if expect "an archive without providers installs (exit $status)" [ "$status" -eq 0 ] &&
	expect "no providers directory is created" not test -e "$bindir/providers"; then
	pass "an archive that predates the bundle still installs"
fi

# A digest that does not match refuses, and nothing is installed.
release_json "$asset" yes
printf '%s  %s\n' "1111111111111111111111111111111111111111111111111111111111111111" "$asset" >"$site/checksums.txt"
run_install
if expect "a wrong digest is refused" [ "$status" -ne 0 ] &&
	expect "the refusal names the mismatch" grep -q "checksum mismatch" "$work/out" &&
	expect "nothing was installed" not test -e "$bindir/ptah"; then
	pass "a checksum mismatch is refused and installs nothing"
fi

# checksums.txt that does not list the archive refuses. The sbom line must not
# stand in for it.
printf '%s  %s\n' "$good" "$asset.sbom.json" >"$site/checksums.txt"
run_install
if expect "no entry for the archive is refused" [ "$status" -ne 0 ] &&
	expect "the refusal says the entry is missing" grep -q "no entry for" "$work/out" &&
	expect "nothing was installed" not test -e "$bindir/ptah"; then
	pass "a checksums.txt without the archive is refused"
fi

# A release with the archive and no checksums.txt refuses, and does not fall
# back to a build from source.
release_json "$asset" no
run_install
if expect "a release without checksums.txt is refused" [ "$status" -ne 0 ] &&
	expect "the refusal names checksums.txt" grep -q "no checksums.txt" "$work/out" &&
	expect "no source build was attempted" not grep -q "installing from source" "$work/out" &&
	expect "nothing was installed" not test -e "$bindir/ptah"; then
	pass "a release without checksums.txt is refused, not built from source"
fi

echo
if [ "$failures" -ne 0 ]; then
	echo "check-action-install: $failures of $checks assertions failed" >&2
	exit 1
fi
echo "check-action-install: OK ($checks assertions)"
