#!/usr/bin/env bash
# Exercises docs/site/public/install.sh, the script published at
# https://stokaro.github.io/ptah/install.sh and read by `curl ... | sh`.
#
# The subject is a program people run with one line and no review, so the
# properties worth holding are the ones a reader cannot check for themselves:
# that a corrupted archive is refused, that a platform with no build is refused
# rather than guessed at, that a missing hash tool stops the run instead of
# turning verification off, and that a failed run leaves nothing behind.
#
# Every case runs against a fixture release served over HTTP from a temporary
# directory on an ephemeral port, so the suite is deterministic and offline.
# `--live` adds two runs against the published release: a pinned install of
# v0.2.0, end to end, and a dry run that resolves `latest` against GitHub.
#
# Each case runs under every POSIX shell on this machine, because `| sh` is
# dash on Debian, busybox ash on Alpine and bash-in-sh-mode on macOS, and a
# bashism is invisible under the last of those. MIN_SHELLS is the floor that
# turns "the other shells were not installed" into a failure rather than into a
# quieter pass; CI raises it, having installed busybox.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# PTAH_INSTALL_SCRIPT lets check-install-script-selftest.sh aim this gate at a
# deliberately broken copy of the installer and require it to go red.
script="${PTAH_INSTALL_SCRIPT:-$repo_root/docs/site/public/install.sh}"
min_shells="${PTAH_INSTALL_TEST_MIN_SHELLS:-2}"
live=false

while [ "$#" -gt 0 ]; do
	case "$1" in
	--live) live=true ;;
	--help | -h)
		sed -n '2,25p' "$0"
		exit 0
		;;
	*)
		echo "check-install-script: unknown option: $1" >&2
		exit 2
		;;
	esac
	shift
done

if [ ! -f "$script" ]; then
	echo "check-install-script: no installer at $script" >&2
	exit 1
fi

for tool in python3 tar curl; do
	if ! command -v "$tool" >/dev/null 2>&1; then
		echo "check-install-script: need $tool to run this suite" >&2
		exit 1
	fi
done
# The suite needs a sha256 tool of its own, to build the fixture checksums and
# to stock the restricted PATHs below. Which one is not fixed: sha256sum is
# absent from macOS before 26 and shasum is a perl package a slim Linux image
# may not carry, so the suite asks for either rather than naming one and
# failing on the machine that has the other.
if command -v sha256sum >/dev/null 2>&1; then
	host_hasher=sha256sum
elif command -v shasum >/dev/null 2>&1; then
	host_hasher=shasum
else
	echo "check-install-script: need sha256sum or shasum to build the fixture checksums" >&2
	exit 1
fi
if ! command -v shellcheck >/dev/null 2>&1; then
	echo "check-install-script: need shellcheck; it is the only mechanical guard against a" >&2
	echo "  bashism, because /bin/sh on a maintainer's macOS is bash and accepts nearly anything" >&2
	exit 1
fi

work="$(mktemp -d "${TMPDIR:-/tmp}/ptah-install-check.XXXXXX")"
server_pid=""
cleanup() {
	if [ -n "$server_pid" ]; then
		kill "$server_pid" >/dev/null 2>&1 || true
		wait "$server_pid" 2>/dev/null || true
	fi
	chmod -R u+w "$work" >/dev/null 2>&1 || true
	rm -rf "$work"
}
trap cleanup EXIT INT TERM

failures=0
checks=0
current=""

pass() { printf '    ok    %s\n' "$1"; }
report() {
	printf '    FAIL  %s\n' "$1" >&2
	failures=$((failures + 1))
}

# ---------------------------------------------------------------- the fixture

host_os="$(uname -s)"
case "$host_os" in
Linux) fixture_os=linux ;;
Darwin) fixture_os=darwin ;;
*)
	echo "check-install-script: this suite runs on Linux and macOS; uname -s said $host_os" >&2
	exit 1
	;;
esac
case "$(uname -m)" in
x86_64 | amd64) fixture_arch=amd64 ;;
aarch64 | arm64) fixture_arch=arm64 ;;
*)
	echo "check-install-script: this suite runs on amd64 and arm64; uname -m said $(uname -m)" >&2
	exit 1
	;;
esac

site="$work/site"
mkdir -p "$site/download"

# build_release writes one fixture release: three stub binaries that report the
# version they came from, packed flat exactly as the published archives are --
# LICENSE, README.md and the three programs, with no top-level directory.
build_release() {
	local version="$1" name
	local stage="$work/stage-$version"
	local asset="ptah_${version}_${fixture_os}_${fixture_arch}.tar.gz"
	local dir="$site/download/v${version}"

	rm -rf "$stage"
	mkdir -p "$stage" "$dir"
	printf 'fixture license\n' >"$stage/LICENSE"
	printf 'fixture readme\n' >"$stage/README.md"
	for name in ptah ptah-compat ptah-ls; do
		{
			printf '#!/bin/sh\n'
			printf 'printf "%%s\\n" "Version: %s"\n' "$version"
			printf 'printf "%%s\\n" "binary: %s"\n' "$name"
		} >"$stage/$name"
		chmod 0755 "$stage/$name"
	done
	tar -czf "$dir/$asset" -C "$stage" LICENSE README.md ptah ptah-compat ptah-ls
	printf '%s\n' "$asset"
}

sha256_hex() {
	if [ "$host_hasher" = sha256sum ]; then
		sha256sum "$1" | awk '{print $1}'
	else
		shasum -a 256 "$1" | awk '{print $1}'
	fi
}

# The .sbom.json line is written FIRST and deliberately. The published
# checksums.txt carries one beside every archive, and a grep for the archive's
# name that is not anchored matches both -- so an unanchored lookup here would
# read the sbom's hash and every install would fail verification. Putting it
# above the archive's own line is what makes that mistake visible instead of
# lucky.
write_checksums() {
	local version="$1" hash="$2"
	local asset="ptah_${version}_${fixture_os}_${fixture_arch}.tar.gz"
	local dir="$site/download/v${version}"
	{
		printf '%s  %s.sbom.json\n' "0000000000000000000000000000000000000000000000000000000000000000" "$asset"
		printf '%s  %s\n' "$hash" "$asset"
		printf '%s  ptah_%s_windows_amd64.zip\n' "1111111111111111111111111111111111111111111111111111111111111111" "$version"
	} >"$dir/checksums.txt"
}

# v0.1.0 and v0.2.0 are sound releases. `latest` resolves to v0.2.0.
asset_010="$(build_release 0.1.0)"
write_checksums 0.1.0 "$(sha256_hex "$site/download/v0.1.0/$asset_010")"
asset_020="$(build_release 0.2.0)"
write_checksums 0.2.0 "$(sha256_hex "$site/download/v0.2.0/$asset_020")"
printf '{"id":1,"tag_name":"v0.2.0","name":"v0.2.0"}\n' >"$site/latest"

# v0.3.0 is v0.2.0's archive with one byte changed, published under v0.2.0's
# checksum. This is the tampered-download case, built by corrupting a real
# archive rather than by inventing a hash that never matched anything.
asset_030="$(build_release 0.3.0)"
good_030="$(sha256_hex "$site/download/v0.3.0/$asset_030")"
printf 'x' | dd of="$site/download/v0.3.0/$asset_030" bs=1 seek=64 conv=notrunc status=none
write_checksums 0.3.0 "$good_030"
if [ "$(sha256_hex "$site/download/v0.3.0/$asset_030")" = "$good_030" ]; then
	echo "check-install-script: the corruption fixture did not change the archive" >&2
	exit 1
fi

# v0.4.0 publishes a checksums.txt that never mentions the archive. A lookup
# that returns nothing must refuse, not verify vacuously.
asset_040="$(build_release 0.4.0)"
{
	printf '%s  ptah-0.4.0.tar.gz\n' "2222222222222222222222222222222222222222222222222222222222222222"
	printf '%s  %s.sbom.json\n' "3333333333333333333333333333333333333333333333333333333333333333" "$asset_040"
} >"$site/download/v0.4.0/checksums.txt"

# The fake uname, for the platform this project publishes no build for.
mkdir -p "$work/fakeuname"
cat >"$work/fakeuname/uname" <<'FAKE'
#!/bin/sh
case "${1:-}" in
-s) echo FreeBSD ;;
-m) echo riscv64 ;;
*) echo FreeBSD ;;
esac
FAKE
chmod 0755 "$work/fakeuname/uname"

# A curl that reports what Apple's curl reports over HTTP/2.
#
# `curl -f` aborts on any status of 400 or above, but the exit code it then
# reports depends on the HTTP version: 22 over HTTP/1.1 and 56 over HTTP/2.
# Measured with curl 8.7.1 on macOS 26.5 against github.com, which serves
# HTTP/2, in the same minute as a Debian curl 7.88.1 that answered 22.
#
# The fixture server below is python3's http.server, which speaks HTTP/1.1 and
# nothing else, so it can only ever produce the 22 half of that. This shim
# supplies the other half offline and deterministically: it is the real curl
# with one exit code rewritten, which is the whole of the platform difference.
# The `--live` run at the end of this file exercises the real protocol.
curl_http2_shim() {
	local dir="$work/curl-http2" real
	real="$(command -v curl)"
	mkdir -p "$dir"
	cat >"$dir/curl" <<SHIM
#!/bin/sh
"$real" "\$@"
s=\$?
if [ "\$s" -eq 22 ]; then s=56; fi
exit "\$s"
SHIM
	chmod 0755 "$dir/curl"
	printf '%s\n' "$dir"
}

# A PATH holding only the named tools, used to prove that a missing
# prerequisite stops the run. Everything else the installer needs before it
# reaches the check under test is a shell builtin.
restricted_path() {
	local dir="$work/restricted-$1" tool source
	shift
	rm -rf "$dir"
	mkdir -p "$dir"
	for tool in "$@"; do
		source="$(command -v "$tool" || true)"
		if [ -z "$source" ]; then
			echo "check-install-script: cannot build a restricted PATH without $tool" >&2
			exit 1
		fi
		ln -s "$source" "$dir/$tool"
	done
	printf '%s\n' "$dir"
}

# ---------------------------------------------------------- the fixture server

python3 -u -m http.server 0 --bind 127.0.0.1 --directory "$site" >"$work/server.log" 2>&1 &
server_pid=$!

# The wait is a wall-clock deadline, not an iteration count. A count is a budget
# that shrinks on the machine that most needs it: every turn of the loop pays for
# two forks as well as its sleep, so "100 iterations of 0.1s" is ten seconds on a
# fast machine and fifteen on a slow one -- while the thing being waited for, a
# cold interpreter reaching its first bind, is slower there too. The macOS leg of
# this job timed out here on every run it ever made (stokaro/ptah#2533).
server_deadline=$((SECONDS + 60))
port=""
while [ "$SECONDS" -lt "$server_deadline" ]; do
	port="$(sed -n 's/.*port \([0-9][0-9]*\).*/\1/p' "$work/server.log" | head -n 1)"
	[ -n "$port" ] && break
	sleep 0.2
done
if [ -z "$port" ]; then
	# An empty server.log is what this said the first time, and it sends the
	# reader nowhere: it cannot tell a missing interpreter from a slow one from a
	# process that died without a word. These three lines answer that.
	echo "check-install-script: the fixture server did not report a port within 60 seconds" >&2
	echo "  python3: $(command -v python3 || echo "not on PATH")" >&2
	ps -p "$server_pid" -o pid=,stat=,comm= >&2 || echo "  the server process is gone" >&2
	cat "$work/server.log" >&2
	exit 1
fi
base_url="http://127.0.0.1:$port"
if ! curl -fsS -o /dev/null "$base_url/latest"; then
	echo "check-install-script: the fixture server did not serve $base_url/latest" >&2
	exit 1
fi

# ------------------------------------------------------------------ the shells

# Deduplicated by the file the name resolves to. On Debian /bin/sh IS dash, and
# counting them as two would let the shell floor below be met without a second
# shell implementation ever running -- the floor's whole subject. The symlink
# itself has to be followed for that, not only its directory, and readlink -f is
# GNU-only, so python3 (already required above) resolves it.
shell_specs=()
shell_names=()
seen=""
add_shell() {
	local label="$1" path="$2" real
	[ -x "$path" ] || return 0
	real="$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$path")"
	case " $seen " in
	*" $real "*) return 0 ;;
	esac
	seen="$seen $real"
	shell_specs+=("$path")
	shell_names+=("$label")
}
# PTAH_INSTALL_TEST_SHELLS replaces discovery with an explicit list. It exists
# for check-install-script-selftest.sh, which runs this gate once per mutation
# and is measuring the gate's assertions rather than the shell matrix. A CI job
# must not set it: the matrix is the point there, which is what MIN_SHELLS
# guards. The shell it names has to be one where PATH decides, or the floor
# below refuses the run.
if [ -n "${PTAH_INSTALL_TEST_SHELLS:-}" ]; then
	for candidate in $PTAH_INSTALL_TEST_SHELLS; do
		shell_specs+=("$candidate")
		shell_names+=("$candidate")
	done
	min_shells="${PTAH_INSTALL_TEST_MIN_SHELLS:-1}"
fi
if [ "${#shell_specs[@]}" -eq 0 ]; then
add_shell sh /bin/sh
for candidate in dash ash mksh ksh; do
	found="$(command -v "$candidate" || true)"
	[ -n "$found" ] && add_shell "$candidate" "$found"
done
busybox_path="$(command -v busybox || true)"
if [ -n "$busybox_path" ]; then
	# busybox ash is one applet of one binary, so the dedup above cannot see it
	# as distinct. It is appended directly.
	shell_specs+=("$busybox_path ash")
	shell_names+=("busybox ash")
fi
fi

# Which of those shells let PATH decide what exists.
#
# busybox's shell resolves its own applets BEFORE consulting PATH. Measured on
# ubuntu 24.04 with busybox-static: under `PATH=/nonexistent`, `busybox ash`
# still finds wget, tar, sha256sum and uname, and it finds them in preference to
# a directory placed at the front of PATH. Four cases below need a tool to be
# absent or shadowed, and under such a shell neither is achievable from outside
# the process -- the installer's own `command -v` answers before PATH is read.
#
# So those four run under the shells where PATH decides, and are reported as not
# run under the others. A skip nobody counts is a pass, which is why the floor
# below refuses a run in which no shell could have made them.
shell_path_authoritative=()
path_authoritative_count=0
for entry in "${shell_specs[@]}"; do
	read -r -a probe_argv <<<"$entry"
	if "${probe_argv[@]}" -c 'PATH=/ptah-install-check-no-such-directory
command -v tar >/dev/null 2>&1 ||
	command -v wget >/dev/null 2>&1 ||
	command -v sha256sum >/dev/null 2>&1 ||
	command -v uname >/dev/null 2>&1' 2>/dev/null; then
		shell_path_authoritative+=(false)
	else
		shell_path_authoritative+=(true)
		path_authoritative_count=$((path_authoritative_count + 1))
	fi
done

if [ "$path_authoritative_count" -eq 0 ]; then
	echo "check-install-script: every shell found here carries its own applets, so the four" >&2
	echo "  cases that need a tool to be MISSING could not run under any of them. They would" >&2
	echo "  have been reported as not run, and a suite that reports nothing is not a pass." >&2
	exit 1
fi

if [ "${#shell_specs[@]}" -lt "$min_shells" ]; then
	echo "check-install-script: found ${#shell_specs[@]} POSIX shell(s), expected at least $min_shells" >&2
	echo "  a bashism is invisible under the /bin/sh a maintainer's macOS ships; install dash" >&2
	echo "  and busybox, or lower PTAH_INSTALL_TEST_MIN_SHELLS and say why" >&2
	exit 1
fi

# ------------------------------------------------------------------ assertions

out="$work/out"
err="$work/err"
status=0

# invoke runs the installer under the shell in $shell_argv. Every variable the
# installer recognizes is cleared first, so a value exported into this process
# cannot silently decide a case.
shell_argv=()
invoke() {
	status=0
	env -u PTAH_INSTALL_VERSION -u PTAH_INSTALL_DIR -u PTAH_INSTALL_BINARIES \
		-u PTAH_INSTALL_NO_MODIFY_PATH -u PTAH_INSTALL_VERIFY_SIGNATURE \
		-u PTAH_INSTALL_DRY_RUN -u PTAH_INSTALL_QUIET -u PTAH_INSTALL_BASE_URL \
		-u GITHUB_PATH -u XDG_BIN_HOME \
		HOME="$work/home" TMPDIR="$work/tmp" \
		"$@" >"$out" 2>"$err" || status=$?
}

fresh_bindir() {
	rm -rf "${work:?}/bin" "${work:?}/home" "${work:?}/tmp"
	mkdir -p "$work/bin" "$work/home" "$work/tmp"
	printf '%s\n' "$work/bin"
}

# invoke_deferred starts the installer under a wrapper that first publishes the
# process id it will run as, then waits for a go file, then execs. exec keeps
# the pid, so $$ inside the installer is the number printed before it started.
#
# That is what makes case_symlink_in_bindir deterministic rather than a race: it
# learns the pid, plants what it wants at the names that pid produces, and only
# then lets the run proceed. It works because a process id is public, which is
# the property the case is about.
#
# WRAPPER is a program the shell under test runs, so the process that execs the
# installer is that shell rather than this one. Every expansion in it belongs to
# that shell, which is why it is single-quoted here.
# shellcheck disable=SC2016
WRAPPER='printf "%s\n" "$$" >"$1"
while [ ! -e "$2" ]; do sleep 0.05; done
shift 2
exec "$@"'
deferred_pid=""
deferred_job=""
invoke_deferred() {
	deferred_pid=""
	deferred_job=""
	rm -f "$work/pid" "$work/go"
	env -u PTAH_INSTALL_VERSION -u PTAH_INSTALL_DIR -u PTAH_INSTALL_BINARIES \
		-u PTAH_INSTALL_NO_MODIFY_PATH -u PTAH_INSTALL_VERIFY_SIGNATURE \
		-u PTAH_INSTALL_DRY_RUN -u PTAH_INSTALL_QUIET -u PTAH_INSTALL_BASE_URL \
		-u GITHUB_PATH -u XDG_BIN_HOME \
		HOME="$work/home" TMPDIR="$work/tmp" \
		"$@" >"$out" 2>"$err" &
	deferred_job=$!
	for _ in $(seq 1 200); do
		if [ -s "$work/pid" ]; then
			deferred_pid="$(cat "$work/pid")"
			return 0
		fi
		sleep 0.05
	done
	kill "$deferred_job" >/dev/null 2>&1 || true
	report "$current / deferred: the wrapper never published the installer's pid"
	return 1
}

release_deferred() {
	: >"$work/go"
	status=0
	wait "$deferred_job" || status=$?
}

expect_status() {
	local want="$1" name="$2"
	checks=$((checks + 1))
	if [ "$status" -eq "$want" ]; then
		return 0
	fi
	report "$current / $name: exit $status, wanted $want"
	sed 's/^/      out| /' "$out" >&2
	sed 's/^/      err| /' "$err" >&2
	return 1
}

expect_contains() {
	local file="$1" text="$2" name="$3"
	checks=$((checks + 1))
	if grep -qF -- "$text" "$file"; then
		return 0
	fi
	report "$current / $name: no line containing \"$text\""
	sed 's/^/      | /' "$file" >&2
	return 1
}

expect_absent() {
	local path="$1" name="$2"
	checks=$((checks + 1))
	if [ ! -e "$path" ]; then
		return 0
	fi
	report "$current / $name: $path exists and should not"
	return 1
}

expect_empty_dir() {
	local dir="$1" name="$2" listing
	checks=$((checks + 1))
	listing="$(ls -A "$dir" 2>/dev/null || true)"
	if [ -z "$listing" ]; then
		return 0
	fi
	report "$current / $name: $dir is not empty: $listing"
	return 1
}

# ---------------------------------------------------------------------- cases

case_help() {
	fresh_bindir >/dev/null
	invoke "${shell_argv[@]}" "$script" --help
	expect_status 0 "--help exits 0" || return 0
	expect_contains "$out" "PTAH_INSTALL_VERSION" "--help names the environment twins" || return 0
	expect_contains "$out" "6  integrity failure" "--help documents the exit codes" || return 0
	pass "--help"
}

case_unknown_option() {
	invoke "${shell_argv[@]}" "$script" --colour
	expect_status 2 "an unknown option exits 2" || return 0
	expect_contains "$err" "unknown option: --colour" "the message names the option" || return 0
	pass "unknown option"
}

case_install() {
	local bindir name
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script"
	expect_status 0 "a clean install exits 0" || return 0
	expect_contains "$out" "resolved latest to v0.2.0" "latest resolves from the fixture" || return 0
	expect_contains "$out" "sha256 verified against checksums.txt" "the archive is verified" || return 0
	for name in ptah ptah-compat ptah-ls; do
		checks=$((checks + 1))
		if [ ! -x "$bindir/$name" ]; then
			report "$current / install: $name is missing or not executable"
			return 0
		fi
		checks=$((checks + 1))
		if [ "$("$bindir/$name" version)" != "Version: 0.2.0
binary: $name" ]; then
			report "$current / install: $bindir/$name did not run and report 0.2.0"
			return 0
		fi
	done
	expect_contains "$out" "Version: 0.2.0" "the install ends by running ptah" || return 0
	# The archive holds LICENSE and README.md beside the binaries; unpacking it
	# into the install directory instead of picking the programs out of it would
	# leave those there too.
	expect_absent "$bindir/README.md" "only the binaries are installed" || return 0
	expect_empty_dir "$work/tmp" "the temporary directory is removed" || return 0
	pass "install, all three binaries land and run"
}

case_install_twice() {
	local bindir
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" "${shell_argv[@]}" "$script"
	expect_status 0 "the first install exits 0" || return 0
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" "${shell_argv[@]}" "$script"
	expect_status 0 "the second install exits 0" || return 0
	expect_contains "$out" "replacing $bindir/ptah" "an overwrite is announced" || return 0
	checks=$((checks + 1))
	if [ "$("$bindir/ptah" version | head -n 1)" != "Version: 0.2.0" ]; then
		report "$current / twice: ptah stopped running after the second install"
		return 0
	fi
	pass "running twice is safe and says what it replaced"
}

case_pinned_version() {
	local bindir
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script" --version v0.1.0
	expect_status 0 "a pinned install exits 0" || return 0
	checks=$((checks + 1))
	if [ "$("$bindir/ptah" version | head -n 1)" != "Version: 0.1.0" ]; then
		report "$current / pinned: installed $("$bindir/ptah" version | head -n 1), wanted 0.1.0"
		return 0
	fi
	checks=$((checks + 1))
	if grep -qF "resolved latest" "$out"; then
		report "$current / pinned: a pinned run still resolved latest"
		return 0
	fi

	# The tag takes the v and the asset name does not, so both spellings have to
	# arrive at the same release.
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script" --version 0.1.0
	expect_status 0 "a version without the v exits 0" || return 0
	checks=$((checks + 1))
	if [ "$("$bindir/ptah" version | head -n 1)" != "Version: 0.1.0" ]; then
		report "$current / pinned: 0.1.0 without the v installed something else"
		return 0
	fi

	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		PTAH_INSTALL_VERSION=v0.1.0 "${shell_argv[@]}" "$script"
	expect_status 0 "PTAH_INSTALL_VERSION exits 0" || return 0
	checks=$((checks + 1))
	if [ "$("$bindir/ptah" version | head -n 1)" != "Version: 0.1.0" ]; then
		report "$current / pinned: PTAH_INSTALL_VERSION installed something else"
		return 0
	fi
	pass "--version, a bare number, and PTAH_INSTALL_VERSION all pin"
}

case_checksum_mismatch() {
	local bindir
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script" --version v0.3.0
	expect_status 6 "a corrupted archive exits 6" || return 0
	expect_contains "$err" "checksum mismatch for ptah_0.3.0_" "the message names the archive" || return 0
	expect_contains "$err" "want " "the refusal prints the wanted digest" || return 0
	expect_contains "$err" "got  " "the refusal prints the computed digest" || return 0
	expect_empty_dir "$bindir" "a refused install writes no binary" || return 0
	expect_empty_dir "$work/tmp" "a refused install leaves no temporary directory" || return 0
	pass "a corrupted archive is refused and nothing is installed"
}

case_checksum_absent() {
	local bindir
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script" --version v0.4.0
	expect_status 6 "an archive with no checksum line exits 6" || return 0
	expect_contains "$err" "checksums.txt has no entry for ptah_0.4.0_" "the message names the archive" || return 0
	expect_empty_dir "$bindir" "nothing is installed" || return 0
	pass "an archive checksums.txt does not mention is refused"
}

case_unsupported_platform() {
	local bindir
	bindir="$(fresh_bindir)"
	invoke PATH="$work/fakeuname:$PATH" PTAH_INSTALL_BASE_URL="$base_url" \
		PTAH_INSTALL_DIR="$bindir" "${shell_argv[@]}" "$script"
	expect_status 3 "an unsupported platform exits 3" || return 0
	expect_contains "$err" "no Ptah release for this platform" "the refusal says so" || return 0
	expect_contains "$err" "uname -s = FreeBSD" "the refusal names uname -s" || return 0
	expect_contains "$err" "uname -m = riscv64" "the refusal names uname -m" || return 0
	expect_empty_dir "$bindir" "nothing is installed" || return 0
	pass "an unsupported platform is refused, naming both uname answers"
}

case_missing_hasher() {
	local bindir path
	bindir="$(fresh_bindir)"
	path="$(restricted_path nohash curl tar uname)"
	invoke PATH="$path" PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script"
	expect_status 4 "no sha256 tool exits 4" || return 0
	expect_contains "$err" "refusing to install an unverified archive" "the refusal says why" || return 0
	expect_empty_dir "$bindir" "nothing is installed unverified" || return 0
	pass "a machine with no sha256 tool is refused, not served unverified"
}

case_missing_tar() {
	local bindir path
	bindir="$(fresh_bindir)"
	path="$(restricted_path notar curl uname "$host_hasher")"
	invoke PATH="$path" PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script"
	expect_status 4 "no tar exits 4" || return 0
	expect_contains "$err" "need tar to unpack" "the refusal names tar" || return 0
	pass "a machine with no tar is refused"
}

case_missing_downloader() {
	local bindir path
	bindir="$(fresh_bindir)"
	path="$(restricted_path nodl tar uname "$host_hasher")"
	invoke PATH="$path" PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script"
	expect_status 4 "no curl and no wget exits 4" || return 0
	expect_contains "$err" "need curl or wget" "the refusal names both" || return 0
	pass "a machine with neither curl nor wget is refused"
}

case_signature_without_cosign() {
	local bindir path
	bindir="$(fresh_bindir)"
	# A restricted PATH rather than this machine's, so the case says the same
	# thing whether or not cosign happens to be installed on the runner.
	path="$(restricted_path nocosign curl tar uname "$host_hasher")"
	invoke PATH="$path" PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script" --verify-signature
	expect_status 4 "--verify-signature without cosign exits 4" || return 0
	expect_contains "$err" "needs cosign on PATH" "the refusal names cosign" || return 0
	expect_empty_dir "$bindir" "nothing is installed" || return 0
	# The refusal belongs with the other prerequisite checks. Reaching for cosign
	# at verification time would refuse the run after the archive was downloaded
	# for it.
	expect_empty_dir "$work/tmp" "the refusal happens before the download" || return 0
	pass "--verify-signature without cosign is refused before the download"
}

case_release_not_found() {
	local bindir
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script" --version v9.9.9
	expect_status 5 "a release that does not exist exits 5" || return 0
	expect_contains "$err" "no release v9.9.9" "the refusal names the version" || return 0
	# The actionable half of the sentence, pinned separately. "failed to
	# download <url>" is also an exit 5 naming the version, and it is what the
	# reader got when the classification below stopped firing.
	expect_contains "$err" "check the version" "the refusal says what to do about it" || return 0
	expect_empty_dir "$bindir" "nothing is installed" || return 0
	pass "a release that does not exist is refused"
}

# The same refusal, with curl reporting the exit code it reports over HTTP/2.
#
# curl -f aborts on a 404 either way, but it exits 22 over HTTP/1.1 and 56 over
# HTTP/2 -- and github.com serves HTTP/2, so every macOS reader who asked for a
# version that does not exist was told "failed to download <url>" instead. The
# fixture server speaks HTTP/1.1 only and therefore always produced the 22 that
# the old classification recognized; this case supplies the other answer.
case_release_not_found_http2() {
	local bindir path
	bindir="$(fresh_bindir)"
	path="$(curl_http2_shim)"
	invoke PATH="$path:$PATH" PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script" --version v9.9.9
	expect_status 5 "a 404 reported as exit 56 still exits 5" || return 0
	expect_contains "$err" "no release v9.9.9" "the refusal names the version" || return 0
	expect_contains "$err" "check the version" "the refusal is the actionable one, not the generic one" || return 0
	expect_empty_dir "$bindir" "nothing is installed" || return 0
	pass "a 404 is classified from the status line, not from curl's exit code"
}

# A symlink planted in the install directory must not be followed.
#
# The names the installer writes there used to be built from its process id --
# `.ptah-install-probe.$$` and `.ptah-install-$$-ptah` -- and neither was
# created with O_EXCL. A process id is public, so anyone else able to write to
# the install directory could put a symlink at both names: the probe truncated
# whatever it pointed at, `cp` wrote the binary through it, and `mv` renamed the
# symlink into place. The run exited 0 and reported three binaries installed,
# one of which was a link to a file outside the directory.
#
# The default install directory is one only its owner writes. --bin-dir is what
# makes this reachable: it exists to name a shared one.
case_symlink_in_bindir() {
	local bindir victim
	bindir="$(fresh_bindir)"
	victim="$work/victim"
	printf 'do not overwrite me\n' >"$victim"

	invoke_deferred PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" -c "$WRAPPER" _ "$work/pid" "$work/go" \
		"${shell_argv[@]}" "$script" || return 0
	ln -s "$victim" "$bindir/.ptah-install-probe.$deferred_pid"
	ln -s "$victim" "$bindir/.ptah-install-$deferred_pid-ptah"
	release_deferred

	expect_status 0 "the install still succeeds" || return 0
	checks=$((checks + 1))
	if [ "$(cat "$victim")" != "do not overwrite me" ]; then
		report "$current / symlink: a symlink in $bindir was written through to $victim"
		return 0
	fi
	checks=$((checks + 1))
	if [ -L "$bindir/ptah" ]; then
		report "$current / symlink: $bindir/ptah is a symlink to $(readlink "$bindir/ptah")"
		return 0
	fi
	checks=$((checks + 1))
	if [ "$("$bindir/ptah" version | head -n 1)" != "Version: 0.2.0" ]; then
		report "$current / symlink: the installed ptah did not run"
		return 0
	fi
	pass "a symlink planted at a predictable staging name is neither followed nor renamed into place"
}

case_unwritable_bindir() {
	local bindir
	fresh_bindir >/dev/null
	bindir="$work/readonly"
	rm -rf "$bindir"
	mkdir -p "$bindir"
	chmod 0555 "$bindir"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" "${shell_argv[@]}" "$script"
	chmod 0755 "$bindir"
	expect_status 7 "an unwritable install directory exits 7" || return 0
	expect_contains "$err" "cannot write to $bindir" "the refusal names the directory" || return 0
	# Before the transfer, not after it: the download is tens of megabytes and
	# there is nothing to do with it once the destination is known to be unusable.
	expect_empty_dir "$work/tmp" "the refusal happens before anything is downloaded" || return 0
	pass "an unwritable install directory is refused before the download"
}

# The tilde and the $HOME in this function are the installer's OUTPUT, quoted so
# they stay that way; neither is an expansion this script wanted.
# shellcheck disable=SC2088,SC2016
case_default_location() {
	local home
	fresh_bindir >/dev/null
	home="$work/home"
	invoke PTAH_INSTALL_BASE_URL="$base_url" "${shell_argv[@]}" "$script"
	expect_status 0 "the default location install exits 0" || return 0
	checks=$((checks + 1))
	if [ ! -x "$home/.local/bin/ptah" ]; then
		report "$current / default: nothing landed in \$HOME/.local/bin"
		return 0
	fi
	expect_contains "$out" "in ~/.local/bin" "the install location is reported in its ~ form" || return 0
	expect_contains "$out" "~/.local/bin is not on your PATH" "the PATH consequence is reported" || return 0
	# $HOME rather than ~ on the line the reader pastes: a tilde does not expand
	# inside the double quotes, so the ~ form would set a PATH entry named "~".
	expect_contains "$out" 'export PATH="$HOME/.local/bin:$PATH"' "the line to paste is correct" || return 0
	# Reporting, not editing. Nothing under $HOME but the install directory may
	# change, which is the single loudest complaint against installers read from
	# a pipe.
	expect_absent "$home/.bashrc" "no shell startup file is created" || return 0
	expect_absent "$home/.zshrc" "no shell startup file is created" || return 0
	expect_absent "$home/.profile" "no shell startup file is created" || return 0
	pass "the default is \$HOME/.local/bin, and PATH is reported rather than edited"
}

case_only_subset() {
	local bindir
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script" --only ptah,ptah-ls
	expect_status 0 "--only exits 0" || return 0
	checks=$((checks + 1))
	if [ ! -x "$bindir/ptah" ] || [ ! -x "$bindir/ptah-ls" ]; then
		report "$current / only: the requested binaries are missing"
		return 0
	fi
	expect_absent "$bindir/ptah-compat" "an unrequested binary is not installed" || return 0

	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script" --only ptah,nope
	expect_status 2 "an unknown name in --only exits 2" || return 0
	expect_contains "$err" 'unknown binary "nope"' "the refusal names it" || return 0
	pass "--only installs the subset and refuses a name that is not one"
}

case_booleans() {
	local bindir
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		PTAH_INSTALL_DRY_RUN=ture "${shell_argv[@]}" "$script"
	expect_status 2 "a misspelled boolean exits 2" || return 0
	expect_contains "$err" 'invalid boolean value "ture" for PTAH_INSTALL_DRY_RUN' "the refusal names both" || return 0
	expect_empty_dir "$bindir" "a misspelled boolean installs nothing" || return 0

	# An exported empty value is the shape a typo in a CI environment file takes,
	# and os.Getenv cannot tell it from an absent variable. This one can.
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		PTAH_INSTALL_QUIET= "${shell_argv[@]}" "$script"
	expect_status 2 "an exported empty boolean exits 2" || return 0

	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR= "${shell_argv[@]}" "$script"
	expect_status 2 "an exported empty value exits 2" || return 0
	expect_contains "$err" "PTAH_INSTALL_DIR is set to an empty value" "the refusal names it" || return 0

	# The accepted spellings still work, or the rule above could be satisfied by
	# rejecting everything.
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		PTAH_INSTALL_DRY_RUN=YES "${shell_argv[@]}" "$script"
	expect_status 0 "PTAH_INSTALL_DRY_RUN=YES is accepted" || return 0
	expect_empty_dir "$bindir" "a dry run writes nothing" || return 0
	pass "boolean variables are strict, and the documented spellings still work"
}

case_dry_run() {
	local bindir
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$script" --dry-run
	expect_status 0 "--dry-run exits 0" || return 0
	expect_contains "$out" "would install" "the plan is printed" || return 0
	expect_empty_dir "$bindir" "--dry-run writes no binary" || return 0
	expect_empty_dir "$work/tmp" "--dry-run downloads nothing" || return 0
	pass "--dry-run prints the plan and writes nothing"
}

case_github_path() {
	local bindir
	bindir="$(fresh_bindir)"
	: >"$work/github_path"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		GITHUB_PATH="$work/github_path" "${shell_argv[@]}" "$script"
	expect_status 0 "a run inside a job exits 0" || return 0
	expect_contains "$work/github_path" "$bindir" "the install directory is appended" || return 0
	expect_contains "$out" "added $bindir to \$GITHUB_PATH" "the append is announced" || return 0

	bindir="$(fresh_bindir)"
	: >"$work/github_path"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		GITHUB_PATH="$work/github_path" "${shell_argv[@]}" "$script" --no-modify-path
	expect_status 0 "--no-modify-path exits 0" || return 0
	checks=$((checks + 1))
	if [ -s "$work/github_path" ]; then
		report "$current / github_path: --no-modify-path appended anyway"
		return 0
	fi
	pass "\$GITHUB_PATH is appended inside a job, and --no-modify-path stops it"
}

case_truncated_pipe() {
	local bindir head_bytes
	bindir="$(fresh_bindir)"
	# Everything that writes is inside a function and the only top-level call is
	# the last line, so a transfer cut short cannot half-install. Half the file
	# is the worst case: enough to define most of the program, not enough to
	# reach the call.
	head_bytes=$(($(wc -c <"$script") / 2))
	head -c "$head_bytes" "$script" >"$work/truncated.sh"
	invoke PTAH_INSTALL_BASE_URL="$base_url" PTAH_INSTALL_DIR="$bindir" \
		"${shell_argv[@]}" "$work/truncated.sh"
	expect_empty_dir "$bindir" "a truncated script installs nothing" || return 0
	expect_empty_dir "$work/tmp" "a truncated script downloads nothing" || return 0
	pass "a pipe cut in half installs nothing"
}

# run_suite takes whether PATH decides what this shell can find. The four cases
# that need a tool to be absent or shadowed are the only ones that care, and
# they are named individually rather than skipped as a block so that a reader of
# the output can see exactly what did not run here.
run_suite() {
	local path_decides="$1"
	case_help
	case_unknown_option
	case_install
	case_install_twice
	case_pinned_version
	case_checksum_mismatch
	case_checksum_absent
	if [ "$path_decides" = true ]; then
		case_unsupported_platform
		case_missing_hasher
		case_missing_tar
		case_missing_downloader
	else
		printf '    --    an unsupported platform, and the three missing prerequisites:\n'
		printf '          this shell answers command -v from its own applets before reading PATH\n'
	fi
	case_release_not_found
	case_release_not_found_http2
	case_symlink_in_bindir
	case_signature_without_cosign
	case_unwritable_bindir
	case_default_location
	case_only_subset
	case_booleans
	case_dry_run
	case_github_path
	case_truncated_pipe
}

# ------------------------------------------------------------------- the runs

echo "check-install-script: $script"

echo "  shellcheck -s sh"
if shellcheck -s sh "$script"; then
	pass "no bashism"
else
	report "shellcheck rejected the installer"
fi

index=0
for entry in "${shell_specs[@]}"; do
	# The one two-word entry is "busybox ash", an applet rather than a file.
	read -r -a shell_argv <<<"$entry"
	current="${shell_names[$index]}"
	path_decides="${shell_path_authoritative[$index]}"
	index=$((index + 1))
	echo "  under $current ($entry)"
	run_suite "$path_decides"
done

if [ "$live" = true ]; then
	current="live"
	shell_argv=(/bin/sh)
	echo "  against the published release"

	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_DIR="$bindir" "${shell_argv[@]}" "$script" --version v0.2.0
	expect_status 0 "the published v0.2.0 installs" || true
	expect_contains "$out" "sha256 verified against checksums.txt" "the published archive verifies" || true
	for name in ptah ptah-compat ptah-ls; do
		checks=$((checks + 1))
		if ! "$bindir/$name" version </dev/null >/dev/null 2>&1; then
			report "live: $bindir/$name did not run"
		fi
	done
	pass "v0.2.0 installs from the published release and all three binaries run"

	# The refusal for a version that does not exist, over the protocol
	# github.com actually serves. The fixture suite covers this too, but only
	# by simulating curl's HTTP/2 exit code; this is the real one. curl 8.7.1
	# on macOS reports 56 here and Debian's curl 7.88.1 reports 22, and the
	# sentence has to be the same either way.
	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_DIR="$bindir" "${shell_argv[@]}" "$script" --version v9.9.9
	expect_status 5 "a release that does not exist exits 5 against github.com" || true
	expect_contains "$err" "no release v9.9.9 at https://github.com/stokaro/ptah/releases" \
		"the refusal names the release page" || true
	expect_contains "$err" "check the version" "the refusal is the actionable one over HTTP/2" || true
	expect_empty_dir "$bindir" "nothing is installed" || true
	pass "a version github.com does not have is refused with the actionable sentence"

	bindir="$(fresh_bindir)"
	invoke PTAH_INSTALL_DIR="$bindir" "${shell_argv[@]}" "$script" --dry-run
	expect_status 0 "resolving latest against GitHub exits 0" || true
	checks=$((checks + 1))
	if ! grep -qE 'resolved latest to v[0-9]' "$out"; then
		report "live: latest did not resolve to a version-looking tag"
		sed 's/^/      | /' "$out" >&2
	fi
	pass "latest resolves against the published releases page"
fi

echo
if [ "$failures" -ne 0 ]; then
	echo "check-install-script: $failures of $checks assertions failed" >&2
	exit 1
fi
echo "check-install-script: OK ($checks assertions, ${#shell_specs[@]} shell(s): ${shell_names[*]};" \
	"$path_authoritative_count of them let PATH decide what exists)"
