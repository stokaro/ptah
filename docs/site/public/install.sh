#!/bin/sh
#
# Install Ptah.
#
#   curl -fsSL https://stokaro.github.io/ptah/install.sh | sh
#   curl -fsSL https://stokaro.github.io/ptah/install.sh | sh -s -- --version v0.2.0
#   wget -qO- https://stokaro.github.io/ptah/install.sh | sh
#
# What happens, in order: resolve a release, prove the install directory is
# writable, download the archive and checksums.txt into a temporary directory,
# verify the archive's SHA-256 against checksums.txt, unpack, and move the
# binaries into place as the last step.
#
# What does not happen: no shell startup file is edited, no sudo is invoked, and
# nothing is installed that failed verification. The only file written outside
# the install directory is an append to $GITHUB_PATH inside a GitHub Actions
# job, which --no-modify-path turns off.
#
# Verification cannot turn itself off. If none of sha256sum, shasum or openssl
# is present the script exits 4 rather than installing an unverified archive.
# That is a deliberate divergence from installers that print "skipping sha256
# checksum verification" and continue: a check that disables itself when its
# tool is missing reports the same success as a check that ran.
#
# POSIX sh. `| sh` is dash on Debian and Ubuntu and busybox ash on Alpine, so
# there is no [[ ]], no array, no `set -o pipefail`, no `echo -e` and no
# `read -p` here. `local` is the one deliberate non-POSIX extension, as rustup's
# and uv's installers also do, and the alias below covers a ksh without it.
#
# Every step that writes anything is inside a function, and the only top-level
# call is the last line of the file. A pipe truncated mid-transfer therefore
# either does nothing or fails to parse; it cannot half-install.

# shellcheck disable=SC3043 # `local` is the deliberate extension described above.

set -eu

has_local() { local _has_local=1; }
has_local 2>/dev/null || alias local=typeset

PTAH_RELEASES_URL="https://github.com/stokaro/ptah/releases"
PTAH_ALL_BINARIES="ptah ptah-compat ptah-ls"

# Set by main before anything reads them, so `set -u` catches a missing one.
base_url=""
bindir=""
os=""
arch=""
version_tag=""
version_number=""
asset=""
selected=""
downloader=""
hasher=""
tmpdir=""
staging_file=""
resolved_latest=false
opt_version=""
opt_bindir=""
opt_only=""
opt_no_modify_path=false
opt_verify_signature=false
opt_dry_run=false
opt_quiet=false
BOOL_RESULT=""
VALUE_RESULT=""
DOWNLOAD_ERROR=""

usage() {
	cat <<'USAGE'
install.sh -- install the Ptah command-line binaries

Usage:
  curl -fsSL https://stokaro.github.io/ptah/install.sh | sh
  curl -fsSL https://stokaro.github.io/ptah/install.sh | sh -s -- [options]

Options:
  --version <tag>    release to install, such as v0.2.0 (default: the latest)
  --bin-dir <dir>    where to put the binaries (default: ~/.local/bin)
  --only <list>      comma-separated subset of ptah,ptah-compat,ptah-ls
  --no-modify-path   do not append the install directory to $GITHUB_PATH
  --verify-signature also verify checksums.txt with cosign; fails without cosign
  --dry-run          print what would happen, download nothing, write nothing
  --quiet            report errors only
  -h, --help         print this and exit

Every option has an environment twin, because a script read from a pipe cannot
always be given arguments:

  PTAH_INSTALL_VERSION            --version
  PTAH_INSTALL_DIR                --bin-dir
  PTAH_INSTALL_BINARIES           --only
  PTAH_INSTALL_NO_MODIFY_PATH     --no-modify-path
  PTAH_INSTALL_VERIFY_SIGNATURE   --verify-signature
  PTAH_INSTALL_DRY_RUN            --dry-run
  PTAH_INSTALL_QUIET              --quiet
  PTAH_INSTALL_BASE_URL           release root (default: the GitHub releases page)

A boolean variable takes 1, 0, true, false, yes or no, in any case. Anything
else is a configuration error and stops the run; an exported empty value is one
too, rather than a silent default.

Exit codes:
  0  installed
  1  unexpected failure
  2  usage or configuration error
  3  unsupported operating system or architecture
  4  a prerequisite is missing
  5  download failure
  6  integrity failure
  7  the install directory cannot be used
USAGE
}

say() {
	if [ "$opt_quiet" = false ]; then
		printf 'ptah: %s\n' "$*"
	fi
}

fail() {
	local code="$1"
	shift
	printf 'ptah: %s\n' "$*" >&2
	exit "$code"
}

fail_usage() {
	printf 'ptah: %s\n' "$*" >&2
	usage >&2
	exit 2
}

have() {
	command -v "$1" >/dev/null 2>&1
}

# cleanup removes the temporary directory and any staging file left in the
# install directory. It runs on a normal exit and on an interrupt, so an
# abandoned run leaves neither a 28 MB archive in /tmp nor a partial binary
# beside the real one.
#
# The staging file is removed by the name mktemp chose, not by a glob: there is
# exactly one at a time and its name is not derivable from outside the process,
# which is the same property that keeps a planted symlink from being written
# through. A glob would have to be guessable to be writable.
cleanup() {
	if [ -n "$tmpdir" ] && [ -d "$tmpdir" ]; then
		rm -rf "$tmpdir"
	fi
	if [ -n "$staging_file" ]; then
		rm -f "$staging_file" 2>/dev/null || true
	fi
	return 0
}

# parse_bool writes true or false to BOOL_RESULT, or stops the run. It never
# runs inside a command substitution, because an exit from a subshell would
# leave the caller running with a value nobody validated.
parse_bool() {
	local name="$1" value="$2" lowered
	lowered="$(printf '%s' "$value" | tr 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' 'abcdefghijklmnopqrstuvwxyz')"
	case "$lowered" in
	1 | true | yes) BOOL_RESULT=true ;;
	0 | false | no) BOOL_RESULT=false ;;
	*) fail 2 "invalid boolean value \"$value\" for $name; use 1, 0, true, false, yes or no" ;;
	esac
}

# env_bool takes the variable's name, its presence marker and its value
# separately, so that an exported empty value is distinguishable from an absent
# one without an eval. `${VAR+set}` is empty only when VAR is unset.
env_bool() {
	local name="$1" presence="$2" value="$3" default="$4"
	if [ -z "$presence" ]; then
		BOOL_RESULT="$default"
		return 0
	fi
	parse_bool "$name" "$value"
}

env_value() {
	local name="$1" presence="$2" value="$3"
	VALUE_RESULT=""
	if [ -z "$presence" ]; then
		return 0
	fi
	if [ -z "$value" ]; then
		fail 2 "$name is set to an empty value; unset it to take the default"
	fi
	VALUE_RESULT="$value"
}

parse_args() {
	while [ "$#" -gt 0 ]; do
		case "$1" in
		-h | --help)
			usage
			exit 0
			;;
		--version)
			shift
			[ "$#" -gt 0 ] || fail_usage "--version needs a release, for example --version v0.2.0"
			opt_version="$1"
			;;
		--version=*) opt_version="${1#--version=}" ;;
		--bin-dir)
			shift
			[ "$#" -gt 0 ] || fail_usage "--bin-dir needs a directory"
			opt_bindir="$1"
			;;
		--bin-dir=*) opt_bindir="${1#--bin-dir=}" ;;
		--only)
			shift
			[ "$#" -gt 0 ] || fail_usage "--only needs a comma-separated list"
			opt_only="$1"
			;;
		--only=*) opt_only="${1#--only=}" ;;
		--no-modify-path) opt_no_modify_path=true ;;
		--verify-signature) opt_verify_signature=true ;;
		--dry-run) opt_dry_run=true ;;
		--quiet) opt_quiet=true ;;
		*) fail_usage "unknown option: $1" ;;
		esac
		shift
	done
}

# read_environment resolves every variable this script recognizes, before any
# network or filesystem work. A malformed value in a CI environment file must
# stop the run it was exported for, not lie dormant until the branch that reads
# it happens to be taken.
read_environment() {
	env_bool PTAH_INSTALL_QUIET "${PTAH_INSTALL_QUIET+set}" "${PTAH_INSTALL_QUIET-}" false
	if [ "$opt_quiet" = false ]; then opt_quiet="$BOOL_RESULT"; fi

	env_bool PTAH_INSTALL_DRY_RUN "${PTAH_INSTALL_DRY_RUN+set}" "${PTAH_INSTALL_DRY_RUN-}" false
	if [ "$opt_dry_run" = false ]; then opt_dry_run="$BOOL_RESULT"; fi

	env_bool PTAH_INSTALL_NO_MODIFY_PATH "${PTAH_INSTALL_NO_MODIFY_PATH+set}" "${PTAH_INSTALL_NO_MODIFY_PATH-}" false
	if [ "$opt_no_modify_path" = false ]; then opt_no_modify_path="$BOOL_RESULT"; fi

	env_bool PTAH_INSTALL_VERIFY_SIGNATURE "${PTAH_INSTALL_VERIFY_SIGNATURE+set}" "${PTAH_INSTALL_VERIFY_SIGNATURE-}" false
	if [ "$opt_verify_signature" = false ]; then opt_verify_signature="$BOOL_RESULT"; fi

	env_value PTAH_INSTALL_VERSION "${PTAH_INSTALL_VERSION+set}" "${PTAH_INSTALL_VERSION-}"
	if [ -z "$opt_version" ]; then opt_version="$VALUE_RESULT"; fi

	env_value PTAH_INSTALL_DIR "${PTAH_INSTALL_DIR+set}" "${PTAH_INSTALL_DIR-}"
	if [ -z "$opt_bindir" ]; then opt_bindir="$VALUE_RESULT"; fi

	env_value PTAH_INSTALL_BINARIES "${PTAH_INSTALL_BINARIES+set}" "${PTAH_INSTALL_BINARIES-}"
	if [ -z "$opt_only" ]; then opt_only="$VALUE_RESULT"; fi

	env_value PTAH_INSTALL_BASE_URL "${PTAH_INSTALL_BASE_URL+set}" "${PTAH_INSTALL_BASE_URL-}"
	base_url="$VALUE_RESULT"
	if [ -z "$base_url" ]; then base_url="$PTAH_RELEASES_URL"; fi
	base_url="${base_url%/}"
}

resolve_selection() {
	local requested name known
	requested="$opt_only"
	if [ -z "$requested" ]; then
		selected="$PTAH_ALL_BINARIES"
		return 0
	fi
	selected=""
	for name in $(printf '%s' "$requested" | tr ',' ' '); do
		known=false
		case " $PTAH_ALL_BINARIES " in
		*" $name "*) known=true ;;
		esac
		if [ "$known" = false ]; then
			fail 2 "unknown binary \"$name\"; --only takes a subset of $(printf '%s' "$PTAH_ALL_BINARIES" | tr ' ' ',')"
		fi
		selected="$selected $name"
	done
	selected="${selected# }"
	[ -n "$selected" ] || fail 2 "--only was given an empty list"
}

# detect_platform refuses anything outside the four builds Ptah publishes. It
# does not fall through to a default: an installer that guesses an architecture
# produces a binary that fails at the first instruction rather than here.
detect_platform() {
	local raw_os raw_arch
	raw_os="$(uname -s)"
	raw_arch="$(uname -m)"

	case "$raw_os" in
	Linux) os=linux ;;
	Darwin) os=darwin ;;
	*) os="" ;;
	esac

	# Darwin reports i386 from a 32-bit process on a 64-bit machine, and macOS
	# has run nothing 32-bit since 10.15. Correcting that known answer is not
	# the same as guessing at an unknown one.
	if [ "$os" = darwin ] && { [ "$raw_arch" = i386 ] || [ "$raw_arch" = i686 ]; }; then
		raw_arch=x86_64
	fi

	case "$raw_arch" in
	x86_64 | amd64) arch=amd64 ;;
	aarch64 | arm64) arch=arm64 ;;
	*) arch="" ;;
	esac

	if [ -z "$os" ] || [ -z "$arch" ]; then
		fail 3 "no Ptah release for this platform (uname -s = $raw_os, uname -m = $(uname -m)); see $PTAH_RELEASES_URL"
	fi
}

detect_downloader() {
	if have curl; then
		downloader=curl
	elif have wget; then
		downloader=wget
	else
		fail 4 "need curl or wget to download Ptah"
	fi
}

detect_tar() {
	have tar || fail 4 "need tar to unpack the Ptah archive"
}

# detect_mktemp is a prerequisite check because three writes depend on it: the
# temporary directory, the writability probe, and the staging file each binary
# is copied to. Every one of those needs a name created with O_EXCL under a name
# nobody outside this process can predict, and mktemp is the only POSIX way to
# get one. It is checked here, with the other prerequisites, rather than being
# discovered at the first write.
detect_mktemp() {
	have mktemp || fail 4 "need mktemp to stage the Ptah binaries safely"
}

# detect_hasher picks the first sha256 tool present. sha256sum covers Linux and
# busybox; shasum covers every macOS, being Perl; openssl is the last resort and
# prints the digest as the final field under both LibreSSL ("SHA256(f)= h") and
# OpenSSL 3 ("SHA2-256(f)= h").
detect_hasher() {
	if have sha256sum; then
		hasher=sha256sum
	elif have shasum; then
		hasher=shasum
	elif have openssl; then
		hasher=openssl
	else
		fail 4 "need one of sha256sum, shasum or openssl to verify the download; refusing to install an unverified archive"
	fi
}

sha256_of() {
	case "$hasher" in
	sha256sum) sha256sum "$1" | awk '{print $1}' ;;
	shasum) shasum -a 256 "$1" | awk '{print $1}' ;;
	openssl) openssl dgst -sha256 "$1" | awk '{print $NF}' ;;
	esac
}

curl_flags() {
	if [ "$opt_quiet" = false ] && [ -t 2 ]; then
		printf -- '-fL --progress-bar'
	else
		printf -- '-fsSL'
	fi
}

# download writes the URL to a file. It reports whether the failure was an HTTP
# status the server chose -- which means "no such release" -- or anything else,
# which means the transfer did not happen. curl is asked for the status line
# itself; wget cannot be, so a wget run reports the generic failure.
download() {
	local url="$1" dest="$2" status=0 flags code=""
	DOWNLOAD_ERROR=""
	rm -f "$dest"
	if [ "$downloader" = curl ]; then
		flags="$(curl_flags)"
		case "$url" in
		https://*)
			# --proto pins the transfer, and its redirect, to https: a release
			# asset is served from a second host and the hop must not downgrade.
			# shellcheck disable=SC2086 # word splitting of the flag list is the point
			code="$(curl --proto '=https' --proto-redir '=https' --tlsv1.2 $flags -w '%{http_code}' -o "$dest" "$url")" || status=$?
			;;
		*)
			# shellcheck disable=SC2086 # word splitting of the flag list is the point
			code="$(curl $flags -w '%{http_code}' -o "$dest" "$url")" || status=$?
			;;
		esac
		# -f is what makes a 404 a failure at all: without it curl exits 0 and
		# writes a nine-byte file saying "Not Found", which then fails
		# verification as though the release had been tampered with.
		#
		# WHICH failure it is comes from the status line, not from curl's exit
		# code. That code depends on the HTTP version -- 22 over HTTP/1.1 and 56
		# over HTTP/2, measured with curl 8.7.1 against github.com, which serves
		# HTTP/2. Keying on 22 classified a 404 correctly on one protocol only,
		# so every macOS reader asking for a version that does not exist was
		# told the transfer failed instead of being told to check the version.
		# -w prints the status line even on the run -f aborted.
		case "$code" in
		4??) DOWNLOAD_ERROR=notfound ;;
		esac
	else
		wget -q -O "$dest" "$url" || status=$?
	fi
	if [ "$status" -ne 0 ]; then
		rm -f "$dest"
		return 1
	fi
	return 0
}

download_or_fail() {
	local url="$1" dest="$2" missing="$3"
	if download "$url" "$dest"; then
		return 0
	fi
	if [ "$DOWNLOAD_ERROR" = notfound ]; then
		fail 5 "$missing"
	fi
	fail 5 "failed to download $url"
}

fetch_json() {
	if [ "$downloader" = curl ]; then
		curl -fsSL -H 'Accept: application/json' "$1"
	else
		wget -q -O - --header 'Accept: application/json' "$1"
	fi
}

# resolve_version turns "latest" into a tag. The GitHub releases page answers
# with JSON when asked for it and, unlike api.github.com, advertises no hourly
# rate limit -- which is why the published one-liner does not need a token.
resolve_version() {
	local raw body tag number
	raw="$opt_version"
	if [ -z "$raw" ] || [ "$raw" = latest ]; then
		body="$(fetch_json "$base_url/latest")" ||
			fail 5 "could not determine the latest Ptah release from $base_url/latest"
		tag="$(printf '%s\n' "$body" | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"
		# An empty tag here would build a URL with an empty version in it and
		# download a 404 page, so it stops the run instead.
		[ -n "$tag" ] || fail 5 "could not determine the latest Ptah release from $base_url/latest"
		raw="$tag"
		resolved_latest=true
	fi

	case "$raw" in
	v*) number="${raw#v}" ;;
	*) number="$raw" ;;
	esac
	# A version starts with a digit and carries no separator, so it cannot walk
	# out of the release path it is pasted into.
	case "$number" in
	[0-9]*) : ;;
	*) fail 2 "\"$raw\" is not a Ptah release; use a tag such as v0.2.0" ;;
	esac
	case "$number" in
	*[!0-9A-Za-z.+-]*) fail 2 "\"$raw\" is not a Ptah release; use a tag such as v0.2.0" ;;
	esac

	version_number="$number"
	version_tag="v$number"
	asset="ptah_${version_number}_${os}_${arch}.tar.gz"
}

resolve_bindir() {
	if [ -n "$opt_bindir" ]; then
		bindir="$opt_bindir"
		return 0
	fi
	if [ -n "${XDG_BIN_HOME-}" ]; then
		bindir="$XDG_BIN_HOME"
		return 0
	fi
	if [ -z "${HOME-}" ]; then
		fail 2 "HOME is not set; use --bin-dir or PTAH_INSTALL_DIR to say where to install"
	fi
	bindir="$HOME/.local/bin"
}

# ensure_bindir_writable proves the directory can be written by writing to it,
# before the download rather than after it. Permission bits are not the whole
# answer on every filesystem, and a read-only mount answers only to a write.
#
# mktemp, not a name built from $$. A process id is public, so `$bindir/.ptah-
# install-probe.$$` is a name anyone else with write access to the install
# directory can compute and create ahead of the run -- and `: >"$probe"` follows
# a symlink found there and truncates whatever it points at. mktemp creates the
# file with O_EXCL under a name that is not derivable, so a planted symlink is
# neither followed nor able to make the probe fail. --bin-dir is what makes this
# matter: the default is a directory only its owner writes, but the flag exists
# to name a shared one.
ensure_bindir_writable() {
	local probe
	mkdir -p "$bindir" 2>/dev/null ||
		fail 7 "cannot create $bindir; set PTAH_INSTALL_DIR or --bin-dir to a directory you can write"
	probe="$(mktemp "$bindir/.ptah-install-probe.XXXXXXXXXX" 2>/dev/null)" ||
		fail 7 "cannot write to $bindir; set PTAH_INSTALL_DIR or run with different privileges"
	rm -f "$probe"
}

# verify_checksum compares the digest itself instead of running `sha256sum -c`.
# Three measured reasons: the published checksums.txt covers fourteen artifacts
# and `-c` fails on the thirteen absent ones; `--ignore-missing` does not exist
# in the coreutils on RHEL 7; and macOS's sha256sum accepts an EMPTY checksum
# file and exits 0, so a renamed asset would verify vacuously. The [ -n "$want" ]
# line below is what closes that, and it is the line a `-c` invocation cannot
# have.
verify_checksum() {
	local file="$1" name="$2" checksums="$3" pattern want got
	pattern="$(printf '%s' "$name" | sed 's/[.]/\\./g')"
	want="$(grep -e " ${pattern}\$" "$checksums" | tr '\t' ' ' | cut -d ' ' -f 1 | head -n 1)"
	if [ -z "$want" ]; then
		fail 6 "checksums.txt has no entry for $name"
	fi
	got="$(sha256_of "$file")"
	if [ -z "$got" ]; then
		fail 6 "could not compute the SHA-256 of $name with $hasher"
	fi
	if [ "$want" != "$got" ]; then
		printf 'ptah: checksum mismatch for %s\n' "$name" >&2
		printf 'ptah:   want %s\n' "$want" >&2
		printf 'ptah:   got  %s\n' "$got" >&2
		exit 6
	fi
}

# detect_signature_tool runs with the other prerequisite checks, before the
# transfer. Reaching for cosign at verification time would refuse the run after
# thirty megabytes had already been downloaded for it.
detect_signature_tool() {
	if [ "$opt_verify_signature" = false ]; then
		return 0
	fi
	have cosign || fail 4 "--verify-signature needs cosign on PATH; see https://github.com/sigstore/cosign"
}

# verify_signature is opt-in and requires cosign. It is never attempted
# opportunistically: a signature check that quietly does not run when its tool
# is absent is indistinguishable from one that passed.
verify_signature() {
	local bundle="$tmpdir/checksums.txt.sigstore.json"
	download_or_fail "$base_url/download/$version_tag/checksums.txt.sigstore.json" "$bundle" \
		"$version_tag publishes no checksums.txt.sigstore.json"
	cosign verify-blob --bundle "$bundle" \
		--certificate-identity-regexp 'github.com/stokaro/ptah' \
		--certificate-oidc-issuer https://token.actions.githubusercontent.com \
		"$tmpdir/checksums.txt" >/dev/null 2>&1 ||
		fail 6 "the Sigstore signature on checksums.txt did not verify"
	say "sigstore signature on checksums.txt verified"
}

unpack() {
	mkdir -p "$tmpdir/unpack"
	tar -xzf "$tmpdir/$asset" -C "$tmpdir/unpack" ||
		fail 5 "could not unpack $asset"
	local name
	for name in $selected; do
		if [ ! -f "$tmpdir/unpack/$name" ]; then
			fail 5 "$asset does not contain $name"
		fi
	done
}

# install_binaries copies each binary to a staging name inside the install
# directory and renames it into place. The rename is within one directory, so it
# is atomic: there is no moment at which a half-written file carries the name a
# shell would find on PATH.
#
# mktemp chooses the staging name for the reason the probe above does. The name
# it replaced was `.ptah-install-$$-$name`, which anyone able to write to the
# install directory could compute: a symlink planted there was opened by `cp`,
# so the binary was written through it to a file outside the directory, and the
# `mv` then renamed the symlink into place. The run exited 0 and reported three
# binaries installed, one of which was a link somewhere else.
install_binaries() {
	local name dest
	for name in $selected; do
		dest="$bindir/$name"
		staging_file="$(mktemp "$bindir/.ptah-install.XXXXXXXXXX")" ||
			fail 7 "could not create a staging file in $bindir"
		if [ -e "$dest" ]; then
			say "replacing $(display_path "$dest")"
		fi
		cp "$tmpdir/unpack/$name" "$staging_file" ||
			fail 7 "could not write $staging_file"
		chmod 0755 "$staging_file"
		mv -f "$staging_file" "$dest" ||
			fail 7 "could not install $dest"
		staging_file=""
	done
	say "installed $(printf '%s' "$selected" | sed 's/ /, /g') in $(display_path "$bindir")"
}

# display_path shortens a path inside $HOME to its ~ form. The absolute path is
# what the machine uses and what an error should name; the tilde is what a
# person writes when telling somebody else where the binaries went.
# shellcheck disable=SC2088 # the tilde is display text, not a path this opens
display_path() {
	if [ -n "${HOME-}" ]; then
		case "$1" in
		"$HOME")
			printf '~'
			return 0
			;;
		"$HOME"/*)
			printf '~/%s' "${1#"$HOME"/}"
			return 0
			;;
		esac
	fi
	printf '%s' "$1"
}

# home_path is the same shortening for a line the reader pastes into a shell.
# It spells $HOME rather than ~, because a tilde does not expand inside the
# double quotes the rest of that line needs -- and because the resulting line is
# then correct on a second machine, which is where a startup file usually ends
# up.
# shellcheck disable=SC2016 # $HOME is text the reader pastes, not an expansion
home_path() {
	if [ -n "${HOME-}" ]; then
		case "$1" in
		"$HOME")
			printf '$HOME'
			return 0
			;;
		"$HOME"/*)
			printf '$HOME/%s' "${1#"$HOME"/}"
			return 0
			;;
		esac
	fi
	printf '%s' "$1"
}

path_contains() (
	IFS=:
	set -f
	for element in $PATH; do
		if [ "$element" = "$1" ]; then
			exit 0
		fi
	done
	exit 1
)

shell_startup_file() {
	local name=""
	if [ -n "${SHELL-}" ]; then
		name="${SHELL##*/}"
	fi
	# The tilde is text for the reader to paste into their own startup file.
	# Nothing here opens the path, so expanding it would be wrong.
	# shellcheck disable=SC2088
	case "$name" in
	zsh) printf '~/.zshrc' ;;
	bash) printf '~/.bashrc' ;;
	ksh) printf '~/.kshrc' ;;
	*) printf '' ;;
	esac
}

# report_path says what to do and does not do it. Editing a person's startup
# files is the loudest complaint against installers read from a pipe, and the
# install has already succeeded either way, so this never changes the exit code.
report_path() {
	local startup shell_name=""
	if path_contains "$bindir"; then
		return 0
	fi
	if [ "$opt_quiet" = true ]; then
		return 0
	fi
	if [ -n "${SHELL-}" ]; then
		shell_name="${SHELL##*/}"
	fi
	printf 'ptah: %s is not on your PATH.\n' "$(display_path "$bindir")"
	if [ "$shell_name" = fish ]; then
		printf 'ptah: add it with:\n'
		printf '    fish_add_path %s\n' "$(display_path "$bindir")"
		return 0
	fi
	printf 'ptah: add it for this shell:\n'
	# shellcheck disable=SC2016 # $PATH is part of the line the reader pastes.
	printf '    export PATH="%s:$PATH"\n' "$(home_path "$bindir")"
	startup="$(shell_startup_file)"
	if [ -n "$startup" ]; then
		printf 'ptah: to keep it, add that line to %s.\n' "$startup"
	else
		printf "ptah: to keep it, add that line to your shell's startup file.\n"
	fi
}

# update_github_path is the one file this script appends to, and only inside a
# GitHub Actions job. Without it the step after `curl ... | sh` cannot find
# ptah, because a PATH exported in one step is gone by the next.
update_github_path() {
	if [ "$opt_no_modify_path" = true ] || [ -z "${GITHUB_PATH-}" ]; then
		return 0
	fi
	printf '%s\n' "$bindir" >>"$GITHUB_PATH" ||
		fail 1 "could not append $bindir to \$GITHUB_PATH"
	say "added $(display_path "$bindir") to \$GITHUB_PATH"
}

report_version() {
	local first
	case " $selected " in
	*" ptah "*) : ;;
	*) return 0 ;;
	esac
	first="$("$bindir/ptah" version </dev/null 2>/dev/null | head -n 1 || true)"
	if [ -z "$first" ]; then
		fail 1 "$bindir/ptah was installed but does not run on this machine"
	fi
	if [ "$opt_quiet" = false ]; then
		printf '%s\n' "$first"
	fi
}

plan_only() {
	say "would download $asset from $base_url/download/$version_tag"
	say "would verify its sha256 against checksums.txt with $hasher"
	say "would install $(printf '%s' "$selected" | sed 's/ /, /g') in $(display_path "$bindir")"
	if [ "$opt_no_modify_path" = false ] && [ -n "${GITHUB_PATH-}" ]; then
		say "would add $(display_path "$bindir") to \$GITHUB_PATH"
	fi
}

main() {
	parse_args "$@"
	read_environment
	resolve_selection

	detect_downloader
	detect_platform
	detect_tar
	detect_hasher
	detect_signature_tool
	detect_mktemp

	resolve_version
	if [ "$resolved_latest" = true ]; then
		say "resolved latest to $version_tag"
	fi
	say "platform $os/$arch"

	resolve_bindir
	if [ "$opt_dry_run" = true ]; then
		plan_only
		return 0
	fi
	ensure_bindir_writable

	trap cleanup EXIT
	trap 'cleanup; exit 130' INT
	trap 'cleanup; exit 129' HUP
	trap 'cleanup; exit 143' TERM
	tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-install.XXXXXX")" ||
		fail 1 "could not create a temporary directory"

	download_or_fail "$base_url/download/$version_tag/checksums.txt" "$tmpdir/checksums.txt" \
		"no release $version_tag at $base_url; check the version"
	say "downloading $asset"
	download_or_fail "$base_url/download/$version_tag/$asset" "$tmpdir/$asset" \
		"no release asset $asset in $version_tag; check the version"

	if [ "$opt_verify_signature" = true ]; then
		verify_signature
	fi
	verify_checksum "$tmpdir/$asset" "$asset" "$tmpdir/checksums.txt"
	say "sha256 verified against checksums.txt"

	unpack
	install_binaries
	report_version
	update_github_path
	report_path
}

main "$@"
