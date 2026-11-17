#!/usr/bin/env bash
# Every registry that carries a release image carries a signature for it, made
# by the release workflow, for the digest that release published.
#
# A signature is stored in the registry beside the image it signs, so a
# signature pushed to GHCR is not on Docker Hub. Equal digests on both hosts
# (check-release-image-mirror.sh) say the hosts serve the same image. They say
# nothing about whether a reader at either host can find a signature for it, so
# each host is signed and each host is verified (stokaro/ptah#5977).
#
# The release workflow runs four subcommands:
#
#   preflight <tag>                    release job, before anything is built:
#                                      the tag and the workflow can sign
#   digest <version> <artifacts.json>  release job, after GoReleaser: the one
#                                      index digest it published
#   sign <tag> <digest>                signing job: a keyless cosign signature
#                                      at every image
#   verify <tag> <digest>              signing job: anonymous cosign verify at
#                                      every image
#
# Signing goes by digest, never by tag. A tag is a pointer that the next push
# can move; a signature attaches to a digest and stays with it.
#
# Signing runs in a job of its own, after the job that publishes, and reads the
# digest from that job's output. Re-running the failed signing job therefore
# signs the digest that was published, under the same identity, without
# building anything again. Signing a digest that already carries a signature
# adds a second one beside it, which costs nothing. A run that signed one host
# and then reported success would leave a reader at the other host with no way
# to check the image, and the verify step refuses that run: it reads every host
# anonymously, the way a reader does, and fails unless each one returns a
# signature from the pinned identity for the published digest.
set -euo pipefail

# The images a release publishes, as .goreleaser.yaml `dockers_v2.images` names
# them. `digest` refuses an artifacts.json that names any other image, so a
# registry added there fails the release here until it is signed as well.
IMAGES=(ghcr.io/stokaro/ptah docker.io/stokaro/ptah)

readonly REPOSITORY_URL="https://github.com/stokaro/ptah"
readonly WORKFLOW=".github/workflows/release.yml"
readonly ISSUER="https://token.actions.githubusercontent.com"

# The tags a release is signed for: semantic versions with an optional
# prerelease part. GoReleaser refuses a tag that is not a semantic version, and
# a container tag cannot carry semver build metadata (`+`), so this is every
# tag a release can publish an image for. install-options.mdx publishes the
# same rule as a certificate identity pattern, and the self-test holds the page
# to IDENTITY_REGEXP below.
readonly TAG_PATTERN='^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'

# A signature that a registry has not served yet is retried, a bounded number of
# times: GHCR stores a signature under a referrers tag, and an anonymous read
# that resolves the tag before it lands sees no signature. Not readonly, so the
# self-test can drive the retry without waiting.
RETRY_ATTEMPTS=6
RETRY_INTERVAL=10

# GoReleaser writes one record per image and tag a `dockers_v2` build pushed,
# with the type "Docker Image", the digest buildx reported, and the platforms
# the build covered. The filter is used by `digest` and by the self-test, so the
# self-test reads its fixtures through the same expression the release reads
# dist/artifacts.json with.
readonly PUSHED_FILTER='.[] | select(.type == "Docker Image") | [.name, (.extra.Digest // ""), ((.extra.Platforms // []) | length)] | @tsv'

# The verified payloads cosign prints on stdout, one signed digest per line.
# testdata/cosign-verify-keyless.json is a real cosign v3.1.4 output this filter
# is held to, so a change of shape fails the self-test, not a release.
readonly SIGNED_FILTER='.[] | (.critical.image["docker-manifest-digest"] // "")'

refuse() {
	echo "release image signatures: $*" >&2
	return 1
}

# identity_regexp prints the certificate identity pattern that accepts the
# release workflow at any release tag: the identity identity_for writes, with
# the tag replaced by TAG_PATTERN.
identity_regexp() {
	local prefix
	prefix="$(printf '%s' "${REPOSITORY_URL}/${WORKFLOW}@refs/tags/" | sed 's/[.]/\\./g')"
	printf '^%s%s\n' "$prefix" "${TAG_PATTERN#^}"
}

# identity_for prints the certificate identity a release tag is signed under:
# the release workflow file at the tag's ref. It refuses a tag outside
# TAG_PATTERN, so neither signing nor verification can be pointed at an identity
# the documentation does not publish.
identity_for() {
	local tag=$1
	[[ "$tag" =~ $TAG_PATTERN ]] ||
		refuse "${tag:-an empty name} is not a release tag (vMAJOR.MINOR.PATCH, with an optional -PRERELEASE)" ||
		return 1
	printf '%s/%s@refs/tags/%s\n' "$REPOSITORY_URL" "$WORKFLOW" "$tag"
}

is_digest() {
	[[ "$1" =~ ^sha256:[0-9a-f]{64}$ ]]
}

check_digest() {
	is_digest "$1" || refuse "${1:-an empty value} is not an image digest (sha256:<64 hex>)"
}

# pushed_digest reads "<image>:<tag><TAB><digest><TAB><platforms>" records on
# stdin, as PUSHED_FILTER writes them, and prints the one digest every image
# carries for the version. It refuses a missing image, two digests for the
# version, a malformed digest, a single-platform build, and an image nothing
# here would sign. The platform count is what makes the digest an index rather
# than one architecture's manifest: buildx writes an index only for a build that
# covers more than one platform. `latest` records only have to name a known
# image: two release tags pushed together race for `latest`, and the version tag
# is what this run owns.
pushed_digest() {
	local version=$1 name digest platforms image tag known candidate found="" covered=" "
	while IFS=$'\t' read -r name digest platforms; do
		[ -n "$name" ] || continue
		image=${name%:*}
		tag=${name##*:}
		known=""
		for candidate in "${IMAGES[@]}"; do
			if [ "$candidate" = "$image" ]; then
				known=yes
			fi
		done
		[ -n "$known" ] ||
			refuse "GoReleaser pushed ${name}, and ${image} is not an image this release signs" ||
			return 1
		[ "$tag" = "$version" ] || continue
		is_digest "$digest" ||
			refuse "GoReleaser recorded ${digest:-no digest} for ${name}, which is not an image digest" ||
			return 1
		[ "${platforms:-0}" -gt 1 ] ||
			refuse "GoReleaser built ${name} for ${platforms:-no} platform(s), so its digest is not a multi-platform index" ||
			return 1
		if [ -z "$found" ]; then
			found=$digest
		elif [ "$digest" != "$found" ]; then
			refuse "GoReleaser recorded two digests for ${version}: ${found} and ${digest}" || return 1
		fi
		covered="${covered}${image} "
	done
	for candidate in "${IMAGES[@]}"; do
		case "$covered" in
		*" ${candidate} "*) ;;
		*) refuse "GoReleaser recorded no push of ${candidate}:${version}" || return 1 ;;
		esac
	done
	printf '%s\n' "$found"
}

# same_signer refuses a run whose OIDC identity is not the documented one. The
# certificate Fulcio issues names the running workflow, so signing from a copy
# of this job in another workflow, or from a branch, would publish a signature
# that the documented `cosign verify` command never accepts.
same_signer() {
	local documented=$1 running=$2
	[ "$documented" = "$running" ] ||
		refuse "this run would sign as ${running:-an unknown identity}, and the documented identity is ${documented}"
}

# signer_identity prints the identity this run signs a tag under, and refuses
# when the tag is not a release tag or the running workflow is not the one the
# identity names. `preflight` and `sign` both ask it, so the release job refuses
# before publishing exactly what the signing job would refuse after.
signer_identity() {
	local tag=$1 identity
	identity="$(identity_for "$tag")" || return 1
	same_signer "$identity" "https://github.com/${GITHUB_WORKFLOW_REF:-}" || return 1
	printf '%s\n' "$identity"
}

# signed_digest_is reads the signed digests of the verified payloads on stdin
# and requires at least one, every one naming the expected digest. cosign
# already checks the payload against the reference it was given; reading the
# claim back proves that the reference it checked was the published digest.
signed_digest_is() {
	local image=$1 expected=$2 line count=0
	while IFS= read -r line; do
		[ "$line" = "$expected" ] ||
			refuse "${image}: a verified signature names ${line:-no digest}, and this release published ${expected}" ||
			return 1
		count=$((count + 1))
	done
	[ "$count" -gt 0 ] || refuse "${image}: cosign verified no signature for ${expected}"
}

cmd_preflight() {
	local tag=${1:-} identity
	if [ -z "$tag" ]; then
		echo "usage: $0 preflight <tag>" >&2
		exit 2
	fi
	identity="$(signer_identity "$tag")" || exit 1
	echo "release image signatures: ${tag} will be signed as ${identity}"
}

cmd_digest() {
	local version=${1:-} artifacts=${2:-}
	if [ -z "$version" ] || [ -z "$artifacts" ]; then
		echo "usage: $0 digest <version-without-v> <artifacts.json>" >&2
		exit 2
	fi
	jq -r "$PUSHED_FILTER" "$artifacts" | pushed_digest "$version" || exit 1
}

cmd_sign() {
	local tag=${1:-} digest=${2:-} identity image
	if [ -z "$tag" ] || [ -z "$digest" ]; then
		echo "usage: $0 sign <tag> <digest>" >&2
		exit 2
	fi
	identity="$(signer_identity "$tag")" || exit 1
	check_digest "$digest" || exit 1
	for image in "${IMAGES[@]}"; do
		echo "release image signatures: signing ${image}@${digest} as ${identity}"
		cosign sign --yes "${image}@${digest}" || exit 1
	done
}

# The empty Docker configuration cmd_verify reads registries through. Global so
# the exit trap can still name it after the function returns.
ANONYMOUS_CONFIG=""

# verify_image runs cosign verify for one image until it succeeds or the
# attempts run out, and prints the payloads of the attempt that succeeded. Any
# refusal is retried: the attempts are few, and the last refusal is what the log
# shows.
verify_image() {
	local image=$1 digest=$2 identity=$3 attempt
	for ((attempt = 1; ; attempt++)); do
		if DOCKER_CONFIG="$ANONYMOUS_CONFIG" cosign verify \
			--certificate-identity "$identity" \
			--certificate-oidc-issuer "$ISSUER" \
			"${image}@${digest}"; then
			return 0
		fi
		[ "$attempt" -lt "$RETRY_ATTEMPTS" ] || return 1
		echo "  ${image}: no signature verifies on read ${attempt} of ${RETRY_ATTEMPTS}; reading again in ${RETRY_INTERVAL}s" >&2
		sleep "$RETRY_INTERVAL"
	done
}

cmd_verify() {
	local tag=${1:-} digest=${2:-} identity image payloads status=0
	if [ -z "$tag" ] || [ -z "$digest" ]; then
		echo "usage: $0 verify <tag> <digest>" >&2
		exit 2
	fi
	identity="$(identity_for "$tag")" || exit 1
	check_digest "$digest" || exit 1

	# An empty Docker configuration is what makes this anonymous. The job holds
	# a push credential for both hosts, and a verification that used it could
	# pass for a signature a reader cannot fetch.
	ANONYMOUS_CONFIG="$(mktemp -d)"
	trap 'rm -rf "$ANONYMOUS_CONFIG"' EXIT

	echo "release image signatures: ${digest}, expecting a signature from ${identity}"
	for image in "${IMAGES[@]}"; do
		if ! payloads="$(verify_image "$image" "$digest" "$identity")"; then
			echo "  ${image}: no signature from the release workflow verifies" >&2
			status=1
			continue
		fi
		if ! printf '%s\n' "$payloads" | jq -r "$SIGNED_FILTER" | signed_digest_is "$image" "$digest"; then
			status=1
			continue
		fi
		printf '  %-24s verified\n' "$image"
	done
	if [ "$status" -ne 0 ]; then
		echo "release image signatures: a registry serves no verifiable signature for this release; see stokaro/ptah#5977" >&2
		exit 1
	fi
	echo "release image signatures: OK (every registry serves a signature from the release workflow)"
}

# The scratch directory the self-test keeps its cosign stand-in in. Global so
# the exit trap can still name it after the function returns.
SELFTEST_DIR=""

selftest() {
	local failures=0 checks=0 good bad tag_identity root
	good="sha256:$(printf 'a%.0s' $(seq 64))"
	bad="sha256:$(printf 'b%.0s' $(seq 64))"
	root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
	SELFTEST_DIR="$(mktemp -d)"
	trap 'rm -rf "$SELFTEST_DIR"' EXIT
	RETRY_INTERVAL=0

	pass() {
		checks=$((checks + 1))
		printf '  %-64s %s\n' "$1" "$2"
	}
	fail() {
		failures=$((failures + 1))
		echo "release image signatures self-test: $1" >&2
	}

	# Every rule below is driven through its accepting case first. A rule that
	# refused everything would satisfy each rejection, so the controls are what
	# make the rejections mean anything.
	echo "check-release-image-signatures-selftest: breaking each rule and requiring it to notice"

	# identity_for and TAG_PATTERN
	local name want
	while IFS='|' read -r name want; do
		tag_identity="$(identity_for "$name" 2>/dev/null)" || tag_identity=""
		if [ "$tag_identity" = "$want" ]; then
			pass "identity: ${name} names release.yml at the tag" accepted
		else
			fail "identity for ${name} is ${tag_identity:-refused}"
		fi
	done <<-'TAGS'
		v1.2.3|https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.3
		v1.2.3-rc.1|https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.3-rc.1
	TAGS
	for name in provider-sdk/v0.3.0 v1.2.3/extra 1.2.3 vnext v1.2 v1.2.3+build.1 "v1.2.3 x" ""; do
		if identity_for "$name" >/dev/null 2>&1; then
			fail "identity accepted ${name:-an empty tag}"
		else
			pass "identity: ${name:-an empty tag}" rejected
		fi
	done

	# The identity pattern install-options.mdx publishes is the one the signer
	# signs under: it accepts every identity identity_for writes, refuses the
	# identities same_signer refuses, and appears on the page verbatim.
	local pattern page="$root/docs/site/src/content/docs/start/install-options.mdx"
	pattern="$(identity_regexp)"
	if grep -qF -- "'${pattern}'" "$page"; then
		pass "identity pattern: install-options.mdx publishes it verbatim" accepted
	else
		fail "install-options.mdx does not publish the identity pattern ${pattern}"
	fi
	for name in v1.2.3 v1.2.3-rc.1; do
		if [[ "$(identity_for "$name")" =~ $pattern ]]; then
			pass "identity pattern: matches the identity for ${name}" accepted
		else
			fail "the identity pattern refused the identity for ${name}"
		fi
	done
	local identity description
	while IFS='|' read -r description identity; do
		if [[ "$identity" =~ $pattern ]]; then
			fail "the identity pattern accepted ${description}: ${identity}"
		else
			pass "identity pattern: ${description}" rejected
		fi
	done <<-'IDENTITIES'
		a branch|https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/heads/master
		another workflow file|https://github.com/stokaro/ptah/.github/workflows/release-master-check.yml@refs/tags/v1.2.3
		a fork|https://github.com/someone/ptah/.github/workflows/release.yml@refs/tags/v1.2.3
		a dot matched as any character|https://github.com/stokaro/ptah/.github/workflows/releaseXyml@refs/tags/v1.2.3
		a tag with build metadata|https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.3+build.1
		a path after the tag|https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.3-rc.1/extra
		a prefix before the repository|https://example.com/https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.3
	IDENTITIES

	# pushed_digest, through the same jq filter the release reads with.
	assert_pushed() {
		local description=$1 want=$2 json=$3 got
		got="$(printf '%s' "$json" | jq -r "$PUSHED_FILTER" | pushed_digest 1.2.3 2>/dev/null)" || got=""
		if [ -n "$want" ] && [ "$got" = "$want" ]; then
			pass "pushed digest: ${description}" accepted
		elif [ -z "$want" ] && [ -z "$got" ]; then
			pass "pushed digest: ${description}" rejected
		else
			fail "pushed digest for ${description} was ${got:-refused}"
		fi
	}
	# record writes one artifacts.json entry in the shape GoReleaser v2 writes
	# for a dockers_v2 push. The platforms default to the release's two.
	record() {
		printf '{"name":"%s","path":"%s","type":"Docker Image","extra":{"Digest":"%s","ID":"ptah","Platforms":%s}}' \
			"$1" "$1" "$2" "${3:-[\"linux/amd64\",\"linux/arm64\"]}"
	}
	assert_pushed "both hosts, version and latest, one digest" "$good" "[$(record ghcr.io/stokaro/ptah:1.2.3 "$good"),$(record ghcr.io/stokaro/ptah:latest "$good"),$(record docker.io/stokaro/ptah:1.2.3 "$good"),$(record docker.io/stokaro/ptah:latest "$good")]"
	assert_pushed "latest names another digest" "$good" "[$(record ghcr.io/stokaro/ptah:1.2.3 "$good"),$(record docker.io/stokaro/ptah:1.2.3 "$good"),$(record docker.io/stokaro/ptah:latest "$bad")]"
	assert_pushed "Docker Hub was not pushed" "" "[$(record ghcr.io/stokaro/ptah:1.2.3 "$good"),$(record ghcr.io/stokaro/ptah:latest "$good")]"
	assert_pushed "GHCR was not pushed" "" "[$(record docker.io/stokaro/ptah:1.2.3 "$good")]"
	assert_pushed "the hosts carry two digests" "" "[$(record ghcr.io/stokaro/ptah:1.2.3 "$good"),$(record docker.io/stokaro/ptah:1.2.3 "$bad")]"
	assert_pushed "a record carries no digest" "" "[$(record ghcr.io/stokaro/ptah:1.2.3 ""),$(record docker.io/stokaro/ptah:1.2.3 "$good")]"
	assert_pushed "a digest is not sha256" "" "[$(record ghcr.io/stokaro/ptah:1.2.3 "sha256:aaa"),$(record docker.io/stokaro/ptah:1.2.3 "sha256:aaa")]"
	assert_pushed "an image this release does not sign" "" "[$(record ghcr.io/stokaro/ptah:1.2.3 "$good"),$(record docker.io/stokaro/ptah:1.2.3 "$good"),$(record quay.io/stokaro/ptah:1.2.3 "$good")]"
	assert_pushed "a build for one platform" "" "[$(record ghcr.io/stokaro/ptah:1.2.3 "$good" '["linux/amd64"]'),$(record docker.io/stokaro/ptah:1.2.3 "$good" '["linux/amd64"]')]"
	assert_pushed "a snapshot: one image per platform, no index" "" "[$(record ghcr.io/stokaro/ptah:1.2.3-amd64 "$good" '["linux/amd64"]'),$(record docker.io/stokaro/ptah:1.2.3-amd64 "$good" '["linux/amd64"]')]"
	assert_pushed "the archives, and no image at all" "" '[{"name":"ptah_1.2.3_linux_amd64.tar.gz","type":"Archive","extra":{}}]'
	assert_pushed "another version was pushed" "" "[$(record ghcr.io/stokaro/ptah:1.2.4 "$good"),$(record docker.io/stokaro/ptah:1.2.4 "$good")]"

	# same_signer
	local documented=https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.3
	if same_signer "$documented" "$documented" 2>/dev/null; then
		pass "signer: the release workflow at the tag" accepted
	else
		fail "the documented signer was refused"
	fi
	local running
	while IFS='|' read -r description running; do
		if same_signer "$documented" "$running" 2>/dev/null; then
			fail "signer accepted ${description}: ${running}"
		else
			pass "signer: ${description}" rejected
		fi
	done <<-'RUNS'
		a run from a branch|https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/heads/master
		a run for another tag|https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.4
		another workflow file|https://github.com/stokaro/ptah/.github/workflows/release-master-check.yml@refs/tags/v1.2.3
		a fork|https://github.com/someone/ptah/.github/workflows/release.yml@refs/tags/v1.2.3
		no workflow ref in the environment|https://github.com/
	RUNS

	# signed_digest_is, first over a real cosign v3.1.4 output. The fixture is
	# the stdout of
	#
	#   cosign verify \
	#     --certificate-identity keyless@projectsigstore.iam.gserviceaccount.com \
	#     --certificate-oidc-issuer https://accounts.google.com \
	#     ghcr.io/sigstore/cosign/cosign@sha256:0f06154836d2de34f887a5c273506d52ca43edaa1d9e53e6510fa78a48f69c2d
	#
	# the index of the v3.1.4 image, signed keylessly by the cosign release
	# with cosign v3: a Sigstore bundle under GHCR's `sha256-<hex>` referrers
	# tag, which is where a release signature is stored on GHCR too. It is cut
	# to the `critical` block, which is what the filter reads. A floor comes
	# first, so a missing or emptied fixture cannot pass as a refusal.
	local fixture="$root/scripts/testdata/cosign-verify-keyless.json" real_digest
	real_digest="$(jq -r "$SIGNED_FILTER" "$fixture" 2>/dev/null)" || real_digest=""
	if is_digest "$real_digest"; then
		pass "signed digest: the real cosign v3 output carries one" accepted
	else
		fail "the real cosign output yields ${real_digest:-no digest}"
	fi
	if jq -r "$SIGNED_FILTER" "$fixture" | signed_digest_is ghcr.io/sigstore/cosign/cosign "$real_digest" 2>/dev/null; then
		pass "signed digest: the real output names the verified digest" accepted
	else
		fail "the real cosign output was refused for its own digest"
	fi
	if jq -r "$SIGNED_FILTER" "$fixture" | signed_digest_is ghcr.io/sigstore/cosign/cosign "$good" 2>/dev/null; then
		fail "the real cosign output was accepted for another digest"
	else
		pass "signed digest: the real output, held to another digest" rejected
	fi

	# Then the shapes one real output cannot show. One payload is a first run;
	# two are a signing job that ran twice.
	payload() {
		printf '{"critical":{"identity":{"docker-reference":"ghcr.io/stokaro/ptah@%s"},"image":{"docker-manifest-digest":"%s"},"type":"https://sigstore.dev/cosign/sign/v1"},"optional":{}}' "$1" "$1"
	}
	assert_signed() {
		local description=$1 verdict=$2 output=$3 result=rejected
		if printf '%s\n' "$output" | jq -r "$SIGNED_FILTER" 2>/dev/null | signed_digest_is ghcr.io/stokaro/ptah "$good" 2>/dev/null; then
			result=accepted
		fi
		if [ "$result" = "$verdict" ]; then
			pass "signed digest: ${description}" "$result"
		else
			fail "signed digest for ${description} was ${result}"
		fi
	}
	assert_signed "one signature for the published digest" accepted "[$(payload "$good")]"
	assert_signed "a rerun signed the published digest again" accepted "[$(payload "$good"),$(payload "$good")]"
	assert_signed "no verified signature" rejected "[]"
	assert_signed "a signature for a substituted digest" rejected "[$(payload "$bad")]"
	assert_signed "one of two signatures names another digest" rejected "[$(payload "$good"),$(payload "$bad")]"
	assert_signed "a payload with no digest claim" rejected '[{"critical":{"type":"https://sigstore.dev/cosign/sign/v1"}}]'
	assert_signed "output that is not JSON" rejected "Verified OK"

	# sign and verify end to end, with cosign replaced by a function. The stub
	# records every call, and for a verification whether it read through an
	# empty Docker configuration. It refuses a host's first N verifications as
	# a registry that has not served the signature yet, and otherwise prints the
	# payload file. Its state lives in files because the script calls it in
	# command substitutions, which are subshells.
	cosign() {
		local ref="" arg host left reader=anonymous
		for arg in "$@"; do
			ref=$arg
		done
		if [ "$1" != verify ]; then
			printf '%s\n' "$*" >>"$SELFTEST_DIR/calls"
			return 0
		fi
		[ -n "${DOCKER_CONFIG:-}" ] && [ -d "$DOCKER_CONFIG" ] && [ -z "$(ls -A "$DOCKER_CONFIG")" ] ||
			reader=credentialed
		printf '%s %s\n' "$*" "$reader" >>"$SELFTEST_DIR/calls"
		host=${ref%%/*}
		left="$(cat "$SELFTEST_DIR/refuse-$host" 2>/dev/null || echo 0)"
		if [ "$left" -gt 0 ]; then
			echo $((left - 1)) >"$SELFTEST_DIR/refuse-$host"
			echo "Error: no signatures found" >&2
			return 10
		fi
		cat "$SELFTEST_DIR/payload"
	}
	# scenario resets the stub: the digest the payload names, then how many reads
	# GHCR and Docker Hub refuse before they serve a signature.
	scenario() {
		rm -f "$SELFTEST_DIR/calls" "$SELFTEST_DIR"/refuse-*
		: >"$SELFTEST_DIR/calls"
		printf '[%s]\n' "$(payload "$1")" >"$SELFTEST_DIR/payload"
		echo "$2" >"$SELFTEST_DIR/refuse-ghcr.io"
		echo "$3" >"$SELFTEST_DIR/refuse-docker.io"
	}
	calls() {
		grep -c -- "$1" "$SELFTEST_DIR/calls" || true
	}
	assert_run() {
		local description=$1 verdict=$2 result=rejected
		shift 2
		if ("$@") >/dev/null 2>&1; then
			result=accepted
		fi
		if [ "$result" = "$verdict" ]; then
			pass "$description" "$result"
		else
			fail "${description} was ${result}"
		fi
	}
	assert_calls() {
		local description=$1 pattern=$2 want=$3 got
		got="$(calls "$pattern")"
		if [ "$got" = "$want" ]; then
			pass "$description" "${got} call(s)"
		else
			fail "${description}: ${got} call(s), wanted ${want}"
		fi
	}

	scenario "$good" 0 0
	GITHUB_WORKFLOW_REF=stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.3 \
		assert_run "sign: the release workflow at the tag" accepted cmd_sign v1.2.3 "$good"
	assert_calls "sign: GHCR signed by digest" "^sign --yes ghcr.io/stokaro/ptah@${good}\$" 1
	assert_calls "sign: Docker Hub signed by digest" "^sign --yes docker.io/stokaro/ptah@${good}\$" 1
	scenario "$good" 0 0
	GITHUB_WORKFLOW_REF=stokaro/ptah/.github/workflows/release.yml@refs/heads/master \
		assert_run "sign: a run from a branch" rejected cmd_sign v1.2.3 "$good"
	assert_calls "sign: a refused signer signs nothing" "^sign " 0
	scenario "$good" 0 0
	GITHUB_WORKFLOW_REF=stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.3 \
		assert_run "preflight: the release workflow at the tag" accepted cmd_preflight v1.2.3
	GITHUB_WORKFLOW_REF=stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.3 \
		assert_run "preflight: a tag the identity cannot name" rejected cmd_preflight 1.2.3
	GITHUB_WORKFLOW_REF=stokaro/ptah/.github/workflows/release-master-check.yml@refs/tags/v1.2.3 \
		assert_run "preflight: another workflow file" rejected cmd_preflight v1.2.3
	assert_calls "preflight: calls no cosign" "." 0

	scenario "$good" 0 0
	assert_run "verify: both registries serve a signature" accepted cmd_verify v1.2.3 "$good"
	assert_calls "verify: one read per registry" "^verify " 2
	assert_calls "verify: each read carries no credential" " anonymous\$" 2
	scenario "$good" 2 0
	assert_run "verify: GHCR serves the signature on the third read" accepted cmd_verify v1.2.3 "$good"
	assert_calls "verify: GHCR read three times" "ghcr.io/stokaro/ptah@" 3
	scenario "$good" 0 99
	assert_run "verify: Docker Hub never serves a signature" rejected cmd_verify v1.2.3 "$good"
	assert_calls "verify: Docker Hub read ${RETRY_ATTEMPTS} times, then refused" "docker.io/stokaro/ptah@" "$RETRY_ATTEMPTS"
	scenario "$bad" 0 0
	assert_run "verify: the signature names a substituted digest" rejected cmd_verify v1.2.3 "$good"

	[ "$failures" -eq 0 ] || exit 1
	echo "check-release-image-signatures-selftest: OK (${checks} cases, each control accepted and each broken rule noticed)"
}

main() {
	case "${1:-}" in
	--selftest) selftest ;;
	preflight) cmd_preflight "${@:2}" ;;
	digest) cmd_digest "${@:2}" ;;
	sign) cmd_sign "${@:2}" ;;
	verify) cmd_verify "${@:2}" ;;
	*)
		echo "usage: $0 preflight <tag> | digest <version> <artifacts.json> | sign <tag> <digest> | verify <tag> <digest> | --selftest" >&2
		exit 2
		;;
	esac
}

main "$@"
