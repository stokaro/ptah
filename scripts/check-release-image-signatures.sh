#!/usr/bin/env bash
# Every registry that carries a release image carries a signature for it, made
# by the release workflow, for the digest that release pushed.
#
# A signature is stored in the registry beside the image it signs, so a
# signature pushed to GHCR is not on Docker Hub. Equal digests on both hosts
# (check-release-image-mirror.sh) say the hosts serve the same image. They say
# nothing about whether a reader at either host can find a signature for it, so
# each host is signed and each host is verified (stokaro/ptah#5977).
#
# The release job runs three subcommands, in this order, after the mirror check
# has tied the version tag on both hosts to the pushed digest:
#
#   digest <version> <artifacts.json>  the one index digest GoReleaser pushed
#   sign <tag> <digest>                a keyless cosign signature at every image
#   verify <tag> <digest>              anonymous cosign verify at every image
#
# Signing goes by digest, never by tag. A tag is a pointer that the next push
# can move; a signature attaches to a digest and stays with it.
#
# The verify step is what makes a rerun safe. A rerun of a partly published
# release pushes and signs again under the same identity: the identity names the
# workflow file and the tag, and neither changes between attempts. Signing a
# digest that already carries a signature adds a second one beside it, which
# costs nothing. A run that signed one host and then reported success would
# leave a reader at the other host with no way to check the image. The verify
# step refuses that run: it reads every host anonymously, the way a reader does,
# and fails unless each one returns a signature from the pinned identity for the
# digest this run pushed.
set -euo pipefail

# The images a release publishes, as .goreleaser.yaml `dockers_v2.images` names
# them. `digest` refuses an artifacts.json that names any other image, so a
# registry added there fails the release here until it is signed as well.
IMAGES=(ghcr.io/stokaro/ptah docker.io/stokaro/ptah)

readonly REPOSITORY_URL="https://github.com/stokaro/ptah"
readonly WORKFLOW=".github/workflows/release.yml"
readonly ISSUER="https://token.actions.githubusercontent.com"

# GoReleaser writes one record per image and tag a `dockers_v2` build pushed,
# with the type "Docker Image", the digest buildx reported, and the platforms
# the build covered. The filter is used by `digest` and by the self-test, so the
# self-test reads its fixtures through the same expression the release reads
# dist/artifacts.json with.
readonly PUSHED_FILTER='.[] | select(.type == "Docker Image") | [.name, (.extra.Digest // ""), ((.extra.Platforms // []) | length)] | @tsv'

# The verified payloads cosign prints on stdout, one signed digest per line.
readonly SIGNED_FILTER='.[] | (.critical.image["docker-manifest-digest"] // "")'

refuse() {
	echo "release image signatures: $*" >&2
	return 1
}

# identity_for prints the certificate identity a release tag is signed under:
# the release workflow file at the tag's ref. It refuses a name the release
# workflow does not run for, so neither signing nor verification can be pointed
# at an identity the documentation does not publish.
identity_for() {
	local tag=$1
	case "$tag" in
	*/* | *[[:space:]]*) refuse "${tag} is not a release tag (v<version>)" || return 1 ;;
	v[0-9]*) ;;
	*) refuse "${tag:-an empty name} is not a release tag (v<version>)" || return 1 ;;
	esac
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

# signed_digest_is reads the signed digests of the verified payloads on stdin
# and requires at least one, every one naming the expected digest. cosign
# already checks the payload against the reference it was given; reading the
# claim back proves that the reference it checked was the pushed digest.
signed_digest_is() {
	local image=$1 expected=$2 line count=0
	while IFS= read -r line; do
		[ "$line" = "$expected" ] ||
			refuse "${image}: a verified signature names ${line:-no digest}, and this run pushed ${expected}" ||
			return 1
		count=$((count + 1))
	done
	[ "$count" -gt 0 ] || refuse "${image}: cosign verified no signature for ${expected}"
}

cmd_digest() {
	local version=${1:-} artifacts=${2:-}
	if [ -z "$version" ] || [ -z "$artifacts" ]; then
		echo "usage: $0 digest <version-without-v> <artifacts.json>" >&2
		exit 2
	fi
	jq -r "$PUSHED_FILTER" "$artifacts" | pushed_digest "$version"
}

cmd_sign() {
	local tag=${1:-} digest=${2:-} identity image
	if [ -z "$tag" ] || [ -z "$digest" ]; then
		echo "usage: $0 sign <tag> <digest>" >&2
		exit 2
	fi
	identity="$(identity_for "$tag")"
	check_digest "$digest"
	same_signer "$identity" "https://github.com/${GITHUB_WORKFLOW_REF:-}"
	for image in "${IMAGES[@]}"; do
		echo "release image signatures: signing ${image}@${digest} as ${identity}"
		cosign sign --yes "${image}@${digest}"
	done
}

# The empty Docker configuration cmd_verify reads registries through. Global so
# the exit trap can still name it after the function returns.
ANONYMOUS_CONFIG=""

cmd_verify() {
	local tag=${1:-} digest=${2:-} identity image payloads status=0
	if [ -z "$tag" ] || [ -z "$digest" ]; then
		echo "usage: $0 verify <tag> <digest>" >&2
		exit 2
	fi
	identity="$(identity_for "$tag")"
	check_digest "$digest"

	# An empty Docker configuration is what makes this anonymous. The job holds
	# a push credential for both hosts, and a verification that used it could
	# pass for a signature a reader cannot fetch.
	ANONYMOUS_CONFIG="$(mktemp -d)"
	trap 'rm -rf "$ANONYMOUS_CONFIG"' EXIT

	echo "release image signatures: ${digest}, expecting a signature from ${identity}"
	for image in "${IMAGES[@]}"; do
		if ! payloads="$(DOCKER_CONFIG="$ANONYMOUS_CONFIG" cosign verify \
			--certificate-identity "$identity" \
			--certificate-oidc-issuer "$ISSUER" \
			"${image}@${digest}")"; then
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

selftest() {
	local failures=0 checks=0 good bad tag_identity
	good="sha256:$(printf 'a%.0s' $(seq 64))"
	bad="sha256:$(printf 'b%.0s' $(seq 64))"

	pass() {
		checks=$((checks + 1))
		printf '  %-60s %s\n' "$1" "$2"
	}
	fail() {
		failures=$((failures + 1))
		echo "release image signatures self-test: $1" >&2
	}

	# Every rule below is driven through its accepting case first. A rule that
	# refused everything would satisfy each rejection, so the controls are what
	# make the rejections mean anything.
	echo "check-release-image-signatures-selftest: breaking each rule and requiring it to notice"

	# identity_for
	tag_identity="$(identity_for v1.2.3 2>/dev/null)" || tag_identity=""
	if [ "$tag_identity" = "https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/tags/v1.2.3" ]; then
		pass "identity: v1.2.3 names release.yml at the tag" accepted
	else
		fail "identity for v1.2.3 is ${tag_identity:-empty}"
	fi
	local name
	for name in provider-sdk/v0.3.0 v1.2.3/extra 1.2.3 vnext ""; do
		if identity_for "$name" >/dev/null 2>&1; then
			fail "identity accepted ${name:-an empty tag}"
		else
			pass "identity: ${name:-an empty tag}" rejected
		fi
	done

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
	local description running
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

	# signed_digest_is, over the payloads cosign v3 prints for a verified
	# image signature. One payload is a first run; two are a rerun that signed
	# the same digest again.
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
	assert_signed "one signature for the pushed digest" accepted "[$(payload "$good")]"
	assert_signed "a rerun signed the pushed digest again" accepted "[$(payload "$good"),$(payload "$good")]"
	assert_signed "no verified signature" rejected "[]"
	assert_signed "a signature for a substituted digest" rejected "[$(payload "$bad")]"
	assert_signed "one of two signatures names another digest" rejected "[$(payload "$good"),$(payload "$bad")]"
	assert_signed "a payload with no digest claim" rejected '[{"critical":{"type":"https://sigstore.dev/cosign/sign/v1"}}]'
	assert_signed "output that is not JSON" rejected "Verified OK"

	[ "$failures" -eq 0 ] || exit 1
	echo "check-release-image-signatures-selftest: OK (${checks} cases, each control accepted and each broken rule noticed)"
}

main() {
	case "${1:-}" in
	--selftest) selftest ;;
	digest) cmd_digest "${@:2}" ;;
	sign) cmd_sign "${@:2}" ;;
	verify) cmd_verify "${@:2}" ;;
	*)
		echo "usage: $0 digest <version> <artifacts.json> | sign <tag> <digest> | verify <tag> <digest> | --selftest" >&2
		exit 2
		;;
	esac
}

main "$@"
