#!/usr/bin/env bash
# Both registries carry this release, and they carry the same image.
#
# Docker Hub mirrors GHCR rather than replacing it, so every tag exists on both
# hosts. A tag that lands in one and not the other is worse than a single
# registry: a reader following the install page cannot tell which host is
# behind, which is how stokaro/ptah#2362 came to describe three faults instead
# of one.
#
# The property is about what a READER can pull, so it is checked the way a
# reader would -- anonymously, over the registry API, resolving each tag to the
# digest of its manifest list. The release job holds a push credential for both
# hosts, and a check that used it would pass for an image the public cannot
# reach, which is the fault that issue opened with.
#
# Equal digests are the assertion. Two registries that both answer 200 for a
# tag can still serve different images: a partial re-run, a manual push, or a
# manifest built from the wrong architecture set. Comparing content is what
# distinguishes a mirror from two registries that merely both have something.
#
# Given the digest this run pushed, the version tag must also resolve to it on
# both hosts. Agreement alone would accept two registries that both serve an
# image from another run, and the release job signs the pushed digest right
# after this check (scripts/check-release-image-signatures.sh), so a tag that
# names any other digest would leave a reader resolving it to an image nothing
# signed. `latest` is held to agreement only: two release tags pushed together
# race for it, and neither run owns the result.
set -euo pipefail

readonly REPOSITORY="stokaro/ptah"

# Each registry answers a token endpoint and a manifest endpoint, and the two
# are not the same host on Docker Hub. Declared once so a third registry is a
# row rather than an edit to the logic.
readonly GHCR_AUTH="https://ghcr.io/token?scope=repository:${REPOSITORY}:pull"
readonly GHCR_API="https://ghcr.io/v2/${REPOSITORY}/manifests"
readonly HUB_AUTH="https://auth.docker.io/token?service=registry.docker.io&scope=repository:${REPOSITORY}:pull"
readonly HUB_API="https://registry-1.docker.io/v2/${REPOSITORY}/manifests"

# Every media type a manifest list can arrive as. Omitting the OCI index makes
# a registry answer with a single-architecture manifest instead, so the digests
# would differ for a reason that is about this script rather than the release.
readonly ACCEPT='application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json'

# digest_of prints the manifest digest a tag resolves to, or nothing.
digest_of() {
	local auth=$1 api=$2 tag=$3 token
	token="$(curl -fsS "$auth" 2>/dev/null | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')" || return 0
	[ -n "$token" ] || return 0
	curl -fsSI -H "Authorization: Bearer $token" -H "Accept: ${ACCEPT}" "${api}/${tag}" 2>/dev/null |
		tr -d '\r' | sed -n 's/^[Dd]ocker-[Cc]ontent-[Dd]igest: //p'
}

# compare_digests is the whole rule, and it is separate from the network so the
# self-test can drive every branch without a registry. The fourth argument is
# the digest this run pushed, or empty where the tag is held to agreement only.
compare_digests() {
	local tag=$1 ghcr=$2 hub=$3 pushed=${4:-}
	if [ -z "$ghcr" ] && [ -z "$hub" ]; then
		echo "  ${tag}: neither registry answers" >&2
		return 1
	fi
	if [ -z "$ghcr" ]; then
		echo "  ${tag}: only Docker Hub answers; GHCR is behind" >&2
		return 1
	fi
	if [ -z "$hub" ]; then
		echo "  ${tag}: only GHCR answers; Docker Hub is behind" >&2
		return 1
	fi
	if [ "$ghcr" != "$hub" ]; then
		echo "  ${tag}: the registries serve different images" >&2
		echo "    ghcr.io   ${ghcr}" >&2
		echo "    docker.io ${hub}" >&2
		return 1
	fi
	if [ -n "$pushed" ] && [ "$ghcr" != "$pushed" ]; then
		echo "  ${tag}: both registries serve an image this run did not push" >&2
		echo "    served ${ghcr}" >&2
		echo "    pushed ${pushed}" >&2
		return 1
	fi
	printf '  %-12s %s\n' "$tag" "$ghcr"
}

selftest() {
	local failures=0
	assert_refused() {
		local name=$1 tag=$2 ghcr=$3 hub=$4 pushed=${5:-}
		if compare_digests "$tag" "$ghcr" "$hub" "$pushed" >/dev/null 2>&1; then
			echo "release image mirror self-test: ${name} was accepted" >&2
			failures=$((failures + 1))
			return
		fi
		printf '  %-44s rejected\n' "$name"
	}

	assert_accepted() {
		local name=$1 tag=$2 ghcr=$3 hub=$4 pushed=${5:-}
		if ! compare_digests "$tag" "$ghcr" "$hub" "$pushed" >/dev/null 2>&1; then
			echo "release image mirror self-test: ${name} was rejected" >&2
			failures=$((failures + 1))
			return
		fi
		printf '  %-44s accepted\n' "$name"
	}

	echo "check-release-image-mirror-selftest: breaking the rule and requiring it to notice"

	# The controls first. A rule that refused everything would satisfy every
	# case below, so these are what make the five rejections mean anything.
	assert_accepted "the control: equal digests" 1.0.0 "sha256:aaa" "sha256:aaa"
	assert_accepted "the control: equal and the pushed digest" 1.0.0 "sha256:aaa" "sha256:aaa" "sha256:aaa"

	assert_refused "GHCR has the tag and Docker Hub does not" 1.0.0 "sha256:aaa" ""
	assert_refused "Docker Hub has the tag and GHCR does not" 1.0.0 "" "sha256:aaa"
	assert_refused "neither registry answers" 1.0.0 "" ""
	assert_refused "both answer with different images" 1.0.0 "sha256:aaa" "sha256:bbb"
	assert_refused "both agree on a digest this run did not push" 1.0.0 "sha256:bbb" "sha256:bbb" "sha256:aaa"

	[ "$failures" -eq 0 ] || exit 1
	echo "check-release-image-mirror-selftest: OK (5 broken rules noticed, 2 controls accepted)"
}

main() {
	if [ "${1:-}" = "--selftest" ]; then
		selftest
		return
	fi
	local version=${1:-} pushed=${2:-}
	if [ -z "$version" ]; then
		echo "usage: $0 <version-without-v> [<pushed-digest>] | --selftest" >&2
		exit 2
	fi

	echo "release image mirror: ${REPOSITORY} at ${version} and latest"
	local status=0
	compare_digests "$version" \
		"$(digest_of "$GHCR_AUTH" "$GHCR_API" "$version")" \
		"$(digest_of "$HUB_AUTH" "$HUB_API" "$version")" \
		"$pushed" || status=1
	compare_digests latest \
		"$(digest_of "$GHCR_AUTH" "$GHCR_API" latest)" \
		"$(digest_of "$HUB_AUTH" "$HUB_API" latest)" || status=1
	if [ "$status" -ne 0 ]; then
		echo "release image mirror: the registries do not serve this release; see stokaro/ptah#2362" >&2
		exit 1
	fi
	echo "release image mirror: OK (both registries serve the same image for both tags)"
}

main "$@"
