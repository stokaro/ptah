#!/usr/bin/env bash
# Deploy this run's Pages artifact under a build version the tag owns.
#
# GitHub Pages identifies a deployment by `pages_build_version`, and
# actions/deploy-pages sets that from GITHUB_SHA and from nothing else. A
# release tag points at a commit master has already deployed, so the tag's
# deployment carries a build version Pages has already served: the create call
# answers without a deployment id, the action falls back to polling the build
# version, reads the status of the master deployment that succeeded earlier,
# and reports success seconds after starting a 26 MB upload that never rolled
# out. Measured on v0.5.0 in run 34220970258 (stokaro/ptah#3082).
#
# GITHUB_SHA cannot be overridden -- Actions refuses a workflow that assigns a
# GITHUB_* variable -- so the lever left is to create the deployment here. The
# annotated tag object's own id is the build version: a real object id, it
# differs from the commit the tag points at, and it is the same on every re-run
# of the same tag, so a re-run replaces its own deployment instead of creating a
# second one.
#
# Everything else is what actions/deploy-pages does, in the order it does it:
# find this run's artifact, mint an OIDC token, create the deployment, poll
# until Pages reports `succeed`. A push to master keeps using the action, where
# the commit is already a build version of its own.
set -euo pipefail

# artifactName is what actions/upload-pages-artifact writes.
readonly artifactName="github-pages"

# deployTimeoutSeconds bounds the poll. actions/deploy-pages allows ten minutes
# and this job is capped at ten, so the script has to give up first to leave a
# message behind rather than being killed mid-poll.
readonly deployTimeoutSeconds=480
readonly pollIntervalSeconds=5

# tagBuildVersion prints the object id of the annotated tag.
#
# A lightweight tag is refused rather than deployed: its id IS the commit id, so
# it reproduces the collision this script exists to avoid, and it would do it
# silently. docs/release_process.md creates annotated tags.
tagBuildVersion() {
	local tag=$1 kind
	kind="$(git cat-file -t "refs/tags/${tag}" 2>/dev/null || true)"
	if [ "$kind" != tag ]; then
		echo "deploy-pages-tag: ${tag} is not an annotated tag (git says '${kind:-nothing}')." >&2
		echo "  Its build version would be the commit id, which is the one master already deployed." >&2
		return 1
	fi
	git rev-parse "refs/tags/${tag}"
}

# runArtifactID prints the id of this run's Pages artifact, refusing anything
# but exactly one match: two would make the deployment's content a coin toss,
# and none means the build job uploaded nothing.
runArtifactID() {
	local ids count
	ids="$(gh api "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts" \
		--paginate --jq ".artifacts[] | select(.name == \"${artifactName}\" and .expired == false) | .id")"
	count="$(printf '%s' "$ids" | grep -c . || true)"
	if [ "$count" != 1 ]; then
		echo "deploy-pages-tag: run ${GITHUB_RUN_ID} carries ${count} unexpired ${artifactName} artifacts, expected 1" >&2
		return 1
	fi
	printf '%s' "$ids"
}

# oidcToken mints the token the Pages deployment API requires. The audience is
# the default one, which is what core.getIDToken() asks for in the action.
oidcToken() {
	curl -fsS -H "Authorization: bearer ${ACTIONS_ID_TOKEN_REQUEST_TOKEN}" \
		"${ACTIONS_ID_TOKEN_REQUEST_URL}" | jq -r '.value'
}

# awaitDeployment polls until Pages reports the deployment as succeeded.
#
# The terminal statuses are the ones actions/deploy-pages treats as final;
# anything else is an intermediate state and keeps the poll running until the
# deadline. `deployment_attempt_error` is temporary there and stays temporary
# here: Pages schedules its own retry.
awaitDeployment() {
	local deploymentID=$1 status deadline
	deadline=$((SECONDS + deployTimeoutSeconds))
	while [ "$SECONDS" -lt "$deadline" ]; do
		status="$(gh api "repos/${GITHUB_REPOSITORY}/pages/deployments/${deploymentID}" --jq '.status' 2>/dev/null || echo unknown_status)"
		case "$status" in
		succeed)
			echo "deploy-pages-tag: Pages reports ${deploymentID} succeeded"
			return 0
			;;
		deployment_failed | deployment_content_failed | deployment_cancelled | deployment_lost)
			echo "deploy-pages-tag: Pages reports ${status} for ${deploymentID}" >&2
			return 1
			;;
		*)
			echo "  ${status}"
			;;
		esac
		sleep "$pollIntervalSeconds"
	done
	echo "deploy-pages-tag: ${deploymentID} did not reach 'succeed' within ${deployTimeoutSeconds}s" >&2
	return 1
}

# selftest drives the one rule this script can be held to without a Pages
# deployment: which object id a tag deploys under, and the refusal that keeps a
# lightweight tag from reproducing the collision.
selftest() {
	local work status=0
	work="$(mktemp -d)"
	selftestIn "$work" || status=$?
	rm -rf "$work"
	return "$status"
}

# selftestIn is selftest's body, split out so the temporary repository is
# removed on both paths without a RETURN trap, which would outlive this
# function and fire again in its caller.
selftestIn() {
	local work=$1 commit annotated lightweight

	git -C "$work" init -q
	git -C "$work" -c user.email=selftest@example.com -c user.name=selftest \
		commit -q --allow-empty -m "the commit both tags point at"
	commit="$(git -C "$work" rev-parse HEAD)"
	git -C "$work" -c user.email=selftest@example.com -c user.name=selftest \
		tag -a v9.9.9 -m "an annotated tag"
	git -C "$work" tag v9.9.8

	echo "deploy-pages-tag-selftest: the build version a tag deploys under"

	annotated="$(cd "$work" && tagBuildVersion v9.9.9)"
	if [ "$annotated" = "$commit" ]; then
		echo "  an annotated tag deployed under the commit id, which is the collision" >&2
		return 1
	fi
	printf '  %-52s %s\n' "an annotated tag deploys under its own object id" "${annotated:0:12}"

	if [ "$annotated" != "$(cd "$work" && tagBuildVersion v9.9.9)" ]; then
		echo "  the same tag answered two build versions, so a re-run would deploy twice" >&2
		return 1
	fi
	printf '  %-52s %s\n' "and answers the same id on a re-run" "stable"

	if lightweight="$(cd "$work" && tagBuildVersion v9.9.8 2>/dev/null)"; then
		echo "  a lightweight tag was accepted, deploying under ${lightweight}" >&2
		return 1
	fi
	printf '  %-52s %s\n' "a lightweight tag is refused" "rejected"

	echo "deploy-pages-tag-selftest: OK"
}

main() {
	if [ "${1:-}" = "--selftest" ]; then
		selftest
		return
	fi

	local tag=${GITHUB_REF_NAME:?the tag to deploy} version artifact token created deploymentID pageURL
	version="$(tagBuildVersion "$tag")"
	artifact="$(runArtifactID)"
	token="$(oidcToken)"
	echo "deploy-pages-tag: deploying artifact ${artifact} for ${tag} under build version ${version}"

	created="$(gh api --method POST "repos/${GITHUB_REPOSITORY}/pages/deployments" \
		-F artifact_id="$artifact" \
		-f pages_build_version="$version" \
		-f oidc_token="$token")"
	# The id is read the way actions/deploy-pages reads it, including the
	# fallback to the build version -- the create response carried no id in the
	# run that opened stokaro/ptah#3082. The fallback is safe here for the
	# reason this script exists: the build version names this tag alone.
	deploymentID="$(printf '%s' "$created" | jq -r '.id // (.status_url // "" | split("/") | last) // ""')"
	if [ -z "$deploymentID" ] || [ "$deploymentID" = null ]; then
		deploymentID="$version"
	fi
	pageURL="$(printf '%s' "$created" | jq -r '.page_url // ""')"
	if [ -n "${GITHUB_OUTPUT:-}" ]; then
		echo "page_url=${pageURL}" >>"$GITHUB_OUTPUT"
	fi

	awaitDeployment "$deploymentID"
}

main "$@"
