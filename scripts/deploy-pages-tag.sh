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
# GITHUB_* variable -- so the lever left is to create the deployment here, under
# a build version of this tag's own.
#
# That version has to name a commit the repository holds. Measured against the
# API with one artifact and four values: the annotated tag object's id and an
# invented id are both refused with `404 Not Found`, and two real commits are
# not. So the tag object cannot be the build version, and the commit the tag
# points at is the one master already deployed -- which is the collision itself.
#
# The tag therefore deploys under a commit of its own: the tag's tree, the tag's
# commit as its parent, and the tagger as author and committer with the tag's
# own date. Nothing references it, so it appears in no history and in no
# listing; it exists, which is what a build version has to do, and its parent is
# the tag's commit, so it can never be the commit master deployed.
#
# Its id is not predicted here. The API renders the fields it is given its own
# way -- the same inputs through `git commit-tree` hash to something else, which
# is measured rather than assumed -- so a re-run of the tag creates another
# commit and deploys again under it. That is what a re-run asks for; what must
# not repeat is the collision with master, and that is decided by the parent.
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

# deploymentCommitFields prints what the tag's deployment commit is built from,
# tab-separated: tree, parent, tagger name, tagger email, tagger date.
#
# It is separate from the call that creates the commit so the rule it carries --
# which inputs decide the id -- can be driven without a registry or a token, and
# so a reader can see that every one of them comes from the tag.
#
# A lightweight tag is refused rather than deployed under the commit it names.
# That commit is the one master already deployed, so the collision would come
# back, silently; and a lightweight tag carries no tagger for the deployment to
# be built from. docs/release_process.md creates annotated tags.
deploymentCommitFields() {
	local tag=$1 kind
	kind="$(git cat-file -t "refs/tags/${tag}" 2>/dev/null || true)"
	if [ "$kind" != tag ]; then
		echo "deploy-pages-tag: ${tag} is not an annotated tag (git says '${kind:-nothing}')." >&2
		echo "  It carries no tagger, so its deployment would have to borrow the commit master already deployed." >&2
		return 1
	fi
	printf '%s\t%s\t%s\t%s\t%s\n' \
		"$(git rev-parse "refs/tags/${tag}^{tree}")" \
		"$(git rev-parse "refs/tags/${tag}^{commit}")" \
		"$(git for-each-ref --format='%(taggername)' "refs/tags/${tag}")" \
		"$(git for-each-ref --format='%(taggeremail:trim)' "refs/tags/${tag}")" \
		"$(git for-each-ref --format='%(taggerdate:iso-strict)' "refs/tags/${tag}")"
}

# deploymentCommit creates the commit this tag deploys under and prints its id.
#
# Every field is the tag's, so the commit says which release it carries, and its
# parent is the tag's commit, so it is never the id master deployed.
deploymentCommit() {
	local tag=$1 fields tree parent name email date
	fields="$(deploymentCommitFields "$tag")" || return 1
	IFS=$'\t' read -r tree parent name email date <<<"$fields"
	gh api --method POST "repos/${GITHUB_REPOSITORY}/git/commits" \
		-f message="Pages deployment for ${tag}" \
		-f tree="$tree" \
		-f "parents[]=$parent" \
		-f "author[name]=$name" \
		-f "author[email]=$email" \
		-f "author[date]=$date" \
		-f "committer[name]=$name" \
		-f "committer[email]=$email" \
		-f "committer[date]=$date" \
		--jq '.sha'
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

# selftest drives the rule this script can be held to without a registry: which
# commit a tag deploys under.
#
# The commit is built locally here from the same fields the API is given. The id
# is not the one the API answers -- it renders the fields its own way -- and the
# property under test is not the id: a commit whose parent is the tag's commit
# is not that commit, whoever hashes it.
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
	local work=$1 commit first lightweight

	git -C "$work" init -q
	git -C "$work" -c user.email=selftest@example.com -c user.name=selftest \
		commit -q --allow-empty -m "the commit both tags point at"
	commit="$(git -C "$work" rev-parse HEAD)"
	git -C "$work" -c user.email=tagger@example.com -c user.name=tagger \
		tag -a v9.9.9 -m "an annotated tag"
	git -C "$work" tag v9.9.8

	echo "deploy-pages-tag-selftest: the commit a tag deploys under"

	first="$(cd "$work" && buildDeploymentCommitLocally v9.9.9)"
	if [ "$first" = "$commit" ]; then
		echo "  the tag deployed under the commit it points at, which is the collision" >&2
		return 1
	fi
	printf '  %-52s %s\n' "an annotated tag deploys under a commit of its own" "${first:0:12}"

	if [ "$(cd "$work" && git rev-parse "${first}^")" != "$commit" ]; then
		echo "  the deployment commit does not descend from the tag, so it names another release" >&2
		return 1
	fi
	printf '  %-52s %s\n' "with the tag's own commit as its parent" "${commit:0:12}"

	if lightweight="$(cd "$work" && deploymentCommitFields v9.9.8 2>/dev/null)"; then
		echo "  a lightweight tag was accepted: ${lightweight}" >&2
		return 1
	fi
	printf '  %-52s %s\n' "a lightweight tag is refused" "rejected"

	echo "deploy-pages-tag-selftest: OK"
}

# buildDeploymentCommitLocally writes a deployment commit with git rather than
# through the API, from the same fields, so the selftest can measure what they
# produce without a token.
buildDeploymentCommitLocally() {
	local tag=$1 fields tree parent name email date
	fields="$(deploymentCommitFields "$tag")" || return 1
	IFS=$'\t' read -r tree parent name email date <<<"$fields"
	GIT_AUTHOR_NAME="$name" GIT_AUTHOR_EMAIL="$email" GIT_AUTHOR_DATE="$date" \
		GIT_COMMITTER_NAME="$name" GIT_COMMITTER_EMAIL="$email" GIT_COMMITTER_DATE="$date" \
		git commit-tree "$tree" -p "$parent" -m "Pages deployment for ${tag}"
}

main() {
	if [ "${1:-}" = "--selftest" ]; then
		selftest
		return
	fi

	local tag=${GITHUB_REF_NAME:?the tag to deploy} version artifact token created deploymentID pageURL
	version="$(deploymentCommit "$tag")"
	artifact="$(runArtifactID)"
	token="$(oidcToken)"
	echo "deploy-pages-tag: deploying artifact ${artifact} for ${tag} under build version ${version}"

	if ! created="$(gh api --method POST "repos/${GITHUB_REPOSITORY}/pages/deployments" \
		-F artifact_id="$artifact" \
		-f pages_build_version="$version" \
		-f oidc_token="$token" 2>&1)"; then
		# What the API said, rather than gh's summary of it. A refused build
		# version and a refused token are both "Not Found" to a reader who only
		# sees the status.
		echo "deploy-pages-tag: Pages refused the deployment for ${version}:" >&2
		echo "  ${created}" >&2
		return 1
	fi
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
