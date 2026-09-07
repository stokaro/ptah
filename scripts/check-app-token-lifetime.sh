#!/usr/bin/env bash
# Refuses a job that mints a GitHub App token and may outlive it.
#
# actions/create-github-app-token mints an INSTALLATION token, and an
# installation token is valid for one hour from the moment it is issued. Nothing
# renews it: the job holds one string for as long as it runs.
#
# A job that mints early and uses the token late is the shape at risk, and it is
# the shape worth having -- release.yml mints before the checkout on purpose, so
# that a key which cannot mint fails in seconds rather than after the release
# assets and the container images are already pushed. The cost of that ordering
# is a token whose clock starts at the top of the job.
#
# So the timeout is the guarantee. A job capped below the token's life cannot
# reach a step with an expired one, whatever it does in between. Raising the cap
# is what silently removes the guarantee, and the failure it buys arrives at the
# worst moment: after everything else in the release has already been published.
#
# The rule: a job whose steps use create-github-app-token declares
# timeout-minutes, and declares it below the margin here. A job that genuinely
# needs longer mints immediately before the step that spends the token, in a job
# of its own, rather than raising this.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# The token's life, and the cap a minting job may declare. The margin covers the
# steps that run before the mint and the difference between "the job started"
# and "the token was issued", neither of which timeout-minutes counts.
readonly tokenLifeMinutes=60
readonly maxMinutes=50

status=0
minting=0

for workflow in .github/workflows/*.yml; do
	# One record per job: file, job, timeout-minutes (or `-`), and whether any
	# of its steps mint. A job key sits at two spaces and a job-level
	# timeout-minutes at four, which is what separates it from a step's own.
	while IFS=$'\t' read -r job timeout mints; do
		[ "$mints" = "1" ] || continue
		minting=$((minting + 1))

		if [ "$timeout" = "-" ]; then
			echo "check-app-token-lifetime: $workflow job \`$job\` mints an app token and declares no timeout-minutes" >&2
			echo "  Without a cap the job may still be running when the token expires ${tokenLifeMinutes} minutes after it was issued." >&2
			echo "  Declare timeout-minutes at most ${maxMinutes}." >&2
			status=1
			continue
		fi

		if [ "$timeout" -gt "$maxMinutes" ]; then
			echo "check-app-token-lifetime: $workflow job \`$job\` mints an app token and may run ${timeout} minutes" >&2
			echo "  An installation token is valid for ${tokenLifeMinutes} minutes from issue and nothing renews it, so a step" >&2
			echo "  reached after that holds an expired string. Cap the job at ${maxMinutes} minutes, or mint the token in a" >&2
			echo "  job of its own, immediately before the step that spends it." >&2
			status=1
		fi
	done < <(awk '
		function flush() {
			if (job != "") printf "%s\t%s\t%s\n", job, (timeout == "" ? "-" : timeout), mints
			job = ""; timeout = ""; mints = 0
		}
		BEGIN { in_jobs = 0; job = ""; timeout = ""; mints = 0 }
		/^jobs:[[:space:]]*$/ { flush(); in_jobs = 1; next }
		/^[^[:space:]#]/ { flush(); in_jobs = 0; next }
		in_jobs && /^  [A-Za-z0-9_.-]+:[[:space:]]*$/ {
			flush()
			job = $0
			sub(/^  /, "", job)
			sub(/:[[:space:]]*$/, "", job)
			next
		}
		in_jobs && job != "" && /^    timeout-minutes:[[:space:]]*[0-9]+/ {
			timeout = $0
			sub(/^[^0-9]*/, "", timeout)
			sub(/[^0-9].*$/, "", timeout)
			next
		}
		in_jobs && job != "" && /create-github-app-token/ { mints = 1; next }
		END { flush() }
	' "$workflow")
done

# A sweep that found nothing to judge reports success at exactly the moment the
# extraction stopped working, and this one parses YAML with awk.
if [ "$minting" -eq 0 ]; then
	echo "check-app-token-lifetime: no job mints an app token; the extraction is broken" >&2
	exit 1
fi

if [ "$status" -eq 0 ]; then
	echo "check-app-token-lifetime: OK ($minting jobs mint a token, each capped below $tokenLifeMinutes minutes)"
fi

exit "$status"
