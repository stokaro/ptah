#!/usr/bin/env bash
#
# Fail closed when a pull-request workflow scopes expensive jobs by diff.
#
# A skipped GitHub Actions job reports success to branch protection. That is
# useful only when the classifier itself succeeded and the skip agrees with its
# decision. This gate checks both facts and gives branch protection one stable
# status to require.
set -euo pipefail

usage() {
	cat >&2 <<'EOF'
usage: check-ci-scope-gate.sh --scope-result RESULT --scope true|false \
  [--required NAME=RESULT]... [--scoped NAME=RESULT]...
       check-ci-scope-gate.sh --selftest
       check-ci-scope-gate.sh --check-workflows
EOF
}

fail() {
	printf 'CI scope gate: %s\n' "$*" >&2
	return 1
}

split_result() {
	local pair="$1"
	if [[ "$pair" != *=* || "$pair" == =* ]]; then
		fail "invalid job result '$pair'; expected NAME=RESULT"
		return 1
	fi
	JOB_NAME="${pair%%=*}"
	JOB_RESULT="${pair#*=}"
}

evaluate() {
	local scope_result="$1" scope="$2"
	shift 2
	local mode="" pair

	if [[ "$scope_result" != success ]]; then
		fail "scope job concluded '$scope_result', not 'success'"
		return 1
	fi
	if [[ "$scope" != true && "$scope" != false ]]; then
		fail "scope output is '$scope'; expected 'true' or 'false'"
		return 1
	fi

	while [[ $# -gt 0 ]]; do
		case "$1" in
		--required | --scoped)
			mode="$1"
			shift
			if [[ $# -eq 0 ]]; then
				fail "$mode needs NAME=RESULT"
				return 1
			fi
			pair="$1"
			;;
		*)
			fail "unknown argument '$1'"
			return 1
			;;
		esac
		split_result "$pair" || return 1
		case "$mode" in
		--required)
			if [[ "$JOB_RESULT" != success ]]; then
				fail "required job '$JOB_NAME' concluded '$JOB_RESULT', not 'success'"
				return 1
			fi
			;;
		--scoped)
			local expected=skipped
			[[ "$scope" == true ]] && expected=success
			if [[ "$JOB_RESULT" != "$expected" ]]; then
				fail "scoped job '$JOB_NAME' concluded '$JOB_RESULT'; scope=$scope requires '$expected'"
				return 1
			fi
			;;
		esac
		shift
	done

	printf 'CI scope gate: OK (scope=%s)\n' "$scope"
}

selftest() {
	local failures=0
	expect() {
		local name="$1" want="$2"
		shift 2
		local status=0
		evaluate "$@" >/dev/null 2>&1 || status=$?
		if [[ ( "$want" == pass && "$status" -ne 0 ) ||
			( "$want" == fail && "$status" -eq 0 ) ]]; then
			printf 'check-ci-scope-gate selftest FAILED: %s\n' "$name" >&2
			failures=$((failures + 1))
		fi
	}

	expect 'applicable jobs pass' pass success true \
		--required policy=success --scoped linux=success --scoped windows=success
	expect 'inapplicable jobs are visibly skipped' pass success false \
		--required policy=success --scoped linux=skipped --scoped windows=skipped
	expect 'scope failure fails closed' fail failure false --scoped linux=skipped
	expect 'missing scope output fails closed' fail success '' --scoped linux=skipped
	expect 'invalid scope output fails closed' fail success maybe --scoped linux=skipped
	expect 'applicable skipped job is refused' fail success true --scoped linux=skipped
	expect 'failed applicable job is refused' fail success true --scoped linux=failure
	expect 'unexpected inapplicable execution is refused' fail success false --scoped linux=success
	expect 'failed required job is refused' fail success false --required policy=failure --scoped linux=skipped
	expect 'malformed job result is refused' fail success true --scoped malformed

	# The workflow check, against workflows written for it: five gates, each
	# with the condition given.
	expect_workflows() {
		local name="$1" want="$2" condition="$3" dir gate status=0
		dir="$(mktemp -d)"
		mkdir -p "$dir/.github/workflows"
		{
			printf 'jobs:\n'
			for gate in one two three four five; do
				printf '  %s-gate:\n' "$gate"
				[[ -z "$condition" ]] || printf '    if: %s\n' "$condition"
				printf '    runs-on: ubuntu-latest\n'
			done
		} >"$dir/.github/workflows/fixture.yml"
		check_workflows "$dir" 5 >/dev/null 2>&1 || status=$?
		rm -rf "$dir"
		if [[ ( "$want" == pass && "$status" -ne 0 ) ||
			( "$want" == fail && "$status" -eq 0 ) ]]; then
			printf 'check-ci-scope-gate selftest FAILED: %s\n' "$name" >&2
			failures=$((failures + 1))
		fi
	}
	expect_workflows 'a gate that gives a canceled run no verdict passes' pass \
		"\${{ !cancelled() && github.event_name == 'pull_request' }}"
	expect_workflows 'a gate that runs in a canceled run is refused' fail \
		"always() && github.event_name == 'pull_request'"
	expect_workflows 'a gate that names !cancelled() and still runs in a canceled run is refused' fail \
		"\${{ !cancelled() || always() }}"
	expect_workflows 'a gate with no status function is refused' fail "github.event_name == 'pull_request'"
	expect_workflows 'a gate with no condition is refused' fail ''

	if [[ "$failures" -ne 0 ]]; then
		printf 'check-ci-scope-gate: %d selftest(s) failed\n' "$failures" >&2
		exit 1
	fi
	printf 'check-ci-scope-gate: OK (15 selftests)\n'
}

# check_workflows requires every gate job under root to carry
# `if: ${{ !cancelled() }}`, and refuses fewer than floor gates.
#
# A gate aggregates other jobs through `needs`, and a job with `needs` runs only
# when everything it needs succeeded. Without a status function a gate is
# therefore skipped the moment one of the jobs it judges fails or is scoped out
# -- the one case it was written for -- and a skipped job reports success to
# branch protection, so the gate stays green about that failure. A gate
# carrying no `if:` at all is the same defect in an ordinary-looking job.
#
# `always()` closes that hole and opens another: it also runs the gate in a run
# that was canceled, where every job it judges reads `cancelled`, so the gate
# fails. Concurrency cancels a pull request's run whenever a newer one starts
# for it, and a label added as the pull request opens is enough, so that
# failure stays on the pull request beside the newer run's verdict.
# `!cancelled()` runs the gate on success and on failure and leaves a canceled
# run without one.
#
# Gates are discovered by the `-gate` job-id suffix rather than listed, so a
# workflow added later is governed without an edit here. The check cannot see a
# gate named something else; the naming convention is what makes the rule hold.
check_workflows() {
	local root="$1" floor="$2" found=0 failures=0

	local file job condition
	while IFS=: read -r file job condition; do
		found=$((found + 1))
		# Reported separately because a gate with no condition at all reads as an
		# ordinary job, so the message has to name what is missing.
		if [[ -z "$condition" ]]; then
			printf 'CI scope gate: %s job %s has no `if:`, so it is skipped whenever a job it aggregates fails; use ${{ !cancelled() }}\n' \
				"$file" "$job" >&2
			failures=$((failures + 1))
			continue
		fi
		if [[ "$condition" == *"always()"* ]]; then
			printf 'CI scope gate: %s job %s uses always(), which also runs it in a run a newer one canceled and leaves that failure on the pull request; use ${{ !cancelled() }}\n' \
				"$file" "$job" >&2
			failures=$((failures + 1))
			continue
		fi
		if [[ "$condition" != *'!cancelled()'* ]]; then
			printf 'CI scope gate: %s job %s must use ${{ !cancelled() }}; without it the gate is skipped when a job it judges fails, and a skip reads as success\n' \
				"$file" "$job" >&2
			failures=$((failures + 1))
		fi
	done < <(
		cd "$root" && awk '
			function flush() {
				if (job != "") {
					print f ":" job ":" condition
					job = ""
					condition = ""
				}
			}
			FNR == 1 { flush(); f = FILENAME }
			/^  [A-Za-z0-9_-]+-gate:[[:space:]]*$/ {
				flush()
				job = $1
				sub(/:$/, "", job)
				next
			}
			/^  [A-Za-z0-9_-]+:[[:space:]]*$/ { flush(); next }
			job != "" && condition == "" && /^    if:/ {
				condition = $0
				sub(/^    if:[[:space:]]*/, "", condition)
			}
			END { flush() }
		' .github/workflows/*.yml
	)

	# A pattern that matches nothing reports exactly what a clean tree reports.
	# Eleven gate jobs were present when this was written; the floor sits well
	# below that so removing a workflow does not fail the check, and well above
	# zero so a broken pattern does.
	if ((found < floor)); then
		printf 'CI scope gate: found only %d gate job(s); the discovery pattern is broken\n' "$found" >&2
		return 1
	fi
	if ((failures > 0)); then
		return 1
	fi
	printf 'check-ci-scope-gate: OK (%d gate jobs cannot be skipped)\n' "$found"
}

# Both entry points run after every function they call is defined.
if [[ "${1:-}" == --selftest ]]; then
	selftest
	exit 0
fi

if [[ "${1:-}" == --check-workflows ]]; then
	check_workflows "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" 5
	exit 0
fi


scope_result=""
scope=""
arguments=()
while [[ $# -gt 0 ]]; do
	case "$1" in
	--scope-result)
		[[ $# -ge 2 ]] || { usage; exit 2; }
		scope_result="$2"
		shift 2
		;;
	--scope)
		[[ $# -ge 2 ]] || { usage; exit 2; }
		scope="$2"
		shift 2
		;;
	--required | --scoped)
		[[ $# -ge 2 ]] || { usage; exit 2; }
		arguments+=("$1" "$2")
		shift 2
		;;
	*)
		usage
		exit 2
		;;
	esac
done

evaluate "$scope_result" "$scope" "${arguments[@]}"
