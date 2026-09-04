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

	if [[ "$failures" -ne 0 ]]; then
		printf 'check-ci-scope-gate: %d selftest(s) failed\n' "$failures" >&2
		exit 1
	fi
	printf 'check-ci-scope-gate: OK (10 selftests)\n'
}

if [[ "${1:-}" == --selftest ]]; then
	selftest
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
