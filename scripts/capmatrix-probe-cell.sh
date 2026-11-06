#!/usr/bin/env bash
# Probe a capability cell, and start its server once more if the server died.
#
# Oracle's image can stop its own startup on ORA-04021 while it resets a password.
# The container exits, the probe knocks on a closed port until its deadline, and
# the cell reads as a capability disagreement that says nothing about the
# capability. The next start of the same image works.
#
# Only a dead container gets a second start. A server that stays up and answers
# wrongly is the cell's result, and so is one that refuses connections while
# running. A cell with no container has nothing to restart. A server that dies
# twice fails the cell, so an image that can never start still reads as red.
#
# Usage: scripts/capmatrix-probe-cell.sh <tier> <wait> <log file>
#
# Reads CELL_ID and RUNNER_TEMP, and the `docker-args` file the start step wrote.
# The exit code is the probe's: it is the measurement, so nothing here pipes it.
# scripts/capmatrix-probe-cell-selftest.sh holds this to the above.
set -uo pipefail

readonly CONTAINER=capability-probe-server
tier="$1"
wait_for="$2"
log="$3"

probe() {
	"${RUNNER_TEMP}/capmatrix" probe \
		--cell "${CELL_ID}" \
		--tier "${tier}" \
		--wait "${wait_for}" \
		--result "results/${CELL_ID}.json" >>"${log}" 2>&1
}

: >"${log}"
probe
code=$?
if [ "${code}" -eq 0 ] || [ "$(docker inspect --format '{{.State.Running}}' "${CONTAINER}" 2>/dev/null)" != false ]; then
	exit "${code}"
fi

{
	echo "the server for ${CELL_ID} exited during the probe, so it starts once more; its last output:"
	docker logs --tail 15 "${CONTAINER}" 2>&1
} >>"${log}"
docker rm --force "${CONTAINER}" >/dev/null 2>&1
args=()
while IFS= read -r arg; do
	args+=("${arg}")
done <"${RUNNER_TEMP}/docker-args"
docker run --detach --name "${CONTAINER}" "${args[@]}" >/dev/null 2>>"${log}"
probe
