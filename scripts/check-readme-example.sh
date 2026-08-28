#!/usr/bin/env bash
# Runs the worked example the README prints, and requires the output it
# promises.
#
# The README teaches four commands and shows what each one answers. Nothing ran
# them. A change to the render header, to the apply message, or to the drift
# message falsified the front page silently, and the front page is the one file
# every reader meets first. `docs/site/scripts/*` covers the prose and
# `check-documented-install.sh` covers the `go install` lines; between them sat
# the example itself, which is the part a reader copies.
#
# The example is deliberately the cheapest thing in the documentation to run:
# SQLite, no server, no credentials, no Docker. "Where practical" is not an
# escape here.
#
# What it reads out of README.md, and nothing else:
#
#   * the section marked by `<!-- ptah:readme-example -->`, from the heading
#     below it up to the next `## `;
#   * inside it, a fenced block is EXPECTED OUTPUT when the nearest preceding
#     prose line is `Expected output includes:`;
#   * the first `sql` block that is not expected output is the schema file, and
#     its name is the last `filename.sql` in a code span before it;
#   * every `bash` block is a command, run in order in a throwaway directory.
#
# Each command has to exit 0, and each expectation's non-blank lines have to
# appear in that command's output, in order. "Includes" is the README's own
# word: the blocks are a subset of what the command prints, not the whole of it.
#
# Discovery failing closed is the point of the two minimums. A pattern that
# stopped matching would leave this script running nothing and reporting the
# success it reports on a healthy tree, which is the failure mode it exists to
# prevent.
#
# Two knobs exist for scripts/check-readme-example-selftest.sh, which points
# this gate at fixture READMEs and a stub binary so that its own ability to fail
# can be measured without a build:
#
#   README_EXAMPLE_FILE   the README to read      (default: README.md)
#   README_EXAMPLE_BIN    the ptah binary to run  (default: a fresh build)
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

readme="${README_EXAMPLE_FILE:-README.md}"
marker="${README_EXAMPLE_MARKER:-<!-- ptah:readme-example -->}"

if [ ! -f "$readme" ]; then
	echo "check-readme-example: ${readme} is missing" >&2
	exit 1
fi

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-readme-example.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

binary="${README_EXAMPLE_BIN:-}"
if [ -z "$binary" ]; then
	binary="$work_dir/bin/ptah"
	echo "check-readme-example: building ./cmd/ptah"
	go build -o "$binary" ./cmd/ptah
fi

if [ ! -x "$binary" ]; then
	echo "check-readme-example: ${binary} is not an executable" >&2
	exit 1
fi

README_EXAMPLE_READ="$readme" \
	README_EXAMPLE_MARKER_VALUE="$marker" \
	README_EXAMPLE_RUN="$binary" \
	README_EXAMPLE_DIR="$work_dir/sandbox" \
	python3 - <<'PY'
import os
import re
import subprocess
import sys

README = os.environ["README_EXAMPLE_READ"]
MARKER = os.environ["README_EXAMPLE_MARKER_VALUE"]
BINARY = os.path.abspath(os.environ["README_EXAMPLE_RUN"])
SANDBOX = os.environ["README_EXAMPLE_DIR"]

# Floors, not counts. The example may grow a step without editing this file;
# it may not shrink to nothing while the gate keeps reporting success.
MIN_COMMANDS = 3
MIN_EXPECTATIONS = 3

FENCE = re.compile(r"^```([a-z]*)\s*$")
SQL_FILENAME = re.compile(r"`([A-Za-z0-9_.-]+\.sql)`")
EXPECTATION_CUE = "Expected output includes:"


def fail(message):
    print(f"check-readme-example: {message}", file=sys.stderr)
    raise SystemExit(1)


def section_lines(source):
    """The section the README MARKS as its runnable example.

    Matched by an invisible marker rather than by the heading's words. A heading
    is prose: `## Run it end to end` became `## Try Ptah with SQLite` in an
    ordinary rewrite, and this gate failed with "0 sections headed ..." -- which
    reads as though the example had been deleted rather than renamed. The marker
    says the section is the example, and no rewording of the sentence above it
    changes that. Removing the example still fails, which is the case the gate
    exists for.
    """
    lines = source.split("\n")
    starts = [i for i, line in enumerate(lines) if line.strip() == MARKER]
    if len(starts) != 1:
        fail(
            f"{README} carries {len(starts)} {MARKER!r} markers; expected exactly one."
            " Put it on the line above the heading of the section holding the"
            " runnable example."
        )
    start = starts[0] + 1
    # The marker sits above the heading, so skip it and any blank line after it.
    while start < len(lines) and (not lines[start].strip() or lines[start].startswith("## ")):
        heading = lines[start].startswith("## ")
        start += 1
        if heading:
            break
    end = len(lines)
    for i in range(start, len(lines)):
        if lines[i].startswith("## "):
            end = i
            break
    return lines[start:end]


def blocks(lines):
    """Yield (language, body, cue) for each fenced block in the section.

    `cue` is the nearest preceding non-blank prose line, which is what marks a
    block as the expected output of the command above it.
    """
    found = []
    fence = None
    body = []
    cue = ""
    for line in lines:
        marker = FENCE.match(line)
        if fence is None:
            if marker:
                fence = marker.group(1)
                body = []
                continue
            if line.strip():
                cue = line.strip()
            continue
        if line.strip() == "```":
            found.append((fence, "\n".join(body), cue))
            fence = None
            continue
        body.append(line)
    if fence is not None:
        fail(f"{README} leaves a fenced block open inside the marked example section")
    return found


def plan(lines):
    """Split the section into the schema file, the commands, and what they print."""
    schema_name = None
    schema_body = None
    steps = []
    seen_names = SQL_FILENAME.findall("\n".join(lines))

    for language, body, cue in blocks(lines):
        expected = cue == EXPECTATION_CUE
        if expected:
            if not steps:
                fail(f"{README} shows expected output before any command")
            steps[-1]["expected"].append(body)
            continue
        if language == "bash":
            steps.append({"command": body, "expected": []})
            continue
        if language == "sql" and schema_body is None:
            schema_body = body
            continue

    if schema_body is None:
        fail(f"{README} shows no schema file in the marked example section")
    if not seen_names:
        fail(f"{README} never names the schema file it asks the reader to save")
    schema_name = seen_names[0]
    return schema_name, schema_body, steps


with open(README, encoding="utf-8") as handle:
    lines = section_lines(handle.read())

schema_name, schema_body, steps = plan(lines)
expectations = sum(len(step["expected"]) for step in steps)

# Count the command LINES, not the fenced blocks. A block is the unit this gate
# EXECUTES -- its whole body goes to one `bash -c` -- but it is not the unit a
# README is written in: three commands in one block is a better thing to hand a
# reader than three blocks, and the floor exists to catch a section that runs
# nothing, not to insist on a layout.
commands = sum(
    len([line for line in step["command"].splitlines() if line.strip()])
    for step in steps
)
if commands < MIN_COMMANDS:
    fail(
        f"found {commands} command(s) in the marked example section, expected at least {MIN_COMMANDS}; "
        "either the example changed shape or the pattern stopped matching, and a gate "
        "that runs nothing must not report success"
    )
if expectations < MIN_EXPECTATIONS:
    fail(
        f"found {expectations} expected-output block(s), expected at least {MIN_EXPECTATIONS}; "
        "a command whose output nobody checks is a command this gate did not verify"
    )

os.makedirs(SANDBOX, exist_ok=True)
with open(os.path.join(SANDBOX, schema_name), "w", encoding="utf-8") as handle:
    handle.write(schema_body + "\n")

env = dict(os.environ)
env["PATH"] = os.path.dirname(BINARY) + os.pathsep + env.get("PATH", "")

print(f"check-readme-example: {schema_name}, {commands} command(s) in {len(steps)} block(s), {expectations} expectation(s)")

failed = 0
for step in steps:
    command = step["command"]
    print(f"-- {command.splitlines()[0]}")
    done = subprocess.run(
        ["bash", "-c", command],
        cwd=SANDBOX,
        env=env,
        capture_output=True,
        text=True,
    )
    output = done.stdout + done.stderr
    if done.returncode != 0:
        print(f"check-readme-example: exited {done.returncode}\n{output}", file=sys.stderr)
        failed += 1
        continue

    matched = True
    for block in step["expected"]:
        wanted = [line.rstrip() for line in block.split("\n") if line.strip()]
        remaining = [line.rstrip() for line in output.split("\n")]
        for line in wanted:
            if line in remaining:
                remaining = remaining[remaining.index(line) + 1:]
                continue
            print(
                f"check-readme-example: the README promises a line the command did not print\n"
                f"  wanted: {line}\n"
                f"  in:\n{output}",
                file=sys.stderr,
            )
            matched = False
            break
    if not matched:
        failed += 1
        continue
    print("   ok")

if failed:
    print(f"check-readme-example: {failed} step(s) do not match the README", file=sys.stderr)
    raise SystemExit(1)

print("check-readme-example: the worked example runs and prints what the README promises")
PY
