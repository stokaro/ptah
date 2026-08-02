#!/usr/bin/env python3
"""Mutation check: break one rule at a time and record which tests go red.

A rule that no test fails on is a rule nobody is holding. This harness applies
one textual mutation to the working tree, runs the selected packages with
-count=1, restores the file, and reports whether the suite noticed.

Usage:

    scripts/mutation-check.py scripts/mutations/<spec>.json [--repo PATH]

A spec is JSON:

    {
      "packages": ["./cmd/atlas/", "./internal/migratesum/"],
      "env": {"PTAH_ATLAS_FUZZ_N": "60"},
      "mutations": [
        {"name": "the query no longer wins over the flag",
         "file": "cmd/atlas/migrate_integrity.go",
         "old": "...exact source text, must occur exactly once...",
         "new": "..."}
      ]
    }

Environment values already set in the caller's environment win over the spec's,
so machine-specific ones (an oracle binary path, for instance) stay out of the
committed file.

Two rules this harness exists to enforce, both learned the hard way:

1.  RESTORE BY BYTE-COPY, never `git checkout --`. The original bytes are read
    before the mutation is written and written back afterwards. An earlier
    version restored with `git checkout --`, which silently discarded
    uncommitted work in the mutated files and then reported `0/N killed` — a
    result that reads as "every mutation survived" when it actually means "the
    sweep ate the code under test". It cost a full reimplementation.

2.  A NON-COMPILING MUTATION IS NOT A RESULT. BUILD-FAILED counts as neither
    killed nor survived, which shrinks the table silently in exactly the way
    PATCH-FAILED does. The build is checked separately from the test run so the
    two are distinguishable, and a run that produced any BUILD-FAILED row exits
    non-zero.

3.  RUN THIS ALONE. The sweep mutates files in the working tree, so anything
    else reading them concurrently -- a lint task, an editor's language server,
    a second sweep -- can observe a mutated tree and report a failure that is
    not the mutation's doing. A spurious survivor or an off-by-one total is the
    usual symptom.

4.  REFUSE A DIRTY TREE. Even with a byte-copy restore, an interrupted run
    leaves a mutated file behind, and a run started from a dirty tree cannot
    tell its own damage from the caller's work in progress. The tree is checked
    before the sweep and again after it, and the final state is printed.

A package's test binary is rebuilt on every run because Go's cache key covers
the mutated file. Packages whose tests build another binary at run time (see
cmd/ptah-compat/main_test.go) are the exception: their cache key does NOT cover
the mutated command source, so include the package that owns the mutated code
in "packages", not only the one that shells out to it.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_DEFAULT = Path(__file__).resolve().parents[1]


def run(repo, cmd, **kwargs):
    return subprocess.run(cmd, cwd=repo, capture_output=True, text=True, **kwargs)


def parse_failures(output):
    """Return the failing top-level tests and the count of failing subtests."""
    top = []
    subtests = 0
    for line in output.splitlines():
        match = re.match(r"--- FAIL: (\S+)", line.strip())
        if match and not line.startswith(" "):
            top.append(match.group(1))
        elif "--- FAIL:" in line and line.startswith(" "):
            subtests += 1
    return top, subtests


def load_spec(path):
    spec = json.loads(Path(path).read_text())
    packages = spec.get("packages")
    mutations = spec.get("mutations")
    if not packages or not mutations:
        raise SystemExit(f"{path}: spec needs non-empty 'packages' and 'mutations'")
    return spec, packages, mutations


def dirty_files(repo, paths):
    status = run(repo, ["git", "status", "--porcelain", "--"] + sorted(paths))
    return status.stdout.strip()


def apply_mutation(repo, mutation):
    """Write the mutation and return the original bytes, or None if it does not apply."""
    target = repo / mutation["file"]
    original = target.read_bytes()
    old = mutation["old"].encode()
    if original.count(old) != 1:
        return None, original.count(old)
    target.write_bytes(original.replace(old, mutation["new"].encode()))
    return original, 1


def test_env(spec):
    env = dict(spec.get("env", {}))
    env.update(os.environ)
    return env


def sweep(repo, spec, packages, mutations, only):
    rows = []
    env = test_env(spec)
    for mutation in mutations:
        name = mutation["name"]
        if only and only not in name:
            continue
        target = repo / mutation["file"]
        original, occurrences = apply_mutation(repo, mutation)
        if original is None:
            rows.append((name, "PATCH-FAILED", f"anchor matched {occurrences} times"))
            continue
        try:
            # Compile first, with no test selected. A mutation that does not
            # build tells you nothing about whether the rule is held, and
            # inferring that from the test run's output means guessing at
            # compiler strings -- which reads a genuine failure containing the
            # words "cannot use" as a build error and drops it from the table.
            build = subprocess.run(
                ["go", "test", "-run", "^$", "-count=1", *packages],
                cwd=repo,
                capture_output=True,
                text=True,
                env=env,
            )
            if build.returncode != 0:
                rows.append((name, "BUILD-FAILED", first_error_line(build.stdout + build.stderr)))
                continue
            result = subprocess.run(
                ["go", "test", *packages, "-count=1"],
                cwd=repo,
                capture_output=True,
                text=True,
                env=env,
            )
            combined = result.stdout + result.stderr
            top, subtests = parse_failures(combined)
            if result.returncode == 0:
                rows.append((name, "SURVIVED", "suite still green - rule untested"))
                continue
            rows.append((name, "killed", f"{', '.join(sorted(set(top))) or 'unknown'} ({subtests} subtests)"))
        finally:
            # Byte-copy restore. See this module's docstring for why this is
            # never `git checkout --`.
            target.write_bytes(original)
    return rows


def first_error_line(output):
    """Return the first compiler diagnostic, for a BUILD-FAILED row."""
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            continue
        return stripped[:120]
    return "mutation does not compile"


def report(repo, rows, paths):
    width = max(len(row[0]) for row in rows)
    print()
    print(f"{'mutation'.ljust(width)}  {'result'.ljust(12)}  reddened")
    print(f"{'-' * width}  {'-' * 12}  {'-' * 40}")
    for name, status, detail in rows:
        print(f"{name.ljust(width)}  {status.ljust(12)}  {detail}")
    survivors = [row for row in rows if row[1] != "killed"]
    unusable = [row for row in rows if row[1] in ("BUILD-FAILED", "PATCH-FAILED")]
    print()
    print(f"{len(rows) - len(survivors)}/{len(rows)} mutations killed")
    if unusable:
        # Neither killed nor survived. Reported separately so a shrinking table
        # cannot be mistaken for a smaller suite.
        print(f"{len(unusable)} mutation(s) produced no result at all: "
              + ", ".join(f"{name} ({status})" for name, status, _ in unusable))
    remaining = dirty_files(repo, paths)
    print(f"git status --porcelain after restore: {remaining or '(clean)'}")
    return 1 if survivors or remaining else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("spec", help="path to a mutation spec JSON file")
    parser.add_argument("--repo", default=str(REPO_DEFAULT), help="repository root (default: this script's repository)")
    parser.add_argument("--only", default="", help="run only mutations whose name contains this substring")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    spec, packages, mutations = load_spec(args.spec)
    paths = {mutation["file"] for mutation in mutations}

    dirty = dirty_files(repo, paths)
    if dirty:
        print("refusing to mutate a dirty tree; commit or stash first:\n" + dirty)
        return 2

    rows = sweep(repo, spec, packages, mutations, args.only)
    if not rows:
        print("no mutations selected")
        return 2
    return report(repo, rows, paths)


if __name__ == "__main__":
    sys.exit(main())
