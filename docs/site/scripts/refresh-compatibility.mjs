#!/usr/bin/env node
// Rewrites the derived compatibility copy from a catalog fetched at one commit.
//
// The network belongs to the workflow: it resolves the commit, asks the
// operator repository how that commit relates to the one already recorded, and
// downloads the file. This program applies the policy to those answers, so the
// policy is testable without a network and without either repository.
//
// Two rules matter more than the rewrite itself. A late event must not move the
// copy backwards, which is what the relation is for. And an unchanged catalog
// must produce no change at all, so a repeated event opens no second pull
// request.

import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { DATA_PATH } from './publish-compatibility.mjs';
import { problems as copyProblems } from './check-compatibility.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const defaultRepoRoot = join(scriptDir, '..', '..', '..');

const commitPattern = /^[0-9a-f]{40}$/;
const datePattern = /^\d{4}-\d{2}-\d{2}$/;

export const SOURCE_REPOSITORY = 'stokaro/ptah-operator';
export const SOURCE_PATH = 'support/ptah.json';

// RELATIONS are the answers GitHub's compare endpoint gives for
// base=<recorded> head=<incoming>.
//
// `ahead` and `identical` mean the incoming commit is the recorded one or a
// descendant of it. `behind` means the event is late and its catalog is older
// than the one already published. `diverged` means neither commit reaches the
// other.
export const RELATIONS = ['ahead', 'behind', 'identical', 'diverged'];

// decide answers what to do with an incoming commit, given what is recorded.
//
// `recordedOnDefaultBranch` is what separates the two histories that both
// compare as `diverged`. The operator squash-merges, so a pull request's head
// commit stays a real object that the default branch never contains: recording
// one leaves every later run comparing against a commit no descendant can
// reach, and the copy is then frozen for as long as nobody notices. That is a
// record to re-derive from, not a rewrite. A recorded commit the branch does
// contain, diverging from the incoming one, is the rewrite, and it fails.
//
// The default is `undefined`, which is read as on the branch: a caller that
// cannot answer gets the stricter of the two.
export function decide({ recordedCommit, incomingCommit, relation, recordedOnDefaultBranch }) {
  if (!commitPattern.test(incomingCommit ?? '')) {
    return { act: false, reason: `incoming commit ${incomingCommit} is not an exact commit`, failure: true };
  }
  if (!recordedCommit) {
    return { act: true, reason: 'no commit is recorded yet' };
  }
  if (!commitPattern.test(recordedCommit)) {
    return { act: false, reason: `the recorded commit ${recordedCommit} is not an exact commit`, failure: true };
  }
  if (recordedCommit === incomingCommit) {
    return { act: false, reason: 'the recorded commit is already this one' };
  }
  if (recordedOnDefaultBranch === false) {
    return {
      act: true,
      reason: `the recorded ${recordedCommit} is not on the operator's default branch, so the copy is re-derived at ${incomingCommit}`,
    };
  }
  if (!RELATIONS.includes(relation)) {
    return { act: false, reason: `the operator repository answered ${relation}`, failure: true };
  }
  if (relation === 'behind') {
    return {
      act: false,
      reason: `${incomingCommit} is behind the recorded ${recordedCommit}; a late event must not move the copy backwards`,
    };
  }
  if (relation === 'diverged') {
    return {
      act: false,
      reason: `${incomingCommit} and the recorded ${recordedCommit} have diverged, and the branch contains the recorded one; the operator history was rewritten`,
      failure: true,
    };
  }
  return { act: true, reason: `${incomingCommit} is ${relation} of the recorded ${recordedCommit}` };
}

export function derivedCopy({ catalog, commit, retrieved }) {
  return {
    generated: true,
    generator: 'stokaro/ptah .github/workflows/operator-compatibility.yml',
    editSource: `${SOURCE_REPOSITORY} ${SOURCE_PATH}`,
    source: {
      repository: SOURCE_REPOSITORY,
      path: SOURCE_PATH,
      commit,
      retrieved,
    },
    catalog,
  };
}

export function serialize(derived) {
  return `${JSON.stringify(derived, null, 2)}\n`;
}

function value(arguments_, name) {
  const index = arguments_.indexOf(name);
  return index === -1 ? undefined : arguments_[index + 1];
}

function selftest() {
  const older = '1111111111111111111111111111111111111111';
  const newer = '2222222222222222222222222222222222222222';

  const first = decide({ recordedCommit: null, incomingCommit: newer, relation: 'ahead' });
  if (!first.act) throw new Error('the first refresh was refused');

  const same = decide({ recordedCommit: newer, incomingCommit: newer, relation: 'identical' });
  if (same.act) throw new Error('a repeated event acted again');

  const forward = decide({ recordedCommit: older, incomingCommit: newer, relation: 'ahead' });
  if (!forward.act) throw new Error('a descendant commit was refused');

  const late = decide({ recordedCommit: newer, incomingCommit: older, relation: 'behind' });
  if (late.act || late.failure) throw new Error('a late event either acted or failed instead of resting');
  if (!late.reason.includes('backwards')) throw new Error('a late event did not say why');

  const rewritten = decide({ recordedCommit: newer, incomingCommit: older, relation: 'diverged' });
  if (rewritten.act || !rewritten.failure) throw new Error('a rewritten history was not a failure');

  const onBranch = decide({
    recordedCommit: newer, incomingCommit: older, relation: 'diverged', recordedOnDefaultBranch: true,
  });
  if (onBranch.act || !onBranch.failure) throw new Error('a rewrite stopped failing once the branch was named');

  const squashed = decide({
    recordedCommit: newer, incomingCommit: older, relation: 'diverged', recordedOnDefaultBranch: false,
  });
  if (!squashed.act) throw new Error('a recorded commit the branch does not contain was not re-derived');
  if (squashed.failure) throw new Error('re-deriving from an unreachable record failed instead');

  const lateOffBranch = decide({
    recordedCommit: newer, incomingCommit: older, relation: 'behind', recordedOnDefaultBranch: false,
  });
  if (!lateOffBranch.act) throw new Error('an unreachable record was held back by the relation it cannot support');

  const nonsense = decide({ recordedCommit: older, incomingCommit: 'HEAD', relation: 'ahead' });
  if (nonsense.act || !nonsense.failure) throw new Error('a non-commit was accepted');

  const copy = derivedCopy({ catalog: { axis: 'a', releases: [] }, commit: newer, retrieved: '2026-09-12' });
  if (copy.generated !== true) throw new Error('the copy does not mark itself generated');
  if (copy.source.repository !== SOURCE_REPOSITORY) throw new Error('the copy names another repository');

  console.log('refresh-compatibility.mjs --selftest: OK (first, repeat, forward, late, rewritten, off-branch record, non-commit)');
}

function main() {
  const arguments_ = process.argv.slice(2);
  if (arguments_.includes('--selftest')) {
    selftest();
    return;
  }
  const commit = value(arguments_, '--source-commit');
  const relation = value(arguments_, '--relation');
  const recordedOnBranch = value(arguments_, '--recorded-on-default-branch');
  const catalogPath = value(arguments_, '--catalog');
  const retrieved = value(arguments_, '--retrieved') ?? new Date().toISOString().slice(0, 10);
  if (!commit || !catalogPath) {
    console.error('usage: refresh-compatibility.mjs --source-commit <sha> --catalog <path> [--relation <r>] [--recorded-on-default-branch true|false] [--retrieved <date>]');
    process.exit(2);
  }
  if (recordedOnBranch !== undefined && recordedOnBranch !== 'true' && recordedOnBranch !== 'false') {
    console.error(`refresh-compatibility.mjs: --recorded-on-default-branch ${recordedOnBranch} is not true or false`);
    process.exit(2);
  }
  if (!datePattern.test(retrieved)) {
    console.error(`refresh-compatibility.mjs: --retrieved ${retrieved} is not a date`);
    process.exit(2);
  }

  const target = join(defaultRepoRoot, DATA_PATH);
  const recorded = existsSync(target) ? JSON.parse(readFileSync(target, 'utf8')).source?.commit ?? null : null;
  const decision = decide({
    recordedCommit: recorded,
    incomingCommit: commit,
    relation,
    recordedOnDefaultBranch: recordedOnBranch === undefined ? undefined : recordedOnBranch === 'true',
  });
  if (!decision.act) {
    console.log(`refresh-compatibility.mjs: nothing to do; ${decision.reason}`);
    process.exit(decision.failure ? 1 : 0);
  }

  const catalog = JSON.parse(readFileSync(catalogPath, 'utf8'));
  const text = serialize(derivedCopy({ catalog, commit, retrieved }));
  const found = copyProblems(text);
  if (found.length > 0) {
    console.error(`refresh-compatibility.mjs: the fetched catalog does not render:\n- ${found.join('\n- ')}`);
    process.exit(1);
  }
  if (existsSync(target) && readFileSync(target, 'utf8') === text) {
    console.log('refresh-compatibility.mjs: the catalog is unchanged; no pull request is needed');
    return;
  }
  writeFileSync(target, text);
  console.log(`refresh-compatibility.mjs: rewrote ${DATA_PATH} at ${commit}; ${decision.reason}`);
}

main();
