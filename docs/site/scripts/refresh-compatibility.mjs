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
// than the one already published. `diverged` means the operator's history was
// rewritten, which is not something automation should paper over.
export const RELATIONS = ['ahead', 'behind', 'identical', 'diverged'];

export function decide({ recordedCommit, incomingCommit, relation }) {
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
      reason: `${incomingCommit} and the recorded ${recordedCommit} have diverged; the operator history was rewritten`,
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

  const nonsense = decide({ recordedCommit: older, incomingCommit: 'HEAD', relation: 'ahead' });
  if (nonsense.act || !nonsense.failure) throw new Error('a non-commit was accepted');

  const copy = derivedCopy({ catalog: { axis: 'a', releases: [] }, commit: newer, retrieved: '2026-09-12' });
  if (copy.generated !== true) throw new Error('the copy does not mark itself generated');
  if (copy.source.repository !== SOURCE_REPOSITORY) throw new Error('the copy names another repository');

  console.log('refresh-compatibility.mjs --selftest: OK (first, repeat, forward, late, rewritten, non-commit)');
}

function main() {
  const arguments_ = process.argv.slice(2);
  if (arguments_.includes('--selftest')) {
    selftest();
    return;
  }
  const commit = value(arguments_, '--source-commit');
  const relation = value(arguments_, '--relation');
  const catalogPath = value(arguments_, '--catalog');
  const retrieved = value(arguments_, '--retrieved') ?? new Date().toISOString().slice(0, 10);
  if (!commit || !catalogPath) {
    console.error('usage: refresh-compatibility.mjs --source-commit <sha> --catalog <path> [--relation <r>] [--retrieved <date>]');
    process.exit(2);
  }
  if (!datePattern.test(retrieved)) {
    console.error(`refresh-compatibility.mjs: --retrieved ${retrieved} is not a date`);
    process.exit(2);
  }

  const target = join(defaultRepoRoot, DATA_PATH);
  const recorded = existsSync(target) ? JSON.parse(readFileSync(target, 'utf8')).source?.commit ?? null : null;
  const decision = decide({ recordedCommit: recorded, incomingCommit: commit, relation });
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
