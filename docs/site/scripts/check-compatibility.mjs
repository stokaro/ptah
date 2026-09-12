#!/usr/bin/env node
// Holds the derived compatibility copy to the shape the page renders from.
//
// The canonical file lives in stokaro/ptah-operator and is validated there,
// offline, by a program that knows the whole contract. This gate is the
// receiving end: it refuses a copy that arrived without its provenance, that
// lost the fields this site renders, or that somebody edited here -- because
// the copy is derived and an edit to it is a claim the operator repository
// never made.
//
// What it cannot see is a content change that keeps the shape and the
// formatting. That is what the provenance commit is for: the automation
// rewrites both together, so a copy whose commit does not move while its
// content does is visible in review as a diff with no provenance change.

import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { DATA_PATH, ROOT_PATH } from './publish-compatibility.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const defaultRepoRoot = join(scriptDir, '..', '..', '..');

const commitPattern = /^[0-9a-f]{40}$/;
const datePattern = /^\d{4}-\d{2}-\d{2}$/;

// problems returns one entry per way the copy is not usable.
export function problems(text) {
  const found = [];
  let derived;
  try {
    derived = JSON.parse(text);
  } catch (error) {
    return [`${DATA_PATH} is not JSON: ${error.message}`];
  }

  // Written by a generator, so it is exactly what a generator writes. A hand
  // edit almost always changes the formatting, and this is what says so.
  if (`${JSON.stringify(derived, null, 2)}\n` !== text) {
    found.push(`${DATA_PATH} is not the serialization its generator writes; it looks hand-edited`);
  }
  if (derived.generated !== true) {
    found.push(`${DATA_PATH} does not mark itself generated, so nothing tells a reader not to edit it`);
  }

  const source = derived.source ?? {};
  if (source.repository !== 'stokaro/ptah-operator') {
    found.push(`${DATA_PATH} names source repository ${source.repository}`);
  }
  if (source.path !== 'support/ptah.json') {
    found.push(`${DATA_PATH} names source path ${source.path}`);
  }
  if (!commitPattern.test(source.commit ?? '')) {
    found.push(`${DATA_PATH} names source commit ${source.commit}, which is not an exact commit`);
  }
  if (!datePattern.test(source.retrieved ?? '')) {
    found.push(`${DATA_PATH} names retrieval date ${source.retrieved}, which is not a date`);
  }

  const catalog = derived.catalog ?? {};
  if (typeof catalog.axis !== 'string' || catalog.axis.trim() === '') {
    found.push('the copied catalog states no axis, and the page renders it as the table\'s subject');
  }
  const releases = catalog.releases ?? [];
  if (releases.length === 0) {
    found.push('the copied catalog lists no operator version; an empty table reads as complete');
  }
  for (const entry of releases) {
    const name = entry.operator ?? '(unnamed)';
    if (typeof entry.operator !== 'string' || entry.operator === '') {
      found.push('a copied row names no operator version');
    }
    if (typeof entry.documentation?.published !== 'boolean') {
      found.push(`${name} does not say whether its guide is published, and the page decides a link on it`);
    }
    if (entry.declared?.range == null && (entry.declared?.statement ?? '').trim() === '') {
      found.push(`${name} declares no range and states no reason, so the page would render a blank promise`);
    }
    const verified = entry.verified ?? [];
    if (verified.length === 0 && (entry.unverifiedReason ?? '').trim() === '') {
      found.push(`${name} records nothing verified and names no missing check`);
    }
    for (const measurement of verified) {
      if (!commitPattern.test(measurement.ptahCommit ?? '')) {
        found.push(`${name} verifies ${measurement.ptahCommit}, which is not an exact commit`);
      }
      if ((measurement.scope ?? '').trim() === '') {
        found.push(`${name} verifies ${measurement.ptahCommit} and does not say what ran`);
      }
    }
  }
  return found;
}

function selftest() {
  const good = {
    generated: true,
    source: {
      repository: 'stokaro/ptah-operator',
      path: 'support/ptah.json',
      commit: '0123456789abcdef0123456789abcdef01234567',
      retrieved: '2026-09-12',
    },
    catalog: {
      axis: 'an axis',
      releases: [
        {
          operator: 'edge',
          documentation: { published: true },
          declared: { range: null, statement: 'no range' },
          verified: [{ ptahCommit: '00fc362c943bfb9d0363d5890bf449a2a9b5e7cf', scope: 'the lifecycle' }],
        },
      ],
    },
  };
  const serialize = (value) => `${JSON.stringify(value, null, 2)}\n`;
  if (problems(serialize(good)).length !== 0) throw new Error('a correct copy was refused');

  const cases = [
    [{ ...good, generated: false }, 'does not mark itself generated'],
    [{ ...good, source: { ...good.source, commit: 'master' } }, 'not an exact commit'],
    [{ ...good, source: { ...good.source, repository: 'someone/else' } }, 'names source repository'],
    [
      { ...good, catalog: { ...good.catalog, releases: [{ ...good.catalog.releases[0], verified: [] }] } },
      'names no missing check',
    ],
    [
      {
        ...good,
        catalog: {
          ...good.catalog,
          releases: [{ ...good.catalog.releases[0], declared: { range: null, statement: '  ' } }],
        },
      },
      'states no reason',
    ],
  ];
  for (const [broken, needle] of cases) {
    const found = problems(serialize(broken));
    if (!found.some((problem) => problem.includes(needle))) {
      throw new Error(`accepted a copy that should fail on ${needle}: ${found.join('; ')}`);
    }
  }
  // A hand edit that only reformats.
  if (!problems(`${JSON.stringify(good)}\n`).some((problem) => problem.includes('hand-edited'))) {
    throw new Error('a reformatted copy was accepted');
  }
  console.log('check-compatibility.mjs --selftest: OK (correct copy, five refusals, formatting)');
}

function main() {
  const arguments_ = process.argv.slice(2);
  if (arguments_.includes('--selftest')) {
    selftest();
    return;
  }
  const siteIndex = arguments_.indexOf('--site');
  if (siteIndex >= 0) {
    const root = arguments_[siteIndex + 1];
    const page = join(root, ROOT_PATH);
    if (!existsSync(page)) {
      console.error(`check-compatibility.mjs: ${root} has no ${ROOT_PATH}; the deploy would publish no matrix`);
      process.exit(1);
    }
    if (readFileSync(page, 'utf8').length === 0) {
      console.error(`check-compatibility.mjs: ${ROOT_PATH} is empty`);
      process.exit(1);
    }
    console.log('check-compatibility.mjs: OK (the assembled root serves the matrix)');
    return;
  }

  const path = join(defaultRepoRoot, DATA_PATH);
  if (!existsSync(path)) {
    console.error(`check-compatibility.mjs: ${DATA_PATH} is missing`);
    process.exit(1);
  }
  const found = problems(readFileSync(path, 'utf8'));
  if (found.length > 0) {
    console.error(`check-compatibility.mjs: ${found.length} problem(s):\n- ${found.join('\n- ')}`);
    process.exit(1);
  }
  console.log('check-compatibility.mjs: OK (the derived copy carries its provenance and renders)');
}

main();
