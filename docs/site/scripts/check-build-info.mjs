#!/usr/bin/env node

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { sourceRefForVersion } from '../src/lib/source-ref.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const fullCommit = /^[0-9a-f]{40}$/;

export function validateBuildInfo(value, { version, commit, sourceRef }) {
  const problems = [];
  if (!value || typeof value !== 'object' || Array.isArray(value)) return ['build information must be a JSON object'];
  if (value.documentation_version !== version) {
    problems.push(`documentation_version is ${JSON.stringify(value.documentation_version)}, want ${JSON.stringify(version)}`);
  }
  if (value.source_ref !== sourceRef) {
    problems.push(`source_ref is ${JSON.stringify(value.source_ref)}, want ${JSON.stringify(sourceRef)}`);
  }
  if (typeof value.source_commit !== 'string' || !fullCommit.test(value.source_commit)) {
    problems.push('source_commit must be a full lowercase Git SHA');
  } else if (value.source_commit !== commit) {
    problems.push(`source_commit is ${value.source_commit}, want ${commit}`);
  }
  if (typeof value.built_at !== 'string' || Number.isNaN(Date.parse(value.built_at))) {
    problems.push('built_at must be an ISO-8601 timestamp');
  }
  return problems;
}

function currentCommit() {
  return execFileSync('git', ['rev-parse', 'HEAD'], { cwd: siteRoot, encoding: 'utf8' }).trim();
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const commit = '0123456789abcdef0123456789abcdef01234567';
  const valid = {
    documentation_version: 'edge',
    source_ref: 'master',
    source_commit: commit,
    built_at: '2026-08-30T08:00:00.000Z',
  };
  assert(validateBuildInfo(valid, { version: 'edge', commit, sourceRef: 'master' }).length === 0, 'valid edge build failed');
  assert(sourceRefForVersion('v1.2.3') === 'v1.2.3', 'release source ref changed');
  assert(
    validateBuildInfo({ ...valid, source_commit: 'short' }, { version: 'edge', commit, sourceRef: 'master' })
      .some((problem) => problem.includes('full lowercase')),
    'short commit was accepted',
  );
  assert(
    validateBuildInfo({ ...valid, built_at: 'not-a-date' }, { version: 'edge', commit, sourceRef: 'master' })
      .some((problem) => problem.includes('ISO-8601')),
    'invalid timestamp was accepted',
  );
  console.log('check-build-info.mjs --selftest: OK (edge, release, SHA, and timestamp assertions)');
}

function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }
  if (process.argv.length > 2) {
    console.error('usage: node scripts/check-build-info.mjs [--selftest]');
    process.exitCode = 2;
    return;
  }

  const version = process.env.DOCS_VERSION?.trim() || 'edge';
  const commit = process.env.DOCS_EXPECTED_COMMIT?.trim() || currentCommit();
  const sourceRef = sourceRefForVersion(version);
  let value;
  try {
    value = JSON.parse(readFileSync(join(siteRoot, 'dist', 'build-info.json'), 'utf8'));
  } catch (error) {
    console.error(`build information cannot be read: ${error instanceof Error ? error.message : error}`);
    process.exitCode = 1;
    return;
  }
  const problems = validateBuildInfo(value, { version, commit, sourceRef });
  if (problems.length > 0) {
    console.error('build information: FAILED');
    for (const problem of problems) console.error(`  ${problem}`);
    process.exitCode = 1;
    return;
  }
  console.log(`build information: OK (${version} from ${commit})`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
