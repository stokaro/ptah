#!/usr/bin/env node

import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { sourceRefForVersion } from '../src/lib/source-ref.mjs';

const fullCommit = /^[0-9a-f]{40}$/;

export function buildInfo({ version, commit, builtAt = new Date().toISOString() }) {
  if (typeof version !== 'string' || version.trim() === '') throw new Error('version must be a non-empty string');
  if (typeof commit !== 'string' || !fullCommit.test(commit)) throw new Error('source commit must be a full lowercase Git SHA');
  if (Number.isNaN(Date.parse(builtAt))) throw new Error('built-at must be an ISO-8601 timestamp');
  return {
    documentation_version: version,
    source_ref: sourceRefForVersion(version),
    source_commit: commit,
    built_at: builtAt,
  };
}

function value(arguments_, name) {
  const index = arguments_.indexOf(name);
  return index === -1 ? undefined : arguments_[index + 1];
}

function selftest() {
  const commit = '0123456789abcdef0123456789abcdef01234567';
  const edge = buildInfo({ version: 'edge', commit, builtAt: '2026-08-30T08:00:00Z' });
  if (edge.source_ref !== 'master') throw new Error('edge source ref is not master');
  const release = buildInfo({ version: 'v1.2.3', commit, builtAt: '2026-08-30T08:00:00Z' });
  if (release.source_ref !== 'v1.2.3') throw new Error('release source ref is not its tag');
  console.log('write-build-info.mjs --selftest: OK (edge and release refs)');
}

function main() {
  const arguments_ = process.argv.slice(2);
  if (arguments_.includes('--selftest')) {
    selftest();
    return;
  }
  const output = value(arguments_, '--output');
  const version = value(arguments_, '--version');
  const commit = value(arguments_, '--source-commit');
  const builtAt = value(arguments_, '--built-at');
  const expectedArguments = builtAt ? 8 : 6;
  if (!output || !version || !commit || arguments_.length !== expectedArguments) {
    console.error('usage: node scripts/write-build-info.mjs --output <path> --version <version> --source-commit <full-sha> [--built-at <timestamp>]');
    process.exitCode = 2;
    return;
  }
  const payload = buildInfo({ version, commit, ...(builtAt ? { builtAt } : {}) });
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, `${JSON.stringify(payload, null, 2)}\n`);
  console.log(`build information: wrote ${output}`);
}

main();
