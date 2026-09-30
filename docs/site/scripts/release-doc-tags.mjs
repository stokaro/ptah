#!/usr/bin/env node
// Every release tag the Docs workflow builds, oldest first, one per line.
//
//   git tag -l 'v*' | node docs/site/scripts/release-doc-tags.mjs
//   node docs/site/scripts/release-doc-tags.mjs --selftest
//
// The release grammar and order come from lib/doc-versions.mjs, which the
// version index and deployment guard also read.

import { readFileSync } from 'node:fs';

import { releaseVersions } from './lib/doc-versions.mjs';

function selftest() {
  const assert = (condition, message) => {
    if (!condition) throw new Error(message);
  };
  const releases = Array.from({ length: 12 }, (_, index) => `v0.${index + 1}.0`);
  const tags = [...releases.toReversed(), 'v1.0.0-rc.1', 'vnext', 'v0.2', 'v0.2.0'];
  const selected = releaseVersions(tags);
  assert(
    selected.join(',') === [releases[0], 'v0.2', ...releases.slice(1)].join(','),
    'release tags were dropped, duplicated, or not ordered numerically',
  );
  assert(selected.length > 10 && selected.includes('v0.2.0'), 'an old release was lost after ten releases');
  assert(releaseVersions([]).length === 0, 'an empty tag list produced releases');
  console.log('release-doc-tags.mjs --selftest: OK (all releases, numeric order, duplicates, empty list)');
}

function main(arguments_) {
  if (arguments_.length === 1 && arguments_[0] === '--selftest') {
    selftest();
    return;
  }
  if (arguments_.length !== 0) {
    throw new Error("usage: git tag -l 'v*' | node scripts/release-doc-tags.mjs");
  }
  const names = readFileSync(0, 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  for (const tag of releaseVersions(names)) console.log(tag);
}

try {
  main(process.argv.slice(2));
} catch (error) {
  console.error(`release-doc-tags: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 2;
}
