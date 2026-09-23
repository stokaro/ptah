#!/usr/bin/env node
// Which release tags the Docs workflow builds: the newest --keep of the tag
// names on standard input, oldest first, one per line.
//
//   git tag -l 'v*' | node docs/site/scripts/retained-doc-tags.mjs --keep 10
//   node docs/site/scripts/retained-doc-tags.mjs --selftest
//
// The order comes from lib/doc-versions.mjs, the same module the deployment
// guard reads, so the build and the guard agree on which versions the window
// holds.

import { readFileSync } from 'node:fs';

import { retainedReleases } from './lib/doc-versions.mjs';

function selftest() {
  const assert = (condition, message) => {
    if (!condition) throw new Error(message);
  };
  const tags = ['v0.1.0', 'v0.10.0', 'v0.2.0', 'v0.9.1', 'v0.9.0', 'v1.0.0-rc.1', 'vnext', 'v0.2'];
  assert(
    retainedReleases(tags, 3).join(',') === 'v0.9.0,v0.9.1,v0.10.0',
    'the newest releases were not chosen by number, oldest first',
  );
  assert(
    retainedReleases(tags, 20).join(',') === 'v0.1.0,v0.2,v0.2.0,v0.9.0,v0.9.1,v0.10.0',
    'a window wider than the tags did not keep every release, or kept a name that is not one',
  );
  assert(retainedReleases([], 10).length === 0, 'no tags did not keep nothing');
  for (const keep of [0, -1, 1.5, Number.NaN]) {
    let refused = false;
    try {
      retainedReleases(tags, keep);
    } catch {
      refused = true;
    }
    assert(refused, `--keep ${keep} was accepted`);
  }
  console.log('retained-doc-tags.mjs --selftest: OK (numeric order, wide window, empty list, invalid keep)');
}

function main(arguments_) {
  if (arguments_.length === 1 && arguments_[0] === '--selftest') {
    selftest();
    return;
  }
  if (arguments_.length !== 2 || arguments_[0] !== '--keep' || !/^\d+$/.test(arguments_[1])) {
    throw new Error("usage: git tag -l 'v*' | node scripts/retained-doc-tags.mjs --keep <n>");
  }
  const names = readFileSync(0, 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  for (const tag of retainedReleases(names, Number(arguments_[1]))) console.log(tag);
}

try {
  main(process.argv.slice(2));
} catch (error) {
  console.error(`retained-doc-tags: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 2;
}
