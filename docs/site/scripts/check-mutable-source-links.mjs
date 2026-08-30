#!/usr/bin/env node

import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, extname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repositoryRoot = join(siteRoot, '..', '..');
const mutableSourcePattern = String.raw`https:\/\/(?:raw\.githubusercontent\.com\/stokaro\/ptah\/master\/|github\.com\/stokaro\/ptah\/(?:blob|edit)\/master\/)[^\s)>'"]+`;
const mutableSource = new RegExp(mutableSourcePattern, 'g');
const markdownMutableLink = new RegExp(
  String.raw`\[([^\]\n]+)\]\(\s*(${mutableSourcePattern})(?:\s+(?:"[^"]*"|'[^']*'))?\s*\)`,
  'g',
);
const htmlMutableLink = new RegExp(
  String.raw`<a\b[^>]*\bhref\s*=\s*["'](${mutableSourcePattern})["'][^>]*>([\s\S]*?)<\/a>`,
  'gi',
);
const latestLabel = /latest development source/i;

function filesBelow(root) {
  const files = [];
  for (const name of readdirSync(root)) {
    const fullPath = join(root, name);
    if (statSync(fullPath).isDirectory()) files.push(...filesBelow(fullPath));
    else if (['.md', '.mdx', '.astro'].includes(extname(name))) files.push(fullPath);
  }
  return files;
}

function labeledMutableSourceIndexes(source) {
  const labeled = new Set();
  for (const match of source.matchAll(markdownMutableLink)) {
    if (!latestLabel.test(match[1])) continue;
    const linkTargetStart = match[0].indexOf('](') + 2;
    const urlOffset = match[0].indexOf(match[2], linkTargetStart);
    labeled.add(`${match.index + urlOffset}:${match[2]}`);
  }
  for (const match of source.matchAll(htmlMutableLink)) {
    const visibleLabel = match[2].replace(/<[^>]*>/g, ' ');
    if (!latestLabel.test(visibleLabel)) continue;
    const hrefStart = match[0].search(/\bhref\s*=/i);
    const urlOffset = match[0].indexOf(match[1], hrefStart);
    labeled.add(`${match.index + urlOffset}:${match[1]}`);
  }
  return labeled;
}

export function mutableSourceProblems(entries) {
  const problems = [];
  for (const { path, source } of entries) {
    const labeled = labeledMutableSourceIndexes(source);
    for (const match of source.matchAll(mutableSource)) {
      if (labeled.has(`${match.index}:${match[0]}`)) continue;
      const line = source.slice(0, match.index).split('\n').length;
      problems.push({
        path,
        line,
        url: match[0],
        message: 'mutable master link must be labeled as latest development source',
      });
    }
  }
  return problems;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const accepted = mutableSourceProblems([{
    path: 'accepted.md',
    source: 'Open the [latest development source](https://github.com/stokaro/ptah/blob/master/example.go).',
  }]);
  assert(accepted.length === 0, 'explicit latest-development link failed');

  const acceptedHtml = mutableSourceProblems([{
    path: 'accepted.mdx',
    source: '<a href="https://github.com/stokaro/ptah/blob/master/example.go"><code>example.go</code> in the latest development source</a>',
  }]);
  assert(acceptedHtml.length === 0, 'explicit HTML latest-development link failed');

  const siblingLink = mutableSourceProblems([{
    path: 'sibling.md',
    source: [
      '[latest development source](https://github.com/stokaro/ptah/blob/master/allowed.go)',
      'and [displayed source](https://github.com/stokaro/ptah/blob/master/rejected.go).',
    ].join(' '),
  }]);
  assert(
    siblingLink.length === 1 && siblingLink[0].url.endsWith('/rejected.go'),
    'one labeled link waived a different master link in the same paragraph',
  );

  const nearbyLabel = mutableSourceProblems([{
    path: 'nearby.md',
    source: 'Latest development source: [open source](https://github.com/stokaro/ptah/blob/master/example.go).',
  }]);
  assert(nearbyLabel.length === 1, 'nearby prose waived an unlabeled master link');

  for (const url of [
    'https://github.com/stokaro/ptah/blob/master/example.go',
    'https://github.com/stokaro/ptah/edit/master/example.go',
    'https://raw.githubusercontent.com/stokaro/ptah/master/example.go',
  ]) {
    const rejected = mutableSourceProblems([{ path: 'rejected.md', source: `Open [source](${url}).` }]);
    assert(rejected.length === 1, `${url} passed without a latest-development label`);
  }
  console.log('check-mutable-source-links.mjs --selftest: OK (blob, edit, and raw master links)');
}

function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }
  if (process.argv.length > 2) {
    console.error('usage: node scripts/check-mutable-source-links.mjs [--selftest]');
    process.exitCode = 2;
    return;
  }

  const roots = [
    join(siteRoot, 'src', 'content', 'docs'),
    join(siteRoot, 'src', 'components'),
  ];
  const entries = roots.flatMap(filesBelow).map((path) => ({
    path: relative(repositoryRoot, path),
    source: readFileSync(path, 'utf8'),
  }));
  const problems = mutableSourceProblems(entries);
  if (problems.length > 0) {
    console.error('check-mutable-source-links.mjs: FAILED');
    for (const problem of problems) {
      console.error(`- ${problem.path}:${problem.line}: ${problem.message}: ${problem.url}`);
    }
    process.exitCode = 1;
    return;
  }
  console.log(`check-mutable-source-links.mjs: OK (${entries.length} reader-facing source files)`);
}

main();
