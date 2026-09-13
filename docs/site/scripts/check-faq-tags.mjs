#!/usr/bin/env node
// Holds the FAQ tag map to the page it describes.
//
// The tags live in src/faq-tags.mjs, keyed by the anchor each question
// declares. Nothing else connects the two: a question can be added, reworded or
// given a new anchor without the map noticing, and the filter would then hide a
// question that no tag can bring back. That failure is silent, because a page
// with a working rail and one unreachable question looks exactly like a page
// that works.
//
// So this compares the map with the built page in both directions, and refuses
// a tag that no question carries or that the rail cannot label.

import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(here, '..');

/**
 * faqTagProblems compares a tag map with the anchors a page published.
 * `anchors` is what the built page carries; `questionTags` and `tagLabels` are
 * the declaration.
 */
export function faqTagProblems(anchors, questionTags, tagLabels) {
  const problems = [];
  const declared = new Set(Object.keys(questionTags));
  const published = new Set(anchors);

  for (const anchor of declared) {
    if (!published.has(anchor)) {
      problems.push(`tagged question ${anchor} is not on the page`);
    }
  }
  for (const anchor of published) {
    if (!declared.has(anchor)) {
      problems.push(`question ${anchor} carries no tags, so no filter reaches it`);
    }
  }

  const used = new Set();
  for (const [anchor, tags] of Object.entries(questionTags)) {
    if (!Array.isArray(tags) || tags.length === 0) {
      problems.push(`question ${anchor} has an empty tag list`);
      continue;
    }
    for (const tag of tags) {
      used.add(tag);
      if (!tagLabels[tag]) problems.push(`tag "${tag}" on ${anchor} has no label`);
    }
  }
  for (const tag of Object.keys(tagLabels)) {
    if (!used.has(tag)) problems.push(`tag "${tag}" is labelled but no question carries it`);
  }

  return problems;
}

/**
 * publishedAnchors returns the ids of the h2 headings in a page's article,
 * which is not the same as the h2 headings in its document. The contents rail
 * heads itself with `<h2 id="starlight__on-this-page">` before the article
 * starts, and reading the whole document reports that as an untagged question.
 */
export function publishedAnchors(html) {
  const article = html.indexOf('sl-markdown-content');
  const body = article === -1 ? html : html.slice(article);
  return [...body.matchAll(/<h2[^>]*\bid="([^"]+)"/g)].map((match) => match[1]);
}

function selftest() {
  const labels = { alpha: 'Alpha', beta: 'Beta' };
  const cases = [
    {
      name: 'a matching map passes',
      anchors: ['one', 'two'],
      tags: { one: ['alpha'], two: ['beta'] },
      labels,
      expect: 0,
    },
    {
      name: 'a tagged question that left the page',
      anchors: ['one'],
      tags: { one: ['alpha'], gone: ['beta'] },
      labels,
      expect: 1,
    },
    {
      name: 'a question no tag reaches',
      anchors: ['one', 'untagged'],
      tags: { one: ['alpha'] },
      labels,
      expect: 2, // unreachable question, and Beta now labels nothing
    },
    {
      name: 'an unlabelled tag',
      anchors: ['one', 'two'],
      tags: { one: ['alpha'], two: ['gamma'] },
      labels,
      expect: 2, // gamma has no label, and Beta now labels nothing
    },
    {
      name: 'an empty tag list',
      anchors: ['one', 'two'],
      tags: { one: ['alpha'], two: [] },
      labels,
      expect: 2, // the empty list, and Beta now labels nothing
    },
  ];

  let failures = 0;
  for (const testCase of cases) {
    const got = faqTagProblems(testCase.anchors, testCase.tags, testCase.labels).length;
    if (got !== testCase.expect) {
      console.error(`  ${testCase.name}: expected ${testCase.expect} problem(s), got ${got}`);
      failures += 1;
    }
  }
  if (publishedAnchors('<h2 id="a">A</h2><h3 id="b">B</h3>').join(',') !== 'a') {
    console.error('  publishedAnchors read a heading that is not an h2');
    failures += 1;
  }
  // The contents rail heads itself before the article begins.
  const withRail =
    '<h2 id="starlight__on-this-page">On this page</h2>' +
    '<div class="sl-markdown-content"><h2 id="a">A</h2></div>';
  if (publishedAnchors(withRail).join(',') !== 'a') {
    console.error('  publishedAnchors read a heading from the page furniture');
    failures += 1;
  }
  if (failures) {
    console.error(`check-faq-tags.mjs --selftest: ${failures} case(s) failed`);
    process.exit(1);
  }
  console.log(`check-faq-tags.mjs --selftest: OK (${cases.length} cases)`);
}

async function main() {
  if (process.argv[2] === '--selftest') return selftest();

  const page = join(siteRoot, 'dist/faq/index.html');
  if (!existsSync(page)) {
    console.error('check-faq-tags.mjs: dist/faq/index.html is missing; run "npm run build" first.');
    process.exit(1);
  }

  const { questionTags, tagLabels } = await import('../src/faq-tags.mjs');
  const anchors = publishedAnchors(readFileSync(page, 'utf8'));
  const problems = faqTagProblems(anchors, questionTags, tagLabels);

  if (problems.length) {
    console.error('check-faq-tags.mjs: FAILED');
    for (const problem of problems) console.error(`  ${problem}`);
    process.exit(1);
  }

  console.log(
    `check-faq-tags.mjs: OK (${anchors.length} questions, ${Object.keys(tagLabels).length} tags)`,
  );
}

await main();
