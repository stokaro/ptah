#!/usr/bin/env node
// Holds the FAQ's moving parts to each other: the page, the tags, the symptom
// map, and the count the navigation advertises.
//
// Each fails silently on its own. A question written outside any group renders
// as prose the filter cannot reach and the contents rail cannot list. A
// question nobody tagged is reachable only by reading the whole page, and a tag
// entry naming a renamed anchor tags nothing. An unlabelled tag is a facet the
// rail cannot name, and a labelled tag no question carries is a chip that
// filters to nothing. A symptom naming an anchor that was renamed routes
// nowhere. A sidebar badge is a number nobody regenerates, so it drifts the
// first time a question is added.
//
// Usage:
//   node scripts/check-faq-groups.mjs [--selftest]

import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const pagePath = join(siteRoot, 'src', 'content', 'docs', 'faq.md');
const builtPage = join(siteRoot, 'dist', 'faq', 'index.html');

/** Reads the page's structure: the groups in order, each with its questions. */
export function readStructure(markdown) {
  const groups = [];
  let current = null;
  const orphans = [];
  for (const line of markdown.replace(/^---\n[\s\S]*?\n---\n/, '').split('\n')) {
    const group = line.match(/^## (.+?)\s*\{#([a-z0-9-]+)\}$/);
    if (group) {
      current = { id: group[2], name: group[1], questions: [] };
      groups.push(current);
      continue;
    }
    const question = line.match(/^### (.+?)\s*\{#([a-z0-9-]+)\}$/);
    if (!question) continue;
    if (current) current.questions.push(question[2]);
    else orphans.push(question[2]);
  }
  return { groups, orphans };
}

/**
 * structureProblems reports what a reader would hit. `badge` is the count the
 * sidebar advertises, `symptoms` the curated chips, `rendered` the question
 * anchors the built page carries.
 */
export function structureProblems({ groups, orphans }, symptoms, badge, aliases, rendered, tags) {
  const problems = [];
  const anchors = new Set(groups.flatMap((g) => g.questions));
  const { questionTags = {}, tagLabels = {} } = tags ?? {};

  for (const anchor of orphans) {
    problems.push(`${anchor}: sits under no group, so no filter or rail entry reaches it`);
  }
  for (const group of groups) {
    if (group.questions.length === 0) problems.push(`${group.id}: names a group with no questions`);
  }
  for (const symptom of symptoms) {
    if (symptom.questions.length === 0) {
      problems.push(`symptom "${symptom.label}": names no question`);
    }
    for (const anchor of symptom.questions) {
      if (!anchors.has(anchor)) {
        problems.push(`symptom "${symptom.label}": names ${anchor}, which the page does not ask`);
      }
    }
    if (!aliases.includes(symptom.label)) {
      problems.push(`symptom "${symptom.label}": is not a searchAlias, so the site search misses it`);
    }
  }
  if (tags) {
    const used = new Set();
    for (const anchor of anchors) {
      const carried = questionTags[anchor] ?? [];
      if (carried.length === 0) {
        problems.push(`${anchor}: carries no tag, so only a reader of the whole page finds it`);
      }
      for (const tag of carried) used.add(tag);
    }
    for (const anchor of Object.keys(questionTags)) {
      if (!anchors.has(anchor)) {
        problems.push(`tags name ${anchor}, which the page does not ask`);
      }
    }
    for (const tag of used) {
      if (!tagLabels[tag]) problems.push(`tag "${tag}": has no label, so the rail cannot name it`);
    }
    for (const tag of Object.keys(tagLabels)) {
      if (!used.has(tag)) problems.push(`tag "${tag}": is labelled and carried by no question`);
    }
  }
  if (badge !== null && badge !== anchors.size) {
    problems.push(`the sidebar badge says ${badge} questions and the page asks ${anchors.size}`);
  }
  if (rendered !== null && rendered !== anchors.size) {
    problems.push(`the page asks ${anchors.size} questions and the build rendered ${rendered}`);
  }
  return problems;
}

function sidebarBadge(source) {
  const match = source.match(/\{\s*slug:\s*'faq',\s*badge:\s*\{\s*text:\s*'(\d+)'/);
  return match ? Number(match[1]) : null;
}

function selftest() {
  const page = { groups: [{ id: 'g-a', name: 'A', questions: ['one'] }], orphans: [] };
  const symptoms = [{ label: 'sym', questions: ['one'] }];
  const aliases = ['sym'];
  const tags = { questionTags: { one: ['alpha'] }, tagLabels: { alpha: 'Alpha' } };
  const cases = [
    { name: 'a page whose parts agree', page, symptoms, badge: 1, aliases, expect: 0 },
    {
      name: 'a question outside every group',
      page: { groups: page.groups, orphans: ['loose'] },
      symptoms, badge: 1, aliases, expect: 1,
    },
    {
      name: 'a symptom naming a question that is not asked',
      page, symptoms: [{ label: 'sym', questions: ['gone'] }], badge: 1, aliases, expect: 1,
    },
    {
      name: 'a symptom the site search cannot find',
      page, symptoms, badge: 1, aliases: [], expect: 1,
    },
    { name: 'a badge that drifted', page, symptoms, badge: 7, aliases, expect: 1 },
    {
      name: 'a group with no questions',
      page: { groups: [...page.groups, { id: 'g-b', name: 'B', questions: [] }], orphans: [] },
      symptoms, badge: 1, aliases, expect: 1,
    },
    {
      name: 'a build that dropped a question',
      page, symptoms, badge: 1, aliases, rendered: 0, expect: 1,
    },
    { name: 'a page whose tags agree', page, symptoms, badge: 1, aliases, tags, expect: 0 },
    {
      name: 'a question nobody tagged',
      page, symptoms, badge: 1, aliases,
      tags: { questionTags: {}, tagLabels: {} }, expect: 1,
    },
    {
      name: 'a tag entry naming a question that is not asked',
      page, symptoms, badge: 1, aliases,
      tags: { questionTags: { one: ['alpha'], gone: ['alpha'] }, tagLabels: { alpha: 'Alpha' } },
      expect: 1,
    },
    {
      name: 'a tag the rail cannot name',
      page, symptoms, badge: 1, aliases,
      tags: { questionTags: { one: ['alpha'] }, tagLabels: {} }, expect: 1,
    },
    {
      name: 'a label no question carries',
      page, symptoms, badge: 1, aliases,
      tags: { questionTags: { one: ['alpha'] }, tagLabels: { alpha: 'Alpha', beta: 'Beta' } },
      expect: 1,
    },
  ];

  let failures = 0;
  for (const testCase of cases) {
    const got = structureProblems(
      testCase.page,
      testCase.symptoms,
      testCase.badge,
      testCase.aliases,
      testCase.rendered ?? null,
      testCase.tags,
    ).length;
    if (got !== testCase.expect) {
      console.error(`  ${testCase.name}: expected ${testCase.expect}, got ${got}`);
      failures += 1;
    }
  }
  const read = readStructure('---\nx: 1\n---\n## G {#g-a}\n\n### Q {#one}\n');
  if (read.groups.length !== 1 || read.groups[0].questions[0] !== 'one') {
    console.error('  readStructure did not read a group and its question');
    failures += 1;
  }
  if (failures) {
    console.error(`check-faq-groups.mjs --selftest: ${failures} case(s) failed`);
    process.exit(1);
  }
  console.log(`check-faq-groups.mjs --selftest: OK (${cases.length} cases)`);
}

async function main() {
  if (process.argv.includes('--selftest')) return selftest();

  const markdown = readFileSync(pagePath, 'utf8');
  const structure = readStructure(markdown);
  const { faqSymptoms } = await import('../src/faq-symptoms.mjs');
  const { questionTags, tagLabels } = await import('../src/faq-tags.mjs');
  const aliases = [...markdown.matchAll(/^\s+- "([^"]+)"$/gm)].map((m) => m[1]);
  const badge = sidebarBadge(readFileSync(join(siteRoot, 'src', 'sidebar.mjs'), 'utf8'));

  // The built page is the only proof the structure survived rendering. Without
  // a build that half is skipped rather than silently passed.
  const rendered = existsSync(builtPage)
    ? (readFileSync(builtPage, 'utf8').match(/<h3[^>]*\bid="/g) ?? []).length
    : null;

  const problems = structureProblems(structure, faqSymptoms, badge, aliases, rendered, {
    questionTags,
    tagLabels,
  });
  if (problems.length) {
    console.error('check-faq-groups.mjs: FAILED');
    for (const problem of problems) console.error(`  ${problem}`);
    process.exit(1);
  }

  const questions = structure.groups.reduce((n, g) => n + g.questions.length, 0);
  console.log(
    `check-faq-groups.mjs: OK (${questions} questions in ${structure.groups.length} groups, ` +
      `${Object.keys(tagLabels).length} tags, ${faqSymptoms.length} symptoms, ` +
      `${rendered === null ? 'page not built' : 'rendered'})`,
  );
}

await main();
