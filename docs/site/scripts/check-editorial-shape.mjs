#!/usr/bin/env node
// Reports editorial heuristics that need judgment. The current tree has no
// unwaived findings, so a new finding fails until it is fixed or receives a
// specific reviewed waiver. Identical tab panels may never be waived.

import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repoRoot = join(siteRoot, '..', '..');
const inventoryPath = join(siteRoot, 'content-inventory.json');
const waiverPath = join(scriptDir, 'data', 'editorial-waivers.json');

const wordLimits = {
  landing: 1400,
  tutorial: 2500,
  'how-to': 4000,
  concept: 2500,
  reference: 5000,
  troubleshooting: 3000,
  status: 5000,
  contributor: 5000,
};

const waiverChecks = new Set([
  'generic-introduction',
  'mixed-page-type',
  'near-duplicate',
  'page-length',
]);

function bodyOf(source) {
  return source.replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n/, '');
}

function withoutFencedCode(source) {
  return source.replace(/```[\s\S]*?```/g, '\n').replace(/~~~[\s\S]*?~~~/g, '\n');
}

function normalizedProse(value) {
  return value
    .toLowerCase()
    .replace(/<!--[^]*?-->/g, ' ')
    .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
    .replace(/<[^>]+>/g, ' ')
    .replace(/[`*_{}()[\]#:;,.!?"'“”‘’—–/\\|=+~-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function firstParagraph(source) {
  const blocks = withoutFencedCode(bodyOf(source)).split(/\r?\n\s*\r?\n/);
  for (const raw of blocks) {
    const value = raw.trim();
    if (value === '' || value.startsWith('import ') || value.startsWith('export ')) continue;
    if (/^(?:#|<|\{|:::|\||!\[|[-*+]\s|\d+[.)]\s)/.test(value)) continue;
    return normalizedProse(value);
  }
  return '';
}

function paragraphs(source) {
  return withoutFencedCode(bodyOf(source))
    .split(/\r?\n\s*\r?\n/)
    .map((raw) => raw.trim())
    .filter((value) => value !== '' && !/^(?:#|<|\{|:::|\||!\[|[-*+]\s|\d+[.)]\s|import |export )/.test(value))
    .map((value) => normalizedProse(value))
    .filter((value) => value.split(' ').length >= 30);
}

function shingleSet(value) {
  const words = value.split(' ');
  const shingles = new Set();
  for (let index = 0; index + 2 < words.length; index += 1) {
    shingles.add(words.slice(index, index + 3).join(' '));
  }
  return shingles;
}

function jaccard(left, right) {
  let intersection = 0;
  for (const item of left) if (right.has(item)) intersection += 1;
  return intersection / (left.size + right.size - intersection);
}

function fingerprint(value) {
  return createHash('sha256').update(value).digest('hex').slice(0, 12);
}

function findingKey({ route, check, target }) {
  return `${route}\0${check}\0${target}`;
}

export function duplicateTabFindings(source) {
  const findings = [];
  for (const [tabsIndex, tabs] of [...source.matchAll(/<Tabs\b[^>]*>([\s\S]*?)<\/Tabs>/g)].entries()) {
    const seen = new Map();
    for (const item of tabs[1].matchAll(/<TabItem\b[^>]*\blabel=(['"])(.*?)\1[^>]*>([\s\S]*?)<\/TabItem>/g)) {
      const label = item[2];
      const normalized = item[3].replace(/<!--[^]*?-->/g, '').replace(/\s+/g, ' ').trim();
      const first = seen.get(normalized);
      if (normalized !== '' && first !== undefined) {
        findings.push(`tab set ${tabsIndex + 1} repeats the same content in ${JSON.stringify(first)} and ${JSON.stringify(label)}`);
      } else {
        seen.set(normalized, label);
      }
    }
  }
  return findings;
}

export function pageFindings(page, source) {
  const findings = [];
  const limit = wordLimits[page.type];
  if (page.visibleWords > limit) {
    findings.push({
      route: page.route,
      path: page.path,
      check: 'page-length',
      target: 'page',
      message: `${page.type} page has ${page.visibleWords} visible words; review it above ${limit}`,
    });
  }

  if (/^(?:this page|this guide|in this guide|in this page)\b/.test(firstParagraph(source))) {
    findings.push({
      route: page.route,
      path: page.path,
      check: 'generic-introduction',
      target: 'first-paragraph',
      message: 'first paragraph describes the page instead of the reader situation or lookup scope',
    });
  }

  const headings = [...withoutFencedCode(bodyOf(source)).matchAll(/^##\s+(.+)$/gm)].map((match) => match[1].trim());
  const referenceHeading = headings.find((heading) =>
    /^(?:all flags|command reference|complete command list|configuration reference|flag reference)\b/i.test(heading),
  );
  if (['tutorial', 'how-to', 'concept', 'landing'].includes(page.type) && referenceHeading) {
    findings.push({
      route: page.route,
      path: page.path,
      check: 'mixed-page-type',
      target: `heading:${normalizedProse(referenceHeading).replaceAll(' ', '-')}`,
      message: `${page.type} page contains reference section ${JSON.stringify(referenceHeading)}`,
    });
  }
  if (page.type === 'tutorial' && headings.length > 10) {
    findings.push({
      route: page.route,
      path: page.path,
      check: 'mixed-page-type',
      target: 'section-count',
      message: `tutorial has ${headings.length} level-two sections; review the five-to-seven-step learning path`,
    });
  }
  return findings;
}

export function duplicateParagraphFindings(entries) {
  const findings = [];
  const buckets = new Map();
  for (const entry of entries) {
    if (entry.page.generated) continue;
    for (const paragraph of paragraphs(entry.source)) {
      const words = paragraph.split(' ');
      const key = words.slice(0, 5).join(' ');
      const bucket = buckets.get(key) ?? [];
      const shingles = shingleSet(paragraph);
      for (const previous of bucket) {
        if (previous.page.route === entry.page.route) continue;
        const ratio = words.length / previous.words;
        if (ratio < 0.85 || ratio > 1.18) continue;
        const similarity = jaccard(shingles, previous.shingles);
        if (similarity >= 0.9) {
          findings.push({
            route: entry.page.route,
            path: entry.page.path,
            check: 'near-duplicate',
            target: `paragraph:${fingerprint(paragraph)}:matches:${previous.page.route}`,
            message: `paragraph is ${Math.round(similarity * 100)}% similar to ${previous.page.route}`,
          });
          break;
        }
      }
      bucket.push({ page: entry.page, words: words.length, shingles });
      buckets.set(key, bucket);
    }
  }
  return findings;
}

export function waiverProblems(waivers, pages, findings) {
  const problems = [];
  const liveRoutes = new Set(pages.map((page) => page.route));
  const findingKeys = new Set(findings.map(findingKey));
  const seen = new Set();
  if (!Array.isArray(waivers)) return ['editorial waivers must be a JSON array'];

  for (const [index, waiver] of waivers.entries()) {
    const where = `waiver ${index + 1}`;
    if (!waiver || typeof waiver !== 'object' || Array.isArray(waiver)) {
      problems.push(`${where}: must be an object`);
      continue;
    }
    if (!liveRoutes.has(waiver.route)) problems.push(`${where}: ${waiver.route} is not a live route`);
    if (!waiverChecks.has(waiver.check)) problems.push(`${where}: unknown check ${waiver.check}`);
    if (typeof waiver.target !== 'string' || waiver.target.trim() === '') {
      problems.push(`${where}: target must identify the exact finding being waived`);
    }
    if (typeof waiver.reason !== 'string' || waiver.reason.trim().length < 30) {
      problems.push(`${where}: reason must contain at least 30 characters`);
    }
    const key = findingKey(waiver);
    if (seen.has(key)) problems.push(`${where}: duplicates ${waiver.route} ${waiver.check} ${waiver.target}`);
    seen.add(key);
    if (!findingKeys.has(key)) problems.push(`${where}: stale; the waived finding no longer exists`);
  }
  return problems;
}

export function unwaivedFindings(waivers, findings) {
  const waived = new Set(waivers.map(findingKey));
  return findings.filter((finding) => !waived.has(findingKey(finding)));
}

function selftest() {
  const page = { route: '/guide/', path: 'guide.md', type: 'tutorial', visibleWords: 2600, generated: false };
  const source = '---\ntitle: Guide\n---\n\nThis guide explains the task.\n\n## Command reference\n';
  const findings = pageFindings(page, source);
  const checks = findings.map((finding) => finding.check).sort();
  if (checks.join(',') !== 'generic-introduction,mixed-page-type,page-length') {
    throw new Error(`page findings: got ${checks.join(',')}`);
  }

  const duplicateTabs = duplicateTabFindings('<Tabs><TabItem label="A">same\nbody</TabItem><TabItem label="B">same body</TabItem></Tabs>');
  if (duplicateTabs.length !== 1) throw new Error('identical tab panels were not detected');
  if (duplicateTabFindings('<Tabs><TabItem label="A">one</TabItem><TabItem label="B">two</TabItem></Tabs>').length !== 0) {
    throw new Error('different tab panels were reported');
  }

  const duplicates = duplicateParagraphFindings([
    { page: { route: '/a/', path: 'a.md', generated: false }, source: `${'A repeated paragraph carries enough distinct words to make the similarity check meaningful for a documentation reader. '.repeat(4)}` },
    { page: { route: '/b/', path: 'b.md', generated: false }, source: `${'A repeated paragraph carries enough distinct words to make the similarity check meaningful for a documentation reader. '.repeat(4)}` },
  ]);
  if (duplicates.length !== 1) throw new Error('near-duplicate paragraphs were not detected');

  const problems = waiverProblems(
    [{ route: '/guide/', check: 'page-length', target: 'page', reason: 'The complete reference is intentionally kept together.' }],
    [page],
    findings,
  );
  if (problems.length !== 0) throw new Error(`valid waiver failed: ${problems.join('; ')}`);
  const active = unwaivedFindings(
    [{ route: '/guide/', check: 'page-length', target: 'page', reason: 'The complete reference is intentionally kept together.' }],
    findings,
  );
  if (active.length !== 2 || active.some((finding) => finding.check === 'page-length')) {
    throw new Error('new unwaived findings were not separated from reviewed waivers');
  }
  const sameCheckFindings = [
    { route: '/guide/', path: 'guide.md', check: 'mixed-page-type', target: 'heading:flags', message: 'flags' },
    { route: '/guide/', path: 'guide.md', check: 'mixed-page-type', target: 'section-count', message: 'sections' },
  ];
  const exactlyOne = unwaivedFindings(
    [{ route: '/guide/', check: 'mixed-page-type', target: 'heading:flags', reason: 'The local flag lookup is intentionally task-scoped.' }],
    sameCheckFindings,
  );
  if (exactlyOne.length !== 1 || exactlyOne[0].target !== 'section-count') {
    throw new Error('one waiver masked another finding with the same route and check');
  }
  console.log('check-editorial-shape.mjs --selftest: OK (9 assertions)');
}

function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }

  const inventory = JSON.parse(readFileSync(inventoryPath, 'utf8'));
  const entries = inventory.pages.map((page) => ({
    page,
    source: readFileSync(join(repoRoot, page.path), 'utf8'),
  }));
  const findings = entries.flatMap(({ page, source }) => pageFindings(page, source));
  findings.push(...duplicateParagraphFindings(entries));

  const hardProblems = [];
  for (const { page, source } of entries) {
    for (const message of duplicateTabFindings(source)) {
      hardProblems.push(`${page.path}: ${message}; use one shared block or make the platform-specific instructions distinct`);
    }
  }

  const waivers = JSON.parse(readFileSync(waiverPath, 'utf8'));
  hardProblems.push(...waiverProblems(waivers, inventory.pages, findings));
  if (hardProblems.length > 0) {
    console.error('check-editorial-shape.mjs: FAILED');
    for (const problem of hardProblems) console.error(`  ${problem}`);
    process.exit(1);
  }

  const active = unwaivedFindings(waivers, findings);
  for (const finding of active) {
    const message = `${finding.check}: ${finding.message}`;
    if (process.env.GITHUB_ACTIONS === 'true') {
      console.log(`::warning file=${finding.path},title=Editorial review::${message}`);
    } else {
      console.warn(`warning: ${finding.path}: ${message}`);
    }
  }
  if (active.length > 0) {
    console.error(
      `check-editorial-shape.mjs: FAILED (${active.length} unwaived review finding${active.length === 1 ? '' : 's'}; fix each finding or add a specific reviewed waiver)`,
    );
    process.exitCode = 1;
    return;
  }
  console.log(
    `check-editorial-shape.mjs: OK (${inventory.pages.length} pages, ${active.length} review warnings, ${waivers.length} active waivers)`,
  );
}

main();
