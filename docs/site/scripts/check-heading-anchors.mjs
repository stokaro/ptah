#!/usr/bin/env node
// Proves that a heading which declares its own anchor gets that anchor, and
// that no page ships the declaration as visible text.
//
// `## Question {#explicit-id}` is read by src/lib/markdown-heading-ids.mjs, a
// Sätteri hast plugin that has to run before Sätteri's own heading-ids plugin.
// Nothing else measures it, and the two ways it fails are both silent:
//
//   - removed from `hastPlugins`, or ordered after the built-in, the suffix
//     renders as literal braces in the heading and the id becomes the slug of
//     the whole string, braces included;
//   - a published anchor that changes breaks every link anyone shared, which no
//     link checker sees, because a page's own anchors are not linked from
//     inside the site.
//
// So this reads what the source declared and what the build produced, and
// compares them. Removing the plugin turns this check red, which is the whole
// reason it exists.

import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, extname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(here, '..');
const contentRoot = join(siteRoot, 'src/content/docs');
const distRoot = join(siteRoot, 'dist');

const DECLARED = /^#{2,6}\s+.*?\{#([a-z0-9-]+)\}\s*$/gm;

/** Every `{#id}` a page declares, in source order. */
export function declaredAnchors(markdown) {
  const body = markdown.replace(/^---\n[\s\S]*?\n---\n/, '');
  // A fenced block can hold an example of the syntax; it is not a heading.
  const withoutFences = body.replace(/^```[\s\S]*?^```$/gm, '');
  return [...withoutFences.matchAll(DECLARED)].map((match) => match[1]);
}

/**
 * anchorProblems compares what each page declared with what its built HTML
 * carries. `pages` is [{ route, source, html }]; a missing `html` means the
 * route was not built, which is itself a finding.
 */
export function anchorProblems(pages) {
  const problems = [];
  for (const page of pages) {
    const declared = declaredAnchors(page.source);
    if (declared.length === 0) continue;
    if (typeof page.html !== 'string') {
      problems.push(`${page.route}: declares ${declared.length} anchor(s) but the route was not built`);
      continue;
    }
    const ids = new Set([...page.html.matchAll(/<h[1-6][^>]*\bid="([^"]+)"/g)].map((m) => m[1]));
    for (const anchor of declared) {
      if (!ids.has(anchor)) {
        problems.push(`${page.route}: declares {#${anchor}} but no heading in the built page carries that id`);
      }
    }
    if (/\{#[a-z0-9-]+\}/.test(stripTags(headings(page.html)))) {
      problems.push(`${page.route}: a heading renders the {#id} suffix as visible text; the heading-id plugin did not run`);
    }
  }
  return problems;
}

function headings(html) {
  return [...html.matchAll(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/g)].map((m) => m[1]).join('\n');
}

function stripTags(html) {
  return html.replace(/<[^>]+>/g, '');
}

function walk(dir, keep) {
  const out = [];
  if (!existsSync(dir)) return out;
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...walk(full, keep));
    else if (keep(entry.name)) out.push(full);
  }
  return out;
}

function routeOf(file) {
  const rel = relative(contentRoot, file).replace(/\\/g, '/').replace(/\.mdx?$/, '');
  const slug = rel.replace(/(^|\/)index$/, '');
  return slug === '' ? '/' : `/${slug}/`;
}

function builtHtml(route) {
  const candidate = join(distRoot, route.replace(/^\//, ''), 'index.html');
  return existsSync(candidate) && statSync(candidate).isFile()
    ? readFileSync(candidate, 'utf8')
    : undefined;
}

function selftest() {
  const cases = [
    {
      name: 'an honoured anchor passes',
      pages: [{ route: '/a/', source: '## Q {#q-one}\n', html: '<h2 id="q-one">Q</h2>' }],
      expect: 0,
    },
    {
      name: 'the plugin not running is caught twice',
      pages: [{
        route: '/a/',
        source: '## Q {#q-one}\n',
        html: '<h2 id="q-q-one">Q {#q-one}</h2>',
      }],
      expect: 2,
    },
    {
      name: 'a page declaring nothing is not inspected',
      pages: [{ route: '/a/', source: '## Plain heading\n', html: '<h2 id="other">Plain heading</h2>' }],
      expect: 0,
    },
    {
      name: 'an unbuilt route is a finding',
      pages: [{ route: '/a/', source: '## Q {#q-one}\n' }],
      expect: 1,
    },
    {
      name: 'the syntax inside a fenced block is not a declaration',
      pages: [{ route: '/a/', source: '```md\n## Q {#q-one}\n```\n', html: '<p>x</p>' }],
      expect: 0,
    },
  ];
  let failures = 0;
  for (const testCase of cases) {
    const got = anchorProblems(testCase.pages).length;
    if (got !== testCase.expect) {
      console.error(`  ${testCase.name}: expected ${testCase.expect} problem(s), got ${got}`);
      failures += 1;
    }
  }
  if (failures) {
    console.error(`check-heading-anchors.mjs --selftest: ${failures} case(s) failed`);
    process.exit(1);
  }
  console.log(`check-heading-anchors.mjs --selftest: OK (${cases.length} cases)`);
}

function main() {
  if (process.argv[2] === '--selftest') return selftest();

  if (!existsSync(distRoot)) {
    console.error('check-heading-anchors.mjs: dist not found; run "npm run build" first.');
    process.exit(1);
  }

  const pages = walk(contentRoot, (name) => ['.md', '.mdx'].includes(extname(name)))
    .map((file) => {
      const route = routeOf(file);
      return { route, source: readFileSync(file, 'utf8'), html: builtHtml(route) };
    });

  const declaring = pages.filter((page) => declaredAnchors(page.source).length > 0);
  const problems = anchorProblems(pages);
  if (problems.length) {
    console.error('check-heading-anchors.mjs: FAILED');
    for (const problem of problems) console.error(`  ${problem}`);
    process.exit(1);
  }

  const total = declaring.reduce((sum, page) => sum + declaredAnchors(page.source).length, 0);
  console.log(`check-heading-anchors.mjs: OK (${total} declared anchors on ${declaring.length} page(s))`);
}

main();
