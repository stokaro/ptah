#!/usr/bin/env node
// Holds the glossary map to the pages around it.
//
// The two failures it holds are both about content:
//
//   - an entry whose `learnMore` names a page that does not exist, which is a
//     dead link a reader meets only after deciding they want to know more;
//   - a term in the map that the glossary page does not render, which is a
//     definition nobody can reach.
//
// It measures the content and nothing about presentation. The inline tooltip it
// used to report coverage for is gone: it never placed a panel beside its term
// in any browser the gate ran, reached three pages, and cost a four-minute
// check to say so (stokaro/ptah#3224). A term is reached by a page linking to
// the glossary now.
//
// Usage:
//   node scripts/check-glossary-coverage.mjs [--selftest]

import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const contentRoot = join(siteRoot, 'src', 'content', 'docs');

/**
 * coverageProblems reports what a reader would hit. `entries` is the glossary
 * map, `routes` the routes the site publishes, and `rendered` the terms the
 * built glossary page carries.
 */
export function coverageProblems(entries, routes, rendered, groups, built = true) {
  const problems = [];
  // An empty render and an unbuilt page are different states. Treating them as
  // one made a page that rendered nothing read as a page nobody had built, so
  // the check that the list exists could never fail.
  if (built && rendered.length === 0 && Object.keys(entries).length > 0) {
    problems.push('the glossary page rendered no entries at all');
  }
  for (const [term, entry] of Object.entries(entries)) {
    if (!entry.definition || !entry.definition.trim()) {
      problems.push(`${term}: has no definition`);
    }
    if (!entry.group || !groups.includes(entry.group)) {
      problems.push(`${term}: group ${JSON.stringify(entry.group)} is not one the page renders`);
    }
    if (!entry.learnMore) {
      problems.push(`${term}: names no page that teaches it`);
    } else if (!routes.includes(`/${entry.learnMore}/`)) {
      problems.push(`${term}: learnMore names /${entry.learnMore}/, which the site does not publish`);
    }
    if (built && rendered.length > 0 && !rendered.includes(term)) {
      problems.push(`${term}: is in the map but the glossary page does not render it`);
    }
  }
  return problems;
}

/** renderedTerms returns the terms the built glossary page carries as entries. */
export function renderedTerms(html) {
  return [...html.matchAll(/id="term-([^"]+)"/g)].map((match) => match[1]);
}

function pageRoutes(root, prefix = '') {
  const routes = [];
  for (const name of readdirSync(root)) {
    const full = join(root, name);
    if (statSync(full).isDirectory()) {
      routes.push(...pageRoutes(full, `${prefix}${name}/`));
      continue;
    }
    if (!['.md', '.mdx'].includes(extname(name))) continue;
    const slug = name.replace(/\.mdx?$/, '');
    routes.push(slug === 'index' ? `/${prefix}` : `/${prefix}${slug}/`);
  }
  return routes;
}

function selftest() {
  const groups = ['schema'];
  const routes = ['/concepts/desired-schema-and-sources/'];
  const entry = {
    definition: 'A definition.',
    group: 'schema',
    learnMore: 'concepts/desired-schema-and-sources',
  };
  const cases = [
    { name: 'a complete entry passes', entries: { a: entry }, rendered: ['a'], expect: 0 },
    {
      name: 'a learnMore the site does not publish',
      entries: { a: { ...entry, learnMore: 'nowhere/at/all' } },
      rendered: ['a'],
      expect: 1,
    },
    {
      name: 'a term the page does not render',
      entries: { a: entry },
      rendered: ['b'],
      expect: 1,
    },
    {
      name: 'an unknown group',
      entries: { a: { ...entry, group: 'invented' } },
      rendered: ['a'],
      expect: 1,
    },
    {
      name: 'an empty definition',
      entries: { a: { ...entry, definition: '  ' } },
      rendered: ['a'],
      expect: 1,
    },
    // A built page that rendered nothing is the failure an empty-means-skip
    // reading turns into a pass.
    { name: 'a built page that rendered nothing', entries: { a: entry }, rendered: [], expect: 1 },
    {
      name: 'an unbuilt page leaves the list unchecked',
      entries: { a: entry },
      rendered: [],
      built: false,
      expect: 0,
    },
  ];

  let failures = 0;
  for (const testCase of cases) {
    const got = coverageProblems(
      testCase.entries,
      routes,
      testCase.rendered,
      groups,
      testCase.built ?? true,
    ).length;
    if (got !== testCase.expect) {
      console.error(`  ${testCase.name}: expected ${testCase.expect}, got ${got}`);
      failures += 1;
    }
  }
  if (renderedTerms('<div id="term-drift"></div><div id="other"></div>').join() !== 'drift') {
    console.error('  renderedTerms read an id that is not a glossary entry');
    failures += 1;
  }
  if (failures) {
    console.error(`check-glossary-coverage.mjs --selftest: ${failures} case(s) failed`);
    process.exit(1);
  }
  console.log(`check-glossary-coverage.mjs --selftest: OK (${cases.length} cases)`);
}

async function main() {
  if (process.argv.includes('--selftest')) return selftest();

  const { glossary, glossaryGroups } = await import('../src/glossary.ts');
  const groups = Object.keys(glossaryGroups);
  const routes = pageRoutes(contentRoot);

  // The built page is the only place that proves the list rendered. Without a
  // build the other rules still hold, and this one is skipped rather than
  // silently passed.
  const page = join(siteRoot, 'dist', 'reference', 'glossary', 'index.html');
  const built = existsSync(page);
  const rendered = built ? renderedTerms(readFileSync(page, 'utf8')) : [];

  const problems = coverageProblems(glossary, routes, rendered, groups, built);
  if (problems.length) {
    console.error('check-glossary-coverage.mjs: FAILED');
    for (const problem of problems) console.error(`  ${problem}`);
    process.exit(1);
  }

  const total = Object.keys(glossary).length;
  const renderNote = built ? `${rendered.length} rendered` : 'page not built, list unchecked';
  console.log(`check-glossary-coverage.mjs: OK (${total} terms, ${renderNote})`);
}

await main();
