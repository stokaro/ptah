#!/usr/bin/env node
// Runs the documentation version picker in a browser against the built site.
//
// The picker is public/version-picker.js and public/version-picker.css, served
// at the Pages root and loaded by every documentation version from there, so a
// defect in it reaches every version at once. This check serves the built edge
// site under its base, the two picker files at the root as the deploy
// publishes them, and a version index and two other versions it makes up, then
// reads what a reader would get:
//
//   - the mount point, the files it loads, and the control that replaces it;
//   - the index's versions in the index's order, with the page's own selected;
//   - a page whose version the index does not list, which keeps its version
//     at the top rather than claiming to be another;
//   - no index at all, which leaves the page's version alone;
//   - a choice that lands on the same page in the other version, and one that
//     lands on that version's home page because the page does not exist there;
//   - scripting disabled, where the mount point shows the version as text.
//
//   node scripts/check-version-picker.mjs            # needs `npm run build`
//   node scripts/check-version-picker.mjs --selftest
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { loadChromium, startBuiltSite } from './lib/built-site.mjs';
import { mountFixture, mountProblems, pickerRoute, readPicker } from './lib/version-picker-check.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const distRoot = join(scriptDir, '..', 'dist');

const PAGE = 'versioned/generate/';
// Two versions that exist only in this check: the first has the page, the
// second has only a home page.
const WITH_PAGE = 'v9.9.0';
const WITHOUT_PAGE = 'v9.8.0';
// The page's own version sits in the middle of the index on purpose. A select
// with nothing marked selected shows its first option, so with the page's
// version first a control that selected nothing would read as correct.
const LISTED = [WITH_PAGE, 'edge', WITHOUT_PAGE];
// An index that does not name the page's version: the control puts it first.
const UNLISTED = [WITH_PAGE, WITHOUT_PAGE];

// pickerBehaviorProblems judges every reading one run takes.
export function pickerBehaviorProblems(readings) {
  const problems = [];
  const version = readings.version;
  for (const [name, reading] of [['listed', readings.listed], ['unlisted', readings.unlisted], ['no index', readings.missing]]) {
    problems.push(...mountProblems(reading, { version }).map((problem) => `${name}: ${problem}`));
  }
  problems.push(
    ...mountProblems(readings.noScript, { version, hydrated: false }).map((problem) => `no scripting: ${problem}`),
  );
  const expect = (label, got, want) => {
    if (JSON.stringify(got) !== JSON.stringify(want)) {
      problems.push(`${label}: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);
    }
  };
  expect('listed: the options', readings.listed.options, LISTED);
  expect('unlisted: the options', readings.unlisted.options, [version, ...UNLISTED]);
  expect('no index: the options', readings.missing.options, [version]);
  expect(`choosing ${WITH_PAGE} leads to`, readings.samePage, `/${WITH_PAGE}/${PAGE}`);
  expect(`choosing ${WITHOUT_PAGE} leads to`, readings.homePage, `/${WITHOUT_PAGE}/`);
  expect('page errors', readings.errors, []);
  return problems;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const listed = { ...mountFixture('edge'), options: LISTED };
  const valid = {
    version: 'edge',
    listed,
    unlisted: { ...mountFixture('edge'), options: ['edge', ...UNLISTED] },
    missing: mountFixture('edge'),
    noScript: mountFixture('edge', { hydrated: false }),
    samePage: `/${WITH_PAGE}/${PAGE}`,
    homePage: `/${WITHOUT_PAGE}/`,
    errors: [],
  };
  assert(pickerBehaviorProblems(valid).length === 0, `a valid run was refused: ${pickerBehaviorProblems(valid).join('; ')}`);
  const mutations = [
    ['a listed version missing from the control', (r) => { r.listed.options = ['edge', WITH_PAGE]; }],
    ['the index order reversed', (r) => { r.listed.options = [...LISTED].reverse(); }],
    ['the page\'s version moved to the top', (r) => { r.listed.options = ['edge', WITH_PAGE, WITHOUT_PAGE]; }],
    ['an unlisted version dropped from the control', (r) => { r.unlisted.options = UNLISTED; }],
    ['another version selected', (r) => { r.listed.selected = WITH_PAGE; }],
    ['a choice that lands on the home page although the page exists', (r) => { r.samePage = `/${WITH_PAGE}/`; }],
    ['a choice that lands on a missing page', (r) => { r.homePage = `/${WITHOUT_PAGE}/${PAGE}`; }],
    ['the root stylesheet not applied', (r) => { r.listed.appearance = 'auto'; }],
    ['the picker loaded from inside the version', (r) => { r.listed.scripts = ['/edge/version-picker.js']; }],
    ['a stylesheet loaded twice', (r) => { r.missing.stylesheets = ['/version-picker.css', '/version-picker.css']; }],
    ['a mount point the script never replaced', (r) => { r.missing.hydrated = false; }],
    ['a mount point naming another version', (r) => { r.listed.current = 'v0.1.0'; }],
    ['a second mount point', (r) => { r.unlisted.mounts = 2; }],
    ['no text without scripting', (r) => { r.noScript.text = ''; }],
    ['a page error', (r) => { r.errors = ['boom']; }],
  ];
  for (const [label, mutate] of mutations) {
    const broken = structuredClone(valid);
    mutate(broken);
    assert(pickerBehaviorProblems(broken).length > 0, `${label} was accepted`);
  }
  console.log(`check-version-picker.mjs --selftest: OK (${mutations.length} refused readings)`);
}

const page = (text) => ({ status: 200, body: `<!doctype html><title>${text}</title><p>${text}</p>`, type: 'text/html' });

async function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }
  if (!existsSync(join(distRoot, 'index.html'))) {
    throw new Error('no built site in dist/; run "npm run build" first');
  }
  const chromium = await loadChromium('check-version-picker.mjs');
  if (!chromium) return;

  const state = { index: undefined };
  const others = new Map([
    [`/${WITH_PAGE}/`, page(WITH_PAGE)],
    [`/${WITH_PAGE}/${PAGE}`, page(`${WITH_PAGE} ${PAGE}`)],
    [`/${WITHOUT_PAGE}/`, page(WITHOUT_PAGE)],
  ]);
  const route = pickerRoute((path) => {
    if (path === '/versions.json') {
      return state.index
        ? { status: 200, body: JSON.stringify(state.index), type: 'application/json' }
        : { status: 404, body: 'not found' };
    }
    return others.get(path);
  });
  const built = await startBuiltSite(distRoot, undefined, route);
  const version = built.base.split('/').filter(Boolean).pop() ?? 'edge';
  const url = `http://127.0.0.1:${built.port}${built.base}/${PAGE}`;
  const index = (slugs) => ({ default: 'edge', versions: slugs.map((slug) => ({ slug, label: slug })) });

  let browser;
  try {
    browser = await chromium.launch();
    const errors = [];
    const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
    const tab = await context.newPage();
    tab.on('pageerror', (error) => errors.push(error.message));
    // The script requests the index after it replaces the mount point and
    // fills the control when the answer arrives, so waiting for the network
    // to go quiet waits for the options too.
    const open = async (slugs) => {
      state.index = slugs ? index(slugs) : undefined;
      await tab.goto(url, { waitUntil: 'networkidle' });
      return readPicker(tab);
    };
    const choose = async (target) => {
      await open(LISTED);
      await Promise.all([
        tab.waitForURL((address) => address.pathname.startsWith(`/${target}/`)),
        tab.selectOption('header [data-ptah-version-picker] select', target),
      ]);
      return new URL(tab.url()).pathname;
    };
    const readings = {
      version,
      listed: await open(LISTED),
      unlisted: await open(UNLISTED),
      missing: await open(undefined),
      samePage: await choose(WITH_PAGE),
      homePage: await choose(WITHOUT_PAGE),
      errors,
    };
    const noScript = await browser.newContext({ javaScriptEnabled: false });
    const plain = await noScript.newPage();
    await plain.goto(url, { waitUntil: 'load' });
    readings.noScript = await readPicker(plain, { hydrate: false });

    const problems = pickerBehaviorProblems(readings);
    if (problems.length > 0) throw new Error(problems.join('; '));
  } finally {
    if (browser) await browser.close();
    await new Promise((resolveClose) => built.server.close(resolveClose));
  }
  console.log(`check-version-picker.mjs: OK (${version}: index order, unlisted version, no index, same page, home page, no scripting)`);
}

main().catch((error) => {
  console.error(`check-version-picker.mjs: FAILED: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
});
