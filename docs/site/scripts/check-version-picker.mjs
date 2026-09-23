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
//   - the mount point, the files it loads, and the button that replaces it;
//   - the panel: the index's versions in order under their groups, the page's
//     own version marked, the latest release badged, each release's date;
//   - the filter, which narrows the list, and Escape, which clears it and
//     then closes the panel and returns focus to the button;
//   - the keyboard: Enter opens the panel with focus in the filter, and the
//     arrow keys walk the list;
//   - a page whose version the index does not list, which keeps its version
//     at the top rather than claiming to be another;
//   - no index at all, which leaves the page's version alone;
//   - a choice that lands on the same page in the other version, and one that
//     lands on that version's home page because the page does not exist there;
//   - scripting disabled, where the mount point shows the version as text;
//   - axe's WCAG rules over the open panel, in the light and the dark theme.
//
//   node scripts/check-version-picker.mjs            # needs `npm run build`
//   node scripts/check-version-picker.mjs --selftest
import AxeBuilder from '@axe-core/playwright';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { loadChromium, startBuiltSite } from './lib/built-site.mjs';
import { mountFixture, mountProblems, pickerRoute, readPicker } from './lib/version-picker-check.mjs';
import { WCAG_TAGS } from './lib/wcag.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const distRoot = join(scriptDir, '..', 'dist');

const PAGE = 'versioned/generate/';
// Two releases that exist only in this check: the first has the page, the
// second has only a home page. The first is the latest.
const WITH_PAGE = 'v9.9.0';
const WITHOUT_PAGE = 'v9.8.0';
const RELEASED = { [WITH_PAGE]: '2030-02-01', [WITHOUT_PAGE]: '2030-01-01' };
const LISTED = ['edge', WITH_PAGE, WITHOUT_PAGE];
// An index that does not name the page's version: the panel puts it first.
const UNLISTED = [WITH_PAGE, WITHOUT_PAGE];
const GROUPS = ['In development', 'Releases'];

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
  const panel = readings.panel;
  expect('the open panel', panel.open, true);
  expect('the button while the panel is open: aria-expanded', panel.expanded, 'true');
  expect('focus when the panel opens', panel.focus, 'filter');
  expect('the listed versions', panel.slugs, LISTED);
  expect('the groups', panel.groups, GROUPS);
  expect('the version marked as the page\'s own', panel.current, [version]);
  expect('the versions badged latest', panel.latest, [WITH_PAGE]);
  expect('the release dates', panel.dates, RELEASED);
  expect(`the filter "${WITHOUT_PAGE}" leaves`, readings.filtered, [WITHOUT_PAGE]);
  expect('the first Escape', readings.escaped.once, { open: true, filter: '', shown: LISTED.length });
  expect('the second Escape', readings.escaped.twice, { open: false, focus: 'button' });
  expect('the keyboard: Enter, then two ArrowDowns, focuses', readings.keyboard, WITH_PAGE);
  expect('unlisted: the listed versions', readings.unlisted.slugs, [version, ...UNLISTED]);
  expect('no index: the listed versions', readings.missing.slugs, [version]);
  expect(`choosing ${WITH_PAGE} leads to`, readings.samePage, `/${WITH_PAGE}/${PAGE}`);
  expect(`choosing ${WITHOUT_PAGE} leads to`, readings.homePage, `/${WITHOUT_PAGE}/`);
  expect('accessibility violations in the open panel', readings.violations, []);
  expect('page errors', readings.errors, []);
  return problems;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const valid = {
    version: 'edge',
    listed: mountFixture('edge'),
    unlisted: { ...mountFixture('edge'), slugs: ['edge', ...UNLISTED] },
    missing: { ...mountFixture('edge'), slugs: ['edge'] },
    noScript: mountFixture('edge', { hydrated: false }),
    panel: {
      open: true,
      expanded: 'true',
      focus: 'filter',
      slugs: LISTED,
      groups: GROUPS,
      current: ['edge'],
      latest: [WITH_PAGE],
      dates: RELEASED,
    },
    filtered: [WITHOUT_PAGE],
    escaped: { once: { open: true, filter: '', shown: LISTED.length }, twice: { open: false, focus: 'button' } },
    keyboard: WITH_PAGE,
    samePage: `/${WITH_PAGE}/${PAGE}`,
    homePage: `/${WITHOUT_PAGE}/`,
    violations: [],
    errors: [],
  };
  assert(pickerBehaviorProblems(valid).length === 0, `a valid run was refused: ${pickerBehaviorProblems(valid).join('; ')}`);
  const mutations = [
    ['a listed version missing from the panel', (r) => { r.panel.slugs = ['edge', WITH_PAGE]; }],
    ['the index order reversed', (r) => { r.panel.slugs = [...LISTED].reverse(); }],
    ['the groups swapped', (r) => { r.panel.groups = [...GROUPS].reverse(); }],
    ['no version marked as the page\'s own', (r) => { r.panel.current = []; }],
    ['another version marked', (r) => { r.panel.current = [WITH_PAGE]; }],
    ['the latest badge on an older release', (r) => { r.panel.latest = [WITHOUT_PAGE]; }],
    ['a release date missing', (r) => { r.panel.dates = { [WITH_PAGE]: RELEASED[WITH_PAGE] }; }],
    ['a panel that does not open', (r) => { r.panel.open = false; }],
    ['a button that does not say it is expanded', (r) => { r.panel.expanded = 'false'; }],
    ['focus left on the button', (r) => { r.panel.focus = 'button'; }],
    ['a filter that narrows nothing', (r) => { r.filtered = LISTED; }],
    ['Escape that closes before clearing the filter', (r) => { r.escaped.once = { open: false, focus: 'button' }; }],
    ['Escape that does not return focus', (r) => { r.escaped.twice = { open: false, focus: 'body' }; }],
    ['arrow keys that do not move', (r) => { r.keyboard = 'filter'; }],
    ['an unlisted version dropped', (r) => { r.unlisted.slugs = UNLISTED; }],
    ['a choice that lands on the home page although the page exists', (r) => { r.samePage = `/${WITH_PAGE}/`; }],
    ['a choice that lands on a missing page', (r) => { r.homePage = `/${WITHOUT_PAGE}/${PAGE}`; }],
    ['the root stylesheet not applied', (r) => { r.listed.panelPosition = 'static'; }],
    ['the picker loaded from inside the version', (r) => { r.listed.scripts = ['/edge/version-picker.js']; }],
    ['a stylesheet loaded twice', (r) => { r.missing.stylesheets = ['/version-picker.css', '/version-picker.css']; }],
    ['a mount point the script never replaced', (r) => { r.missing.hydrated = false; }],
    ['a mount point naming another version', (r) => { r.listed.current = 'v0.1.0'; }],
    ['a second mount point', (r) => { r.unlisted.mounts = 2; }],
    ['a button with no name', (r) => { r.listed.label = null; }],
    ['no text without scripting', (r) => { r.noScript.text = ''; }],
    ['an accessibility violation', (r) => { r.violations = ['color-contrast: .ptah-version-picker__meta']; }],
    ['a page error', (r) => { r.errors = ['boom']; }],
  ];
  for (const [label, mutate] of mutations) {
    const broken = structuredClone(valid);
    mutate(broken);
    assert(pickerBehaviorProblems(broken).length > 0, `${label} was accepted`);
  }
  console.log(`check-version-picker.mjs --selftest: OK (${mutations.length} refused readings)`);
}

// settled waits for the panel's opening animation to end. axe reads colors as
// they are painted, and a panel still fading in reads as low contrast.
function settled(tab) {
  return tab.waitForFunction(() => document.getAnimations().every((animation) => animation.playState !== 'running'));
}

const html = (text) => ({ status: 200, body: `<!doctype html><title>${text}</title><p>${text}</p>`, type: 'text/html' });

// readPanel reads the open panel: the rows in order, the group titles, which
// row is the page's own, which carry the latest badge, and each row's date.
function readPanel(tab) {
  return tab.evaluate(() => {
    const trigger = document.querySelector('header [data-ptah-version-picker] button');
    const panel = document.getElementById(trigger.getAttribute('aria-controls'));
    const rows = [...panel.querySelectorAll('.ptah-version-picker__item:not([hidden]) a')];
    const focused = document.activeElement;
    return {
      open: !panel.hidden,
      expanded: trigger.getAttribute('aria-expanded'),
      focus: focused?.classList.contains('ptah-version-picker__input') ? 'filter' : focused === trigger ? 'button' : focused?.tagName?.toLowerCase(),
      slugs: rows.map((row) => row.dataset.version),
      groups: [...panel.querySelectorAll('.ptah-version-picker__group:not([hidden]) .ptah-version-picker__group-title')].map((title) => title.textContent),
      current: rows.filter((row) => row.getAttribute('aria-current') === 'page').map((row) => row.dataset.version),
      latest: rows.filter((row) => row.querySelector('.ptah-version-picker__badge')).map((row) => row.dataset.version),
      dates: Object.fromEntries(
        rows.filter((row) => row.querySelector('time')).map((row) => [row.dataset.version, row.querySelector('time').dateTime]),
      ),
    };
  });
}

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
    [`/${WITH_PAGE}/`, html(WITH_PAGE)],
    [`/${WITH_PAGE}/${PAGE}`, html(`${WITH_PAGE} ${PAGE}`)],
    [`/${WITHOUT_PAGE}/`, html(WITHOUT_PAGE)],
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
  const index = (slugs) => ({
    default: 'edge',
    latest: WITH_PAGE,
    versions: slugs.map((slug) => (RELEASED[slug] ? { slug, label: slug, released: RELEASED[slug] } : { slug, label: slug })),
  });
  const trigger = 'header [data-ptah-version-picker] button';

  let browser;
  try {
    browser = await chromium.launch();
    const errors = [];
    const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
    const tab = await context.newPage();
    tab.on('pageerror', (error) => errors.push(error.message));
    // The script requests the index after it replaces the mount point, so
    // waiting for the network to go quiet after loading waits for the list.
    const load = async (slugs) => {
      state.index = slugs ? index(slugs) : undefined;
      await tab.goto(url, { waitUntil: 'networkidle' });
    };
    const openPanel = async () => {
      await tab.click(trigger);
      await settled(tab);
      return readPanel(tab);
    };
    const choose = async (target) => {
      await load(LISTED);
      await openPanel();
      await Promise.all([
        tab.waitForURL((address) => address.pathname.startsWith(`/${target}/`)),
        tab.click(`.ptah-version-picker__option[data-version="${target}"]`),
      ]);
      return new URL(tab.url()).pathname;
    };

    await load(LISTED);
    const listed = await readPicker(tab);
    const panel = await openPanel();
    await tab.keyboard.type(WITHOUT_PAGE);
    const filtered = (await readPanel(tab)).slugs;
    await tab.keyboard.press('Escape');
    const once = await readPanel(tab);
    const filter = await tab.inputValue('.ptah-version-picker__input');
    await tab.keyboard.press('Escape');
    const twice = await readPanel(tab);

    await load(LISTED);
    await tab.focus(trigger);
    await tab.keyboard.press('Enter');
    await tab.keyboard.press('ArrowDown');
    await tab.keyboard.press('ArrowDown');
    const keyboard = await tab.evaluate(() => document.activeElement?.dataset?.version ?? 'filter');

    await load(UNLISTED);
    const unlisted = { ...(await readPicker(tab)), slugs: (await openPanel()).slugs };
    await load(undefined);
    const missing = { ...(await readPicker(tab)), slugs: (await openPanel()).slugs };

    // axe reads the panel open, in both themes: the colors are the part most
    // likely to fail, and they differ between the two.
    const violations = [];
    for (const colorScheme of ['light', 'dark']) {
      const themed = await browser.newContext({ viewport: { width: 1280, height: 800 }, colorScheme });
      const view = await themed.newPage();
      state.index = index(LISTED);
      await view.goto(url, { waitUntil: 'networkidle' });
      await view.click(trigger);
      await settled(view);
      const result = await new AxeBuilder({ page: view }).include('.ptah-version-picker__panel').withTags(WCAG_TAGS).analyze();
      for (const violation of result.violations) {
        violations.push(`${colorScheme} ${violation.id}: ${violation.nodes.slice(0, 3).map((node) => node.target.join(' ')).join(', ')}`);
      }
      await themed.close();
    }

    const readings = {
      version,
      violations,
      listed,
      unlisted,
      missing,
      panel,
      filtered,
      escaped: {
        once: { open: once.open, filter, shown: once.slugs.length },
        twice: { open: twice.open, focus: twice.focus },
      },
      keyboard,
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
  console.log(
    `check-version-picker.mjs: OK (${version}: panel, groups, latest, dates, filter, Escape, keyboard, unlisted version, no index, same page, home page, no scripting, axe in both themes)`,
  );
}

main().catch((error) => {
  console.error(`check-version-picker.mjs: FAILED: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
});
