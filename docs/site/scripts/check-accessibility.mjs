#!/usr/bin/env node
// Runs WCAG axe checks on the canonical page shapes and exercises keyboard
// interaction that static markup checks cannot establish.
import { existsSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import AxeBuilder from '@axe-core/playwright';
import { loadChromium, startBuiltSite } from './lib/built-site.mjs';
import { WCAG_TAGS } from './lib/wcag.mjs';
import { components, relationships } from '../src/ecosystem.mjs';
import { scenarios, stepResult, steps } from '../src/ydb-walkthrough.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const routes = [
  '/',
  '/start/quick-start/',
  '/start/ecosystem/',
  '/extend/write-a-provider/ydb-walkthrough-explore/',
  '/databases/support-matrix/',
  '/reference/native-commands/',
  '/schema/document/',
  '/schema/serve/',
];
const viewports = [
  { name: 'mobile', width: 390, height: 844 },
  { name: 'desktop', width: 1280, height: 900 },
];
// Both, because the site ships both and the palettes are separate declarations.
// This ran in the default context once, which resolves to the light theme, so
// every dark color the design introduced was unmeasured while the code and the
// README said otherwise (stokaro/ptah#2930).
const colorSchemes = ['light', 'dark'];
// Both shortcut hints. Starlight's search button shows "Ctrl K", or the
// command key on Apple platforms, chosen by navigator.platform, so a run
// measures only the hint of the host it runs on. The two read differently to
// label-content-name-mismatch, which drops the command key symbol and compares
// "Ctrl" as text, so a mismatch in one hint passes on a host that shows the
// other.
const platforms = [
  { name: 'ctrl', navigatorPlatform: 'Linux x86_64' },
  { name: 'command', navigatorPlatform: 'MacIntel' },
];

function formatViolation(route, viewport, scheme, platform, violation) {
  const targets = violation.nodes.slice(0, 3).map((node) => node.target.join(' ')).join(', ');
  return `${route} [${viewport}, ${scheme}, ${platform}] ${violation.id} (${violation.impact ?? 'unknown'}): ${targets}`;
}

// Expressive Code makes a code block that scrolls sideways focusable from the
// browser, not in the HTML: a ResizeObserver waits 250ms and an idle callback
// before it sets tabindex. axe run at `load` can read the page before that and
// report scrollable-region-focusable on a block that is about to be fixed --
// measured at 320 and 375px, where the same block had no tabindex at load and
// had one a second later. So wait, up to five seconds, for every overflowing
// block to carry it; a block that never gets one is still there for axe.
//
// The fonts come first: until the web fonts load, the fallback monospace is
// narrower, no block overflows yet, and the wait below would pass at once.
async function codeBlocksSettled(page) {
  await page.evaluate(() => document.fonts.ready);
  await page
    .waitForFunction(
      () =>
        [...document.querySelectorAll('.expressive-code pre')].every(
          (pre) => pre.scrollWidth <= pre.clientWidth || pre.hasAttribute('tabindex'),
        ),
      null,
      { timeout: 5_000 },
    )
    .catch(() => {});
}

async function axeViolations(page) {
  const { violations } = await new AxeBuilder({ page }).withTags(WCAG_TAGS).analyze();
  return violations;
}

async function selftest(chromium) {
  const browser = await chromium.launch();
  try {
    const context = await browser.newContext();
    const page = await context.newPage();
    await page.setContent('<html><body><button></button><img src="missing.png"></body></html>');
    const violations = await axeViolations(page);
    const ids = new Set(violations.map(({ id }) => id));
    const expected = ['button-name', 'html-has-lang', 'image-alt'];
    if (!expected.every((id) => ids.has(id))) {
      console.error(`check-accessibility.mjs --selftest: FAILED (reported ${[...ids].join(', ')})`);
      process.exitCode = 1;
      return;
    }
    console.log('check-accessibility.mjs --selftest: OK (unnamed button, missing language, and missing alt rejected)');
    await context.close();
  } finally {
    await browser.close();
  }
}

async function keyboardChecks(page, origin, base) {
  const problems = [];

  await page.goto(`${origin}${base}/schema/document/`, { waitUntil: 'load' });
  const sidebarSummary = page.locator('nav.sidebar summary', { hasText: 'Schemas' }).first();
  if (await sidebarSummary.count() !== 1) {
    problems.push('/schema/document/: schema sidebar disclosure is absent');
  } else {
    const details = sidebarSummary.locator('xpath=..');
    const before = await details.getAttribute('open');
    await sidebarSummary.focus();
    await page.keyboard.press('Enter');
    const after = await details.getAttribute('open');
    if ((before === null) === (after === null)) problems.push('/schema/document/: Enter did not toggle the sidebar group');
  }

  await page.goto(`${origin}${base}/start/quick-start/`, { waitUntil: 'load' });
  const tabs = page.getByRole('tab');
  if (await tabs.count() > 1) {
    await tabs.first().focus();
    await page.keyboard.press('ArrowRight');
    if (await tabs.nth(1).getAttribute('aria-selected') !== 'true') {
      problems.push('/start/quick-start/: ArrowRight did not select the next tab');
    }
  }

  await page.goto(`${origin}${base}/reference/native-commands/`, { waitUntil: 'load' });
  const commandFilter = page.getByLabel('Filter commands');
  await commandFilter.fill('ptah schema apply');
  const commandRows = page.locator('.sl-markdown-content table').first().locator('tbody tr:visible');
  if (await commandRows.count() !== 1 || !(await commandRows.first().innerText()).includes('ptah schema apply')) {
    problems.push('/reference/native-commands/: command filter did not isolate ptah schema apply');
  }

  await page.goto(`${origin}${base}/reference/command-flags/`, { waitUntil: 'load' });
  const flagFilter = page.getByLabel('Filter commands and flags');
  await flagFilter.fill('allow-database-inspect');
  const flagStatus = await page.locator('[data-ptah-filter-status]').innerText();
  if (!/^\d+ of \d+ entries$/.test(flagStatus) || flagStatus.startsWith('0 of ')) {
    problems.push('/reference/command-flags/: flag filter did not report matching entries');
  }

  return problems;
}

// The ecosystem map's behavior, against the declaration it is drawn from. An
// axe run reads the page at rest and cannot see whether selecting a component
// does anything, so these drive it the three ways a reader can arrive: keyboard
// on the drawing, touch on the narrow layout, and no JavaScript at all.
const ecosystemRoute = '/start/ecosystem/';
const expectedEdges = (id) =>
  relationships.filter(({ from, to }) => from === id || to === id).map((relationship) => relationship.id).sort();

async function ecosystemDrawingChecks(page, url) {
  const problems = [];
  await page.goto(url, { waitUntil: 'load' });
  const prefix = `${ecosystemRoute} [drawing]`;
  const map = page.locator('ptah-ecosystem-map');
  const drawn = page.locator('svg.eco-svg [data-eco-node]');
  if (await drawn.count() !== components.length) problems.push(`${prefix} draws ${await drawn.count()} of ${components.length} components`);
  if (!await page.locator('svg.eco-svg').isVisible()) problems.push(`${prefix} the drawing is not shown at desktop width`);
  if (await page.locator('[data-eco-cards]').isVisible()) problems.push(`${prefix} the narrow layout is shown at desktop width`);

  // Tab reaches a component, and the focus ring is visible on it.
  await page.locator('[data-eco-reset]').evaluate((reset) => {
    reset.disabled = false;
    reset.focus();
    reset.disabled = true;
  });
  await drawn.first().focus();
  await page.keyboard.press('Tab');
  const ring = await page.evaluate(() => {
    const active = document.activeElement;
    const focus = active?.querySelector?.('.eco-node__focus');
    return { id: active?.dataset?.ecoNode, stroke: focus ? getComputedStyle(focus).stroke : null };
  });
  if (!ring.id) problems.push(`${prefix} Tab did not move to the next component`);
  if (!ring.stroke || ring.stroke === 'transparent' || ring.stroke === 'rgba(0, 0, 0, 0)') problems.push(`${prefix} a focused component shows no focus ring (${ring.stroke})`);

  const id = 'ptah-operator';
  const node = page.locator(`svg.eco-svg [data-eco-node="${id}"]`);
  await node.focus();
  await page.keyboard.press('Enter');
  if (await node.getAttribute('aria-pressed') !== 'true') problems.push(`${prefix} Enter did not select ${id}`);
  const active = (await page.locator('[data-eco-edge].is-active').evaluateAll((edges) => edges.map((edge) => edge.dataset.ecoEdge))).sort();
  if (JSON.stringify(active) !== JSON.stringify(expectedEdges(id))) problems.push(`${prefix} highlighted ${active} for ${id}, want ${expectedEdges(id)}`);
  const labels = await page.locator('[data-eco-edge-label].is-active').count();
  if (labels !== expectedEdges(id).length) problems.push(`${prefix} ${labels} relationship labels stayed highlighted, want ${expectedEdges(id).length}`);
  if (!await page.locator(`[data-eco-detail="${id}"]`).isVisible()) problems.push(`${prefix} the details panel does not show ${id}`);
  const shown = await page.locator('[data-eco-details] .eco-detail:visible').count();
  if (shown !== 1) problems.push(`${prefix} the details panel shows ${shown} sections, want 1`);
  const links = await page.locator(`[data-eco-detail="${id}"] a[href*="github.com/stokaro/${id}"]`).count();
  if (links !== 1) problems.push(`${prefix} the details panel has ${links} repository links for ${id}, want 1`);
  const status = await page.locator('[data-eco-status]').innerText();
  if (!status.includes(id)) problems.push(`${prefix} the selection was not announced: ${JSON.stringify(status)}`);
  if (new URL(page.url()).pathname !== new URL(url).pathname) problems.push(`${prefix} selecting a component navigated away`);

  // A related component can be reached from the panel, and focus survives the
  // section it was in being hidden.
  await page.locator(`[data-eco-detail="${id}"] [data-eco-select="ptah"]`).click();
  if (await page.locator('svg.eco-svg [data-eco-node="ptah"]').getAttribute('aria-pressed') !== 'true') problems.push(`${prefix} the panel did not select ptah`);
  const focusInPanel = await page.evaluate(() => document.activeElement?.id);
  if (focusInPanel !== 'ptah-ecosystem-detail-ptah') problems.push(`${prefix} focus went to ${focusInPanel} after selecting from the panel`);

  // Escape clears, from inside the panel, and returns focus to the map.
  await page.keyboard.press('Escape');
  if (await map.getAttribute('data-selection') !== null) problems.push(`${prefix} Escape did not clear the selection`);
  const afterEscape = await page.evaluate(() => document.activeElement?.dataset?.ecoNode);
  if (afterEscape !== 'ptah') problems.push(`${prefix} focus went to ${afterEscape} after Escape`);

  // A usage path highlights the whole path, and Show all restores the map.
  const path = page.locator('[data-eco-path="kubernetes"]');
  await path.click();
  if (await path.getAttribute('aria-pressed') !== 'true') problems.push(`${prefix} the Kubernetes path was not selected`);
  if (await page.locator('[data-eco-edge].is-active').count() === 0) problems.push(`${prefix} the Kubernetes path highlighted nothing`);
  if (!await page.locator('[data-eco-path-detail="kubernetes"]').isVisible()) problems.push(`${prefix} the panel does not show the Kubernetes path`);
  const reset = page.locator('[data-eco-reset]');
  await reset.click();
  if (await page.locator('[data-eco-edge].is-active, .eco-node[aria-pressed="true"], .eco-node.is-related').count() !== 0) problems.push(`${prefix} Show all left something highlighted`);
  if (!await page.locator('[data-eco-intro]').isVisible()) problems.push(`${prefix} Show all did not restore the panel`);
  if (await page.evaluate(() => document.activeElement === document.body)) problems.push(`${prefix} Show all dropped focus to the page`);

  // Full size and Download follow the theme and resolve.
  const figure = page.locator('[data-visual-id="ptah-ecosystem"]');
  for (const theme of ['light', 'dark']) {
    await page.evaluate((value) => { document.documentElement.dataset.theme = value; }, theme);
    await page.waitForFunction(
      (value) => document.querySelector('[data-preview-action="full-size"][data-eco-themed]').href.includes(`-${value}.svg`),
      theme,
      { timeout: 2_000 },
    ).catch(() => problems.push(`${prefix} Full size does not follow the ${theme} theme`));
    for (const action of ['full-size', 'download', 'source']) {
      const href = await figure.locator(`[data-preview-action="${action}"]`).evaluate((link) => link.href);
      const response = await page.request.get(href);
      if (!response.ok()) problems.push(`${prefix} ${action} ${href} returned ${response.status()}`);
    }
  }
  return problems;
}

async function ecosystemTouchChecks(browser, url) {
  const problems = [];
  const prefix = `${ecosystemRoute} [touch, 390px]`;
  const context = await browser.newContext({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });
  const page = await context.newPage();
  await page.goto(url, { waitUntil: 'load' });
  if (await page.locator('svg.eco-svg').isVisible()) problems.push(`${prefix} the drawing is shown at phone width`);
  const cards = page.locator('[data-eco-cards]');
  if (!await cards.isVisible()) problems.push(`${prefix} the narrow layout is not shown`);
  // Each relationship is written on the card it leaves, with nothing selected.
  const lines = await page.locator('[data-eco-line]:visible').count();
  if (lines !== relationships.length) problems.push(`${prefix} ${lines} of ${relationships.length} relationships are written out at rest`);
  const card = page.locator('.eco-card[data-eco-node="homebrew-ptah"]');
  await card.tap();
  if (await card.getAttribute('aria-pressed') !== 'true') problems.push(`${prefix} a tap did not select homebrew-ptah`);
  // Visibility first: innerText reads the text of a hidden element too.
  const note = page.locator('.eco-card[data-eco-node="ptah"] [data-eco-note]');
  if (!await note.isVisible() || !(await note.innerText()).includes('homebrew-ptah')) {
    problems.push(`${prefix} the related card does not say how it is related`);
  }
  const geometry = await page.evaluate(() => ({
    cardsBottom: document.querySelector('[data-eco-cards]').getBoundingClientRect().bottom,
    detailsTop: document.querySelector('[data-eco-details]').getBoundingClientRect().top,
    overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
  }));
  if (geometry.detailsTop < geometry.cardsBottom) problems.push(`${prefix} the details are not below the map`);
  if (geometry.overflow > 1) problems.push(`${prefix} the page overflows by ${geometry.overflow}px after a selection`);
  if (!await page.locator('[data-eco-detail="homebrew-ptah"]').isVisible()) problems.push(`${prefix} the details panel does not show homebrew-ptah`);
  await context.close();
  return problems;
}

async function ecosystemNoScriptChecks(browser, url) {
  const problems = [];
  for (const viewport of viewports) {
    const prefix = `${ecosystemRoute} [no JavaScript, ${viewport.name}]`;
    const context = await browser.newContext({ viewport, javaScriptEnabled: false });
    const page = await context.newPage();
    await page.goto(url, { waitUntil: 'load' });
    const diagram = page.locator(viewport.name === 'mobile' ? '[data-eco-cards]' : 'svg.eco-svg');
    if (!await diagram.isVisible()) problems.push(`${prefix} no readable diagram is shown`);
    for (const selector of ['[data-eco-toolbar]', '[data-eco-details]']) {
      if (await page.locator(selector).isVisible()) problems.push(`${prefix} ${selector} is shown with nothing to drive it`);
    }
    if (await page.locator('[data-eco-node][role="button"]').count() !== 0) problems.push(`${prefix} a component claims to be a button`);
    const article = page.locator('.sl-markdown-content');
    for (const component of components.filter(({ kind }) => kind === 'repository')) {
      const repository = component.links.find(({ label }) => label === 'Repository').href;
      const row = article.locator(`.eco-table tr:has(a[href="${repository}"])`);
      if (await row.count() !== 1) problems.push(`${prefix} the repository table has ${await row.count()} rows for ${component.name}`);
    }
    await context.close();
  }
  return problems;
}

async function ecosystemSelectedAxe(browser, origin, base) {
  const problems = [];
  for (const scheme of colorSchemes) {
    for (const viewport of viewports) {
      const context = await browser.newContext({ viewport, colorScheme: scheme });
      const page = await context.newPage();
      await page.goto(`${origin}${base}${ecosystemRoute}`, { waitUntil: 'load' });
      await page.locator('[data-eco-node="ptah-operator"]:visible').click();
      const violations = await axeViolations(page);
      problems.push(...violations.map((violation) =>
        formatViolation(`${ecosystemRoute} with a selection`, viewport.name, scheme, 'any', violation)));
      await context.close();
    }
  }
  return problems;
}

// The YDB walkthrough's behavior, against the declaration it is rendered from
// (src/ydb-walkthrough.mjs): what a step shows under a scenario is compared
// with the result the declaration states, so a figure that shows the wrong
// result fails here, not only one that shows nothing.
const walkthroughRoute = '/extend/write-a-provider/ydb-walkthrough-explore/';

async function walkthroughKeyboardChecks(page, url) {
  const problems = [];
  const prefix = `${walkthroughRoute} [keyboard]`;
  await page.goto(url, { waitUntil: 'load' });
  const shown = () => page.locator('[data-ydbw-details] .ydbw-detail:visible');
  if (await shown().count() !== 1) problems.push(`${prefix} ${await shown().count()} details sections are shown at rest`);
  const firstStep = `step:${scenarios[0].id}:${steps[0].id}`;
  if (await shown().getAttribute('data-ydbw-detail') !== firstStep) problems.push(`${prefix} the panel does not start at ${firstStep}`);

  const focused = () => page.evaluate(() => document.activeElement?.id || document.activeElement?.dataset?.ydbwSelect || '');
  const core = page.locator('[data-ydbw-node="core"]');
  await core.focus();
  await page.keyboard.press('Enter');
  if (await core.getAttribute('aria-pressed') !== 'true') problems.push(`${prefix} Enter did not select Ptah Core`);
  if (await shown().getAttribute('data-ydbw-detail') !== 'node:core') problems.push(`${prefix} the panel does not show Ptah Core`);
  if (await focused() !== 'ydbw-node-core') problems.push(`${prefix} focus went to ${await focused()}, not the Ptah Core details`);
  if (!(await page.locator('[data-ydbw-status]').innerText()).includes('Ptah Core')) problems.push(`${prefix} the status does not name Ptah Core`);
  await page.keyboard.press('Escape');
  if (await core.getAttribute('aria-pressed') !== 'false') problems.push(`${prefix} Escape left Ptah Core selected`);
  if (await shown().getAttribute('data-ydbw-detail') !== firstStep) problems.push(`${prefix} Escape did not return to the step`);
  if (await focused() !== 'node:core') problems.push(`${prefix} Escape left focus on ${await focused()}, not Ptah Core`);

  const edge = page.locator('[data-ydbw-edge="needs-source"]');
  await edge.focus();
  await page.keyboard.press(' ');
  if (await shown().getAttribute('data-ydbw-detail') !== 'edge:needs-source') problems.push(`${prefix} Space did not show a connection`);
  await page.keyboard.press('Escape');
  if (await edge.getAttribute('aria-pressed') !== 'false') problems.push(`${prefix} Escape left the connection selected`);

  const reading = steps.findIndex(({ id }) => id === 'reading');
  for (let index = 0; index < reading; index += 1) await page.locator('[data-ydbw-next]').click();
  const label = await page.locator('[data-ydbw-step-label]').innerText();
  if (label !== `Step ${reading + 1} of ${steps.length}: ${steps[reading].label}`) problems.push(`${prefix} the step label reads ${label}`);
  for (const component of ['core', 'contract', 'provider', 'ydb']) {
    const badge = page.locator(`[data-ydbw-node="${component}"] [data-ydbw-badge]`);
    if (await badge.isVisible() !== steps[reading].active.includes(component)) {
      problems.push(`${prefix} the active badge of ${component} does not match step ${steps[reading].id}`);
    }
  }
  if (!await page.locator('[data-ydbw-edge="call-inspect"] [data-ydbw-badge]').isVisible()) problems.push(`${prefix} Inspect is not marked active while reading`);

  for (const scenario of scenarios) {
    await page.locator(`[data-ydbw-scenario="${scenario.id}"]`).click();
    if (await page.locator(`[data-ydbw-scenario="${scenario.id}"]`).getAttribute('aria-pressed') !== 'true') {
      problems.push(`${prefix} the ${scenario.id} scenario button is not pressed`);
    }
    // A scenario keeps the current step, so rewind to the first one and walk
    // forward through every step once.
    while (await page.locator('[data-ydbw-prev]').isEnabled()) await page.locator('[data-ydbw-prev]').click();
    for (const [index, step] of steps.entries()) {
      const text = await shown().innerText();
      if (!text.includes(stepResult(scenario, step))) problems.push(`${prefix} ${scenario.id}/${step.id} does not show its result`);
      if (index < steps.length - 1) await page.locator('[data-ydbw-next]').click();
    }
  }
  if (!await page.locator('[data-ydbw-prev]').isEnabled() || await page.locator('[data-ydbw-next]').isEnabled()) {
    problems.push(`${prefix} at the last step Previous should be enabled and Next disabled`);
  }
  return problems;
}

async function walkthroughTouchChecks(browser, url) {
  const problems = [];
  const prefix = `${walkthroughRoute} [touch, 390px]`;
  const context = await browser.newContext({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });
  const page = await context.newPage();
  await page.goto(url, { waitUntil: 'load' });
  const feed = page.locator('[data-ydbw-node="feed"]');
  await feed.tap();
  if (await feed.getAttribute('aria-pressed') !== 'true') problems.push(`${prefix} a tap did not select orders/feed`);
  if (!await page.locator('[data-ydbw-detail="node:feed"]').isVisible()) problems.push(`${prefix} the panel does not show orders/feed`);
  // The panel sits above the diagram, so a tap on an object brings it into
  // view. The site scrolls smoothly, so wait for the scroll to arrive.
  const inView = await page.waitForFunction(() => {
    const { top } = document.querySelector('[data-ydbw-detail="node:feed"]').getBoundingClientRect();
    return top >= 0 && top <= window.innerHeight;
  }, null, { timeout: 3000 }).then(() => true, () => false);
  if (!inView) problems.push(`${prefix} the details are not in view after a tap`);
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  if (overflow > 1) problems.push(`${prefix} the page overflows by ${overflow}px after a selection`);
  await context.close();
  return problems;
}

async function walkthroughNoScriptChecks(browser, url) {
  const problems = [];
  for (const viewport of viewports) {
    const prefix = `${walkthroughRoute} [no JavaScript, ${viewport.name}]`;
    const context = await browser.newContext({ viewport, javaScriptEnabled: false });
    const page = await context.newPage();
    await page.goto(url, { waitUntil: 'load' });
    if (!await page.locator('.ydbw-diagram').isVisible()) problems.push(`${prefix} no readable diagram is shown`);
    for (const selector of ['[data-ydbw-toolbar]', '[data-ydbw-details]']) {
      if (await page.locator(selector).isVisible()) problems.push(`${prefix} ${selector} is shown with nothing to drive it`);
    }
    if (await page.locator('[data-ydbw-select][role="button"]').count() !== 0) problems.push(`${prefix} an element claims to be a button`);
    const article = page.locator('.sl-markdown-content');
    for (const scenario of scenarios) {
      const rows = await article.locator(`table tr:has(td:text-is("${scenario.label}"))`).count();
      if (rows === 0) problems.push(`${prefix} the text has no row for the ${scenario.id} scenario`);
    }
    await context.close();
  }
  return problems;
}

async function walkthroughSelectedAxe(browser, origin, base) {
  const problems = [];
  for (const scheme of colorSchemes) {
    for (const viewport of viewports) {
      const context = await browser.newContext({ viewport, colorScheme: scheme });
      const page = await context.newPage();
      await page.goto(`${origin}${base}${walkthroughRoute}`, { waitUntil: 'load' });
      await page.locator('[data-ydbw-node="transfer"]').click();
      await page.locator('[data-ydbw-scenario="missing"]').click();
      await page.locator('[data-ydbw-next]').click();
      const violations = await axeViolations(page);
      problems.push(...violations.map((violation) =>
        formatViolation(`${walkthroughRoute} with a selection`, viewport.name, scheme, 'any', violation)));
      await context.close();
    }
  }
  return problems;
}

async function main() {
  const chromium = await loadChromium('check-accessibility.mjs');
  if (!chromium) return;
  if (process.argv.includes('--selftest')) {
    await selftest(chromium);
    return;
  }

  const distIndex = process.argv.indexOf('--dist');
  const distRoot = join(siteRoot, distIndex === -1 ? 'dist' : process.argv[distIndex + 1]);
  if (!existsSync(distRoot)) {
    console.error(`check-accessibility.mjs: ${relative(siteRoot, distRoot)} not found; run "npm run build" first.`);
    process.exitCode = 1;
    return;
  }

  const built = await startBuiltSite(distRoot);
  const origin = `http://127.0.0.1:${built.port}`;
  const browser = await chromium.launch();
  const problems = [];
  try {
    for (const platform of platforms) {
      for (const scheme of colorSchemes) {
        for (const viewport of viewports) {
          const context = await browser.newContext({ viewport, colorScheme: scheme });
          await context.addInitScript((value) => {
            Object.defineProperty(Navigator.prototype, 'platform', { get: () => value });
          }, platform.navigatorPlatform);
          const page = await context.newPage();
          for (const route of routes) {
            await page.goto(`${origin}${built.base}${route}`, { waitUntil: 'load' });
            await codeBlocksSettled(page);
            const violations = await axeViolations(page);
            problems.push(...violations.map((violation) =>
              formatViolation(route, viewport.name, scheme, platform.name, violation)));
          }
          await context.close();
        }
      }
    }

    const keyboardPage = await browser.newPage({ viewport: viewports[1] });
    problems.push(...await keyboardChecks(keyboardPage, origin, built.base));
    const ecosystemUrl = `${origin}${built.base}${ecosystemRoute}`;
    problems.push(...await ecosystemDrawingChecks(keyboardPage, ecosystemUrl));
    await keyboardPage.close();
    problems.push(...await ecosystemTouchChecks(browser, ecosystemUrl));
    problems.push(...await ecosystemNoScriptChecks(browser, ecosystemUrl));
    problems.push(...await ecosystemSelectedAxe(browser, origin, built.base));
    const walkthroughUrl = `${origin}${built.base}${walkthroughRoute}`;
    const walkthroughPage = await browser.newPage({ viewport: viewports[1] });
    problems.push(...await walkthroughKeyboardChecks(walkthroughPage, walkthroughUrl));
    await walkthroughPage.close();
    problems.push(...await walkthroughTouchChecks(browser, walkthroughUrl));
    problems.push(...await walkthroughNoScriptChecks(browser, walkthroughUrl));
    problems.push(...await walkthroughSelectedAxe(browser, origin, built.base));
  } finally {
    await browser.close();
    await new Promise((resolve) => built.server.close(resolve));
  }

  if (problems.length > 0) {
    console.error('check-accessibility.mjs: FAILED');
    for (const problem of problems) console.error(`- ${problem}`);
    process.exitCode = 1;
    return;
  }
  console.log(
    `check-accessibility.mjs: OK (${routes.length} routes x ${viewports.length} viewports x ` +
    `${colorSchemes.join(' and ')} x ${platforms.map((platform) => platform.name).join(' and ')} shortcut hints ` +
    'plus keyboard controls, and the ecosystem map and the YDB walkthrough by keyboard, touch and without JavaScript)',
  );
}

await main();
