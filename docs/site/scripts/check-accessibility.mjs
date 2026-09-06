#!/usr/bin/env node
// Runs WCAG axe checks on the canonical page shapes and exercises keyboard
// interaction that static markup checks cannot establish.
import { existsSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import AxeBuilder from '@axe-core/playwright';
import { loadChromium, startBuiltSite } from './lib/built-site.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const routes = [
  '/',
  '/start/quick-start/',
  '/databases/support-matrix/',
  '/reference/native-commands/',
  '/schema/document/',
  '/schema/serve/',
];
const viewports = [
  { name: 'mobile', width: 390, height: 844 },
  { name: 'desktop', width: 1280, height: 900 },
];
const wcagTags = ['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'];
// Both, because the site ships both and the palettes are separate declarations.
// This ran in the default context once, which resolves to the light theme, so
// every dark color the design introduced was unmeasured while the code and the
// README said otherwise (stokaro/ptah#2930).
const colorSchemes = ['light', 'dark'];

function formatViolation(route, viewport, scheme, violation) {
  const targets = violation.nodes.slice(0, 3).map((node) => node.target.join(' ')).join(', ');
  return `${route} [${viewport}, ${scheme}] ${violation.id} (${violation.impact ?? 'unknown'}): ${targets}`;
}

async function axeViolations(page) {
  const { violations } = await new AxeBuilder({ page }).withTags(wcagTags).analyze();
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
  const sidebarSummary = page.locator('nav.sidebar summary', { hasText: 'Define and understand schemas' }).first();
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

  await page.goto(`${origin}${base}/reference/capabilities/`, { waitUntil: 'load' });
  const glossary = page.locator('.ptah-glossary-trigger').first();
  if (await glossary.count() !== 1) {
    problems.push('/reference/capabilities/: glossary trigger is absent');
  } else {
    await glossary.focus();
    await page.keyboard.press('Enter');
    if (await page.locator('.ptah-glossary-panel:popover-open').count() !== 1) {
      problems.push('/reference/capabilities/: Enter did not open the glossary definition');
    }
    await page.keyboard.press('Escape');
    if (await page.locator('.ptah-glossary-panel:popover-open').count() !== 0) {
      problems.push('/reference/capabilities/: Escape did not close the glossary definition');
    }
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
    for (const scheme of colorSchemes) {
      for (const viewport of viewports) {
        const context = await browser.newContext({ viewport, colorScheme: scheme });
        const page = await context.newPage();
        for (const route of routes) {
          await page.goto(`${origin}${built.base}${route}`, { waitUntil: 'load' });
          const violations = await axeViolations(page);
          problems.push(...violations.map((violation) => formatViolation(route, viewport.name, scheme, violation)));
        }
        await context.close();
      }
    }

    const keyboardPage = await browser.newPage({ viewport: viewports[1] });
    problems.push(...await keyboardChecks(keyboardPage, origin, built.base));
    await keyboardPage.close();
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
    `${colorSchemes.join(' and ')} plus keyboard controls)`,
  );
}

await main();
