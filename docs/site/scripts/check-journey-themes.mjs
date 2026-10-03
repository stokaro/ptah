#!/usr/bin/env node
// Exercise the site toggle against the opposite OS preference in both directions.
import assert from 'node:assert/strict';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { startBuiltSite } from './lib/built-site.mjs';

function assertDiagram(reading) {
  assert.equal(reading.siteTheme, reading.theme, 'site theme');
  assert.ok(reading.src.includes(`-${reading.theme}.`), 'diagram theme');
  assert.ok(reading.complete && reading.naturalWidth > 0, 'loaded diagram');
  for (const action of ['full-size', 'full-size-media', 'download']) {
    assert.equal(reading.links[action], reading.src, `${action} theme`);
  }
  assert.ok(reading.sourceOK, 'source response');
  assert.match(reading.sourceText, /^flowchart /, 'Mermaid source');
}

function selftest() {
  const valid = {
    theme: 'dark', siteTheme: 'dark', src: 'https://example.test/diagram-dark.svg',
    complete: true, naturalWidth: 1000,
    links: Object.fromEntries(['full-size', 'full-size-media', 'download'].map((action) => [action, 'https://example.test/diagram-dark.svg'])),
    sourceOK: true, sourceText: 'flowchart LR\n  a --> b',
  };
  assert.doesNotThrow(() => assertDiagram(valid));
  for (const change of [
    { siteTheme: 'light' },
    { src: 'https://example.test/diagram-light.svg', links: Object.fromEntries(Object.keys(valid.links).map((action) => [action, 'https://example.test/diagram-light.svg'])) },
    { complete: false }, { naturalWidth: 0 },
    { sourceOK: false }, { sourceText: '<svg></svg>' },
    ...['full-size', 'full-size-media', 'download'].map((action) => ({ links: { ...valid.links, [action]: 'https://example.test/diagram-light.svg' } })),
  ]) assert.throws(() => assertDiagram({ ...valid, ...change }));
  console.log('Journey themes selftest: OK (wrong themes, unloaded images, mismatched actions, and invalid sources rejected).');
}

async function main() {
  const { chromium } = await import('playwright');
  const siteRoot = join(dirname(fileURLToPath(import.meta.url)), '..');
  const { server, port, base } = await startBuiltSite(join(siteRoot, 'dist'));
  const browser = await chromium.launch();
  try {
    for (const system of ['light', 'dark']) {
      const context = await browser.newContext({ colorScheme: system });
      for (const route of ['/', '/inference/overview/']) {
        const page = await context.newPage();
        await page.goto(`http://127.0.0.1:${port}${base}${route}`);
        const figure = page.locator('[data-site-themed]');
        await figure.waitFor();
        for (const theme of ['light', 'dark', 'light']) {
          const current = await page.evaluate(() => document.documentElement.dataset.theme);
          if (current !== theme) await page.locator('starlight-theme-toggle button:visible').first().click();
          await page.waitForFunction((theme) => {
            const figure = document.querySelector('[data-site-themed]');
            const image = figure.querySelector('img');
            return document.documentElement.dataset.theme === theme
              && image.src.includes(`-${theme}.`)
              && image.complete && image.naturalWidth > 0;
          }, theme);
          const image = figure.locator('img');
          const src = new URL(await image.getAttribute('src'), page.url()).href;
          const links = {};
          for (const action of ['full-size', 'full-size-media', 'download']) {
            const href = await figure.locator(`[data-preview-action="${action}"]`).getAttribute('href');
            links[action] = new URL(href, page.url()).href;
          }
          const source = await figure.locator('[data-preview-action="source"]').getAttribute('href');
          const response = await page.request.get(new URL(source, page.url()).href);
          assertDiagram({
            theme, src, links,
            siteTheme: await page.evaluate(() => document.documentElement.dataset.theme),
            ...await image.evaluate((image) => ({ complete: image.complete, naturalWidth: image.naturalWidth })),
            sourceOK: response.ok(), sourceText: await response.text(),
          });
        }
        await page.close();
      }
      await context.close();
    }
    console.log('Journey themes: OK (both pages, both OS preferences, toggle, links, Mermaid source).');
  } finally {
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
}
if (process.argv.includes('--selftest')) selftest();
else await main();
