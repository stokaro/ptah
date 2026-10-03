#!/usr/bin/env node
// Exercise the site toggle against the opposite OS preference in both directions.
import assert from 'node:assert/strict';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { startBuiltSite } from './lib/built-site.mjs';

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
        const src = await image.getAttribute('src');
        for (const action of ['full-size', 'full-size-media', 'download']) {
          const href = await figure.locator(`[data-preview-action="${action}"]`).getAttribute('href');
          assert.equal(new URL(href, page.url()).href, new URL(src, page.url()).href);
        }
        const source = await figure.locator('[data-preview-action="source"]').getAttribute('href');
        const response = await page.request.get(new URL(source, page.url()).href);
        assert.ok(response.ok());
        assert.match(await response.text(), /^flowchart /);
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
