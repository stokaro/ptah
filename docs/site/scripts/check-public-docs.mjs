#!/usr/bin/env node

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { validateBuildInfo } from './check-build-info.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const manifestPath = join(scriptDir, 'data', 'public-smoke.json');

export function manifestProblems(manifest) {
  const problems = [];
  if (!manifest || !Array.isArray(manifest.routes) || manifest.routes.length === 0) {
    return ['public smoke manifest must declare at least one route'];
  }
  const seen = new Set();
  for (const [index, route] of manifest.routes.entries()) {
    const where = `route ${index + 1}`;
    if (typeof route.path !== 'string' || route.path.startsWith('/')) problems.push(`${where}: path must be site-relative`);
    if (seen.has(route.path)) problems.push(`${where}: duplicate path ${JSON.stringify(route.path)}`);
    seen.add(route.path);
    if (typeof route.requiredText !== 'string' || route.requiredText.trim() === '') problems.push(`${where}: requiredText is missing`);
    for (const [visualIndex, visual] of (route.visuals ?? []).entries()) {
      if (typeof visual.selector !== 'string' || visual.selector.trim() === '') {
        problems.push(`${where} visual ${visualIndex + 1}: selector is missing`);
      }
    }
  }
  return problems;
}

function parseArguments(arguments_) {
  if (arguments_.includes('--selftest')) return { selftest: true };
  const value = (name) => {
    const index = arguments_.indexOf(name);
    return index === -1 ? undefined : arguments_[index + 1];
  };
  const baseUrl = value('--base-url');
  const expectedCommit = value('--expected-commit');
  if (!baseUrl || !expectedCommit || arguments_.length !== 4) {
    throw new Error('usage: node scripts/check-public-docs.mjs --base-url <url> --expected-commit <full-sha>');
  }
  return { baseUrl: new URL(baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`), expectedCommit };
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const valid = { routes: [{ path: '', requiredText: 'Ptah' }, { path: 'schema/visualize/', requiredText: 'Visualize', visuals: [{ selector: 'figure img' }] }] };
  assert(manifestProblems(valid).length === 0, 'valid manifest failed');
  assert(manifestProblems({ routes: [] }).some((problem) => problem.includes('at least one')), 'empty manifest passed');
  assert(
    manifestProblems({ routes: [{ path: '/absolute/', requiredText: 'Page', visuals: [{ selector: '' }] }] }).length === 2,
    'absolute route or empty selector passed',
  );
  console.log('check-public-docs.mjs --selftest: OK (route and visual manifest assertions)');
}

async function waitForBuild(baseUrl, expectedCommit) {
  const infoUrl = new URL('build-info.json', baseUrl);
  let lastProblem = 'not requested';
  for (let attempt = 1; attempt <= 30; attempt += 1) {
    try {
      const attemptUrl = new URL(infoUrl);
      attemptUrl.searchParams.set('expected', expectedCommit);
      attemptUrl.searchParams.set('attempt', String(attempt));
      const response = await fetch(attemptUrl, { cache: 'no-store' });
      if (!response.ok) {
        lastProblem = `${infoUrl} returned HTTP ${response.status}`;
      } else {
        const value = await response.json();
        const problems = validateBuildInfo(value, {
          version: 'edge',
          commit: expectedCommit,
          sourceRef: 'master',
        });
        if (problems.length === 0) return value;
        lastProblem = problems.join('; ');
      }
    } catch (error) {
      lastProblem = error instanceof Error ? error.message : String(error);
    }
    if (attempt < 30) await delay(10_000);
  }
  throw new Error(`public edge did not expose the expected build after 30 attempts: ${lastProblem}`);
}

async function checkRoute(page, baseUrl, route, expectedCommit) {
  const url = new URL(route.path, baseUrl);
  url.searchParams.set('expected', expectedCommit);
  const response = await page.goto(url.href, { waitUntil: 'networkidle' });
  if (!response || response.status() !== 200) throw new Error(`${url} returned HTTP ${response?.status() ?? 'none'}`);
  const body = await page.locator('body').innerText();
  if (!body.includes(route.requiredText)) throw new Error(`${url} omitted ${JSON.stringify(route.requiredText)}`);

  for (const visual of route.visuals ?? []) {
    const locator = page.locator(visual.selector).first();
    await locator.waitFor({ state: 'visible' });
    const result = await locator.evaluate((element) => {
      if (!(element instanceof HTMLImageElement)) return { image: false, width: 0, height: 0, source: '' };
      return {
        image: true,
        width: element.naturalWidth,
        height: element.naturalHeight,
        source: element.currentSrc || element.src,
      };
    });
    if (!result.image || result.width <= 0 || result.height <= 0) {
      throw new Error(`${url} visual ${visual.selector} has no natural dimensions`);
    }
    const asset = await page.request.get(result.source);
    if (asset.status() !== 200) throw new Error(`${result.source} returned HTTP ${asset.status()}`);
  }
}

async function main() {
  let options;
  try {
    options = parseArguments(process.argv.slice(2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 2;
    return;
  }
  if (options.selftest) {
    selftest();
    return;
  }

  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  const problems = manifestProblems(manifest);
  if (problems.length > 0) throw new Error(problems.join('\n'));
  const info = await waitForBuild(options.baseUrl, options.expectedCommit);
  const { chromium } = await import('playwright');
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    for (const route of manifest.routes) await checkRoute(page, options.baseUrl, route, options.expectedCommit);
  } finally {
    await browser.close();
  }
  console.log(`public edge: OK (${info.source_commit}; ${manifest.routes.length} routes)`);
}

main().catch((error) => {
  console.error(`public edge: FAILED: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
});
