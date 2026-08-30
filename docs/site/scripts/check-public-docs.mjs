#!/usr/bin/env node

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { validateBuildInfo } from './check-build-info.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const manifestPath = join(scriptDir, 'data', 'public-smoke.json');
const requiredRoutes = [
  '',
  'inference/quick-start/',
  'schema/visualize/',
  'schema/document/',
  'schema/serve/',
  'schema/security/',
  'schema/lineage/',
  'versioned/generate/',
  'testing/migrations-and-schema/',
];
const previewActions = ['full-size', 'download', 'source'];
const previewActionSet = new Set(previewActions);
const inferenceArtifactContracts = new Map([
  [
    'a[data-ptah-inference-archive][download]',
    { expectedPath: 'samples/inference-quick-start.zip', format: 'zip' },
  ],
  [
    'a[data-ptah-inference-checksum][download]',
    { expectedPath: 'samples/inference-quick-start.zip.sha256', format: 'sha256' },
  ],
]);
const artifactFormats = new Set(['zip', 'sha256']);

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
      if (!Array.isArray(visual.requiredActions)) {
        problems.push(`${where} visual ${visualIndex + 1}: requiredActions must declare the full preview action set`);
      } else {
        for (const action of visual.requiredActions) {
          if (!previewActionSet.has(action)) {
            problems.push(`${where} visual ${visualIndex + 1}: unknown action ${JSON.stringify(action)}`);
          }
        }
        const declared = new Set(visual.requiredActions);
        if (
          declared.size !== visual.requiredActions.length ||
          previewActions.some((action) => !declared.has(action))
        ) {
          problems.push(
            `${where} visual ${visualIndex + 1}: requiredActions must be exactly ${previewActions.join(', ')}`,
          );
        }
      }
      if (visual.checkLinks !== true) {
        problems.push(`${where} visual ${visualIndex + 1}: checkLinks must be true`);
      }
    }
    for (const [artifactIndex, artifact] of (route.artifacts ?? []).entries()) {
      if (typeof artifact.selector !== 'string' || artifact.selector.trim() === '') {
        problems.push(`${where} artifact ${artifactIndex + 1}: selector is missing`);
      }
      if (
        artifact.expectedPath !== undefined &&
        (typeof artifact.expectedPath !== 'string' || artifact.expectedPath.startsWith('/') || artifact.expectedPath.trim() === '')
      ) {
        problems.push(`${where} artifact ${artifactIndex + 1}: expectedPath must be site-relative`);
      }
      if (artifact.format !== undefined && !artifactFormats.has(artifact.format)) {
        problems.push(`${where} artifact ${artifactIndex + 1}: unknown format ${JSON.stringify(artifact.format)}`);
      }
    }
  }
  for (const required of requiredRoutes) {
    if (!seen.has(required)) problems.push(`required public route is missing: ${required || '(home)'}`);
  }
  const inference = manifest.routes.find(({ path }) => path === 'inference/quick-start/');
  const inferenceArtifacts = new Map((inference?.artifacts ?? []).map((artifact) => [artifact.selector, artifact]));
  for (const [selector, contract] of inferenceArtifactContracts) {
    const artifact = inferenceArtifacts.get(selector);
    if (!artifact) {
      problems.push(`inference quick start is missing ${selector}`);
      continue;
    }
    if (artifact.expectedPath !== contract.expectedPath || artifact.format !== contract.format) {
      problems.push(
        `inference quick start ${selector} must use ${contract.expectedPath} as ${contract.format}`,
      );
    }
  }
  return problems;
}

export function artifactResponseProblems({
  href,
  pageUrl,
  baseUrl,
  expectedPath,
  format,
  status,
  content,
  contentType = '',
}) {
  if (typeof href !== 'string' || href.trim() === '') return ['artifact href is missing'];
  const url = new URL(href, pageUrl);
  const problems = [];
  if (expectedPath) {
    const expectedUrl = new URL(expectedPath, baseUrl);
    if (url.pathname !== expectedUrl.pathname) {
      problems.push(`artifact path is ${url.pathname}, want ${expectedUrl.pathname}`);
    }
  }
  if (status !== 200) return [...problems, `artifact returned HTTP ${status}`];
  const body = Buffer.from(content ?? []);
  if (body.length === 0) return [...problems, 'artifact is empty'];
  if (format === 'zip') {
    if (body.length < 4 || !body.subarray(0, 4).equals(Buffer.from([0x50, 0x4b, 0x03, 0x04]))) {
      problems.push('artifact is not a ZIP archive');
    }
  } else if (format === 'sha256') {
    if (!/^[0-9a-f]{64}  inference-quick-start\.zip\r?\n?$/.test(body.toString('utf8'))) {
      problems.push('artifact is not a checksum for inference-quick-start.zip');
    }
  } else if (url.pathname.endsWith('.html')) {
    const html = body.toString('utf8');
    if (!contentType.includes('text/html') || !/<(?:!doctype\s+html|html)[\s>]/i.test(html)) {
      problems.push('artifact is not generated HTML');
    }
  } else if (url.pathname.endsWith('.json')) {
    try {
      JSON.parse(body.toString('utf8'));
    } catch {
      problems.push('artifact is not valid JSON');
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
  const valid = {
    routes: requiredRoutes.map((path) => ({
      path,
      requiredText: path || 'Ptah',
      visuals: [{
        selector: '[data-visual-id="proof"]',
        requiredActions: ['full-size', 'download', 'source'],
        checkLinks: true,
      }],
      ...(path === 'inference/quick-start/' ? {
        artifacts: [...inferenceArtifactContracts].map(([selector, contract]) => ({ selector, ...contract })),
      } : {}),
    })),
  };
  assert(manifestProblems(valid).length === 0, 'valid manifest failed');
  assert(manifestProblems({ routes: [] }).some((problem) => problem.includes('at least one')), 'empty manifest passed');
  assert(
    manifestProblems({ routes: [{ path: '/absolute/', requiredText: 'Page', visuals: [{ selector: '' }] }] })
      .some((problem) => problem.includes('requiredActions')),
    'visual without an action contract passed',
  );
  const partialActions = structuredClone(valid);
  partialActions.routes[0].visuals[0].requiredActions = ['full-size'];
  assert(
    manifestProblems(partialActions).some((problem) => problem.includes('must be exactly')),
    'partial preview action contract passed',
  );
  const duplicateActions = structuredClone(valid);
  duplicateActions.routes[0].visuals[0].requiredActions = ['full-size', 'download', 'source', 'source'];
  assert(
    manifestProblems(duplicateActions).some((problem) => problem.includes('must be exactly')),
    'duplicate preview action contract passed',
  );
  const swappedInference = structuredClone(valid);
  const inferenceArtifacts = swappedInference.routes.find(({ path }) => path === 'inference/quick-start/').artifacts;
  [inferenceArtifacts[0].expectedPath, inferenceArtifacts[1].expectedPath] = [
    inferenceArtifacts[1].expectedPath,
    inferenceArtifacts[0].expectedPath,
  ];
  assert(
    manifestProblems(swappedInference).some((problem) => problem.includes('must use')),
    'swapped inference archive/checksum links passed',
  );
  assert(
    artifactResponseProblems({
      href: '/ptah/edge/samples/inference-quick-start.zip',
      pageUrl: 'https://example.test/ptah/edge/inference/quick-start/',
      baseUrl: 'https://example.test/ptah/edge/',
      expectedPath: 'samples/inference-quick-start.zip',
      format: 'zip',
      status: 200,
      content: Buffer.from([0x50, 0x4b, 0x03, 0x04]),
    }).length === 0,
    'valid ZIP response failed',
  );
  assert(
    artifactResponseProblems({
      href: '/ptah/edge/samples/inference-quick-start.zip.sha256',
      pageUrl: 'https://example.test/ptah/edge/inference/quick-start/',
      baseUrl: 'https://example.test/ptah/edge/',
      expectedPath: 'samples/inference-quick-start.zip.sha256',
      format: 'sha256',
      status: 200,
      content: Buffer.from('not a checksum\n'),
    }).some((problem) => problem.includes('not a checksum')),
    'invalid checksum response passed',
  );
  const incomplete = structuredClone(valid);
  incomplete.routes = incomplete.routes.filter(({ path }) => path !== 'schema/lineage/');
  assert(
    manifestProblems(incomplete).some((problem) => problem.includes('schema/lineage/')),
    'missing required route passed',
  );
  console.log('check-public-docs.mjs --selftest: OK (routes, stable visual selectors, and artifact actions)');
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

async function checkArtifact(request, href, pageUrl, label, contract = {}) {
  if (typeof href !== 'string' || href.trim() === '') {
    throw new Error(`${pageUrl} omitted the href for ${label}`);
  }
  const url = new URL(href, pageUrl);
  const response = await request.get(url.href, { headers: { 'Cache-Control': 'no-cache' } });
  const content = await response.body();
  const problems = artifactResponseProblems({
    href,
    pageUrl,
    status: response.status(),
    content,
    contentType: response.headers()['content-type'] ?? '',
    ...contract,
  });
  if (problems.length > 0) {
    throw new Error(`${url} failed ${label}: ${problems.join('; ')}`);
  }
}

async function checkRoute(page, baseUrl, route, expectedCommit) {
  const url = new URL(route.path, baseUrl);
  url.searchParams.set('expected', expectedCommit);
  const response = await page.goto(url.href, { waitUntil: 'networkidle' });
  if (!response || response.status() !== 200) throw new Error(`${url} returned HTTP ${response?.status() ?? 'none'}`);
  const body = await page.locator('body').innerText();
  if (!body.includes(route.requiredText)) throw new Error(`${url} omitted ${JSON.stringify(route.requiredText)}`);

  for (const visual of route.visuals ?? []) {
    const preview = page.locator(visual.selector).first();
    await preview.waitFor({ state: 'visible' });
    const image = preview.locator('img').first();
    await image.waitFor({ state: 'visible' });
    const result = await image.evaluate((element) => {
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
    await checkArtifact(page.request, result.source, url, `${visual.selector} image`);

    for (const action of visual.requiredActions) {
      const actionLink = preview.locator(`[data-preview-action="${action}"]`).first();
      if (await actionLink.count() !== 1) throw new Error(`${url} visual ${visual.selector} omitted ${action}`);
    }
    if (visual.checkLinks) {
      const hrefs = new Set(await preview.locator('a[href]').evaluateAll((links) =>
        links.map((link) => link.getAttribute('href')).filter(Boolean)));
      for (const href of hrefs) await checkArtifact(page.request, href, url, `${visual.selector} action`);
    }
  }

  for (const artifact of route.artifacts ?? []) {
    const link = page.locator(artifact.selector).first();
    if (await link.count() !== 1) throw new Error(`${url} omitted artifact ${artifact.selector}`);
    await checkArtifact(page.request, await link.getAttribute('href'), url, artifact.selector, {
      baseUrl,
      expectedPath: artifact.expectedPath,
      format: artifact.format,
    });
  }
}

async function assertBuildStillCurrent(baseUrl, expectedCommit) {
  const infoUrl = new URL('build-info.json', baseUrl);
  infoUrl.searchParams.set('expected', expectedCommit);
  infoUrl.searchParams.set('final', 'true');
  const response = await fetch(infoUrl, { cache: 'no-store' });
  if (!response.ok) throw new Error(`${infoUrl} returned HTTP ${response.status} after route verification`);
  const value = await response.json();
  const problems = validateBuildInfo(value, { version: 'edge', commit: expectedCommit, sourceRef: 'master' });
  if (problems.length > 0) throw new Error(`public edge changed during verification: ${problems.join('; ')}`);
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
    await assertBuildStillCurrent(options.baseUrl, options.expectedCommit);
  } finally {
    await browser.close();
  }
  console.log(`public edge: OK (${info.source_commit}; ${manifest.routes.length} routes)`);
}

main().catch((error) => {
  console.error(`public edge: FAILED: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
});
