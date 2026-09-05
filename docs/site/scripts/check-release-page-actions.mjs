#!/usr/bin/env node

import { existsSync, readFileSync } from 'node:fs';
import { PageURL } from '../src/lib/docs-origin.mjs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { validateBuildInfo } from './check-build-info.mjs';
import { loadChromium, startBuiltSite } from './lib/built-site.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = resolve(scriptDir, '..');
const fullCommit = /^[0-9a-f]{40}$/;
const releaseVersion = /^v\d+\.\d+\.\d+$/;

export function releaseActionProblems({ version, sourceCommit, authored, generated, breadcrumb }) {
  const problems = [];
  if (!releaseVersion.test(version)) problems.push('version must use vMAJOR.MINOR.PATCH');
  if (!fullCommit.test(sourceCommit)) problems.push('sourceCommit must be a full lowercase Git SHA');

  const cases = [
    {
      label: 'authored release',
      value: authored,
      generated: false,
      route: 'versioned/generate/',
      labels: [`View source at ${version}`, 'Edit latest documentation', 'Report a documentation issue'],
    },
    {
      label: 'generated release',
      value: generated,
      generated: true,
      route: 'reference/command-flags/',
      labels: [
        `View generated source at ${version}`,
        'Edit generator source in latest documentation',
        'Report a documentation issue',
      ],
    },
  ];
  for (const actionCase of cases) {
    const value = actionCase.value ?? {};
    if (value.markdownLabel !== 'Copy page as Markdown') {
      problems.push(`${actionCase.label}: Copy page as Markdown action is missing`);
    }
    if (value.documentationVersion !== version) problems.push(`${actionCase.label}: wrong documentation version`);
    if (value.viewRef !== version) problems.push(`${actionCase.label}: source view does not use ${version}`);
    if (value.editRef !== 'master') problems.push(`${actionCase.label}: editable source does not use master`);
    if (value.generated !== actionCase.generated) problems.push(`${actionCase.label}: generated classification is wrong`);
    if (JSON.stringify(value.labels) !== JSON.stringify(actionCase.labels)) {
      problems.push(`${actionCase.label}: action labels or order are wrong`);
    }
    if (!value.sourceHref?.includes(`/blob/${version}/${value.renderedSource}`)) {
      problems.push(`${actionCase.label}: source link does not identify the rendered tag file`);
    }
    const wantedEditSource = actionCase.generated ? 'internal/cmdref/markdown.go' : value.renderedSource;
    if (value.editSource !== wantedEditSource || !value.editHref?.includes(`/edit/master/${wantedEditSource}`)) {
      problems.push(`${actionCase.label}: edit link does not identify the latest editable source`);
    }
    if (actionCase.generated && value.editHref?.includes(value.renderedSource)) {
      problems.push(`${actionCase.label}: generated Markdown has a direct edit action`);
    }
    for (const line of [
      `Page: ${PageURL(version, actionCase.route)}`,
      `Documentation version: ${version}`,
      `Source ref: ${version}`,
      `Source commit: ${sourceCommit}`,
      `Rendered source: ${value.renderedSource}`,
      ...(actionCase.generated ? [
        'Generator: internal/cmd/docsync',
        'Edit source: internal/cmdref/markdown.go',
      ] : []),
    ]) {
      if (!value.issueBody?.includes(line)) problems.push(`${actionCase.label}: issue body omitted ${line}`);
    }
  }
  const expectedBreadcrumb = `/${version}/versioned/overview/`;
  if (breadcrumb?.label !== 'Versioned migrations') {
    problems.push('authored release: Versioned migrations parent breadcrumb is not a link');
  }
  if (breadcrumb?.href !== expectedBreadcrumb) {
    problems.push(`authored release: parent breadcrumb links to ${breadcrumb?.href}, want ${expectedBreadcrumb}`);
  }
  if (breadcrumb?.arrived !== expectedBreadcrumb) {
    problems.push(`authored release: parent breadcrumb arrived at ${breadcrumb?.arrived}, want ${expectedBreadcrumb}`);
  }
  return problems;
}

function fixture({ version, commit, generated }) {
  const renderedSource = generated
    ? 'docs/site/src/content/docs/reference/command-flags.md'
    : 'docs/site/src/content/docs/versioned/generate.md';
  const editSource = generated ? 'internal/cmdref/markdown.go' : renderedSource;
  return {
    documentationVersion: version,
    viewRef: version,
    editRef: 'master',
    generated,
    markdownLabel: 'Copy page as Markdown',
    renderedSource,
    editSource,
    labels: generated
      ? [`View generated source at ${version}`, 'Edit generator source in latest documentation', 'Report a documentation issue']
      : [`View source at ${version}`, 'Edit latest documentation', 'Report a documentation issue'],
    sourceHref: `https://github.com/stokaro/ptah/blob/${version}/${renderedSource}`,
    editHref: `https://github.com/stokaro/ptah/edit/master/${editSource}`,
    issueBody: [
      `Page: ${PageURL(version, generated ? 'reference/command-flags/' : 'versioned/generate/')}`,
      `Documentation version: ${version}`,
      `Source ref: ${version}`,
      `Source commit: ${commit}`,
      `Rendered source: ${renderedSource}`,
      ...(generated ? [
        'Generator: internal/cmd/docsync',
        'Edit source: internal/cmdref/markdown.go',
      ] : []),
    ].join('\n'),
  };
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const version = 'v0.3.0';
  const commit = '0123456789abcdef0123456789abcdef01234567';
  const valid = {
    version,
    sourceCommit: commit,
    authored: fixture({ version, commit, generated: false }),
    generated: fixture({ version, commit, generated: true }),
    breadcrumb: {
      label: 'Versioned migrations',
      href: `/${version}/versioned/overview/`,
      arrived: `/${version}/versioned/overview/`,
    },
  };
  assert(releaseActionProblems(valid).length === 0, 'valid release actions failed');
  const directGeneratedEdit = structuredClone(valid);
  directGeneratedEdit.generated.editSource = directGeneratedEdit.generated.renderedSource;
  directGeneratedEdit.generated.editHref = `https://github.com/stokaro/ptah/edit/master/${directGeneratedEdit.generated.renderedSource}`;
  assert(
    releaseActionProblems(directGeneratedEdit).some((problem) => problem.includes('generated Markdown')),
    'direct edit of generated Markdown passed',
  );
  const mutableView = structuredClone(valid);
  mutableView.authored.viewRef = 'master';
  mutableView.authored.sourceHref = mutableView.authored.sourceHref.replace(`/blob/${version}/`, '/blob/master/');
  assert(
    releaseActionProblems(mutableView).some((problem) => problem.includes(`does not use ${version}`)),
    'mutable source view passed for a release',
  );
  const inertBreadcrumb = structuredClone(valid);
  inertBreadcrumb.breadcrumb = { label: undefined, href: undefined, arrived: undefined };
  assert(
    releaseActionProblems(inertBreadcrumb).some((problem) => problem.includes('parent breadcrumb is not a link')),
    'non-clickable release parent breadcrumb passed',
  );
  console.log('check-release-page-actions.mjs --selftest: OK (authored/generated tag source and generator edit routing)');
}

function parseArguments(arguments_) {
  if (arguments_.includes('--selftest')) return { selftest: true };
  const value = (name) => {
    const index = arguments_.indexOf(name);
    return index === -1 ? undefined : arguments_[index + 1];
  };
  const dist = value('--dist');
  const version = value('--version');
  const sourceCommit = value('--source-commit');
  if (!dist || !version || !sourceCommit || arguments_.length !== 6) {
    throw new Error(
      'usage: node scripts/check-release-page-actions.mjs --dist <directory> --version <vX.Y.Z> --source-commit <full-sha>',
    );
  }
  return { dist: resolve(siteRoot, dist), version, sourceCommit };
}

async function readPageActions(page, built, route) {
  await page.goto(`http://127.0.0.1:${built.port}${built.base}${route}`, { waitUntil: 'load' });
  const root = page.locator('ptah-page-actions');
  await root.waitFor({ state: 'visible' });
  const actionLinks = root.locator('[data-page-action]');
  const reportHref = await root.locator('[data-page-action="report"]').getAttribute('href');
  return {
    documentationVersion: await root.getAttribute('data-documentation-version'),
    viewRef: await root.getAttribute('data-view-ref'),
    editRef: await root.getAttribute('data-edit-ref'),
    renderedSource: await root.getAttribute('data-rendered-source'),
    editSource: await root.getAttribute('data-edit-source'),
    generated: await root.getAttribute('data-generated') === 'true',
    markdownLabel: await root.locator('[data-copy-page]').getAttribute('aria-label'),
    labels: await actionLinks.locator('strong').allTextContents(),
    sourceHref: await root.locator('[data-page-action="source"]').getAttribute('href'),
    editHref: await root.locator('[data-page-action="edit"]').getAttribute('href'),
    issueBody: reportHref ? new URL(reportHref).searchParams.get('body') ?? '' : '',
  };
}

async function readVersionedBreadcrumb(page, built) {
  await page.goto(`http://127.0.0.1:${built.port}${built.base}/versioned/generate/`, { waitUntil: 'load' });
  const link = page.locator('a.group-crumb', { hasText: 'Versioned migrations' }).first();
  if (await link.count() !== 1) return {};
  const label = (await link.textContent())?.trim();
  const rawHref = await link.getAttribute('href');
  if (!rawHref) return { label };
  const href = new URL(rawHref, 'http://ptah.invalid').pathname;
  await link.click();
  return { label, href, arrived: new URL(page.url()).pathname };
}

async function main() {
  const options = parseArguments(process.argv.slice(2));
  if (options.selftest) {
    selftest();
    return;
  }
  if (!existsSync(options.dist)) throw new Error(`release build does not exist: ${options.dist}`);
  const buildInfo = JSON.parse(readFileSync(resolve(options.dist, 'build-info.json'), 'utf8'));
  const buildProblems = validateBuildInfo(buildInfo, {
    version: options.version,
    commit: options.sourceCommit,
    sourceRef: options.version,
  });
  if (buildProblems.length > 0) throw new Error(`release build information: ${buildProblems.join('; ')}`);

  const chromium = await loadChromium('check-release-page-actions.mjs');
  if (!chromium) return;
  const built = await startBuiltSite(options.dist);
  let browser;
  try {
    browser = await chromium.launch();
    const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
    const problems = releaseActionProblems({
      version: options.version,
      sourceCommit: options.sourceCommit,
      authored: await readPageActions(page, built, '/versioned/generate/'),
      generated: await readPageActions(page, built, '/reference/command-flags/'),
      breadcrumb: await readVersionedBreadcrumb(page, built),
    });
    if (problems.length > 0) throw new Error(problems.join('; '));
  } finally {
    if (browser) await browser.close();
    await new Promise((resolveClose) => built.server.close(resolveClose));
  }
  console.log(`release page actions: OK (${options.version} at ${options.sourceCommit}; authored and generated)`);
}

main().catch((error) => {
  console.error(`release page actions: FAILED: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
});
