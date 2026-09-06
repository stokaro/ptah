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
    if (value.markdownLabel !== 'Copy as Markdown') {
      problems.push(`${actionCase.label}: Copy as Markdown action is missing`);
    }
    if (value.documentationVersion !== version) problems.push(`${actionCase.label}: wrong documentation version`);
    if (value.viewRef !== version) problems.push(`${actionCase.label}: source view does not use ${version}`);
    if (value.editRef !== 'master') problems.push(`${actionCase.label}: editable source does not use master`);
    if (value.generated !== actionCase.generated) problems.push(`${actionCase.label}: generated classification is wrong`);
    // Each shape has its own expected set, because they carry different links:
    // the heading shows view/edit/report, the footer shows edit/report and no
    // source link. One list covering both would assert neither, and a shape
    // that dropped a link it should have would pass.
    // One list for both shapes, now that they carry the same three actions in
    // the same order.
    const wantedLabels = actionCase.labels;
    if (JSON.stringify(value.labels) !== JSON.stringify(wantedLabels)) {
      problems.push(
        `${actionCase.label}: action labels or order are wrong in the ${value.shape};` +
        ` got ${JSON.stringify(value.labels)}, want ${JSON.stringify(wantedLabels)}`,
      );
    }
    // Required in BOTH shapes. The footer used to render edit and report only,
    // so a released version had no link to its source as it was at that release
    // -- and the first draft of this check skipped the property there rather
    // than reporting the loss. The footer carries the link now, and this asserts
    // it everywhere rather than describing where it happens to be
    // (stokaro/ptah#2956).
    if (!value.sourceHref?.includes(`/blob/${version}/${value.renderedSource}`)) {
      problems.push(
        `${actionCase.label}: source link does not identify the rendered tag file in the ${value.shape}`,
      );
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

// fixture builds the HEADING shape, which is what every tag released before the
// current chrome renders. The footer shape has its own case below, because the
// two carry different link sets and a fixture that stood for both would assert
// neither.
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
    markdownLabel: 'Copy as Markdown',
    renderedSource,
    editSource,
    labels: generated
      ? [`View generated source at ${version}`, 'Edit generator source in latest documentation', 'Report a documentation issue']
      : [`View source at ${version}`, 'Edit latest documentation', 'Report a documentation issue'],
    shape: 'heading',
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

// The edit, source and report links are rendered in one of two places, and
// which one is decided by the release itself rather than by this check.
//
// `PageActions.astro` says so at the row's own declaration: the heading carries
// them "only where the heading has to carry them: a release checkout that
// received this file through the overlay has no footer override to put them in.
// On this site the footer has them and this row does not render."
//
// Every tag released before the current chrome takes the first path. v0.4.0 is
// the first release that IS the current chrome, so it takes the second, and a
// check that only knew the heading waited 30 seconds for an element that was
// never going to appear (stokaro/ptah#2956).
//
// Both markings are declared by their own component -- `data-page-action` in
// the heading row, `data-footer-action` in the footer -- so choosing between
// them is exact rather than a guess about the page's shape. A page carries one
// or the other, never both, which is what makes counting them the way to tell.
async function locateActions(page) {
  const heading = page.locator('ptah-page-actions [data-page-action]');
  if (await heading.count() > 0) {
    return { links: heading, attribute: 'data-page-action' };
  }
  const footer = page.locator('[data-footer-action]');
  if (await footer.count() > 0) {
    return { links: footer, attribute: 'data-footer-action' };
  }
  throw new Error('the page renders no edit/source/report links in either the heading or the footer');
}

async function readPageActions(page, built, route) {
  await page.goto(`http://127.0.0.1:${built.port}${built.base}${route}`, { waitUntil: 'load' });
  const root = page.locator('ptah-page-actions');
  await root.waitFor({ state: 'visible' });
  const { links: actionLinks, attribute } = await locateActions(page);
  const named = (kind) => page.locator(`[${attribute}="${kind}"]`).first();
  const reportHref = await named('report').getAttribute('href');
  return {
    documentationVersion: await root.getAttribute('data-documentation-version'),
    viewRef: await root.getAttribute('data-view-ref'),
    editRef: await root.getAttribute('data-edit-ref'),
    renderedSource: await root.getAttribute('data-rendered-source'),
    editSource: await root.getAttribute('data-edit-source'),
    generated: await root.getAttribute('data-generated') === 'true',
    markdownLabel: await root.locator('[data-copy-page]').getAttribute('aria-label'),
    // The heading wraps each label in <strong>; the footer does not, and reads
    // its label from the link's own text. Taking the strong-only reading for
    // both would report the footer as carrying no labels at all.
    labels: attribute === 'data-page-action'
      ? await actionLinks.locator('strong').allTextContents()
      : (await actionLinks.allTextContents()).map((text) => text.replace(/\s*↗\s*$/, '').trim()),
    // The footer shape declares no source link. Reported as absent rather than
    // waited for, so the run says which property it could not measure.
    sourceHref: await named('source').getAttribute('href'),
    shape: attribute === 'data-page-action' ? 'heading' : 'footer',
    editHref: await named('edit').getAttribute('href'),
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
