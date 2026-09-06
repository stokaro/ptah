#!/usr/bin/env node
// Renders every built page at desktop and mobile widths and fails on layout
// defects a reader would hit: a page that scrolls sideways, or an element wider
// than the viewport.
//
// This replaces the manual "review the site on a desktop and a phone" pass that
// documentation restructuring work kept deferring. A human still judges wording
// and information architecture, but overflow is mechanical, and mechanical
// checks are the ones that should not depend on someone remembering.
//
// Usage:
//   node scripts/check-responsive.mjs [--dist <dir>] [--selftest]
//
// Requires Playwright's chromium. A contributor without it is told what to
// install and the check skips; in CI it fails instead, because a green check
// that measured nothing is worse than a red one.
import { createServer } from 'node:http';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, extname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');

// Widths chosen for what they expose: 390 is a common phone viewport and the
// width at which Starlight collapses navigation; 1280 is a laptop; 1920 fits
// the minimum article-and-TOC frame with balanced outer gutters; and 2560 is
// past the point where those gutters reach their cap. At that last width the
// content shell must stop moving right and the article pane must grow toward
// the TOC instead.
const viewports = [
  { name: 'mobile', width: 390, height: 844 },
  { name: 'desktop', width: 1280, height: 900 },
  { name: 'wide-desktop', width: 1920, height: 1080 },
  { name: 'ultra-wide', width: 2560, height: 1080 },
];

// A few pixels of slack: sub-pixel rounding in the layout engine otherwise
// reports overflow on pages that look correct.
const overflowTolerance = 2;
const targetNavigationWidth = 256;
const targetContentPadding = 24;
const targetDocumentFrameGutterCap = 232;
const alignmentSentinelRoute = '/operate/seed-data/';

// The site ships two layouts and a control that switches between them, so this
// pass runs under each. It measured the envelope alone once, from two module
// constants, and could not have been pointed at the other: under `column` those
// constants are wrong by construction, so the reachable layout was the
// unmeasured one (stokaro/ptah#2930).
//
// Every figure below is measured against a build rather than derived, except
// the frame minimum: global.css computes that one with calc() from the shell,
// the padding and the contents rail, so restating it as a literal would be a
// second copy free to disagree with the first. The rail is a clamp() in the
// column layout and therefore a function of the viewport.
const layouts = [
  {
    name: 'envelope',
    query: '',
    prose: 960,
    wideContent: 1120,
    toc: () => 232,
    // The frame grows to fill the pane, centered, until its outer gutters
    // reach the cap; then it stops and the surplus stays outside it.
    frame: { kind: 'centered' },
    shellToTocGap: 24,
  },
  {
    name: 'column',
    query: '?layout=column',
    prose: 704,
    // The declared `--sl-content-width`, a ceiling the shell never reaches in
    // this layout: the shell is bounded by the article pane less its padding,
    // which the shell assertion below takes as the lesser of the two.
    wideContent: 960,
    // clamp(13rem, 13rem + (100vw - 80rem) * 0.359375, 18.75rem)
    toc: (viewport) => Math.min(300, Math.max(208, 208 + (viewport.width - 1280) * 0.359375)),
    // `minmax(64px, 0.6fr) minmax(0, 1280px) 1fr`: the frame stops at 80rem
    // and the pane's surplus splits 0.6 to 1 around it, so the left gutter
    // takes 0.375 of it. Screen 1b of the concept is drawn this way.
    frame: { kind: 'split', cap: 1280, minimumLeftGutter: 64, leftShare: 0.6 / 1.6 },
    shellToTocGap: 48,
  },
];

const frameMinimum = (layout, viewport) =>
  layout.wideContent + targetContentPadding * 2 + layout.toc(viewport);
const proseCeiling = (layout) => layout.prose + overflowTolerance;
const proseFloor = (layout) => layout.prose - overflowTolerance;

// The tallest a table cell may render at desktop width. Character count cannot
// see this: a short cell in a column squeezed narrow by an unbreakable code
// token in its neighbor renders just as tall as a long one, and that is the
// shape readers actually complain about.
//
// Measured at desktop for every table, and at 390px for the wide ones.
//
// The desktop-only rule rested on an assumption -- that a wide table scrolls
// inside its own container at 390px and keeps its desktop column widths -- and
// the feature matrix did not satisfy it. It scrolled, because Starlight makes
// every table its own scroller, but auto layout still shrank it toward the
// viewport, to 437px against a desktop 632px, and the difference column stacked
// into cells of fifteen rendered lines. 505 cells over the cap at 390px, none
// at 1280px (stokaro/ptah#946).
//
// The fix is a scrolling wrapper with the desktop width on the table inside it,
// and this is what keeps it: deleting the wrapper or the stylesheet puts those
// cells back over the cap at 390px and turns this red. A `min-width` on the
// table alone does not work and is not what to reach for -- Starlight's table
// IS the scroller, so widening it scrolls the page instead, measured at 648px
// against a 390px viewport.
//
// Narrow tables stay desktop-only, and this is the decision rather than a
// deferral of it.
//
// Measured by setting wideTableColumns to 1 against the built site: 77 cells
// across sixteen routes exceed the cap, and every one of them is a two-, three-
// or four-column reference table whose columns still ADD UP to the viewport --
// /atlas/conformance's three columns report 92px, 84px and 108px at 390px. Such
// a table is not shrunk; it is wrapping, which is what a phone should do with a
// sentence in a cell.
//
// The cap exists to catch a table whose LAYOUT broke: the feature matrix
// rendered 437px wide against a desktop 632px, so its columns were squeezed
// while its content stayed long. Applying the same fix to a table that already
// fills the width would replace wrapping with horizontal scrolling on every
// reference table in the site, which is worse for the reader and buys nothing.
//
// So the rule is the column count, and the reason is the mechanism behind it
// (stokaro/ptah#946).
const maxCellLines = 8;
const wideTableColumns = 5;

// Routes whose wide tables are known to fail the mobile cap and are not fixed
// by this rule. Each entry states the measurement and what would fix it.
//
// EMPTY as of the lint-rules fix: its Atlas-check table is emitted by
// internal/lintcatalog/markdown.go, which now wraps it the way
// build-feature-matrix.mjs wraps the matrix. The map stays because the next
// generated wide table will arrive the same way -- outside the docs scripts,
// failing here first.
const mobileCapExemptions = new Map([]);

// Pages whose tables are lookup references, where an exhaustive cell is the
// content rather than a mistake. Each entry states why.
//
// checkStaleAllowlist below fails when an allowlisted route stops having a
// finding, so an entry cannot outlive the reason it was added.
const denseTableAllowlist = new Map([
  [
    '/reference/exit-codes/',
    'per-command cause lists are the reference content, and check-exit-codes.mjs pins these rows verbatim to docs/exit_codes.md',
  ],
]);

const mimeTypes = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.webp': 'image/webp',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ico': 'image/x-icon',
  '.xml': 'application/xml; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8',
  '.pagefind': 'application/octet-stream',
};

function toPosix(value) {
  return value.split(sep).join('/');
}

function argValue(flag, fallback) {
  const index = process.argv.indexOf(flag);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function htmlRoutes(distRoot) {
  const routes = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
        continue;
      }
      if (entry.isFile() && entry.name === 'index.html') {
        // Redirect stubs navigate away the moment they load, so measuring them
        // is meaningless and races the evaluate. check-redirects.mjs owns them.
        if (/http-equiv=["']?refresh/i.test(readFileSync(full, 'utf8'))) continue;
        const route = '/' + toPosix(relative(distRoot, dir)).replace(/^\.$/, '');
        routes.push(route === '/' ? '/' : `${route}/`);
      }
    }
  };
  walk(distRoot);
  return routes;
}

// detectBase reads the base path the site was built with. Astro prefixes every
// asset URL with it, so a server that ignores it 404s every stylesheet and
// silently measures unstyled pages - which look fine to an overflow check
// because none of the layout that causes overflow has been applied.
function detectBase(distRoot) {
  const home = join(distRoot, 'index.html');
  if (!existsSync(home)) return '';
  const match = readFileSync(home, 'utf8').match(/(?:href|src)="([^"]*)\/_astro\//);
  return match ? match[1] : '';
}

function startServer(distRoot, base) {
  const server = createServer((request, response) => {
    let url = decodeURIComponent((request.url ?? '/').split('?')[0]);
    if (base && url.startsWith(base)) url = url.slice(base.length) || '/';
    let filePath = join(distRoot, url);
    if (existsSync(filePath) && statSync(filePath).isDirectory()) {
      filePath = join(filePath, 'index.html');
    }
    if (!existsSync(filePath) || !statSync(filePath).isFile()) {
      response.writeHead(404);
      response.end('not found');
      return;
    }
    response.writeHead(200, { 'content-type': mimeTypes[extname(filePath)] ?? 'application/octet-stream' });
    response.end(readFileSync(filePath));
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port }));
  });
}

// measure runs in the page. It reports the document's own horizontal overflow
// plus the specific elements that stick out, so a failure names the culprit
// instead of only the page.
const measure = ({ tolerance, cellLineLimit }) => {
  const doc = document.documentElement;
  const viewportWidth = doc.clientWidth;

  // An element inside a scrollable container is allowed to be wider than the
  // viewport; that is exactly how a wide table or code block is supposed to
  // behave. Only content that escapes every scroll container can push the page
  // sideways, so ancestors are checked before an element is reported.
  const escapesScrollContainer = (element) => {
    for (let node = element.parentElement; node && node !== doc; node = node.parentElement) {
      const overflowX = getComputedStyle(node).overflowX;
      if (overflowX === 'auto' || overflowX === 'scroll' || overflowX === 'hidden') return false;
    }
    return true;
  };

  const offenders = [];
  for (const element of document.querySelectorAll('main *')) {
    const rect = element.getBoundingClientRect();
    if (rect.width === 0 && rect.height === 0) continue;
    if (rect.right <= viewportWidth + tolerance) continue;
    if (!escapesScrollContainer(element)) continue;
    // Report the outermost offender only: a wide table makes every cell inside
    // it look wide too, and listing all of them buries the cause.
    if (offenders.some((entry) => entry.element.contains(element))) continue;
    offenders.push({
      element,
      tag: element.tagName.toLowerCase(),
      className: typeof element.className === 'string' ? element.className.slice(0, 60) : '',
      right: Math.round(rect.right),
    });
  }
  // A table wider than the container it sits in. Starlight gives tables their
  // own scroll container, so this never reaches the page-level overflow check
  // above - the reader just finds a table whose right-hand columns are cut off
  // at desktop width, with no hint that the rest exists.
  //
  // This fires on content that is too wide and on styling that stops content
  // from wrapping. A `white-space: nowrap` on table code, added to keep short
  // flags like `--dry-run` from breaking at the hyphen, also froze
  // ninety-character error strings and pushed several tables past their
  // container. Nothing caught it, because scrolling inside a container is
  // normally correct.
  const wideTables = [];
  for (const table of document.querySelectorAll('main table')) {
    const holder = table.parentElement;
    if (!holder) continue;
    const overflow = Math.round(table.scrollWidth - holder.clientWidth);
    if (overflow <= tolerance) continue;
    const heading = table.querySelector('th');
    wideTables.push({
      overflow,
      width: Math.round(table.scrollWidth),
      container: Math.round(holder.clientWidth),
      firstColumn: heading ? heading.textContent.trim().slice(0, 30) : '',
    });
  }

  // Rendered height in lines, not characters. A cell squeezed into a narrow
  // column is exactly as unreadable as a cell holding an essay.
  const tallCells = [];
  for (const cell of document.querySelectorAll('main table tbody td')) {
    const rect = cell.getBoundingClientRect();
    if (rect.width === 0) continue;
    const lineHeight = parseFloat(getComputedStyle(cell).lineHeight);
    if (!Number.isFinite(lineHeight) || lineHeight <= 0) continue;
    const lines = Math.round(rect.height / lineHeight);
    if (lines <= cellLineLimit) continue;
    const table = cell.closest('table');
    tallCells.push({
      lines,
      width: Math.round(rect.width),
      text: cell.textContent.trim().slice(0, 50),
      columns: table ? table.querySelectorAll('thead th').length : 0,
    });
  }

  // Prose and reference material use different measures. The page shell may
  // grow for code, diagrams, and tables, but ordinary text must not inherit
  // that width. Image-only paragraphs are visual containers rather than prose.
  const proseWidths = [];
  const proseSelector = [
    '.sl-markdown-content > p:not(:has(> img:only-child))',
    '.sl-markdown-content > ul',
    '.sl-markdown-content > ol',
    '.sl-markdown-content > blockquote',
    '.sl-markdown-content > dl',
    '.sl-markdown-content > .sl-heading-wrapper',
    '.sl-markdown-content > details',
    '.sl-markdown-content > .starlight-aside',
  ].join(',');
  const markdown = document.querySelector('.sl-markdown-content');
  const markdownRect = markdown?.getBoundingClientRect();
  const proseLeftOffsets = [];
  for (const element of document.querySelectorAll(proseSelector)) {
    const rect = element.getBoundingClientRect();
    if (rect.width > 0) {
      proseWidths.push(Math.round(rect.width));
      if (markdownRect) {
        proseLeftOffsets.push(Math.abs(rect.left - markdownRect.left));
      }
    }
  }

  const contentShell = markdown?.parentElement;
  const contentPanel = contentShell?.closest('.content-panel');
  const shellRect = contentShell?.getBoundingClientRect();
  const panelRect = contentPanel?.getBoundingClientRect();
  const heading = document.querySelector('.page-heading');
  const headingRect = heading?.getBoundingClientRect();
  const headingShellRect = heading?.parentElement?.getBoundingClientRect();
  const alignmentRect = (element) => {
    if (!element) return null;
    const rect = element.getBoundingClientRect();
    if (rect.width === 0 && rect.height === 0) return null;
    return { left: rect.left, right: rect.right, width: rect.width };
  };
  const articleAlignment = {
    contentShell: alignmentRect(contentShell),
    pageHeading: alignmentRect(heading),
    markdownShell: alignmentRect(markdown),
    ordinaryParagraph: alignmentRect(
      document.querySelector('.sl-markdown-content > p:not(:has(> img:only-child))'),
    ),
    sectionHeading: alignmentRect(document.querySelector('.sl-markdown-content > .sl-heading-wrapper')),
    codeBlock: alignmentRect(document.querySelector('.sl-markdown-content > .expressive-code')),
  };
  const customWidth = (property) => {
    const value = getComputedStyle(document.documentElement).getPropertyValue(property).trim();
    if (!value) return null;
    const probe = document.createElement('div');
    probe.style.cssText =
      `position:fixed;visibility:hidden;pointer-events:none;inline-size:var(${property});max-inline-size:none`;
    document.body.append(probe);
    const width = probe.getBoundingClientRect().width;
    probe.remove();
    return width;
  };
  const documentFrame = document.querySelector('[data-has-sidebar][data-has-toc] .lg\\:sl-flex');
  const mainFrame = documentFrame?.closest('.main-frame');
  const navigation = alignmentRect(document.querySelector('.sidebar-pane'));
  const articleFrame = {
    navigation,
    availableFrame: navigation
      ? { left: navigation.right, right: viewportWidth, width: viewportWidth - navigation.right }
      : alignmentRect(mainFrame),
    documentFrame: alignmentRect(documentFrame),
    mainPane: alignmentRect(document.querySelector('.main-pane')),
    tocContainer: alignmentRect(document.querySelector('.right-sidebar-container')),
    toc: alignmentRect(document.querySelector('.right-sidebar')),
  };
  const widthContract = {
    prose: customWidth('--ptah-prose-width'),
    wideContent: customWidth('--sl-content-width'),
    navigation: customWidth('--sl-sidebar-width'),
    toc: customWidth('--ptah-toc-width'),
    contentPadding: customWidth('--sl-content-pad-x'),
    documentFrameMin: customWidth('--ptah-doc-frame-min-width'),
    documentFrameGutterCap: customWidth('--ptah-doc-frame-gutter-cap'),
    // Read rather than declared: the column layout pads its panels by 3rem
    // where the envelope takes Starlight's 24px, and the shell is bounded by
    // whichever of the declared width and the padded pane is smaller.
    panelPadding: (() => {
      const panel = document.querySelector('.main-pane .content-panel');
      if (!panel) return null;
      const padding = parseFloat(getComputedStyle(panel).paddingLeft);
      return Number.isFinite(padding) ? padding : null;
    })(),
  };

  return {
    scrollWidth: doc.scrollWidth,
    clientWidth: viewportWidth,
    offenders: offenders.slice(0, 5).map(({ tag, className, right }) => ({ tag, className, right })),
    tallCells: tallCells.sort((a, b) => b.lines - a.lines).slice(0, 5),
    wideTables: wideTables.sort((a, b) => b.overflow - a.overflow).slice(0, 3),
    widestProse: proseWidths.length > 0 ? Math.max(...proseWidths) : 0,
    proseElementCount: proseWidths.length,
    hasMarkdown: Boolean(markdown),
    proseLeftOffset: proseLeftOffsets.length > 0 ? Math.max(...proseLeftOffsets) : 0,
    hasContentShell: Boolean(shellRect),
    hasContentPanel: Boolean(panelRect),
    contentShellCenterOffset: shellRect && panelRect
      ? Math.abs((shellRect.left + shellRect.right - panelRect.left - panelRect.right) / 2)
      : 0,
    pageHeadingWidth: headingRect?.width ?? 0,
    hasPageHeading: Boolean(headingRect),
    pageHeadingLeftOffset: headingRect && headingShellRect
      ? Math.abs(headingRect.left - headingShellRect.left)
      : 0,
    articleAlignment,
    articleFrame,
    widthContract,
  };
};

function checksArticleContract(viewport, route) {
  return (
    route === alignmentSentinelRoute &&
    (viewport.name === 'wide-desktop' || viewport.name === 'ultra-wide')
  );
}

function expandedPastFrameGutterCap(result) {
  const geometry = result.articleFrame ?? {};
  const minimum = result.widthContract?.documentFrameMin;
  const cap = result.widthContract?.documentFrameGutterCap;
  if (
    !geometry.availableFrame ||
    !geometry.documentFrame ||
    minimum == null ||
    cap == null ||
    geometry.documentFrame.width <= minimum + overflowTolerance
  ) {
    return false;
  }
  const leftGutter = geometry.documentFrame.left - geometry.availableFrame.left;
  const rightGutter = geometry.availableFrame.right - geometry.documentFrame.right;
  return (
    Math.abs(leftGutter - cap) <= overflowTolerance &&
    Math.abs(rightGutter - cap) <= overflowTolerance
  );
}

function articleAlignmentProblems(result, route, viewport, layout) {
  if (!checksArticleContract(viewport, route)) return [];

  const problems = [];
  const labels = new Map([
    ['contentShell', 'wide content shell'],
    ['pageHeading', 'page heading'],
    ['markdownShell', 'Markdown shell'],
    ['ordinaryParagraph', 'ordinary paragraph'],
    ['sectionHeading', 'section heading wrapper'],
    ['codeBlock', 'Expressive Code block'],
  ]);
  const geometry = result.articleAlignment ?? {};
  for (const [key, label] of labels) {
    if (!geometry[key]) {
      problems.push(`${route}: ${label} was not rendered for the ${viewport.name} alignment check`);
    }
  }

  const shell = geometry.contentShell;
  if (!shell) return problems;
  const expectedWideWidth = result.widthContract?.wideContent ?? layout.wideContent;
  if (Math.abs(expectedWideWidth - layout.wideContent) > overflowTolerance) {
    problems.push(
      `${route}: --sl-content-width resolves to ${Math.round(expectedWideWidth)}px; ` +
        `expected ${layout.wideContent}px in the ${layout.name} layout`,
    );
  }
  // The declared width is a ceiling, not the answer: the shell also cannot
  // outgrow the article pane less its padding, and in the column layout that
  // pane is the binding constraint. One rule covers both layouts, which is
  // what makes it a rule rather than two numbers.
  const articlePane = result.articleFrame?.mainPane;
  const panelPadding = result.widthContract?.panelPadding;
  const paneBound =
    articlePane && panelPadding != null ? articlePane.width - panelPadding * 2 : Infinity;
  const expectedShellWidth = Math.min(expectedWideWidth, paneBound);
  if (
    shell.width < expectedShellWidth - overflowTolerance ||
    shell.width > expectedShellWidth + overflowTolerance
  ) {
    problems.push(
      `${route}: content shell renders ${Math.round(shell.width)}px wide at ${viewport.width}px; ` +
        `expected ${Math.round(expectedShellWidth)}px in the ${layout.name} layout ` +
        `(the lesser of the ${Math.round(expectedWideWidth)}px declared width and the ` +
        `${Number.isFinite(paneBound) ? Math.round(paneBound) : 'unbounded'}px padded article pane)`,
    );
  }

  const wideKeys = new Map([
    ['markdownShell', 'Markdown shell'],
    ['codeBlock', 'Expressive Code block'],
  ]);
  for (const [key, label] of wideKeys) {
    const rect = geometry[key];
    if (!rect) continue;
    const leftDifference = Math.abs(rect.left - shell.left);
    const rightDifference = Math.abs(rect.right - shell.right);
    if (leftDifference <= overflowTolerance && rightDifference <= overflowTolerance) continue;
    problems.push(
      `${route}: ${label} does not share the content shell edges at ${viewport.width}px ` +
        `(left differs by ${Math.round(leftDifference)}px, right by ${Math.round(rightDifference)}px; ` +
        `limit ${overflowTolerance}px)`,
    );
  }

  const expectedProseWidth = result.widthContract?.prose ?? layout.prose;
  if (Math.abs(expectedProseWidth - layout.prose) > overflowTolerance) {
    problems.push(
      `${route}: --ptah-prose-width resolves to ${Math.round(expectedProseWidth)}px; ` +
        `expected ${layout.prose}px in the ${layout.name} layout`,
    );
  }
  const proseKeys = new Map([
    ['pageHeading', 'page heading'],
    ['ordinaryParagraph', 'ordinary paragraph'],
    ['sectionHeading', 'section heading wrapper'],
  ]);
  for (const [key, label] of proseKeys) {
    const rect = geometry[key];
    if (!rect) continue;
    const leftDifference = Math.abs(rect.left - shell.left);
    const widthDifference = Math.abs(rect.width - expectedProseWidth);
    if (leftDifference <= overflowTolerance && widthDifference <= overflowTolerance) continue;
    problems.push(
      `${route}: ${label} does not share the content shell's left edge and ${Math.round(expectedProseWidth)}px ` +
        `reading measure at ${viewport.width}px (left differs by ${Math.round(leftDifference)}px, ` +
        `width by ${Math.round(widthDifference)}px; limit ${overflowTolerance}px)`,
    );
  }

  return problems;
}

function articleFrameProblems(result, route, viewport, layout) {
  if (!checksArticleContract(viewport, route)) return [];

  const problems = [];
  const geometry = result.articleFrame ?? {};
  const labels = new Map([
    ['navigation', 'navigation rail'],
    ['availableFrame', 'space beside the navigation rail'],
    ['documentFrame', 'article-and-TOC frame'],
    ['mainPane', 'article pane'],
    ['tocContainer', 'TOC container'],
    ['toc', 'TOC rail'],
  ]);
  for (const [key, label] of labels) {
    if (!geometry[key]) {
      problems.push(`${route}: ${label} was not rendered for the ${viewport.name} frame check`);
    }
  }
  if ([...labels.keys()].some((key) => !geometry[key])) return problems;

  const { navigation, availableFrame, documentFrame, mainPane, tocContainer, toc } = geometry;
  const expectedNavigationWidth = result.widthContract?.navigation;
  const expectedTocWidth = result.widthContract?.toc;
  const expectedContentPadding = result.widthContract?.contentPadding;
  const expectedFrameMinWidth = result.widthContract?.documentFrameMin;
  const expectedFrameGutterCap = result.widthContract?.documentFrameGutterCap;
  if (
    expectedNavigationWidth == null ||
    expectedTocWidth == null ||
    expectedContentPadding == null ||
    expectedFrameMinWidth == null ||
    expectedFrameGutterCap == null
  ) {
    problems.push(`${route}: frame width variables were not resolved`);
    return problems;
  }
  if (Math.abs(expectedNavigationWidth - targetNavigationWidth) > overflowTolerance) {
    problems.push(
      `${route}: --sl-sidebar-width resolves to ${Math.round(expectedNavigationWidth)}px; ` +
        `expected ${targetNavigationWidth}px (16rem)`,
    );
  }
  const expectedFrameMinimum = frameMinimum(layout, viewport);
  if (Math.abs(expectedFrameMinWidth - expectedFrameMinimum) > overflowTolerance) {
    problems.push(
      `${route}: --ptah-doc-frame-min-width resolves to ${Math.round(expectedFrameMinWidth)}px; ` +
        `expected ${Math.round(expectedFrameMinimum)}px in the ${layout.name} layout ` +
        '(shell + two paddings + contents rail)',
    );
  }
  if (Math.abs(expectedFrameGutterCap - targetDocumentFrameGutterCap) > overflowTolerance) {
    problems.push(
      `${route}: --ptah-doc-frame-gutter-cap resolves to ${Math.round(expectedFrameGutterCap)}px; ` +
        `expected ${targetDocumentFrameGutterCap}px`,
    );
  }
  const expectedRailWidth = layout.toc(viewport);
  if (Math.abs(expectedTocWidth - expectedRailWidth) > overflowTolerance) {
    problems.push(
      `${route}: --ptah-toc-width resolves to ${Math.round(expectedTocWidth)}px; ` +
        `expected ${Math.round(expectedRailWidth)}px in the ${layout.name} layout ` +
        `at ${viewport.width}px`,
    );
  }
  if (Math.abs(expectedContentPadding - targetContentPadding) > overflowTolerance) {
    problems.push(
      `${route}: --sl-content-pad-x resolves to ${Math.round(expectedContentPadding)}px; ` +
        `expected ${targetContentPadding}px`,
    );
  }
  if (
    Math.abs(expectedFrameGutterCap + expectedContentPadding - expectedNavigationWidth) >
    overflowTolerance
  ) {
    problems.push(
      `${route}: frame gutter cap plus content padding does not equal the navigation width ` +
        `(${Math.round(expectedFrameGutterCap)}px + ${Math.round(expectedContentPadding)}px != ` +
        `${Math.round(expectedNavigationWidth)}px)`,
    );
  }
  if (Math.abs(navigation.width - expectedNavigationWidth) > overflowTolerance) {
    problems.push(
      `${route}: navigation rail renders ${Math.round(navigation.width)}px wide; ` +
        `expected ${Math.round(expectedNavigationWidth)}px`,
    );
  }

  const leftGutter = documentFrame.left - availableFrame.left;
  const rightGutter = availableFrame.right - documentFrame.right;
  const expectedFrameWidth =
    layout.frame.kind === 'centered'
      ? Math.min(
          availableFrame.width,
          Math.max(expectedFrameMinWidth, availableFrame.width - 2 * expectedFrameGutterCap),
        )
      : Math.min(layout.frame.cap, availableFrame.width - layout.frame.minimumLeftGutter);
  if (Math.abs(documentFrame.width - expectedFrameWidth) > overflowTolerance) {
    problems.push(
      `${route}: article-and-TOC frame renders ${Math.round(documentFrame.width)}px wide at ` +
        `${viewport.width}px; ` +
        `expected ${Math.round(expectedFrameWidth)}px in the ${layout.name} layout`,
    );
  }
  if (layout.frame.kind === 'centered') {
    if (
      leftGutter < -overflowTolerance ||
      rightGutter < -overflowTolerance ||
      leftGutter > expectedFrameGutterCap + overflowTolerance ||
      rightGutter > expectedFrameGutterCap + overflowTolerance ||
      Math.abs(leftGutter - rightGutter) > overflowTolerance
    ) {
      problems.push(
        `${route}: article-and-TOC frame is not centered within its capped outer gutters ` +
          `(left gutter ${Math.round(leftGutter)}px, right gutter ${Math.round(rightGutter)}px; ` +
          `cap ${Math.round(expectedFrameGutterCap)}px, balance limit ${overflowTolerance}px)`,
      );
    }
    if (
      viewport.name === 'ultra-wide' &&
      (
        Math.abs(leftGutter - expectedFrameGutterCap) > overflowTolerance ||
        Math.abs(rightGutter - expectedFrameGutterCap) > overflowTolerance
      )
    ) {
      problems.push(
        `${route}: ultra-wide frame gutters have not reached the ${Math.round(expectedFrameGutterCap)}px cap ` +
          `(left ${Math.round(leftGutter)}px, right ${Math.round(rightGutter)}px)`,
      );
    }
  } else {
    // The surplus beside the capped frame is the thing being measured: an
    // even split would put the article in the middle of the pane instead of
    // near the navigation, which is the arrangement the concept draws.
    const surplus = Math.max(0, availableFrame.width - expectedFrameWidth);
    const expectedLeftGutter = surplus * layout.frame.leftShare;
    if (Math.abs(leftGutter - expectedLeftGutter) > overflowTolerance) {
      problems.push(
        `${route}: the ${layout.name} frame's left gutter is ${Math.round(leftGutter)}px at ` +
          `${viewport.width}px; expected ${Math.round(expectedLeftGutter)}px, ` +
          `${layout.frame.leftShare.toFixed(3)} of the ${Math.round(surplus)}px beside the frame`,
      );
    }
    if (Math.abs(leftGutter + rightGutter - surplus) > overflowTolerance) {
      problems.push(
        `${route}: the ${layout.name} frame's gutters total ${Math.round(leftGutter + rightGutter)}px ` +
          `but ${Math.round(surplus)}px sits beside the frame`,
      );
    }
  }
  for (const [rect, label] of [[tocContainer, 'TOC container'], [toc, 'TOC rail']]) {
    if (Math.abs(rect.width - expectedTocWidth) > overflowTolerance) {
      problems.push(
        `${route}: ${label} renders ${Math.round(rect.width)}px wide; expected ${Math.round(expectedTocWidth)}px`,
      );
    }
  }
  const tocLeftDifference = Math.abs(toc.left - tocContainer.left);
  const tocRightDifference = Math.abs(toc.right - tocContainer.right);
  if (tocLeftDifference > overflowTolerance || tocRightDifference > overflowTolerance) {
    problems.push(
      `${route}: TOC rail does not share its moving container edges ` +
        `(left differs by ${Math.round(tocLeftDifference)}px, right by ${Math.round(tocRightDifference)}px; ` +
        `limit ${overflowTolerance}px)`,
    );
  }
  const paneTocGap = Math.abs(mainPane.right - tocContainer.left);
  if (paneTocGap > overflowTolerance) {
    problems.push(
      `${route}: article pane and TOC are ${Math.round(paneTocGap)}px apart; expected adjacent frame columns`,
    );
  }
  const contentsLeft = Math.min(mainPane.left, tocContainer.left);
  const contentsRight = Math.max(mainPane.right, tocContainer.right);
  if (
    Math.abs(contentsLeft - documentFrame.left) > overflowTolerance ||
    Math.abs(contentsRight - documentFrame.right) > overflowTolerance
  ) {
    problems.push(`${route}: article pane and TOC do not fill the declared document frame`);
  }

  const shell = result.articleAlignment?.contentShell;
  if (shell) {
    const proseToNavigationGap = shell.left - navigation.right;
    // The envelope keeps the prose within one rail width of the navigation.
    // The column layout deliberately opens that gap: its frame is capped and
    // the surplus sits in the gutters, which the frame block above measures.
    if (layout.frame.kind === 'centered') {
      if (
        proseToNavigationGap < -overflowTolerance ||
        proseToNavigationGap > navigation.width + overflowTolerance
      ) {
        problems.push(
          `${route}: prose starts ${Math.round(proseToNavigationGap)}px after the navigation rail; ` +
            `the gap must not exceed the rail's ${Math.round(navigation.width)}px width`,
        );
      }
      if (
        viewport.name === 'ultra-wide' &&
        Math.abs(proseToNavigationGap - navigation.width) > overflowTolerance
      ) {
        problems.push(
          `${route}: ultra-wide prose-to-navigation gap is ${Math.round(proseToNavigationGap)}px; ` +
            `expected the ${Math.round(navigation.width)}px navigation width`,
        );
      }
    }

    const frameGrowth =
      layout.frame.kind === 'centered' ? Math.max(0, documentFrame.width - expectedFrameMinWidth) : 0;
    const expectedShellToTocGap = layout.shellToTocGap + frameGrowth;
    const shellToTocGap = tocContainer.left - shell.right;
    if (Math.abs(shellToTocGap - expectedShellToTocGap) > overflowTolerance) {
      problems.push(
        `${route}: content-shell-to-TOC gap is ${Math.round(shellToTocGap)}px at ${viewport.width}px; ` +
          `expected ${Math.round(expectedShellToTocGap)}px in the ${layout.name} layout` +
          (frameGrowth > 0 ? `, so the frame's ${Math.round(frameGrowth)}px growth stays between them` : ''),
      );
    }
  }

  return problems;
}

function readingMeasureProblems(result, route, viewport, layout = layouts[0]) {
  if (viewport.name === 'mobile') return [];

  const problems = [];
  const isHome = route === '/';
  if (!result.hasMarkdown) problems.push(`${route}: Markdown content container was not rendered`);
  if (!result.hasContentShell || !result.hasContentPanel) {
    problems.push(`${route}: content shell or content panel was not rendered`);
  }
  // Every ordinary article must render the shared page heading so width and
  // centering remain measurable instead of falling back to zero and passing
  // silently.
  if (!result.hasPageHeading && !isHome) problems.push(`${route}: page heading was not rendered`);
  if (result.widestProse > proseCeiling(layout)) {
    problems.push(
      `${route}: prose renders ${result.widestProse}px wide, over the ` +
        `${proseCeiling(layout)}px reading measure of the ${layout.name} layout`,
    );
  }
  if (result.proseLeftOffset > overflowTolerance) {
    problems.push(
      `${route}: prose starts ${Math.round(result.proseLeftOffset)}px away from the content shell's left edge`,
    );
  }
  if (
    result.contentShellCenterOffset > overflowTolerance &&
    !expandedPastFrameGutterCap(result)
  ) {
    problems.push(`${route}: content shell is ${Math.round(result.contentShellCenterOffset)}px off center in its panel`);
  }
  if (!isHome && result.pageHeadingWidth > proseCeiling(layout)) {
    problems.push(
      `${route}: page heading renders ${Math.round(result.pageHeadingWidth)}px wide, over ` +
        `${proseCeiling(layout)}px in the ${layout.name} layout`,
    );
  }
  if (!isHome && result.pageHeadingLeftOffset > overflowTolerance) {
    problems.push(`${route}: page heading starts ${Math.round(result.pageHeadingLeftOffset)}px away from its content shell`);
  }

  // An upper bound alone would accept the old 40rem measure. Pin one ordinary
  // article at a viewport where the full 60rem measure fits, while leaving
  // narrow viewports and intentionally compact components responsive.
  if (checksArticleContract(viewport, route)) {
    if (result.proseElementCount === 0) {
      problems.push(`${route}: no ordinary prose element was rendered for the reading-measure check`);
    } else if (result.widestProse < proseFloor(layout)) {
      problems.push(
        `${route}: prose renders ${result.widestProse}px wide at ${viewport.width}px, below the ` +
          `${proseFloor(layout)}px minimum of the ${layout.name} layout's reading measure`,
      );
    }
    if (!isHome && result.pageHeadingWidth < proseFloor(layout)) {
      problems.push(
        `${route}: page heading renders ${Math.round(result.pageHeadingWidth)}px wide at ` +
          `${viewport.width}px, below the ${proseFloor(layout)}px minimum of the ` +
          `${layout.name} layout's reading measure`,
      );
    }
  }

  return problems
    .concat(articleAlignmentProblems(result, route, viewport, layout))
    .concat(articleFrameProblems(result, route, viewport, layout));
}

async function loadChromium() {
  try {
    const { chromium } = await import('playwright');
    return chromium;
  } catch {
    return null;
  }
}

// unavailable reports the browser as missing. In CI that is a failure: the
// dependency is installed by the workflow, so its absence means the check would
// have reported success without rendering a single page.
function unavailable(reason) {
  const install = 'npm i -D playwright && npx playwright install chromium';
  if (process.env.CI) {
    console.error(`check-responsive.mjs: ${reason}; CI must not report success without rendering the site.`);
    console.error(`Install it with: ${install}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-responsive.mjs: SKIPPED (${reason}; run "${install}")`);
}

// watchStylesheets records failed stylesheet responses for the page. A page
// served without its stylesheets has none of the layout that causes overflow,
// so it looks clean and the check reports a false OK. Watching for failed
// STYLESHEET requests catches that directly: it is how a wrong base path, a
// renamed asset, or a broken build shows up. Other 404s are ignored on purpose
// - versions.json, for one, is produced by the deploy workflow rather than by
// `npm run build`, and its absence locally says nothing about layout.
function watchStylesheets(page) {
  let failures = [];
  page.on('response', (response) => {
    if (response.status() < 400) return;
    const path = new URL(response.url()).pathname;
    if (response.request().resourceType() === 'stylesheet' || path.endsWith('.css')) {
      failures.push(`${response.status()} ${path}`);
    }
  });
  return {
    reset: () => {
      failures = [];
    },
    failures: () => [...new Set(failures)],
  };
}

// writeFixtureDist builds the smallest site that exercises base detection: one
// page whose stylesheet lives under the build's base path.
function writeFixtureDist(base) {
  const root = mkdtempSync(join(tmpdir(), 'ptah-responsive-'));
  mkdirSync(join(root, '_astro'), { recursive: true });
  writeFileSync(join(root, '_astro', 'style.css'), 'main { color: #000; }\n');
  writeFileSync(
    join(root, 'index.html'),
    `<!doctype html><html><head><link rel="stylesheet" href="${base}/_astro/style.css"></head>` +
      '<body><main><p>fits</p></main></body></html>\n',
  );
  return root;
}

async function main() {
  if (process.argv.includes('--selftest')) {
    // The self-test proves both halves of the check fire: the overflow detector
    // on a page built to overflow, and the unstyled-page guard on a site served
    // at the wrong base. The second half is not hypothetical - an early version
    // of this script ignored the base path, rendered every page unstyled, and
    // reported 289 overflow findings that were entirely its own fault.
    const chromium = await loadChromium();
    if (!chromium) {
      unavailable('playwright is not installed');
      return;
    }
    const failures = [];
    let browser;
    try {
      browser = await chromium.launch();
    } catch (error) {
      unavailable(`chromium failed to launch (${error.message.split('\n')[0]})`);
      return;
    }
    try {
      const page = await browser.newPage({ viewport: { width: 390, height: 844 } });
      await page.setContent('<main><div style="width:1200px">wide</div></main>');
      const wide = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      await page.setContent('<main><div style="width:100px">narrow</div></main>');
      const narrow = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      if (wide.offenders.length === 0) failures.push('overflow detector did not fire on a 1200px element at 390px');
      if (narrow.offenders.length > 0) failures.push('overflow detector fired on a page that fits');

      // The tall-cell rule, both ways. The fixture reproduces the shape that
      // motivated it: a neighbor cell holding an unbreakable token squeezes a
      // column narrow, and ordinary text in it stacks one word per line.
      // table-layout:fixed is what makes the column widths obeyed; with auto
      // layout the browser widens the column and the fixture proves nothing.
      // The explicit line-height matters too: an unstyled page computes
      // `normal`, which is not a number, and the rule skips cells it cannot
      // measure in lines.
      const row = (cell) =>
        '<main><table style="table-layout:fixed;width:380px;line-height:20px">' +
        `<tbody><tr>${cell}</tr></tbody></table></main>`;
      await page.setContent(
        row(`<td style="width:60px">${'word '.repeat(60)}</td><td style="width:320px">short</td>`),
      );
      const tall = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      await page.setContent(row('<td style="width:200px">a short cell</td><td>another</td>'));
      const short = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      if (tall.tallCells.length === 0) {
        failures.push(`tall-cell detector did not fire on a cell far over ${maxCellLines} lines`);
      }
      if (short.tallCells.length > 0) {
        failures.push(`tall-cell detector fired on a one-line cell (${short.tallCells[0]?.lines} lines)`);
      }

      // The wide-table rule, both ways. The overflowing fixture reproduces the
      // regression that motivated it: content that cannot wrap, inside a
      // container narrower than it needs.
      const inHolder = (style, cell) =>
        `<main><div style="width:300px;overflow-x:auto">` +
        `<table style="${style}"><tbody><tr>${cell}</tr></tbody></table></div></main>`;
      await page.setContent(
        inHolder('line-height:20px', '<td><code style="white-space:nowrap">' + 'x'.repeat(160) + '</code></td>'),
      );
      const wideTable = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      await page.setContent(inHolder('line-height:20px;width:200px', '<td>fits</td>'));
      const narrowTable = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      if (wideTable.wideTables.length === 0) {
        failures.push('wide-table detector did not fire on a table that overflows its container');
      }
      if (narrowTable.wideTables.length > 0) {
        failures.push('wide-table detector fired on a table that fits its container');
      }

      await page.setContent(
        '<main><div class="sl-markdown-content">' +
          '<p style="width:640px">prose</p><pre style="width:1000px">wide reference</pre>' +
          '</div></main>',
      );
      const measures = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
      if (measures.widestProse !== 640) {
        failures.push(`prose-width detector returned ${measures.widestProse}, expected 640`);
      }

      await page.setViewportSize({ width: 1200, height: 900 });
      await page.setContent(
        '<main><div class="content-panel" style="width:1000px">' +
          '<div class="sl-container" style="width:800px;margin-inline:auto">' +
          '<div class="page-heading" style="width:600px;margin-inline:0 auto">heading</div>' +
          '<div class="sl-markdown-content"><p style="width:600px;margin-inline:0 auto">prose</p></div>' +
          '</div></div></main>',
      );
      const alignedColumn = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      await page.setContent(
        '<main><div class="content-panel" style="width:1000px">' +
          '<div class="sl-container" style="width:800px;margin-inline:auto">' +
          '<div class="page-heading" style="width:600px;margin-inline:auto">heading</div>' +
          '<div class="sl-markdown-content"><p style="width:600px;margin-inline:auto">prose</p></div>' +
          '</div></div></main>',
      );
      const staggeredColumn = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      await page.setContent(
        '<main><div class="content-panel" style="width:1000px">' +
          '<div class="sl-container" style="width:800px;margin-inline:0">' +
          '<div class="page-heading" style="width:600px;margin-inline:0 auto">heading</div>' +
          '<div class="sl-markdown-content"><p style="width:600px;margin-inline:0 auto">prose</p></div>' +
          '</div></div></main>',
      );
      const offCenterShell = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      if (
        alignedColumn.proseLeftOffset > overflowTolerance ||
        alignedColumn.contentShellCenterOffset > overflowTolerance ||
        alignedColumn.pageHeadingLeftOffset > overflowTolerance
      ) {
        failures.push('reading-column detector rejected a centered shell with shared left edges');
      }
      if (
        staggeredColumn.proseLeftOffset <= overflowTolerance ||
        staggeredColumn.pageHeadingLeftOffset <= overflowTolerance
      ) {
        failures.push('reading-column detector accepted independently centered prose and page heading');
      }
      if (offCenterShell.contentShellCenterOffset <= overflowTolerance) {
        failures.push('reading-column detector accepted a content shell that is off center in its panel');
      }

      const wideViewport = { name: 'wide-desktop', width: 1920, height: 1080 };
      const ultraWideViewport = { name: 'ultra-wide', width: 2560, height: 1080 };
      const undersized = readingMeasureProblems(alignedColumn, alignmentSentinelRoute, wideViewport);
      if (!undersized.some((problem) => problem.includes('below the 958px minimum'))) {
        failures.push('60rem target detector accepted the old, narrower reading measure');
      }
      await page.setViewportSize({ width: wideViewport.width, height: wideViewport.height });
      const articleFixture = ({
        viewportWidth = wideViewport.width,
        shellWidth = 1120,
        contractWideWidth = 1120,
        frameWidth,
        contractFrameMinWidth = 1400,
        contractFrameGutterCap = 232,
        centerReadingColumn = false,
        centerShell = false,
        offsetFrame = false,
        frameLeftGutter,
        offsetToc = false,
      } = {}) => {
        const navigationWidth = 256;
        const tocWidth = 232;
        const contentPadding = 24;
        const availableWidth = viewportWidth - navigationWidth;
        const expectedFrameWidth = Math.min(
          availableWidth,
          Math.max(contractFrameMinWidth, availableWidth - 2 * contractFrameGutterCap),
        );
        const renderedFrameWidth = frameWidth ?? expectedFrameWidth;
        const readingMargin = centerReadingColumn ? 'auto' : '0 auto';
        const headingWidth = centerReadingColumn ? shellWidth : 960;
        const shellMargin = centerShell ? 'auto' : '0 auto';
        const frameMargin = offsetFrame
          ? 'margin-left:0;margin-right:auto'
          : frameLeftGutter == null
            ? 'margin-inline:auto'
            : `margin-left:${frameLeftGutter}px;margin-right:auto`;
        const mainPaneWidth = renderedFrameWidth - tocWidth;
        const tocPosition = offsetToc ? 'position:fixed;right:0;top:0;' : '';
        return '<style>' +
          `:root{--ptah-prose-width:960px;--sl-content-width:${contractWideWidth}px;` +
          `--sl-sidebar-width:${navigationWidth}px;--ptah-toc-width:${tocWidth}px;` +
          `--sl-content-pad-x:${contentPadding}px;` +
          `--ptah-doc-frame-min-width:${contractFrameMinWidth}px;` +
          `--ptah-doc-frame-gutter-cap:${contractFrameGutterCap}px}` +
          '*{box-sizing:border-box}body{margin:0}' +
          '</style>' +
          '<div data-has-sidebar data-has-toc>' +
          `<aside class="sidebar-pane" style="position:fixed;left:0;top:0;width:${navigationWidth}px;` +
          'height:1080px"></aside>' +
          `<div class="main-frame" style="position:absolute;left:${navigationWidth}px;top:0;` +
          `width:${availableWidth}px;height:1080px">` +
          `<div class="lg:sl-flex" style="display:flex;width:${renderedFrameWidth}px;${frameMargin}">` +
          `<aside class="right-sidebar-container" style="order:2;flex:0 0 ${tocWidth}px;` +
          `width:${tocWidth}px;height:300px">` +
          `<div class="right-sidebar" style="${tocPosition}width:${tocWidth}px;height:300px"></div></aside>` +
          `<div class="main-pane" style="order:1;flex:0 0 ${mainPaneWidth}px;width:${mainPaneWidth}px;height:300px">` +
          `<main><div class="content-panel" style="width:${mainPaneWidth}px;padding-inline:${contentPadding}px">` +
          `<div class="sl-container" style="width:${shellWidth}px;margin-inline:${shellMargin}">` +
          `<div class="page-heading" style="width:960px;margin-inline:${readingMargin}">heading</div>` +
          '</div></div>' +
          `<div class="content-panel" style="width:${mainPaneWidth}px;padding-inline:${contentPadding}px">` +
          `<div class="sl-container" style="width:${shellWidth}px;margin-inline:${shellMargin}">` +
          `<div class="sl-markdown-content" style="width:${shellWidth}px">` +
          `<p style="width:960px;margin-inline:${readingMargin}">prose</p>` +
          `<div class="sl-heading-wrapper" style="width:${headingWidth}px;margin-inline:${readingMargin}">` +
          '<h2>section</h2></div>' +
          `<div class="expressive-code" style="width:${shellWidth}px"><pre>code</pre></div>` +
          '</div></div></div></main></div></div></div></div>';
      };

      // This is the shipped regression: a 1120px Markdown shell and wide
      // children surrounded centered 960px prose and a centered 960px page
      // heading. Every block was individually centered, so the old check
      // accepted the visible 80px stagger on each side.
      await page.setContent(articleFixture({ centerReadingColumn: true }));
      const staggeredMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const staggeredProblems = readingMeasureProblems(
        staggeredMeasure,
        alignmentSentinelRoute,
        wideViewport,
      );
      if (!staggeredProblems.some((problem) => problem.includes('page heading does not share'))) {
        failures.push('article-alignment detector accepted the old page-heading stagger');
      }
      if (!staggeredProblems.some((problem) => problem.includes('ordinary paragraph does not share'))) {
        failures.push('article-alignment detector accepted the old paragraph stagger');
      }

      await page.setContent(articleFixture({ shellWidth: 960, contractWideWidth: 960 }));
      const missingWideMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const missingWideProblems = readingMeasureProblems(
        missingWideMeasure,
        alignmentSentinelRoute,
        wideViewport,
      );
      if (!missingWideProblems.some((problem) => problem.includes('--sl-content-width resolves to 960px'))) {
        failures.push('wide-content detector accepted a 60rem shell with no 70rem breakout');
      }

      await page.setContent(articleFixture({ offsetFrame: true }));
      const offsetFrameMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const offsetFrameProblems = readingMeasureProblems(
        offsetFrameMeasure,
        alignmentSentinelRoute,
        wideViewport,
      );
      if (!offsetFrameProblems.some((problem) => problem.includes('not centered within its capped outer gutters'))) {
        failures.push('document-frame detector accepted a frame pinned beside the navigation rail');
      }

      // A wider article pane alone does not justify left-aligning its shell.
      // The asymmetry is intentional only after both outer gutters reach the
      // declared cap.
      await page.setContent(articleFixture({ frameWidth: 1500 }));
      const prematureAsymmetryMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const prematureAsymmetryProblems = readingMeasureProblems(
        prematureAsymmetryMeasure,
        alignmentSentinelRoute,
        wideViewport,
      );
      if (!prematureAsymmetryProblems.some((problem) => problem.includes('content shell is 50px off center'))) {
        failures.push('reading-column detector accepted shell asymmetry before the outer gutters reached their cap');
      }

      await page.setContent(articleFixture({ offsetToc: true }));
      const offsetTocMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const offsetTocProblems = readingMeasureProblems(
        offsetTocMeasure,
        alignmentSentinelRoute,
        wideViewport,
      );
      if (!offsetTocProblems.some((problem) => problem.includes('TOC rail does not share'))) {
        failures.push('document-frame detector accepted a TOC rail pinned outside its moving container');
      }

      await page.setContent(articleFixture({ contractFrameMinWidth: 1500 }));
      const oversizedFrameMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const oversizedFrameProblems = readingMeasureProblems(
        oversizedFrameMeasure,
        alignmentSentinelRoute,
        wideViewport,
      );
      if (!oversizedFrameProblems.some((problem) => problem.includes('--ptah-doc-frame-min-width resolves to 1500px'))) {
        failures.push('document-frame detector accepted a self-consistent but oversized minimum-frame contract');
      }

      await page.setContent(articleFixture({ contractFrameGutterCap: 200 }));
      const wrongCapMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const wrongCapProblems = readingMeasureProblems(
        wrongCapMeasure,
        alignmentSentinelRoute,
        wideViewport,
      );
      if (!wrongCapProblems.some((problem) => problem.includes('--ptah-doc-frame-gutter-cap resolves to 200px'))) {
        failures.push('document-frame detector accepted the wrong outer-gutter cap contract');
      }

      // Before the cap, the accepted shape has one minimum-width centered
      // article-and-TOC frame. Its 60rem reading column and 70rem wide-content
      // shell share their left edge.
      await page.setContent(articleFixture());
      const alignedMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const alignedProblems = readingMeasureProblems(alignedMeasure, alignmentSentinelRoute, wideViewport);
      if (alignedProblems.length > 0) {
        failures.push(`article-frame detector rejected the aligned fixture: ${alignedProblems.join('; ')}`);
      }

      // The same fixture, judged as the column layout, has to be rejected.
      // Otherwise taking the layout as an axis bought a second pass over one
      // contract rather than a second contract (stokaro/ptah#2930).
      const underColumn = readingMeasureProblems(
        alignedMeasure,
        alignmentSentinelRoute,
        wideViewport,
        layouts[1],
      );
      if (!underColumn.some((problem) => problem.includes('in the column layout'))) {
        failures.push('the column layout accepted the envelope fixture, so the axis measures one contract twice');
      }

      await page.setViewportSize({ width: ultraWideViewport.width, height: ultraWideViewport.height });

      // The old layout kept the 1400px frame centered forever. At 2560px that
      // moves the prose 476px away from a 256px navigation rail and leaves the
      // extra width outside the frame instead of between the shell and TOC.
      await page.setContent(articleFixture({
        viewportWidth: ultraWideViewport.width,
        frameWidth: frameMinimum(layouts[0], wideViewport),
      }));
      const fixedCenteredMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const fixedCenteredProblems = readingMeasureProblems(
        fixedCenteredMeasure,
        alignmentSentinelRoute,
        ultraWideViewport,
      );
      if (!fixedCenteredProblems.some((problem) => problem.includes('frame renders 1400px wide'))) {
        failures.push('ultra-wide frame detector accepted the old fixed centered frame');
      }
      if (!fixedCenteredProblems.some((problem) => problem.includes('gap must not exceed'))) {
        failures.push('prose-gutter detector accepted the old unbounded centered frame');
      }

      // Merely pinning that same fixed frame at the left gutter cap is also a
      // false fix: it strands all remaining width after the TOC. The frame must
      // expand while its two outer gutters stay capped and balanced.
      await page.setContent(articleFixture({
        viewportWidth: ultraWideViewport.width,
        frameWidth: frameMinimum(layouts[0], wideViewport),
        frameLeftGutter: targetDocumentFrameGutterCap,
      }));
      const fixedLeftMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const fixedLeftProblems = readingMeasureProblems(
        fixedLeftMeasure,
        alignmentSentinelRoute,
        ultraWideViewport,
      );
      if (!fixedLeftProblems.some((problem) => problem.includes('frame renders 1400px wide'))) {
        failures.push('ultra-wide frame detector accepted a fixed frame pinned at the left gutter cap');
      }
      if (!fixedLeftProblems.some((problem) => problem.includes('not centered within its capped outer gutters'))) {
        failures.push('outer-gutter detector accepted a fake fixed left-pinned frame');
      }

      // The accepted ultra-wide shape keeps 232px outer frame gutters. Its
      // ContentPanel adds 24px before prose, producing the 256px navigation-
      // width cap, while all 440px of frame growth appears before the TOC.
      await page.setContent(articleFixture({ viewportWidth: ultraWideViewport.width }));
      const expandingMeasure = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const expandingProblems = readingMeasureProblems(
        expandingMeasure,
        alignmentSentinelRoute,
        ultraWideViewport,
      );
      if (expandingProblems.length > 0) {
        failures.push(`article-frame detector rejected the expanding fixture: ${expandingProblems.join('; ')}`);
      }

      await page.setContent('<main></main>');
      const missingLayout = await page.evaluate(measure, {
        tolerance: overflowTolerance,
        cellLineLimit: maxCellLines,
      });
      const missingProblems = readingMeasureProblems(missingLayout, alignmentSentinelRoute, wideViewport);
      if (!missingProblems.some((problem) => problem.includes('was not rendered'))) {
        failures.push('reading-measure detector accepted missing layout selectors');
      }
      const missingHomeProblems = readingMeasureProblems(missingLayout, '/', wideViewport);
      if (
        !missingHomeProblems.some((problem) => problem.includes('Markdown content container was not rendered')) ||
        !missingHomeProblems.some((problem) => problem.includes('content shell or content panel was not rendered'))
      ) {
        failures.push('reading-measure detector silently exempted the documentation home');
      }
      if (missingHomeProblems.some((problem) => problem.includes('page heading was not rendered'))) {
        failures.push('reading-measure detector required an article page heading from the splash home');
      }

      const fixtureBase = '/edge';
      const distRoot = writeFixtureDist(fixtureBase);
      try {
        const detected = detectBase(distRoot);
        if (detected !== fixtureBase) {
          failures.push(`base detection returned ${JSON.stringify(detected)}, expected ${fixtureBase}`);
        }
        if (htmlRoutes(distRoot).length !== 1) failures.push('route discovery did not find the fixture page');

        const watcher = watchStylesheets(page);

        // Correct base: the stylesheet resolves, so the guard stays quiet.
        const good = await startServer(distRoot, detected);
        watcher.reset();
        await page.goto(`http://127.0.0.1:${good.port}${detected}/`, { waitUntil: 'load' });
        if (watcher.failures().length > 0) {
          failures.push(`unstyled-page guard fired on a correctly served page: ${watcher.failures().join(', ')}`);
        }
        good.server.close();

        // Wrong base, which is the defect this guard exists for: the stylesheet
        // 404s and the page renders unstyled.
        const bad = await startServer(distRoot, '');
        watcher.reset();
        await page.goto(`http://127.0.0.1:${bad.port}/`, { waitUntil: 'load' });
        if (watcher.failures().length === 0) {
          failures.push('unstyled-page guard did not fire when every stylesheet 404s');
        }
        bad.server.close();
      } finally {
        rmSync(distRoot, { recursive: true, force: true });
      }
    } finally {
      await browser.close();
    }

    if (failures.length > 0) {
      console.error('check-responsive.mjs --selftest: FAILED');
      for (const failure of failures) console.error(`- ${failure}`);
      process.exitCode = 1;
      return;
    }
    console.log(
      'check-responsive.mjs --selftest: OK ' +
        '(overflow, document-frame, article-alignment, prose-width, wide-table, tall-cell, ' +
        'unstyled-page, and per-layout contract guards verified)',
    );
    return;
  }

  const distRoot = join(siteRoot, argValue('--dist', 'dist'));
  if (!existsSync(distRoot)) {
    console.error(`check-responsive.mjs: ${toPosix(relative(siteRoot, distRoot))} not found; run "npm run build" first.`);
    process.exitCode = 1;
    return;
  }

  const chromium = await loadChromium();
  if (!chromium) {
    unavailable('playwright is not installed');
    return;
  }

  const routes = htmlRoutes(distRoot);
  const base = detectBase(distRoot);
  const { server, port } = await startServer(distRoot, base);
  let browser;
  try {
    browser = await chromium.launch();
  } catch (error) {
    server.close();
    unavailable(`chromium failed to launch (${error.message.split('\n')[0]})`);
    return;
  }
  const errors = [];
  const routesWithTallCells = new Set();

  try {
    for (const layout of layouts) {
      for (const viewport of viewports) {
        const context = await browser.newContext({ viewport: { width: viewport.width, height: viewport.height } });
        const page = await context.newPage();
        const stylesheets = watchStylesheets(page);

        for (const route of routes) {
          stylesheets.reset();
          // The query is carried on every navigation rather than relied on once:
          // Head.astro also remembers the choice, and a pass that depended on
          // that would keep measuring after the mechanism it tests broke.
          await page.goto(`http://127.0.0.1:${port}${base}${route}${layout.query}`, { waitUntil: 'load' });
          const applied = await page.evaluate(() => document.documentElement.dataset.ptahLayout ?? 'envelope');
          if (applied !== layout.name) {
            errors.push(
              `${route} [${viewport.name}]: asked for the ${layout.name} layout and the page rendered ` +
                `${applied}; every measurement below would describe the wrong one`,
            );
            continue;
          }
          if (stylesheets.failures().length > 0) {
            errors.push(
              `${route} [${viewport.name}]: stylesheet request(s) failed ` +
                `(${stylesheets.failures().slice(0, 3).join(', ')}); the page rendered unstyled, ` +
                'so any overflow result would be meaningless',
            );
            continue;
          }
          const result = await page.evaluate(measure, { tolerance: overflowTolerance, cellLineLimit: maxCellLines });
          if (result.scrollWidth > result.clientWidth + overflowTolerance) {
            errors.push(
              `${route} [${layout.name}, ${viewport.name} ${viewport.width}px]: page scrolls horizontally ` +
                `(scrollWidth ${result.scrollWidth} > ${result.clientWidth})`,
            );
          }
          for (const offender of result.offenders) {
            errors.push(
              `${route} [${layout.name}, ${viewport.name} ${viewport.width}px]: <${offender.tag}${offender.className ? ` class="${offender.className}"` : ''}> ` +
                `extends to ${offender.right}px, past the ${result.clientWidth}px viewport`,
            );
          }
          errors.push(...readingMeasureProblems(result, route, viewport, layout));
          // Wide tables are measured under both layouts. The column layout's
          // shell is the article pane less its padding -- 720px at 1280, 884px
          // at the frame's cap -- and a table that needs more scrolls inside
          // `.ptah-wide-table` rather than losing its right-hand columns; the
          // generators that emit five-column tables wrap them (stokaro/ptah#2941).
          // This once measured the envelope only, while the column layout's
          // shell was a recorded gap.
          if (viewport.name !== 'mobile') {
            for (const table of result.wideTables) {
              errors.push(
                `${route}: a table is ${table.width}px wide inside a ${table.container}px container ` +
                  `(+${table.overflow}px), so its right-hand columns are cut off` +
                  (table.firstColumn ? ` — first column "${table.firstColumn}"` : ''),
              );
            }
          }
          // Desktop measures every table. Mobile measures the wide ones, where
          // the cap is a statement about the layout rather than about the phone.
          const capped =
            viewport.name !== 'mobile'
              ? result.tallCells
              : mobileCapExemptions.has(route)
                ? []
                : result.tallCells.filter((cell) => cell.columns >= wideTableColumns);
          if (capped.length > 0) {
            routesWithTallCells.add(route);
            if (!denseTableAllowlist.has(route)) {
              for (const cell of capped) {
                errors.push(
                  `${route}: a table cell renders ${cell.lines} lines tall in a ${cell.width}px column, ` +
                    `over the ${maxCellLines}-line limit — "${cell.text}"`,
                );
              }
            }
          }
        }
        await context.close();
      }
    }
  } finally {
    await browser.close();
    server.close();
  }

  // An allowlist entry that no longer suppresses anything is a claim about the
  // documentation that stopped being true. Failing here is what stops the list
  // from growing into a list of pages nobody checks.
  for (const [route, reason] of denseTableAllowlist) {
    if (!routes.includes(route)) {
      errors.push(`dense-table allowlist names ${route}, which is not a built route; remove the entry`);
      continue;
    }
    if (!routesWithTallCells.has(route)) {
      errors.push(
        `dense-table allowlist still exempts ${route} ("${reason}"), but no cell there exceeds ` +
          `${maxCellLines} lines; remove the entry so the page is checked`,
      );
    }
  }

  if (errors.length > 0) {
    console.error('Documentation responsive check failed:');
    for (const error of errors) console.error(`- ${error}`);
    console.error('\nWide content must scroll inside its own container, not the page.');
    console.error('A cell taller than a short paragraph belongs in a section with a heading.');
    process.exitCode = 1;
    return;
  }
  console.log(
    `check-responsive.mjs: OK (${routes.length} routes x ${viewports.length} viewports x ` +
      `${layouts.map(({ name }) => name).join(' and ')}, ` +
      `${layouts.map(({ name, prose, wideContent }) => `${name} ${prose}/${wideContent}px`).join(', ')} ` +
      'columns stay aligned, outer frame gutters stay capped and surplus width grows before the TOC, ' +
      `cells within ${maxCellLines} lines)`,
  );
}

await main();
