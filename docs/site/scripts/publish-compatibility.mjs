#!/usr/bin/env node
// Writes the operator compatibility matrix into the Pages root.
//
// The page is at the root rather than inside a version directory on purpose.
// Compatibility is current: which operator version works with which Ptah build
// changes when the operator measures something new, and a copy frozen into a
// CLI release would answer with whatever was true the day that CLI shipped.
// /compatibility/operator/ is therefore one address that always holds the
// present answer, beside the version folders rather than in one of them.
//
// The data is the committed derived copy. This deploy never reaches for
// stokaro/ptah-operator: an ordinary build of already-accepted documentation
// must not fail because a neighboring repository is unreachable.

import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { RootURL } from '../src/lib/docs-origin.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const defaultRepoRoot = join(scriptDir, '..', '..', '..');

// ROOT_PATH is where the page is served, relative to the Pages root.
export const ROOT_PATH = 'compatibility/operator/index.html';

// PAGE_URL is that address in full, built from the site's one declaration of
// its own origin rather than spelled again here.
export const PAGE_URL = RootURL('compatibility/operator/');

// DATA_PATH is the derived copy this page is rendered from.
export const DATA_PATH = 'docs/site/data/operator-compatibility.json';

const OPERATOR_SITE = 'https://operator.ptah.run';
const OPERATOR_REPO = 'https://github.com/stokaro/ptah-operator';

function escapeHTML(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// documentationLink is the address of one operator version's guide, or null.
//
// Null when the catalog says that version publishes none. A link to a page
// nobody built is worse than no link: it looks like the guide exists and
// answers 404.
export function documentationLink(entry) {
  return entry.documentation?.published ? `${OPERATOR_SITE}/${entry.operator}/` : null;
}

// verifiedCell renders what was measured, keeping the three states apart.
export function verifiedCell(entry) {
  const rows = entry.verified ?? [];
  if (rows.length === 0) {
    return {
      status: 'not verified',
      detail: entry.unverifiedReason ?? 'no reason recorded',
    };
  }
  const parts = rows.map((row) => {
    const name = row.ptahRelease ?? `${row.ptahDescribe} (${row.ptahCommit.slice(0, 12)})`;
    return `${name} — ${row.scope}`;
  });
  return {
    status: rows.some((row) => row.ptahRelease) ? 'verified' : 'verified, no release',
    detail: parts.join('<br />'),
  };
}

export function declaredCell(entry) {
  return entry.declared?.range ?? entry.declared?.statement ?? 'not stated';
}

export function render(derived) {
  const catalog = derived.catalog;
  const rows = (catalog.releases ?? []).map((entry) => {
    const link = documentationLink(entry);
    const verified = verifiedCell(entry);
    const guide = link
      ? `<a href="${escapeHTML(link)}">${escapeHTML(entry.operator)} guide</a>`
      : '<span class="none">not published</span>';
    const limitations = (entry.limitations ?? [])
      .map((limitation) => `<li>${escapeHTML(limitation)}</li>`)
      .join('');
    return `      <tr>
        <th scope="row">${escapeHTML(entry.operator)}</th>
        <td>${escapeHTML(declaredCell(entry))}</td>
        <td><span class="status ${escapeHTML(verified.status.split(',')[0])}">${escapeHTML(verified.status)}</span><br />${verified.detail}</td>
        <td>${guide}</td>
      </tr>
      <tr class="limitations">
        <td colspan="4">${limitations === '' ? 'No limitations recorded.' : `<ul>${limitations}</ul>`}</td>
      </tr>`;
  });

  const source = derived.source ?? {};
  const commit = escapeHTML(source.commit ?? 'unknown');
  const shortCommit = escapeHTML((source.commit ?? '').slice(0, 12) || 'unknown');

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Ptah Operator compatibility</title>
    <meta name="description" content="Which Ptah build each Ptah Operator version is declared to work with, and which have actually been verified." />
    <link rel="canonical" href="${escapeHTML(PAGE_URL)}" />
    <style>
      :root { color-scheme: light dark; --line: #8884; }
      body { margin: 0 auto; padding: 2rem 1.25rem 4rem; max-width: 60rem;
             font: 16px/1.6 system-ui, -apple-system, "Segoe UI", sans-serif; }
      h1 { font-size: 1.75rem; margin-bottom: 0.25rem; }
      p.lede { margin-top: 0; }
      table { border-collapse: collapse; width: 100%; margin: 1.5rem 0; }
      th, td { border-top: 1px solid var(--line); padding: 0.6rem 0.5rem; text-align: left; vertical-align: top; }
      thead th { border-top: 0; border-bottom: 2px solid var(--line); }
      tr.limitations td { border-top: 0; padding-top: 0; font-size: 0.9rem; opacity: 0.85; }
      tr.limitations ul { margin: 0; padding-left: 1.1rem; }
      .status { font-weight: 600; }
      .status.verified { color: #1a7f37; }
      .status\\ not { color: #9a6700; }
      .none { opacity: 0.7; }
      footer { border-top: 1px solid var(--line); margin-top: 2rem; padding-top: 1rem; font-size: 0.9rem; }
      code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    </style>
  </head>
  <body>
    <h1>Ptah Operator compatibility</h1>
    <p class="lede">${escapeHTML(catalog.axis ?? '')}</p>

    <p>
      Three claims are kept apart below, because a table that blurs them turns
      an untested combination into an unsupported one.
      <strong>Declared</strong> is a promise. <strong>Verified</strong> is a
      measurement, with what it covered. Anything absent from both is untested,
      which is not the same as incompatible.
    </p>

    <table>
      <thead>
        <tr>
          <th scope="col">Operator</th>
          <th scope="col">Declared Ptah support</th>
          <th scope="col">Verified</th>
          <th scope="col">Documentation</th>
        </tr>
      </thead>
      <tbody>
${rows.join('\n')}
      </tbody>
    </table>

    <footer>
      <p>
        This table is generated from
        <a href="${OPERATOR_REPO}/blob/${commit}/support/ptah.json"><code>support/ptah.json</code></a>
        in <a href="${OPERATOR_REPO}">stokaro/ptah-operator</a>, at commit
        <code>${shortCommit}</code>, retrieved ${escapeHTML(source.retrieved ?? 'unknown')}.
        That file is the canonical source; this page is a copy of it.
      </p>
      <p>
        It is published at a permanent address outside the per-version
        documentation archives, so it answers with the current catalog rather
        than with whatever was true when a Ptah release shipped. The operator's
        own guide lives at <a href="${OPERATOR_SITE}/">operator.ptah.run</a>.
      </p>
    </footer>
  </body>
</html>
`;
}

export function readDerived(repoRoot = defaultRepoRoot) {
  const path = join(repoRoot, DATA_PATH);
  if (!existsSync(path)) {
    throw new Error(`${DATA_PATH} is missing; the compatibility page has nothing to render`);
  }
  return JSON.parse(readFileSync(path, 'utf8'));
}

function selftest() {
  const derived = {
    source: { commit: '0123456789abcdef0123456789abcdef01234567', retrieved: '2026-09-12' },
    catalog: {
      axis: 'an axis',
      releases: [
        {
          operator: 'edge',
          documentation: { published: true, source: 'master' },
          declared: { range: null, statement: 'no range is claimed' },
          verified: [
            {
              ptahRelease: null,
              ptahCommit: '00fc362c943bfb9d0363d5890bf449a2a9b5e7cf',
              ptahDescribe: 'v0.3.0-201-g00fc362c9',
              scope: 'the lifecycle',
            },
          ],
          limitations: ['no released Ptah has been verified'],
        },
        {
          operator: 'v0.1.0',
          documentation: { published: false },
          declared: { range: '>=0.5.0', statement: '' },
          verified: [],
          unverifiedReason: 'the suite has not run against it',
        },
      ],
    },
  };
  const html = render(derived);
  if (!html.includes('v0.3.0-201-g00fc362c9')) throw new Error('the verified build is not shown');
  if (!html.includes('operator.ptah.run/edge/')) throw new Error('a published guide got no link');
  if (html.includes('operator.ptah.run/v0.1.0/')) throw new Error('an unpublished guide was linked anyway');
  if (!html.includes('not verified')) throw new Error('an unmeasured version was not marked');
  if (!html.includes('the suite has not run against it')) throw new Error('the missing check was not named');
  if (!html.includes('no range is claimed')) throw new Error('an absent range lost its statement');
  if (!html.includes('&gt;=0.5.0')) throw new Error('a declared range is missing or unescaped');
  if (documentationLink({ operator: 'x', documentation: { published: false } }) !== null) {
    throw new Error('an unpublished version produced a link');
  }
  console.log('publish-compatibility.mjs --selftest: OK (verified, absent, linked, unlinked, escaping)');
}

function main() {
  const arguments_ = process.argv.slice(2);
  if (arguments_.includes('--selftest')) {
    selftest();
    return;
  }
  const root = arguments_[0];
  if (!root) {
    console.error('usage: publish-compatibility.mjs <assembled-site-root>');
    process.exit(2);
  }
  const target = join(root, ROOT_PATH);
  mkdirSync(dirname(target), { recursive: true });
  writeFileSync(target, render(readDerived()));
  console.log(`publish-compatibility.mjs: wrote ${ROOT_PATH}`);
}

// Only when this file is the program. check-compatibility.mjs imports the two
// path constants from here, and an unguarded call would run the publisher --
// with that script's arguments -- as a side effect of the import.
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main();
}
