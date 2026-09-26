#!/usr/bin/env node
// Writes the files that belong at the GitHub Pages ROOT rather than inside a
// versioned documentation directory.
//
// The Pages root holds one directory per documentation version plus a small
// number of files that address the site as a whole. gen-versions.mjs writes two
// of them, versions.json and index.html. This script writes the rest: the
// install scripts the documentation tells a reader to pipe into a shell, which
// have to answer at a stable address that carries no version in it, and a
// redirect page for each root address whose content another site publishes.
//
// The deploy assembles `_site/` from scratch on every run and uploads it whole,
// so there is no incremental Pages state to inherit a file from. A root file
// exists after a deploy only because that deploy wrote it, which is why this is
// a step the workflow runs rather than a file somebody once uploaded.
//
// Sources live under docs/site/public/ rather than in the repository's
// scripts/ directory, and the reason is the workflow's own filters:
// .github/workflows/docs.yml runs on `docs/**` for its style job and on
// `docs/site/**` for its build and deploy jobs. A file under scripts/ would
// change the installer without running the workflow on the pull request and
// without deploying on merge. Astro also copies public/ into each version's
// dist/, so the same bytes appear at /<version>/install.sh; that copy is
// harmless and is not the published address.
import { InstallURL, OperatorOrigin, RootURL } from '../src/lib/docs-origin.mjs';

import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const defaultRepoRoot = join(scriptDir, '..', '..', '..');

// ROOT_ASSETS is the declaration both this script and check-pages-root.mjs
// read. `url` is where this deploy serves the file, at the documentation root;
// `advertised` is the address a reader is given; `published` is the command
// built from it. The gate requires the documentation to name each asset at one
// address or the other, so an asset nobody can reach and a command nothing
// serves are both findings.
//
// `onProjectSite` says which of two addresses a reader is given, and it is a
// property of the asset rather than a rule about all of them. The project site
// fetches `install.sh` and `install.ps1` from master by name, so those two are
// advertised there; a third root file is not on that site, and advertising it
// at an address nothing serves is the failure this whole file exists to
// refuse. Everything else is advertised where this deploy serves it.
//
// Stated as a field rather than inferred, because the two assets that are on
// that site and the one that is not are indistinguishable from here otherwise
// -- and a rule that assumed all of them were is what made the trunk red when
// the schema and the project site landed in the same week
// (stokaro/ptah#2889, stokaro/ptah#2887).
export const ROOT_ASSETS = [
  {
    name: 'install.sh',
    source: 'docs/site/public/install.sh',
    url: RootURL('install.sh'),
    onProjectSite: true,
    advertised: InstallURL('install.sh'),
    published: `curl -fsSL ${InstallURL('install.sh')} | sh`,
  },
  {
    name: 'install.ps1',
    source: 'docs/site/public/install.ps1',
    url: RootURL('install.ps1'),
    onProjectSite: true,
    advertised: InstallURL('install.ps1'),
    published: `irm ${InstallURL('install.ps1')} | iex`,
  },
  {
    // The annotation JSON Schema, published at the address it declares as its
    // own `$id`. An identifier need not resolve to be valid, but a URL-shaped
    // one is what an editor fetches, and this one answered 404 under both the
    // current host and the retired one (stokaro/ptah#2889).
    //
    // Flat, at the root, rather than under a `schemas/` path: the documentation
    // cross-check below collects only root paths with no slash, so a nested
    // asset would be published without that rule covering it.
    name: 'ptah-annotations.schema.json',
    source: 'docs/site/public/ptah-annotations.schema.json',
    url: RootURL('ptah-annotations.schema.json'),
    onProjectSite: false,
    advertised: RootURL('ptah-annotations.schema.json'),
    published: RootURL('ptah-annotations.schema.json'),
  },
  // The version picker, one copy for every documentation version. Each version
  // carries only a mount point (src/components/VersionPicker.astro) that loads
  // these two from the root, so the picker a released version shows is the
  // one this deploy publishes, not the one its tag was built with.
  {
    name: 'version-picker.js',
    source: 'docs/site/public/version-picker.js',
    url: RootURL('version-picker.js'),
    onProjectSite: false,
    advertised: RootURL('version-picker.js'),
    published: RootURL('version-picker.js'),
  },
  {
    name: 'version-picker.css',
    source: 'docs/site/public/version-picker.css',
    url: RootURL('version-picker.css'),
    onProjectSite: false,
    advertised: RootURL('version-picker.css'),
    published: RootURL('version-picker.css'),
  },
];

// ROOT_REDIRECTS are root addresses whose content another site publishes. Each
// one gets a small page at its own address that sends the reader on, so a link
// already in the wild keeps answering. `path` is the address relative to the
// site root, with a trailing slash; the page is written to its index.html.
//
// A redirect is declared rather than documented: no page of this site names
// these addresses, and check-pages-root.mjs requires only that the assembly
// writes each page and that the page sends the reader to its target.
export const ROOT_REDIRECTS = [
  {
    // The operator's compatibility with Ptah is the operator's claim, and the
    // operator publishes it on its own site (stokaro/ptah#3708). The target is
    // the page in the operator's edge documentation, the version that tracks
    // its current claim.
    path: 'compatibility/operator/',
    target: `${OperatorOrigin}/edge/support/ptah/`,
    title: 'Ptah Operator compatibility',
  },
];

// redirectFile is the file, relative to the site root, that serves one
// redirect's address.
export function redirectFile(redirect) {
  return `${redirect.path}index.html`;
}

// renderRedirect is the page written at one redirect's address. The refresh
// and the script send a browser on, the canonical link names the address that
// holds the content, and the plain link is there for a reader whose browser
// follows neither.
//
// The target is refused rather than escaped when it carries a character HTML
// or a script literal would need to quote: it is a declared constant, and one
// that needs quoting is a typo.
export function renderRedirect(redirect) {
  const { target, title } = redirect;
  let url;
  try {
    url = new URL(target);
  } catch {
    throw new Error(`redirect target for ${redirect.path} is not a URL: ${target}`);
  }
  if (url.protocol !== 'https:' || /["'<>&\\\s]/.test(target)) {
    throw new Error(`redirect target for ${redirect.path} must be a plain https URL: ${target}`);
  }
  if (!title || /["<>&]/.test(title)) {
    throw new Error(`redirect title for ${redirect.path} must be plain text: ${title}`);
  }
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="0; url=${target}" />
    <link rel="canonical" href="${target}" />
    <title>${title}</title>
    <script>location.replace(${JSON.stringify(target)});</script>
  </head>
  <body>
    <p>${title} is published at <a href="${target}">${target}</a>.</p>
  </body>
</html>
`;
}

// redirectTarget reads the address a redirect page sends a browser to, or
// null when the page carries no refresh. check-pages-root.mjs holds every
// assembled and every published redirect page to it.
export function redirectTarget(html) {
  const match = /<meta http-equiv="refresh" content="0; url=([^"]+)" \/>/.exec(html);
  return match ? match[1] : null;
}

// GENERATED_ROOT_FILES names what the other producer writes into the same
// directory: gen-versions.mjs writes the version index and the apex stub. This
// script does not write either, and says so here because this list, the assets
// and the redirects together are the whole Pages root: check-pages-root.mjs
// compares that union against what an assembly actually produces.
export const GENERATED_ROOT_FILES = [
  'versions.json',
  'index.html',
];

// advertisedAddress is where a reader is told to find one root asset. It is
// derived from the asset's own declaration so that the table and the check
// cannot disagree: writing the address twice is what let a third asset carry
// none at all.
export function advertisedAddress(asset) {
	return asset.onProjectSite ? InstallURL(asset.name) : RootURL(asset.name);
}

export function sourcePath(asset, repoRoot = defaultRepoRoot) {
  return join(repoRoot, asset.source);
}

// publish copies every root asset into siteDir, writes every redirect page, and
// returns what it wrote.
//
// A missing or empty source throws rather than being skipped. The deploy step
// that calls this is the last chance to notice: past it, the artifact uploads
// and the documented address starts answering 404 with nothing red anywhere.
export function publish(siteDir, repoRoot = defaultRepoRoot) {
  mkdirSync(siteDir, { recursive: true });

  const written = [];
  for (const asset of ROOT_ASSETS) {
    const from = sourcePath(asset, repoRoot);
    let size;
    try {
      size = statSync(from).size;
    } catch {
      throw new Error(`root asset source is missing: ${asset.source}`);
    }
    if (size === 0) {
      throw new Error(`root asset source is empty: ${asset.source}`);
    }
    const to = join(siteDir, asset.name);
    copyFileSync(from, to);
    written.push(asset.name);
  }
  for (const redirect of ROOT_REDIRECTS) {
    const to = join(siteDir, redirectFile(redirect));
    mkdirSync(dirname(to), { recursive: true });
    writeFileSync(to, renderRedirect(redirect));
    written.push(redirectFile(redirect));
  }
  return written;
}

function selftest() {
  const assert = (condition, message) => {
    if (!condition) throw new Error(message);
  };

  const tmp = mkdtempSync(join(tmpdir(), 'ptah-root-assets-'));
  try {
    // A stand-in repository whose sources this run copies, so the self-test
    // exercises the copy rather than the checkout it happens to run in.
    const fakeRepo = join(tmp, 'repo');
    for (const asset of ROOT_ASSETS) {
      const target = join(fakeRepo, asset.source);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, `# ${asset.name}\n`);
    }

    const site = join(tmp, 'site');
    const written = publish(site, fakeRepo);
    const expected = ROOT_ASSETS.length + ROOT_REDIRECTS.length;
    assert(written.length === expected, `publish wrote ${written.length} of ${expected}`);
    for (const asset of ROOT_ASSETS) {
      assert(existsSync(join(site, asset.name)), `${asset.name} is not in the site root`);
      assert(
        readFileSync(join(site, asset.name), 'utf8') === `# ${asset.name}\n`,
        `${asset.name} was copied with different bytes`,
      );
    }

    // Every redirect page is written at its own address, and its refresh, its
    // canonical link and its plain link all name its target.
    assert(ROOT_REDIRECTS.length > 0, 'the declaration holds no redirect, so nothing below is checked');
    for (const redirect of ROOT_REDIRECTS) {
      assert(
        !redirect.path.startsWith('/') && redirect.path.endsWith('/'),
        `${redirect.path} is not a root-relative address with a trailing slash`,
      );
      const file = join(site, redirectFile(redirect));
      assert(existsSync(file), `${redirectFile(redirect)} is not in the site root`);
      const html = readFileSync(file, 'utf8');
      assert(redirectTarget(html) === redirect.target, `${redirectFile(redirect)} refreshes to ${redirectTarget(html)}`);
      assert(html.includes(`<link rel="canonical" href="${redirect.target}" />`), `${redirect.path} has no canonical link`);
      assert(html.includes(`<a href="${redirect.target}">`), `${redirect.path} has no plain link`);
    }

    // Pinned by name, because check-pages-root.mjs reads this same declaration:
    // an entry deleted here is one it stops expecting, and the published
    // address would answer 404 with every gate green.
    const operator = ROOT_REDIRECTS.find((redirect) => redirect.path === 'compatibility/operator/');
    assert(operator?.target === `${OperatorOrigin}/edge/support/ptah/`, 'compatibility/operator/ does not send the reader to the operator');

    // A page without a refresh has no target, and a target that would need
    // quoting is refused rather than written.
    assert(redirectTarget('<!doctype html>\n<p>nothing</p>\n') === null, 'a page with no refresh has no target');
    for (const target of ['http://example.com/', 'https://example.com/"><script>', 'not a url']) {
      let refused = false;
      try {
        renderRedirect({ path: 'x/', target, title: 'X' });
      } catch {
        refused = true;
      }
      assert(refused, `the redirect target ${target} must be refused`);
    }

    // Every asset carries a name, a source under docs/site/public, a URL on the
    // Pages root, the address a reader is given, and the command built from it.
    for (const asset of ROOT_ASSETS) {
      assert(asset.source.startsWith('docs/site/public/'), `${asset.name} is sourced from ${asset.source}`);
      assert(asset.source.endsWith(`/${asset.name}`), `${asset.name} does not match its source path`);
      assert(asset.url === RootURL(asset.name), `${asset.name} has an unexpected URL`);
      assert(
        asset.advertised === advertisedAddress(asset),
        `${asset.name} has an unexpected advertised address`,
      );
      assert(
        asset.published.includes(asset.advertised),
        `${asset.name}'s published command does not use its advertised address`,
      );
    }

    // An empty source is refused, not copied. A zero-byte install.sh would
    // otherwise deploy and every documented command would silently do nothing.
    writeFileSync(join(fakeRepo, ROOT_ASSETS[0].source), '');
    let refusedEmpty = false;
    try {
      publish(join(tmp, 'site-empty'), fakeRepo);
    } catch (error) {
      refusedEmpty = /is empty/.test(error.message);
    }
    assert(refusedEmpty, 'an empty root asset must be refused');

    // A missing source is refused too.
    rmSync(join(fakeRepo, ROOT_ASSETS[0].source));
    let refusedMissing = false;
    try {
      publish(join(tmp, 'site-missing'), fakeRepo);
    } catch (error) {
      refusedMissing = /is missing/.test(error.message);
    }
    assert(refusedMissing, 'a missing root asset must be refused');

    console.log('publish-root-assets.mjs --selftest: OK');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function main() {
  const arg = process.argv[2];
  if (arg === '--selftest') {
    selftest();
    return;
  }
  if (!arg) {
    console.error('usage: node scripts/publish-root-assets.mjs <site-dir> | --selftest');
    process.exitCode = 2;
    return;
  }
  const written = publish(arg);
  console.log(`wrote ${written.join(', ')} into ${arg}`);
}

// Only when this file is the program. check-pages-root.mjs imports the
// declaration above, and a module that runs its CLI on import would print a
// usage line and set a failing exit code inside its own gate.
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main();
}
