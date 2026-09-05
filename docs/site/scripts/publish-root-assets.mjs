#!/usr/bin/env node
// Writes the files that belong at the GitHub Pages ROOT rather than inside a
// versioned documentation directory.
//
// The Pages root holds one directory per documentation version plus a small
// number of files that address the site as a whole. gen-versions.mjs writes two
// of them, versions.json and index.html. This script writes the rest: the
// install scripts the documentation tells a reader to pipe into a shell, which
// have to answer at a stable address that carries no version in it.
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
import { InstallURL, RootURL } from '../src/lib/docs-origin.mjs';

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
// `advertised` is the address a reader is given, on the project site, which
// fetches the same source from master; `published` is the command built from
// it. The gate requires the documentation to name each asset at one address or
// the other, so an asset nobody can reach and a command nothing serves are
// both findings.
export const ROOT_ASSETS = [
  {
    name: 'install.sh',
    source: 'docs/site/public/install.sh',
    url: RootURL('install.sh'),
    advertised: InstallURL('install.sh'),
    published: `curl -fsSL ${InstallURL('install.sh')} | sh`,
  },
  {
    name: 'install.ps1',
    source: 'docs/site/public/install.ps1',
    url: RootURL('install.ps1'),
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
    published: RootURL('ptah-annotations.schema.json'),
  },
];

// GENERATED_ROOT_FILES names what gen-versions.mjs writes into the same
// directory. This script does not write them, and says so here because the two
// lists together are the whole Pages root: check-pages-root.mjs compares that
// union against what an assembly actually produces.
export const GENERATED_ROOT_FILES = ['versions.json', 'index.html'];

export function sourcePath(asset, repoRoot = defaultRepoRoot) {
  return join(repoRoot, asset.source);
}

// publish copies every root asset into siteDir and returns what it wrote.
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
    assert(written.length === ROOT_ASSETS.length, `publish wrote ${written.length} of ${ROOT_ASSETS.length}`);
    for (const asset of ROOT_ASSETS) {
      assert(existsSync(join(site, asset.name)), `${asset.name} is not in the site root`);
      assert(
        readFileSync(join(site, asset.name), 'utf8') === `# ${asset.name}\n`,
        `${asset.name} was copied with different bytes`,
      );
    }

    // Every asset carries a name, a source under docs/site/public, a URL on the
    // Pages root, the address a reader is given, and the command built from it.
    for (const asset of ROOT_ASSETS) {
      assert(asset.source.startsWith('docs/site/public/'), `${asset.name} is sourced from ${asset.source}`);
      assert(asset.source.endsWith(`/${asset.name}`), `${asset.name} does not match its source path`);
      assert(asset.url === RootURL(asset.name), `${asset.name} has an unexpected URL`);
      assert(asset.advertised === InstallURL(asset.name), `${asset.name} has an unexpected advertised address`);
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
