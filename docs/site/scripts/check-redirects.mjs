#!/usr/bin/env node
// Every declared redirect has to be a redirect that works: a source that is no
// longer a page, a target that is one, and no chains.
//
// Both halves of that comparison come from scripts/lib/docroutes.mjs -- the
// redirect map's one parser, and the live route set, enumerated through git.
// Neither is spelled again here. A walk of the content directory is what this
// gate used to ask, and a walk cannot tell this site's pages from a git
// worktree parked under the content root: measured, 68 pages by git against
// 209 by a walk, the extra 141 belonging to somebody else's branch, and the
// gate believing every one of them.
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { liveRoutes } from './lib/docroutes.mjs';
// One parser for the redirect map, shared with the gates that ask which
// retirements were declared. See the header of scripts/lib/docroutes.mjs.
import { parseRedirectRoutes } from './lib/docroutes.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const routePattern = /^\/(?:[a-z0-9-]+\/)+$|^\/$/;

export function checkRedirects(root, configSource) {
  const errors = [];
  const entries = parseRedirectRoutes(configSource);

  if (entries === null) {
    return { errors: ['astro.config.mjs: redirectRoutes map not found; moved pages must keep their redirect entries'], count: 0 };
  }

  const routes = liveRoutes(root);
  const sources = new Set(entries.map(([from]) => from));

  for (const [from, to] of entries) {
    if (!routePattern.test(from)) {
      errors.push(`redirect source ${from} is not a /segment/ route with leading and trailing slashes`);
    }
    if (!routePattern.test(to)) {
      errors.push(`redirect target ${to} is not a /segment/ route with leading and trailing slashes`);
      continue;
    }
    if (from === to) {
      errors.push(`redirect ${from} points to itself`);
      continue;
    }
    if (routes.has(from)) {
      errors.push(`redirect source ${from} still exists as a content page; remove the page or the redirect`);
    }
    if (!routes.has(to)) {
      errors.push(`redirect ${from} targets ${to}, which is not an existing route`);
    }
    if (sources.has(to)) {
      errors.push(`redirect ${from} targets ${to}, which is itself a redirect source (chain)`);
    }
  }

  return { errors, count: entries.length };
}

function writeDoc(root, name, content) {
  const file = join(root, name);
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, content);
}

function configWith(body) {
  return `const redirectRoutes = {\n${body}\n};\n`;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const tmp = mkdtempSync(join(tmpdir(), 'ptah-doc-redirects-'));
  const root = join(tmp, 'src', 'content', 'docs');

  try {
    // The fixture is a real checkout because the live routes are git's answer,
    // not a walk's. A plain directory would make this selftest exercise a
    // discovery path the gate no longer has.
    execFileSync('git', ['-c', 'init.defaultBranch=main', 'init', '-q', tmp], { stdio: ['ignore', 'pipe', 'pipe'] });
    writeDoc(root, 'index.mdx', '---\ntitle: Home\n---\n');
    writeDoc(root, 'start/quick-start.md', '---\ntitle: Quick start\n---\n');
    writeDoc(root, 'lingering.md', '---\ntitle: Lingering\n---\n');

    const ok = checkRedirects(root, configWith("  '/getting-started/': '/start/quick-start/',"));
    assert(ok.errors.length === 0, `expected valid redirect to pass, got ${ok.errors.join('; ')}`);
    assert(ok.count === 1, `expected 1 parsed redirect, got ${ok.count}`);

    const missingTarget = checkRedirects(root, configWith("  '/getting-started/': '/start/missing/',"));
    assert(missingTarget.errors.some((error) => error.includes('not an existing route')), 'catches missing target route');

    const liveSource = checkRedirects(root, configWith("  '/lingering/': '/start/quick-start/',"));
    assert(liveSource.errors.some((error) => error.includes('still exists as a content page')), 'catches source that is still a page');

    const chain = checkRedirects(
      root,
      configWith("  '/a/': '/getting-started/',\n  '/getting-started/': '/start/quick-start/',"),
    );
    assert(chain.errors.some((error) => error.includes('redirect source (chain)')), 'catches redirect chains');

    const selfTarget = checkRedirects(root, configWith("  '/getting-started/': '/getting-started/',"));
    assert(selfTarget.errors.some((error) => error.includes('points to itself')), 'catches self-redirects');

    const malformed = checkRedirects(root, configWith("  '/getting-started/': 'start/quick-start',"));
    assert(malformed.errors.some((error) => error.includes('leading and trailing slashes')), 'catches malformed routes');

    const absent = checkRedirects(root, 'export default {};\n');
    assert(absent.errors.some((error) => error.includes('redirectRoutes map not found')), 'catches missing redirect map');

    console.log('check-redirects.mjs --selftest: OK');
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }

  const docsRoot = join(siteRoot, 'src', 'content', 'docs');
  const configPath = join(siteRoot, 'astro.config.mjs');

  if (!existsSync(docsRoot) || !existsSync(configPath)) {
    console.error(`error: expected ${docsRoot} and ${configPath} to exist`);
    process.exitCode = 2;
    return;
  }

  let errors;
  let count;
  try {
    ({ errors, count } = checkRedirects(docsRoot, readFileSync(configPath, 'utf8')));
  } catch (error) {
    // docroutes refuses an input it cannot answer for rather than handing back
    // a shorter list, and a refusal is exit 2 the way a missing content root
    // already is: nothing was compared, so nothing can be reported.
    console.error(`check-redirects.mjs: ${error.message}`);
    process.exitCode = 2;
    return;
  }

  if (errors.length > 0) {
    console.error('Redirect check failed:');
    for (const error of errors) {
      console.error(`- ${error}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(`check-redirects.mjs: OK (${count} redirects)`);
}

// Importing this file must not run the check. It exports checkRedirects for
// another gate to reuse, and until this guard existed that import printed
// `check-redirects.mjs: OK (30 redirects)` as a side effect and could set an
// exit code its importer never asked for.
const invokedPath = process.argv[1];
if (invokedPath !== undefined && import.meta.url === pathToFileURL(invokedPath).href) {
  main();
}
