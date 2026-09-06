#!/usr/bin/env node
// Rewrites a released worktree's published address to the current one.
//
// The deploy builds every released tag from that tag's own source, so a tag's
// `astro.config.mjs` decides the address of the pages it emits. When the site
// moved to its own apex domain, those constants stayed where they were: the
// version directory was published at the root of the new host while every page
// inside it asked for its stylesheets under `/ptah/`, and the apex stub sent
// readers to a path that did not exist (stokaro/ptah#2884).
//
// This is deliberately NOT part of the release UI overlay. That overlay is
// version-gated -- it declines anything below its `minimumRelease` and the
// deploy carries on with the historical chrome -- and four of the five released
// tags fall below it. Chrome is a thing a historical build may reasonably keep;
// an address that resolves to nothing is not, so this runs unconditionally on
// every version the deploy builds.
//
// What it rewrites is the two constants and nothing else. Copying the current
// `astro.config.mjs` wholesale would carry search ranking and `lastUpdated`
// into a release that never had them, which is a content change wearing an
// address fix's clothes.
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { Origin, BasePath } from '../src/lib/docs-origin.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));

// Both anchors are byte-identical across every tag released BEFORE the address
// became derived: v0.1.0, v0.1.1, v0.1.2, v0.2.0 and v0.3.0. They are matched
// exactly rather than loosely, because the same edit applied to nothing leaves
// the file unchanged and reports success.
//
// A tag released AFTER stokaro/ptah#2884 carries no such literal at all -- its
// config reads the address from `src/lib/docs-origin.mjs`, so it already builds
// at the current one and there is nothing here to rewrite. That is a different
// state from "the anchor moved", and conflating the two is what made the v0.4.0
// documentation fail to build (stokaro/ptah#2947): the guard refused a tag whose
// only fault was postdating the fix this script exists to backfill.
const SITE_ANCHOR = /^const site = '[^']*';$/m;
const BASE_ANCHOR = /^const base = `[^`]*`;$/m;

// The replacement keeps `DOCS_VERSION` an expression in the target file, so the
// rewritten config still builds any version the deploy hands it.
const VERSION_EXPRESSION = '${DOCS_VERSION}';

// A config that imports the declaration derives both constants from it, so it
// names the current address by construction. This is an exact marker rather
// than a guess about the file's shape: the import either is there or it is not,
// and no tag can carry it while still holding a literal, because the change
// that added the import is the change that removed them.
const DERIVES_THE_ADDRESS = /^import \{[^}]*\} from '\.\/src\/lib\/docs-origin\.mjs';$/m;

// alreadyDerived reports whether a target needs no rewrite at all.
export function alreadyDerived(source) {
  return DERIVES_THE_ADDRESS.test(source);
}

export function rewriteConfig(source) {
  if (alreadyDerived(source)) {
    return source;
  }
  const siteMatches = source.match(new RegExp(SITE_ANCHOR, 'gm')) ?? [];
  const baseMatches = source.match(new RegExp(BASE_ANCHOR, 'gm')) ?? [];
  if (siteMatches.length !== 1) {
    throw new Error(`expected exactly one \`const site\` declaration, found ${siteMatches.length}`);
  }
  if (baseMatches.length !== 1) {
    throw new Error(`expected exactly one \`const base\` declaration, found ${baseMatches.length}`);
  }
  // The exactly-once counts above are the anchor guard. There is deliberately
  // no "the text changed" check beside them: a config already carrying the
  // current address rewrites to itself, and refusing that would make the
  // rewrite unsafe to re-run for the sake of a weaker version of a test that
  // has already passed.
  return source
    .replace(SITE_ANCHOR, `const site = '${Origin}';`)
    .replace(BASE_ANCHOR, `const base = \`${BasePath(VERSION_EXPRESSION)}\`;`);
}

function selftest() {
  const released = [
    "import { defineConfig } from 'astro/config';",
    '',
    "const site = 'https://example.invalid';",
    "const DOCS_VERSION = process.env.DOCS_VERSION || 'edge';",
    'const base = `/ptah/${DOCS_VERSION}/`;',
    '',
    'export default defineConfig({ site, base });',
  ].join('\n');

  const rewritten = rewriteConfig(released);
  assert(rewritten.includes(`const site = '${Origin}';`), 'the origin was not rewritten');
  assert(
    rewritten.includes('const base = `/${DOCS_VERSION}/`;'),
    'the base was not rewritten to the apex layout',
  );
  assert(!rewritten.includes('/ptah/'), 'the retired path prefix survived the rewrite');
  assert(
    rewritten.includes("const DOCS_VERSION = process.env.DOCS_VERSION || 'edge';"),
    'the version expression was disturbed',
  );

  // A tag released after the address became derived. This is the shape that
  // broke the v0.4.0 documentation build: the config carries no literal to
  // anchor on, and refusing it treated "nothing to do" as "the anchor moved".
  const derived = [
    "import { defineConfig } from 'astro/config';",
    "import { Origin, BasePath } from './src/lib/docs-origin.mjs';",
    '',
    'const site = Origin;',
    "const DOCS_VERSION = process.env.DOCS_VERSION || 'edge';",
    'const base = BasePath(DOCS_VERSION);',
    '',
    'export default defineConfig({ site, base });',
  ].join('\n');
  assert(alreadyDerived(derived), 'a config importing the declaration is not recognized');
  assert(rewriteConfig(derived) === derived, 'a derived config was rewritten');

  // The control for that: a config with neither the import nor the anchors is
  // still refused, so the acceptance above is about the import rather than
  // about giving up on files this cannot parse.
  assertThrows(
    () => rewriteConfig("const nothing = true;\n"),
    'found 0',
    'a config with neither the import nor the anchors',
  );

  // Applying it twice is the same as applying it once. The deploy builds a
  // fresh worktree per tag, but a rewrite that drifted on a second pass would
  // be a rewrite nobody could safely re-run.
  assert(rewriteConfig(rewritten) === rewritten, 'the rewrite is not idempotent');

  // A file whose anchors have moved must fail rather than pass unchanged. This
  // is the branch that matters: an inline replace with no match rewrites
  // nothing and reports success, which is how an address fix comes to be
  // believed while every page still carries the old one.
  assertThrows(
    () => rewriteConfig("const site = 'https://example.invalid';\n"),
    'found 0',
    'a config with no base declaration',
  );
  assertThrows(
    () => rewriteConfig('const base = `/ptah/${DOCS_VERSION}/`;\n'),
    'found 0',
    'a config with no site declaration',
  );
  assertThrows(
    () =>
      rewriteConfig(
        ["const site = 'https://a.invalid';", "const site = 'https://b.invalid';", 'const base = `/x/`;'].join('\n'),
      ),
    'found 2',
    'a config declaring the origin twice',
  );

  console.log(
    'apply-docs-address.mjs --selftest: OK (both anchors rewritten, a derived config left alone,\n' +
      '  idempotent, and four unmatched shapes refused)',
  );
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function assertThrows(run, expected, label) {
  let threw = false;
  try {
    run();
  } catch (error) {
    threw = true;
    assert(
      String(error.message).includes(expected),
      `${label} failed for the wrong reason: ${error.message}`,
    );
  }
  assert(threw, `${label} was accepted`);
}

function main(argv) {
  if (argv.includes('--selftest')) {
    selftest();
    return;
  }
  const targetIndex = argv.indexOf('--target');
  if (targetIndex === -1 || !argv[targetIndex + 1]) {
    console.error('usage: node scripts/apply-docs-address.mjs --target <worktree> | --selftest');
    process.exit(2);
  }
  const configPath = join(argv[targetIndex + 1], 'docs', 'site', 'astro.config.mjs');
  const source = readFileSync(configPath, 'utf8');
  writeFileSync(configPath, rewriteConfig(source), 'utf8');
  console.log(`docs address: OK (${configPath} publishes at ${Origin}${BasePath('<version>')})`);
}

main(process.argv.slice(2));
