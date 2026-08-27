#!/usr/bin/env node
// check-route-retirement: a route this site has published must not disappear
// without somewhere to send the people who still ask for it.
//
// THE GAP, MEASURED. Rename `schema/dbml.md` to `schema/dbml-export.md`, update
// the one sidebar entry that names it, add no redirect, and every documentation
// gate in this tree reports success: check-redirects, check-page-health,
// check-links, check-core-doc-links, check-style and the Astro build itself,
// seven for seven green. `dist/schema/dbml/` is gone from the artifact, so
// `.../edge/schema/dbml/` 404s for every bookmark, inbound blog link and search
// result, and nothing says a word.
//
// Nothing could have. `check-redirects.mjs` iterates the redirect map, so its
// universe is the declarations that exist and a retirement is precisely the
// declaration that does not; no amount of tightening inside that loop can reach
// a route no entry mentions. `check-links.mjs` iterates links between pages, so
// it fires only when some other page happened to link to the retired one -- an
// accident of popularity, and two of this site's pages have no inbound link at
// all. The observation nobody was making is "this route used to be published
// and is not any more", and making it needs a record of what was published.
//
// THE LEDGER. `scripts/data/published-routes.json` is that record: every route
// this site has published, in the slash-wrapped spelling `redirectRoutes` keys
// use. The invariant is per-entry and permanent -- every ledger route is either
// a live page today or the source of a redirect -- which is why a ledger beats
// the obvious alternative of diffing the content directory against the merge
// base. Measured, that alternative fails three ways: it forgets (a retirement
// that slipped through once is on master, and the second pull request built on
// top of it is asked nothing), it is vacuous on a push to master, where
// `merge-base(HEAD, origin/master)` is `HEAD` and the comparison is a tree
// against itself reporting a pass having compared nothing, and it cannot run at
// all in the shallow checkout most of this workflow uses. The ledger has none
// of those: it needs no history, it works on a depth-1 clone, and a retirement
// that got in yesterday is still red today and tomorrow.
//
// WHERE IT LIVES. `docs/docs.go` carries `//go:embed site/src/content/docs`,
// the whole directory, so a file placed under the content root ships inside
// every `ptah`, `ptah-compat` and `ptah-ls` binary that is built. The ledger is
// a fact about the documentation site's URLs and no user asks Ptah about it, so
// it lives in `docs/site/scripts/data/`, which is outside that embed, outside
// `check-core-doc-links.mjs`'s scan of the content root and outside
// `check-style.mjs`'s `.md`/`.mdx` governance. It is where
// `feature-matrix-rows.json` already sits.
//
// WHY REDIRECT SOURCES ARE IN IT. A redirect source is by definition a route
// this site published once, and recording it is what makes deleting a redirect
// as visible as deleting a page: drop the `redirectRoutes` entry for
// `/getting-started/` and the route the ledger still carries is suddenly
// neither live nor a source, which is the first finding below. Without that,
// the gate would guard pages and leave every already-moved URL unguarded.
//
// WHY `--write` ONLY ADDS. It writes the union of the ledger ON DISK with
// everything this tree can prove was published, and it removes nothing. That
// property is the gate: a `--write` that dropped the route whose page had just
// been deleted would regenerate the evidence away and then report OK.
//
// "On disk" is load-bearing, and getting it wrong is how the property becomes
// a self-consistency trap instead of a gate. Delete the ledger FILE and a
// `--write` that seeded from nothing would rebuild it from the tree, erase
// every retirement it recorded, and leave a two-line diff -- one route out, one
// route in -- that reads like the rename it is hiding. So `--write` refuses to
// run without a ledger, and it says which of the two situations it is in: a
// ledger git still tracks is a DELETION and has to be restored from git, while
// a repository that has never had one is seeded by `--seed`, a flag that names
// what it is doing. Nothing reaches an empty in-memory ledger through `--write`.
//
// TWO WAYS A LINE CAN STILL LEAVE THE LEDGER, AND WHAT ANSWERS EACH.
//
//   - By hand, to escape the gate. That is a deleted line in the diff, which is
//     already better than a retirement hidden inside a `git mv`, but a reviewer
//     is not a gate. `--against <ref>` is: it reads the ledger at the merge base
//     with <ref> and requires that nothing recorded there has gone. It runs in
//     the `build` job, the one checked out with `fetch-depth: 0`, and it says
//     which arm it took -- an unresolvable base is an error, never a pass.
//   - Legitimately, because the route was never published anywhere. A page
//     added on this branch, recorded by `--write`, then renamed before the
//     branch merges leaves a permanent finding `--write` can never clear: the
//     old route needs a redirect it does not deserve, for a URL nobody was ever
//     served. `--forget <route> --against <ref>` is the honest exit. It removes
//     the entry only when the ledger at the merge base does not have it, which
//     is exactly the case where no reader can hold that URL.
//
// ONE LIMIT, MEASURED RATHER THAN LEFT TO BE FOUND. The routes come from
// `docroutes.contentFiles`, which asks git rather than walking the directory,
// because a walk counts a checkout parked under the content root as pages -- 68
// against 209 on this repository, the extra 141 belonging to somebody else's
// branch. The cost is that `--cached` reports the index: a page deleted with a
// plain `rm` and not yet staged still reads as live, and this gate stays green
// until the deletion is staged. Measured on a fixture: deleted-not-staged exits
// 0, the same deletion staged exits 1 and names the route. A checkout in CI has
// an index that matches its commit, so the window exists only in a working tree
// mid-edit and closes the moment the change is committable.
//
// WHAT IT REFUSES TO DO. Report a pass having compared nothing. A missing,
// empty, unparseable or unsorted ledger, zero live routes, and a config with no
// `redirectRoutes` literal are each an error with its own message and exit 2,
// never a quiet OK.

import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, relative, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

// One enumeration of the routes, one parser for the redirect map, shared with
// the gates that validate them. See the header of scripts/lib/docroutes.mjs.
import { astroConfigPath, liveRoutes, parseRedirectRoutes } from './lib/docroutes.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));

/** The committed record of every route this site has published. */
export const ledgerPath = join(scriptDir, 'data', 'published-routes.json');

const ledgerName = basename(ledgerPath);

// The same route shape check-redirects.mjs requires of both ends of a redirect,
// so a ledger entry and a redirect key cannot be spelled differently and both
// be accepted.
const routePattern = /^\/(?:[a-z0-9-]+\/)+$|^\/$/;

/**
 * An input the gate cannot compare with, as distinct from a defect in what it
 * compared. Reported as exit 2, the way check-redirects.mjs reports a missing
 * content root; findings are exit 1.
 */
export class UnusableInput extends Error {
  constructor(message) {
    super(message);
    this.name = 'UnusableInput';
  }
}

/**
 * The ledger's shape, checked before anything is compared against it.
 *
 * `allowEmpty` is true on the `--write` path and only there: seeding a ledger
 * that does not exist yet starts from nothing, while a check against nothing is
 * the vacuous pass this gate exists not to give.
 */
function validateLedger(ledger, { allowEmpty = false, requireSorted = true } = {}) {
  if (!Array.isArray(ledger)) {
    throw new UnusableInput(
      `${ledgerName} is not a JSON array of routes; refusing to pass without comparing anything`,
    );
  }
  if (ledger.length === 0 && !allowEmpty) {
    throw new UnusableInput(`${ledgerName} holds no routes; refusing to pass without comparing anything`);
  }

  for (const entry of ledger) {
    if (typeof entry !== 'string' || !routePattern.test(entry)) {
      throw new UnusableInput(
        `${ledgerName} entry ${JSON.stringify(entry)} is not a /segment/ route with leading and trailing slashes`,
      );
    }
  }

  if (!requireSorted) return;

  const seen = new Set();
  for (const entry of ledger) {
    if (seen.has(entry)) {
      throw new UnusableInput(`${ledgerName} lists ${entry} twice`);
    }
    seen.add(entry);
  }
  const sorted = [...ledger].sort();
  if (ledger.join('\n') !== sorted.join('\n')) {
    throw new UnusableInput(`${ledgerName} is not sorted; run check-route-retirement.mjs --write`);
  }
}

function redirectSources(redirects) {
  if (redirects === null || redirects === undefined) {
    throw new UnusableInput(
      'astro.config.mjs: redirectRoutes map not found; moved pages must keep their redirect entries',
    );
  }
  return new Set(redirects.map(([from]) => from));
}

/**
 * Compare the ledger with what the tree publishes today.
 *
 * `ledger` is the parsed array, `live` the routes the content collection
 * publishes, `redirects` the `[from, to]` pairs `parseRedirectRoutes` returns.
 * Takes values rather than paths so the selftest drives the same function the
 * gate does: a rule that stops firing reddens the fixture instead of passing
 * every route in the repository.
 *
 * Returns `{ findings, checked, live, redirected }`. Throws `UnusableInput`
 * when there is nothing usable to compare.
 */
export function analyze({ ledger, live, redirects }) {
  validateLedger(ledger);

  const liveSet = new Set(live);
  if (liveSet.size === 0) {
    throw new UnusableInput('git reports no documentation pages; refusing to pass without comparing anything');
  }

  const sources = redirectSources(redirects);
  const recorded = new Set(ledger);
  const findings = [];

  // The finding this gate exists for: a URL the site served that now answers
  // nothing. It is listed first because it is the one that reaches users.
  for (const route of ledger) {
    if (liveSet.has(route) || sources.has(route)) continue;
    findings.push(
      `route ${route} was published and is now neither a page nor a redirect source; ` +
        'add a redirectRoutes entry in astro.config.mjs pointing at its new home',
    );
  }

  // A page whose route never joined the ledger could be retired later in
  // complete silence, so the ledger has to keep up with the tree.
  for (const route of [...liveSet].sort()) {
    if (recorded.has(route)) continue;
    findings.push(`route ${route} is published but not in ${ledgerName}; run check-route-retirement.mjs --write`);
  }

  // A redirect source outside the ledger is a route the rule above cannot
  // guard: delete the redirect later and nothing notices. It is also how a
  // mistyped source announces itself, since the route it names was never a page
  // here.
  for (const route of [...sources].sort()) {
    if (recorded.has(route)) continue;
    findings.push(
      `redirect source ${route} is not in ${ledgerName}; run check-route-retirement.mjs --write if it ` +
        'was a published route, or fix the source if it never was',
    );
  }

  return { findings, checked: ledger.length, live: liveSet.size, redirected: sources.size };
}

/**
 * The ledger `--write` should leave on disk: everything already recorded, plus
 * everything this tree can prove was published.
 *
 * A union, deliberately. This function must never be able to drop an entry --
 * the entry it would drop is exactly the retired route the gate is looking for,
 * and a regenerator that erased its own evidence would turn every retirement
 * into a clean run.
 */
export function nextLedger({ ledger, live, redirects }) {
  validateLedger(ledger, { allowEmpty: true, requireSorted: false });
  const sources = redirectSources(redirects);
  return [...new Set([...ledger, ...live, ...sources])].sort();
}

/** The ledger's on-disk form: one route per line, sorted, with a final newline. */
export function serializeLedger(routes) {
  return `${JSON.stringify(routes, null, 2)}\n`;
}

function readLedger(path) {
  let source;
  try {
    source = readFileSync(path, 'utf8');
  } catch (cause) {
    // Deliberately not "run --write to seed it". A ledger that cannot be read
    // is either a deletion or a repository that has never had one, those need
    // opposite repairs, and telling every reader to regenerate it is how the
    // gate would come to agree with itself. main() decides which of the two
    // this is before it gets here; this message is the fallback for a ledger
    // that exists and cannot be parsed off disk.
    throw new UnusableInput(
      `${ledgerName} could not be read at ${path} (${cause.code ?? cause.message}); ` +
        'it is a committed record, so restore it from git rather than regenerating it',
    );
  }
  try {
    return JSON.parse(source);
  } catch (cause) {
    throw new UnusableInput(`${ledgerName} is not valid JSON: ${cause.message}`);
  }
}

// ---------------------------------------------------------------------------
// The ledger's history. `--against` and `--forget` both ask what the ledger
// said at the merge base, and both refuse rather than assume when git cannot
// answer: a base that does not resolve is an error, because a secondary
// assertion that quietly skips is the "gate that reports without running" this
// repository names in check-gate-selftests.sh's own header.

function git(args, { allowFailure = false } = {}) {
  try {
    return execFileSync('git', ['-C', dirname(ledgerPath), ...args], {
      encoding: 'utf8',
      maxBuffer: 64 * 1024 * 1024,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
  } catch (cause) {
    if (allowFailure) return null;
    const detail = String(cause.stderr ?? cause.message ?? cause).trim().split('\n')[0];
    throw new UnusableInput(`git ${args.join(' ')} failed: ${detail}`);
  }
}

/** The ledger's path relative to the repository root, which is how git names it. */
function ledgerRepoPath() {
  const top = git(['rev-parse', '--show-toplevel']).trim();
  return relative(top, ledgerPath).split(sep).join('/');
}

/**
 * Whether git tracks the ledger. Asked only when the file is missing, to tell a
 * deletion from a repository that has never had one; those need opposite
 * repairs and only one of them may regenerate anything.
 *
 * Fails closed. If git cannot answer at all, the answer is "tracked", because
 * the alternative is seeding a ledger in a checkout nobody has inspected.
 */
function ledgerIsTracked() {
  // Two things this call gets right that the obvious spelling does not, both
  // measured on a fixture whose ledger had been removed from the index.
  //
  // The pathspec is the ABSOLUTE path. git resolves a pathspec against the
  // current directory and this helper runs with `-C` inside scripts/data, so
  // `docs/site/scripts/data/...` matches nothing there whether or not the file
  // is tracked.
  //
  // And there is no `--error-unmatch`, because it answers "not tracked" and
  // "that pathspec is nonsense" with the same exit status and the same
  // sentence. Failing closed on the pair means answering "tracked" for a
  // repository that has never had a ledger, which puts --seed permanently out
  // of reach. Plain `ls-files` exits 0 either way and prints the path only when
  // git knows it, so an empty answer is a real "no" and a git that could not
  // run at all is still the fail-closed "yes".
  const answer = git(['ls-files', '--', ledgerPath], { allowFailure: true });
  return answer === null ? true : answer.trim().length > 0;
}

/**
 * The ledger as it stood at the merge base of HEAD and `ref`, or null when the
 * base carries no ledger at all -- the arm that runs on the commit introducing
 * this gate, and the one that must be printed rather than silently taken.
 */
function ledgerAtBase(ref) {
  const base = git(['merge-base', 'HEAD', ref], { allowFailure: true });
  if (base === null) {
    throw new UnusableInput(
      `no merge base between HEAD and ${ref}; this comparison needs history, so fetch it (fetch-depth: 0) rather than skipping the check`,
    );
  }
  const revision = base.trim();
  const source = git(['show', `${revision}:${ledgerRepoPath()}`], { allowFailure: true });
  if (source === null) return { revision, ledger: null };
  try {
    return { revision, ledger: JSON.parse(source) };
  } catch (cause) {
    throw new UnusableInput(`${ledgerName} at ${revision} is not valid JSON: ${cause.message}`);
  }
}

/**
 * The secondary assertion: the ledger must not have LOST an entry.
 *
 * `analyze` iterates the ledger, so a route deleted from it is a route it never
 * visits -- measured, a retirement plus a hand-edited ledger leaves all four
 * documentation gates green. This is the observation `analyze` structurally
 * cannot make, and it is the reason the ledger's diff is worth reading.
 */
export function analyzeAgainstBase({ base, current }) {
  validateLedger(base, { requireSorted: false });
  validateLedger(current, { requireSorted: false });

  const kept = new Set(current);
  const findings = [];
  for (const route of base) {
    if (kept.has(route)) continue;
    findings.push(
      `${ledgerName} no longer records ${route}, which it recorded at the merge base; ` +
        'a published route is retired with a redirectRoutes entry, never by deleting its ledger line',
    );
  }
  return { findings, compared: base.length };
}

/**
 * Drop one route from the ledger, for the one case where dropping it is honest:
 * a route this branch invented and renamed before anybody could have been
 * served it.
 *
 * `base` is the ledger at the merge base, or null when the base has none. A
 * route the base records was published, so it is refused here and needs a
 * redirect instead; a route the ledger does not carry at all is refused too,
 * because silently succeeding would let a typo read as a repair.
 */
export function forget({ ledger, base, route }) {
  validateLedger(ledger, { requireSorted: false });
  if (!ledger.includes(route)) {
    throw new UnusableInput(`${ledgerName} does not record ${route}, so there is nothing to forget`);
  }
  if (base !== null && base.includes(route)) {
    throw new UnusableInput(
      `${route} is recorded in ${ledgerName} at the merge base, so this site published it; ` +
        'retire it with a redirectRoutes entry pointing at its new home rather than forgetting it',
    );
  }
  return ledger.filter((entry) => entry !== route);
}

function assertions() {
  let count = 0;
  return {
    count: () => count,
    assert(condition, message) {
      count += 1;
      if (!condition) throw new Error(`check-route-retirement.mjs selftest: ${message}`);
    },
    throws(run, needle, message) {
      count += 1;
      try {
        run();
      } catch (error) {
        if (!(error instanceof UnusableInput)) {
          throw new Error(`check-route-retirement.mjs selftest: ${message}: threw ${error.name}, wanted UnusableInput`);
        }
        if (String(error.message).includes(needle)) return;
        throw new Error(
          `check-route-retirement.mjs selftest: ${message}: expected a message containing ` +
            `${JSON.stringify(needle)}, got ${JSON.stringify(error.message)}`,
        );
      }
      throw new Error(`check-route-retirement.mjs selftest: ${message}: nothing was thrown`);
    },
  };
}

// A small site, written out so every case below is a one-line edit of it: four
// published routes, three of them live, the fourth already moved.
function fixture() {
  return {
    ledger: ['/', '/getting-started/', '/schema/dbml/', '/start/quick-start/'],
    live: ['/', '/schema/dbml/', '/start/quick-start/'],
    redirects: [['/getting-started/', '/start/quick-start/']],
  };
}

function selftest() {
  const { assert, throws, count } = assertions();

  // 1. Clean. The counts are asserted, not just the absence of findings: a
  //    matcher that stopped matching would otherwise report a clean run.
  const clean = analyze(fixture());
  assert(clean.findings.length === 0, `a consistent ledger produces no findings, got ${clean.findings.join('; ')}`);
  assert(clean.checked === 4, `every ledger route is compared (got ${clean.checked})`);
  assert(clean.live === 3, `every live route is counted (got ${clean.live})`);
  assert(clean.redirected === 1, `every redirect source is counted (got ${clean.redirected})`);

  // 2. Retired WITH a redirect. The control. Without it, deleting this gate's
  //    only rule reads as a fix, because case 3 would still be the only thing
  //    telling the two apart.
  const moved = analyze({
    ledger: ['/', '/getting-started/', '/schema/dbml-export/', '/schema/dbml/', '/start/quick-start/'],
    live: ['/', '/schema/dbml-export/', '/start/quick-start/'],
    redirects: [
      ['/getting-started/', '/start/quick-start/'],
      ['/schema/dbml/', '/schema/dbml-export/'],
    ],
  });
  assert(moved.findings.length === 0, `a move with a redirect is not a finding, got ${moved.findings.join('; ')}`);

  // 3. Retired with NO redirect. The measured gap, reproduced as a fixture.
  const retired = analyze({
    ledger: ['/', '/getting-started/', '/schema/dbml-export/', '/schema/dbml/', '/start/quick-start/'],
    live: ['/', '/schema/dbml-export/', '/start/quick-start/'],
    redirects: [['/getting-started/', '/start/quick-start/']],
  });
  assert(retired.findings.length === 1, `one retirement is one finding, got ${retired.findings.length}: ${retired.findings.join('; ')}`);
  assert(retired.findings[0].includes('/schema/dbml/'), `the finding names the route that vanished, got ${retired.findings[0]}`);
  assert(
    retired.findings[0].includes('neither a page nor a redirect source'),
    `the finding says what happened, got ${retired.findings[0]}`,
  );

  // 4. A page published without joining the ledger.
  const unrecorded = analyze({ ...fixture(), live: ['/', '/schema/dbml/', '/schema/new-page/', '/start/quick-start/'] });
  assert(unrecorded.findings.length === 1, `one unrecorded page is one finding, got ${unrecorded.findings.join('; ')}`);
  assert(unrecorded.findings[0].includes('/schema/new-page/'), `the finding names the new route, got ${unrecorded.findings[0]}`);
  assert(unrecorded.findings[0].includes('--write'), `the finding says how to record it, got ${unrecorded.findings[0]}`);

  // 5. A redirect source the ledger never heard of.
  const strangeSource = analyze({
    ...fixture(),
    redirects: [['/getting-started/', '/start/quick-start/'], ['/never-published/', '/start/quick-start/']],
  });
  assert(strangeSource.findings.length === 1, `one strange source is one finding, got ${strangeSource.findings.join('; ')}`);
  assert(
    strangeSource.findings[0].includes('/never-published/'),
    `the finding names the source, got ${strangeSource.findings[0]}`,
  );

  // 6-7. The two vacuous passes, refused.
  throws(() => analyze({ ...fixture(), ledger: [] }), 'holds no routes', 'refuses an empty ledger');
  throws(() => analyze({ ...fixture(), live: [] }), 'no documentation pages', 'refuses zero live routes');

  // The rest of the unusable inputs, each with its own message so a reader is
  // told which one they have.
  throws(() => analyze({ ...fixture(), redirects: null }), 'redirectRoutes map not found', 'refuses a config with no redirect map');
  throws(() => analyze({ ...fixture(), ledger: {} }), 'not a JSON array', 'refuses a ledger that is not an array');
  throws(
    () => analyze({ ...fixture(), ledger: ['/', 'schema/dbml'] }),
    'leading and trailing slashes',
    'refuses a ledger entry that is not a route',
  );
  throws(
    () => analyze({ ...fixture(), ledger: ['/start/quick-start/', '/'] }),
    'is not sorted',
    'refuses an unsorted ledger',
  );
  throws(
    () => analyze({ ...fixture(), ledger: ['/', '/', '/schema/dbml/'] }),
    'twice',
    'refuses a ledger that lists a route twice',
  );

  // 8. --write is additive. The retired route is in the ledger and in nothing
  //    else; a regenerator that rebuilt the ledger from the tree would drop it
  //    and the next run would be green.
  const written = nextLedger({
    ledger: ['/', '/getting-started/', '/schema/dbml/', '/start/quick-start/'],
    live: ['/', '/schema/dbml-export/', '/start/quick-start/'],
    redirects: [['/getting-started/', '/start/quick-start/']],
  });
  assert(written.includes('/schema/dbml/'), `--write leaves a retired route in the ledger, got ${written.join(' ')}`);
  assert(written.includes('/schema/dbml-export/'), `--write records the new route, got ${written.join(' ')}`);
  assert(written.includes('/getting-started/'), `--write keeps a redirect source, got ${written.join(' ')}`);
  assert(written.join('\n') === [...written].sort().join('\n'), '--write leaves the ledger sorted');
  assert(new Set(written).size === written.length, '--write leaves no duplicates');

  // A source this tree declares but the ledger has not recorded is added by
  // --write, which is how the ledger was seeded over 30 pre-existing redirects.
  const seeded = nextLedger({ ledger: [], live: ['/'], redirects: [['/gone/', '/']] });
  assert(seeded.join(' ') === '/ /gone/', `--write seeds live routes and redirect sources alike, got ${seeded.join(' ')}`);
  throws(() => nextLedger({ ledger: [], live: ['/'], redirects: null }), 'redirectRoutes map not found', '--write refuses a config with no redirect map');

  // The write path itself, through a real file, so the serialization is proven
  // rather than assumed: what is read back has to be what analyze accepts.
  const tmp = mkdtempSync(join(tmpdir(), 'ptah-route-ledger-'));
  try {
    const path = join(tmp, ledgerName);
    writeFileSync(path, serializeLedger(['/', '/getting-started/', '/schema/dbml/', '/start/quick-start/']));
    const roundTrip = nextLedger({
      ledger: JSON.parse(readFileSync(path, 'utf8')),
      live: ['/', '/schema/dbml-export/', '/start/quick-start/'],
      redirects: [['/getting-started/', '/start/quick-start/']],
    });
    writeFileSync(path, serializeLedger(roundTrip));
    const reread = JSON.parse(readFileSync(path, 'utf8'));
    assert(reread.includes('/schema/dbml/'), 'the retired route survives a write and a re-read');
    assert(readFileSync(path, 'utf8').endsWith(']\n'), 'the ledger ends with a newline');
    const afterWrite = analyze({
      ledger: reread,
      live: ['/', '/schema/dbml-export/', '/start/quick-start/'],
      redirects: [['/getting-started/', '/start/quick-start/']],
    });
    assert(
      afterWrite.findings.some((finding) => finding.includes('/schema/dbml/')),
      '--write cannot silence the retirement it just recorded',
    );
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }

  // 9. The secondary assertion. `analyze` iterates the ledger, so a line
  //    deleted from it is a line it never visits: measured, a retirement plus a
  //    hand-edited ledger leaves all four documentation gates green. This is
  //    the observation only history can make.
  const dropped = analyzeAgainstBase({
    base: ['/', '/getting-started/', '/schema/dbml/', '/start/quick-start/'],
    current: ['/', '/getting-started/', '/schema/dbml-export/', '/start/quick-start/'],
  });
  assert(dropped.findings.length === 1, `a deleted ledger line is one finding, got ${dropped.findings.join('; ')}`);
  assert(dropped.findings[0].includes('/schema/dbml/'), `the finding names the route that left the ledger, got ${dropped.findings[0]}`);
  assert(dropped.compared === 4, `every route recorded at the base is compared (got ${dropped.compared})`);

  // 10. The control. A ledger that only GREW is the normal case, and without
  //     this the rule above would read as satisfied by deleting the comparison.
  const grew = analyzeAgainstBase({
    base: ['/', '/schema/dbml/'],
    current: ['/', '/schema/dbml-export/', '/schema/dbml/'],
  });
  assert(grew.findings.length === 0, `adding routes is not a finding, got ${grew.findings.join('; ')}`);
  throws(() => analyzeAgainstBase({ base: [], current: ['/'] }), 'holds no routes', 'refuses an empty base ledger');
  throws(() => analyzeAgainstBase({ base: ['/'], current: [] }), 'holds no routes', 'refuses an empty current ledger');

  // 11. --forget, the honest exit for a route this branch invented. It removes
  //     an entry only where the history proves nobody could have been served
  //     it; the two refusals are what keep it from becoming the hand-deletion
  //     with a flag on it.
  const forgotten = forget({
    ledger: ['/', '/schema/newthing/', '/start/quick-start/'],
    base: ['/', '/start/quick-start/'],
    route: '/schema/newthing/',
  });
  assert(forgotten.join(' ') === '/ /start/quick-start/', `--forget drops the branch-local route, got ${forgotten.join(' ')}`);
  throws(
    () => forget({ ledger: ['/', '/schema/dbml/'], base: ['/', '/schema/dbml/'], route: '/schema/dbml/' }),
    'this site published it',
    '--forget refuses a route the merge base recorded',
  );
  throws(
    () => forget({ ledger: ['/', '/schema/dbml/'], base: ['/'], route: '/schema/typo/' }),
    'nothing to forget',
    '--forget refuses a route the ledger does not carry, so a typo cannot read as a repair',
  );
  // A base with no ledger at all is the branch that introduces one; there is no
  // published route to protect, so the removal is allowed.
  assert(
    forget({ ledger: ['/', '/schema/newthing/'], base: null, route: '/schema/newthing/' }).join(' ') === '/',
    '--forget works where the base carries no ledger',
  );

  console.log(`check-route-retirement.mjs --selftest: OK (${count()} assertions via analyze())`);
}

const usage =
  'usage: node scripts/check-route-retirement.mjs [--write|--seed|--selftest|--against <ref>|--forget <route> --against <ref>]';

function parseArguments(argv) {
  const options = { mode: 'check', against: null, route: null };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === '--selftest' || argument === '--write' || argument === '--seed') {
      options.mode = argument.slice(2);
      continue;
    }
    if (argument === '--forget') {
      options.mode = 'forget';
      options.route = argv[index + 1];
      index += 1;
      if (options.route === undefined) throw new UnusableInput(`--forget wants a route. ${usage}`);
      continue;
    }
    if (argument === '--against') {
      options.against = argv[index + 1];
      index += 1;
      if (options.against === undefined) throw new UnusableInput(`--against wants a ref. ${usage}`);
      continue;
    }
    throw new UnusableInput(`unknown argument ${JSON.stringify(argument)}. ${usage}`);
  }

  // --forget removes a line from a committed record. It may do that only where
  // the history says nobody could have been served the route, so the base it is
  // measured against is required rather than defaulted.
  if (options.mode === 'forget' && options.against === null) {
    throw new UnusableInput(`--forget needs --against <ref> to prove the route was never published. ${usage}`);
  }
  return options;
}

/**
 * The ledger on disk, with the two ways it can be absent kept apart.
 *
 * Only `--seed` may start from nothing, and only in a repository where git has
 * never tracked a ledger. Everything else refuses: regenerating a ledger that
 * was deleted is how this gate would erase its own evidence and then agree with
 * itself.
 */
function loadLedger(mode) {
  if (existsSync(ledgerPath)) {
    if (mode === 'seed') {
      throw new UnusableInput(
        `${ledgerName} already exists at ${ledgerPath}; --seed is for a repository that has never had one, ` +
          'and --write is what records new routes in the one that is there',
      );
    }
    return readLedger(ledgerPath);
  }

  if (ledgerIsTracked()) {
    throw new UnusableInput(
      `${ledgerName} is missing from ${ledgerPath} but git still tracks it; that is a deletion, not a first run. ` +
        'Restore it with `git checkout -- ' +
        `${ledgerRepoPath()}\`: rebuilding it from the tree would erase every retirement it records ` +
        'and leave this gate agreeing with itself',
    );
  }

  if (mode !== 'seed') {
    throw new UnusableInput(
      `${ledgerName} does not exist at ${ledgerPath} and git has never tracked it. ` +
        'If this site has never had a ledger, seed it with check-route-retirement.mjs --seed; ' +
        '--write records new routes in a ledger that is already there',
    );
  }
  return [];
}

function writeLedger(next, ledger, label) {
  const added = next.filter((route) => !ledger.includes(route));
  const removed = ledger.filter((route) => !next.includes(route));
  const serialized = serializeLedger(next);
  if (!existsSync(ledgerPath) || readFileSync(ledgerPath, 'utf8') !== serialized) {
    mkdirSync(dirname(ledgerPath), { recursive: true });
    writeFileSync(ledgerPath, serialized);
  }
  console.log(
    `check-route-retirement.mjs ${label}: ${ledgerName} holds ${next.length} routes ` +
      `(${added.length} added, ${removed.length} removed)`,
  );
  for (const route of added) console.log(`  + ${route}`);
  for (const route of removed) console.log(`  - ${route}`);
}

function main() {
  let options;
  try {
    options = parseArguments(process.argv.slice(2));
  } catch (error) {
    console.error(`check-route-retirement.mjs: ${error.message}`);
    process.exitCode = 2;
    return;
  }

  if (options.mode === 'selftest') {
    selftest();
    return;
  }

  try {
    // The secondary assertion is a comparison with history and nothing else, so
    // it neither reads the tree nor writes anything.
    if (options.mode === 'check' && options.against !== null) {
      const { revision, ledger: base } = ledgerAtBase(options.against);
      const current = readLedger(ledgerPath);
      if (base === null) {
        console.log(
          `check-route-retirement.mjs --against ${options.against}: OK ` +
            `(the merge base ${revision.slice(0, 9)} carries no ${ledgerName}; this branch introduces it)`,
        );
        return;
      }
      const { findings, compared } = analyzeAgainstBase({ base, current });
      if (findings.length > 0) {
        console.error('Route retirement check failed:');
        for (const finding of findings) console.error(`- ${finding}`);
        console.error(
          '\nA route the ledger recorded stays recorded. If this branch invented the route and nothing ever served it, ' +
            'say so with --forget <route> --against <ref>.',
        );
        process.exitCode = 1;
        return;
      }
      console.log(
        `check-route-retirement.mjs --against ${options.against}: OK ` +
          `(${compared} routes recorded at ${revision.slice(0, 9)}, none of them dropped)`,
      );
      return;
    }

    const live = [...liveRoutes()];
    const redirects = parseRedirectRoutes(readFileSync(astroConfigPath, 'utf8'));
    let ledger = loadLedger(options.mode);

    if (options.mode === 'write' || options.mode === 'seed') {
      const next = nextLedger({ ledger, live, redirects });
      writeLedger(next, ledger, `--${options.mode}`);
      // Fall through to the check. --write records what was published; it
      // cannot invent the redirect a retired route needs, so a run that leaves
      // one unresolved still has to say so.
      ledger = next;
    }

    if (options.mode === 'forget') {
      const { ledger: base } = ledgerAtBase(options.against);
      const next = forget({ ledger, base, route: options.route });
      writeLedger(next, ledger, '--forget');
      ledger = next;
    }

    const { findings, checked, live: liveCount, redirected } = analyze({ ledger, live, redirects });

    if (findings.length > 0) {
      console.error('Route retirement check failed:');
      for (const finding of findings) {
        console.error(`- ${finding}`);
      }
      console.error(
        '\nA route this site has published stays reachable: keep the page, or add a redirectRoutes entry pointing at its new home.',
      );
      process.exitCode = 1;
      return;
    }

    console.log(
      `check-route-retirement.mjs: OK (${checked} published routes: ${liveCount} live, ${redirected} redirected)`,
    );
  } catch (error) {
    if (!(error instanceof UnusableInput) && !String(error.message).startsWith('docroutes:')) throw error;
    console.error(`check-route-retirement.mjs: ${error.message}`);
    process.exitCode = 2;
  }
}

// Importing this file must not run the gate. It exports analyze and nextLedger
// so another gate can reuse them, and an import that ran main() would print a
// result nobody asked for and set an exit code with it.
const invokedPath = process.argv[1];
if (invokedPath !== undefined && import.meta.url === pathToFileURL(invokedPath).href) {
  main();
}
