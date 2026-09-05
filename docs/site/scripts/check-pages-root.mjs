#!/usr/bin/env node
// Holds the GitHub Pages ROOT to what the documentation publishes from it.
//
// The root is not a checkout. `.github/workflows/docs.yml` assembles `_site/`
// from scratch on every deploying run and uploads the whole directory, so a
// root file exists after a deploy only because that deploy wrote it. A file
// that survives the run which added it and vanishes on the next one is worse
// than one that was never added: the documented command keeps being published,
// and starts answering 404 with nothing red anywhere.
//
// So this gate does not ask whether a file is in the tree. It runs the
// assembly and asks what came out.
//
// Four rules, and each one is a different way the address stops answering:
//
//   1. Every declared root asset has a tracked, non-empty source, and an
//      install.ps1 is ASCII. That last one is not tidiness: Windows PowerShell
//      5.1 reads a .ps1 with no byte-order mark in the machine's ANSI code
//      page, so a UTF-8 em dash arrives as three Windows-1252 characters whose
//      last is a right double quotation mark -- a character PowerShell accepts
//      as a string delimiter. Measured: one dash inside one message closed the
//      string it sat in and the whole file failed to parse.
//   2. Running the producers the workflow runs, against an empty fixture root,
//      produces every root file, byte-identical to its source.
//   3. The workflow still invokes both producers, against `_site`, on a push,
//      before the artifact is uploaded. Rule 2 passes on a tree whose workflow
//      no longer calls either of them.
//   4. Every root URL the documentation publishes is a file the assembly
//      writes, and every declared asset is named by the documentation -- at
//      the address a reader is given (the project site, InstallURL) or at the
//      root itself. The first direction is the 404; the second is an asset
//      nobody can find.
//
// `--site <dir>` is the third half, and the build job runs it on the real
// `_site` after assembling it. Rules 1 to 4 read the tree; this reads what was
// actually produced, which is the only thing the deploy uploads.
//
// `--live` is the fourth, and it is the only one that asks the published
// address. Everything above can pass while the published apex
// answers 404 for a file: a Pages settings change, a repository rename, or a
// deploy from an older tag whose workflow predates the publish step -- a tag
// deploy replaces the whole site, and the workflow it runs is the one that
// exists AT THAT TAG. None of those is a change to this tree, so nothing here
// can see them. Only a request can, which is why `--live` exists and why it
// runs on a schedule rather than on a pull request.
//
// `--live` also asks the advertised address, the project site, whether each
// installer answers at all. That site is another repository's deploy, which
// fetches these sources from master on its own schedule, so only the answer is
// judged there -- a 200, a body, a shell script that begins like one -- and
// not the bytes.
import { execFileSync } from 'node:child_process';
import { InstallURL, Origin, PageURL, RootURL, SiteOrigin } from '../src/lib/docs-origin.mjs';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { GENERATED_ROOT_FILES, ROOT_ASSETS, sourcePath } from './publish-root-assets.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repoRoot = join(siteRoot, '..', '..');
const workflowPath = join(repoRoot, '.github', 'workflows', 'docs.yml');

// The two programs that write into the Pages root, and the marker that says
// the artifact has been uploaded and the root is closed.
export const ROOT_PRODUCERS = [
  'docs/site/scripts/gen-versions.mjs',
  'docs/site/scripts/publish-root-assets.mjs',
];
const UPLOAD_STEP = 'actions/upload-pages-artifact';
const PUSH_CONDITION = "github.event_name == 'push'";
// The site root, from the one declaration rather than a literal beside it.
// This gate exists to keep the published root honest, and while the address was
// written here it kept the tree honestly pointed at the host the site had left
// (stokaro/ptah#2884).
const PAGES_PREFIX = `${Origin}/`;
// The project site, where the installers are advertised. A reference under
// this prefix documents an asset as well as one under PAGES_PREFIX does, and
// it is what the pages actually give a reader.
const SITE_PREFIX = `${SiteOrigin}/`;

// Discovery has to fail closed. A `git ls-files` that matches nothing would
// make rule 4 report a clean run while reading no documentation at all, which
// is the shape of failure this whole file exists to refuse.
const MIN_SCANNED_FILES = 20;

// workflowSteps splits a workflow into its steps. The list marker sits at six
// spaces in this file, which is what a step is; everything before the first one
// is returned as element zero and is never a step.
export function workflowSteps(workflow) {
  return workflow.split(/\n {6}- /);
}

// analyze takes prepared inputs rather than reading the tree, so the self-test
// can hand it a tree that does not exist and require each rule to fire.
export function analyze(input) {
  const { assets, generated, sources, assembled, workflow, documented, scanned, advertised = new Map() } = input;
  const problems = [];

  const expected = [...generated, ...assets.map((asset) => asset.name)];

  // Rule 1: the sources.
  for (const asset of assets) {
    const source = sources.get(asset.name);
    if (!source || !source.exists) {
      problems.push(`${asset.source} does not exist; ${asset.url} has nothing to serve`);
      continue;
    }
    if (!source.tracked) {
      problems.push(`${asset.source} is not tracked by git; the deploy checkout would not have it`);
    }
    if (source.text.length === 0) {
      problems.push(`${asset.source} is empty`);
    }
    if (asset.name.endsWith('.ps1')) {
      // eslint-disable-next-line no-control-regex
      const offending = source.text.split('\n').findIndex((line) => /[^\x00-\x7F]/.test(line));
      if (offending >= 0) {
        problems.push(
          `${asset.source} line ${offending + 1} is not ASCII; Windows PowerShell 5.1 reads a .ps1 ` +
            'with no byte-order mark in the ANSI code page, where a UTF-8 dash becomes a quotation mark',
        );
      }
    }
  }

  // Rule 2: what the assembly produced.
  for (const name of expected) {
    if (!assembled.has(name)) {
      problems.push(`assembling the Pages root produced no ${name}`);
    }
  }
  for (const asset of assets) {
    const produced = assembled.get(asset.name);
    const source = sources.get(asset.name);
    if (produced === undefined || !source || !source.exists) continue;
    if (produced !== source.text) {
      problems.push(`the assembled ${asset.name} is not the bytes of ${asset.source}`);
    }
  }

  // Rule 3: the workflow still runs the producers.
  const steps = workflowSteps(workflow);
  const uploadIndex = steps.findIndex((step) => step.includes(UPLOAD_STEP));
  if (uploadIndex < 0) {
    problems.push(`.github/workflows/docs.yml has no ${UPLOAD_STEP} step; nothing publishes the Pages root`);
  }
  for (const producer of ROOT_PRODUCERS) {
    const index = steps.findIndex((step) => step.includes(producer) && step.includes('$GITHUB_WORKSPACE/_site'));
    if (index < 0) {
      problems.push(
        `.github/workflows/docs.yml has no step running ${producer} against "$GITHUB_WORKSPACE/_site"; ` +
          'the next deploy assembles a root without what it writes',
      );
      continue;
    }
    if (!steps[index].includes(PUSH_CONDITION)) {
      problems.push(`the step running ${producer} is not guarded by ${PUSH_CONDITION}`);
    }
    if (uploadIndex >= 0 && index > uploadIndex) {
      problems.push(`the step running ${producer} comes after the Pages artifact is uploaded`);
    }
  }

  // Rule 4: the documentation and the root agree in both directions.
  if (scanned < MIN_SCANNED_FILES) {
    problems.push(
      `read ${scanned} documentation file(s), expected at least ${MIN_SCANNED_FILES}; ` +
        'refusing to pass without having read the documentation',
    );
  }
  for (const [name, files] of documented) {
    if (!expected.includes(name)) {
      problems.push(`${files[0]} publishes ${PAGES_PREFIX}${name}, which the Pages root assembly does not write`);
    }
  }
  for (const asset of assets) {
    if (!documented.has(asset.name) && !advertised.has(asset.name)) {
      problems.push(
        `no documentation page names ${asset.advertised} or ${asset.url}; it is served and unreachable`,
      );
    }
  }

  return problems;
}

// analyzeLive judges what the published site answered. `fetched` maps a file
// name to { status, text } for a request that completed, or to { error } for one
// that did not.
//
// The generated files are probed beside the declared assets, and they are the
// control: they have been at the root since before any installer was, so a run
// where everything 404s is a site that moved or a Pages configuration that
// changed, and a run where only an installer 404s is the publish step having
// stopped running. Reporting those two as the same finding would send the
// reader to the wrong file.
//
// An asset is additionally required to be the bytes of its source. The site
// converges on the tree within a minute of every push to master, so on the
// schedule this runs on a difference is not a deploy in flight; it is the root
// serving something other than what the repository says it serves.
export function analyzeLive(input) {
  const { assets, generated, fetched, sources } = input;
  const problems = [];
  const names = [...generated, ...assets.map((asset) => asset.name)];
  const reachable = names.filter((name) => {
    const answer = fetched.get(name);
    return answer && !answer.error && answer.status === 200;
  });

  for (const name of names) {
    const answer = fetched.get(name);
    if (!answer) {
      problems.push(`${PAGES_PREFIX}${name} was not requested`);
      continue;
    }
    if (answer.error) {
      problems.push(`${PAGES_PREFIX}${name} could not be requested: ${answer.error}`);
      continue;
    }
    if (answer.status !== 200) {
      const control = reachable.length > 0 ? `; ${reachable.join(', ')} still answered 200` : '';
      problems.push(`${PAGES_PREFIX}${name} answered ${answer.status}${control}`);
      continue;
    }
    if (answer.text.length === 0) {
      problems.push(`${PAGES_PREFIX}${name} answered 200 with an empty body`);
    }
  }

  for (const asset of assets) {
    const answer = fetched.get(asset.name);
    const source = sources.get(asset.name);
    if (!answer || answer.error || answer.status !== 200 || answer.text.length === 0) continue;
    if (source === undefined) {
      problems.push(`${asset.source} could not be read to compare against ${asset.url}`);
      continue;
    }
    if (answer.text !== source) {
      problems.push(
        `${asset.url} is not the bytes of ${asset.source}; the deployed root is serving something else`,
      );
    }
  }

  return problems;
}

// analyzeAdvertised judges what the project site answered for each installer,
// at the address the documentation gives a reader. `fetched` has the shape
// analyzeLive reads.
//
// No byte comparison here, deliberately. That site fetches these sources from
// master on its own schedule, so between a push here and its next deploy the
// bytes differ without anything being wrong. What must hold at every moment is
// that the address answers with a script: a 200, a body, and for the shell
// installer a first line that a `| sh` can start on.
export function analyzeAdvertised(input) {
  const { assets, fetched } = input;
  const problems = [];
  for (const asset of assets) {
    const answer = fetched.get(asset.name);
    if (!answer) {
      problems.push(`${asset.advertised} was not requested`);
      continue;
    }
    if (answer.error) {
      problems.push(`${asset.advertised} could not be requested: ${answer.error}`);
      continue;
    }
    if (answer.status !== 200) {
      problems.push(`${asset.advertised} answered ${answer.status}; the command the pages publish fails`);
      continue;
    }
    if (answer.text.length === 0) {
      problems.push(`${asset.advertised} answered 200 with an empty body`);
      continue;
    }
    if (asset.name.endsWith('.sh') && !answer.text.startsWith('#!/bin/sh\n')) {
      problems.push(`${asset.advertised} does not begin with #!/bin/sh; it is not the installer`);
    }
  }
  return problems;
}

// rootFileReferences pulls the root-level file names out of one file's Pages
// URLs. A URL with no path, or one whose path enters a version directory, names
// no root file; neither does an extensionless route.
//
// The trailing punctuation is trimmed because most of these URLs are written
// inside sentences, and a period is both a legal file-name character and the
// end of the sentence. Reading it as part of the name reported install.ps1.
// as a published address nothing serves, which is a gate crying wolf about its
// own documentation.
// escapeRegExp quotes a literal for use inside a pattern. The origin carries
// dots and slashes, and an unescaped dot matches any character -- which would
// make this accept a host that merely resembles the one it is looking for.
function escapeRegExp(literal) {
	return literal.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export function rootFileReferences(text, prefix = PAGES_PREFIX) {
  const found = new Set();
  // Built from the one declaration rather than written out. This was a third
  // spelling of the site's address -- after the origin constant and the prose
  // it checks -- and a regexp is the spelling a search for the host finds last
  // (stokaro/ptah#2884). The same scan reads the project site's references when
  // handed SITE_PREFIX.
  const pattern = new RegExp(`${escapeRegExp(prefix)}([A-Za-z0-9._~%\\-/]*)`, 'g');
  let match = pattern.exec(text);
  while (match !== null) {
    const path = match[1].replace(/[.,;:!?)\]}'"]+$/, '');
    if (path !== '' && !path.includes('/') && path.includes('.')) {
      found.add(path);
    }
    match = pattern.exec(text);
  }
  return found;
}

function git(args) {
  return execFileSync('git', ['-C', repoRoot, ...args], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
}

function trackedDocumentationFiles() {
  return git(['ls-files', '-z', '*.md', '*.mdx', '*.yml', '*.yaml']).split('\0').filter(Boolean);
}

function trackedPaths() {
  return new Set(git(['ls-files', '-z']).split('\0').filter(Boolean));
}

// assembleFixture runs the producers exactly as the workflow runs them, into a
// throwaway root holding the version directories a real one holds.
function assembleFixture() {
  const root = mkdtempSync(join(tmpdir(), 'ptah-pages-root-'));
  try {
    for (const version of ['edge', 'v0.2.0']) {
      mkdirSync(join(root, version), { recursive: true });
    }
    for (const producer of ROOT_PRODUCERS) {
      execFileSync('node', [join(repoRoot, producer), root], { stdio: ['ignore', 'pipe', 'pipe'] });
    }
    const produced = new Map();
    for (const name of [...GENERATED_ROOT_FILES, ...ROOT_ASSETS.map((asset) => asset.name)]) {
      const path = join(root, name);
      if (existsSync(path)) {
        produced.set(name, readFileSync(path, 'utf8'));
      }
    }
    return produced;
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

// checkAssembledSite is the `--site` mode: it reads a Pages root that the
// workflow has just built rather than one this gate built for itself.
function checkAssembledSite(siteDir) {
  const problems = [];
  for (const name of [...GENERATED_ROOT_FILES, ...ROOT_ASSETS.map((asset) => asset.name)]) {
    const path = join(siteDir, name);
    if (!existsSync(path)) {
      problems.push(`${siteDir} has no ${name}; the deploy would publish a root without it`);
      continue;
    }
    if (statSync(path).size === 0) {
      problems.push(`${siteDir}/${name} is empty`);
    }
  }
  for (const asset of ROOT_ASSETS) {
    const published = join(siteDir, asset.name);
    if (!existsSync(published)) continue;
    if (readFileSync(published, 'utf8') !== readFileSync(sourcePath(asset), 'utf8')) {
      problems.push(`${siteDir}/${asset.name} is not the bytes of ${asset.source}`);
    }
  }
  return problems;
}

// fetchRoot requests every root file from the published site. A request that
// throws is recorded rather than raised, so one unreachable file does not hide
// what the others answered -- the control in analyzeLive needs all of them.
async function fetchRoot(names) {
  const fetched = new Map();
  for (const name of names) {
    const url = `${PAGES_PREFIX}${name}`;
    try {
      const response = await fetch(url, { redirect: 'follow' });
      fetched.set(name, { status: response.status, text: await response.text() });
    } catch (error) {
      fetched.set(name, { error: error.message });
    }
  }
  return fetched;
}

// fetchAdvertised requests each installer from the address a reader is given,
// recording a failed request the way fetchRoot does.
async function fetchAdvertised(assets) {
  const fetched = new Map();
  for (const asset of assets) {
    try {
      const response = await fetch(asset.advertised, { redirect: 'follow' });
      fetched.set(asset.name, { status: response.status, text: await response.text() });
    } catch (error) {
      fetched.set(asset.name, { error: error.message });
    }
  }
  return fetched;
}

function selftest() {
  const assert = (condition, message) => {
    if (!condition) throw new Error(message);
  };

  const assets = [
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
  ];
  const generated = ['versions.json', 'index.html'];

  const workflow = [
    'jobs:',
    '  build:',
    '    steps:',
    '      - uses: actions/checkout@v7',
    '      - name: Self-test the version generator',
    '        run: node docs/site/scripts/gen-versions.mjs --selftest',
    '      - name: Generate root version index and redirect',
    "        if: github.event_name == 'push'",
    '        run: node docs/site/scripts/gen-versions.mjs "$GITHUB_WORKSPACE/_site"',
    '      - name: Publish the install scripts at the site root',
    "        if: github.event_name == 'push'",
    '        run: node docs/site/scripts/publish-root-assets.mjs "$GITHUB_WORKSPACE/_site"',
    '      - name: Upload Pages artifact',
    "        if: github.event_name == 'push'",
    '        uses: actions/upload-pages-artifact@v5',
  ].join('\n');

  const healthy = () => ({
    assets,
    generated,
    sources: new Map([
      ['install.sh', { exists: true, tracked: true, text: '#!/bin/sh\n' }],
      ['install.ps1', { exists: true, tracked: true, text: "Write-Output 'ptah'\n" }],
    ]),
    assembled: new Map([
      ['versions.json', '{}\n'],
      ['index.html', '<!doctype html>\n'],
      ['install.sh', '#!/bin/sh\n'],
      ['install.ps1', "Write-Output 'ptah'\n"],
    ]),
    workflow,
    documented: new Map([
      ['install.sh', ['docs/site/src/content/docs/start/install.mdx']],
      ['install.ps1', ['docs/site/src/content/docs/start/install.mdx']],
    ]),
    scanned: 400,
  });

  assert(analyze(healthy()).length === 0, `a healthy tree must pass: ${JSON.stringify(analyze(healthy()))}`);

  // Rule 1.
  const untracked = healthy();
  untracked.sources.set('install.sh', { exists: true, tracked: false, text: '#!/bin/sh\n' });
  assert(
    analyze(untracked).some((problem) => problem.includes('not tracked by git')),
    'an untracked source must be reported',
  );

  const empty = healthy();
  empty.sources.set('install.sh', { exists: true, tracked: true, text: '' });
  assert(analyze(empty).some((problem) => problem.includes('is empty')), 'an empty source must be reported');

  const nonAscii = healthy();
  nonAscii.sources.set('install.ps1', { exists: true, tracked: true, text: "Write-Output 'a'\n'no asset — check'\n" });
  const asciiProblems = analyze(nonAscii);
  assert(
    asciiProblems.some((problem) => problem.includes('line 2 is not ASCII')),
    `a non-ASCII .ps1 must be reported with its line: ${JSON.stringify(asciiProblems)}`,
  );
  // The control for that rule. Without it, an ASCII check widened to every
  // asset would read as correct: install.sh is a shell script, where a dash is
  // a dash, and the rule is about one PowerShell decoding behavior.
  const asciiSh = healthy();
  asciiSh.sources.set('install.sh', { exists: true, tracked: true, text: '#!/bin/sh\n# —\n' });
  asciiSh.assembled.set('install.sh', '#!/bin/sh\n# —\n');
  assert(
    analyze(asciiSh).length === 0,
    `the ASCII rule is about .ps1 and must not fire on install.sh: ${JSON.stringify(analyze(asciiSh))}`,
  );

  // Rule 2.
  const missingFromRoot = healthy();
  missingFromRoot.assembled.delete('install.ps1');
  assert(
    analyze(missingFromRoot).some((problem) => problem === 'assembling the Pages root produced no install.ps1'),
    'an asset the assembly does not write must be reported',
  );

  const missingGenerated = healthy();
  missingGenerated.assembled.delete('versions.json');
  assert(
    analyze(missingGenerated).some((problem) => problem.includes('produced no versions.json')),
    'the version index is a root file too',
  );

  const differentBytes = healthy();
  differentBytes.assembled.set('install.sh', '#!/bin/sh\n# stale\n');
  assert(
    analyze(differentBytes).some((problem) => problem.includes('is not the bytes of')),
    'a stale copy must be reported',
  );

  // Rule 3. Deleting the publish step is the failure the whole gate exists for:
  // the tree still has the file, the assembly this gate runs still writes it,
  // and the deploy stops doing so.
  const withoutStep = healthy();
  withoutStep.workflow = workflow.replace(
    '        run: node docs/site/scripts/publish-root-assets.mjs "$GITHUB_WORKSPACE/_site"',
    '        run: echo nothing',
  );
  assert(
    analyze(withoutStep).some((problem) => problem.includes('has no step running docs/site/scripts/publish-root-assets.mjs')),
    'a workflow that stopped running a producer must be reported',
  );

  // The self-test invocation is not the deploy invocation. A gate that matched
  // the script name alone would accept a workflow that only self-tests it.
  const selftestOnly = healthy();
  selftestOnly.workflow = workflow
    .replace('        run: node docs/site/scripts/gen-versions.mjs "$GITHUB_WORKSPACE/_site"', '        run: echo nothing');
  assert(
    analyze(selftestOnly).some((problem) => problem.includes('has no step running docs/site/scripts/gen-versions.mjs')),
    'a producer that only appears in a --selftest step must not count',
  );

  const unguarded = healthy();
  unguarded.workflow = workflow.replace(
    [
      '      - name: Publish the install scripts at the site root',
      "        if: github.event_name == 'push'",
    ].join('\n'),
    '      - name: Publish the install scripts at the site root',
  );
  assert(
    analyze(unguarded).some((problem) => problem.includes('is not guarded by')),
    'a producer step that lost its push condition must be reported',
  );

  const afterUpload = healthy();
  afterUpload.workflow = [
    'jobs:',
    '  build:',
    '    steps:',
    '      - name: Upload Pages artifact',
    "        if: github.event_name == 'push'",
    '        uses: actions/upload-pages-artifact@v5',
    '      - name: Generate root version index and redirect',
    "        if: github.event_name == 'push'",
    '        run: node docs/site/scripts/gen-versions.mjs "$GITHUB_WORKSPACE/_site"',
    '      - name: Publish the install scripts at the site root',
    "        if: github.event_name == 'push'",
    '        run: node docs/site/scripts/publish-root-assets.mjs "$GITHUB_WORKSPACE/_site"',
  ].join('\n');
  assert(
    analyze(afterUpload).filter((problem) => problem.includes('comes after the Pages artifact is uploaded')).length === 2,
    'both producers running after the upload must be reported',
  );

  const noUpload = healthy();
  noUpload.workflow = workflow.replace('        uses: actions/upload-pages-artifact@v5', '        run: echo nothing');
  assert(
    analyze(noUpload).some((problem) => problem.includes('has no actions/upload-pages-artifact step')),
    'a workflow that publishes nothing must be reported',
  );

  // Rule 4.
  const undocumented = healthy();
  undocumented.documented.delete('install.ps1');
  assert(
    analyze(undocumented).some((problem) => problem.includes('no documentation page names')),
    'a served asset no page names must be reported',
  );

  const unserved = healthy();
  unserved.documented.set('install.rb', ['README.md']);
  assert(
    analyze(unserved).some((problem) => problem.includes('which the Pages root assembly does not write')),
    'a documented root URL nothing writes must be reported',
  );

  // An asset named only at the address a reader is given is documented. The
  // pages moved the command to the project site; the root copy is what the
  // addresses already in the wild resolve to, and a page need not name both.
  const advertisedOnly = healthy();
  advertisedOnly.documented.delete('install.ps1');
  advertisedOnly.advertised = new Map([['install.ps1', ['README.md']]]);
  assert(
    analyze(advertisedOnly).length === 0,
    `an asset named at the advertised address is documented: ${JSON.stringify(analyze(advertisedOnly))}`,
  );

  // And the finding for an asset named nowhere says both addresses, so the
  // reader who adds one knows which is the one to give.
  const namedNowhere = healthy();
  namedNowhere.documented.delete('install.ps1');
  assert(
    analyze(namedNowhere).some(
      (problem) => problem.includes(InstallURL('install.ps1')) && problem.includes(RootURL('install.ps1')),
    ),
    'an asset named nowhere is reported with both of its addresses',
  );

  const readNothing = healthy();
  readNothing.scanned = 0;
  assert(
    analyze(readNothing).some((problem) => problem.includes('refusing to pass without having read')),
    'discovery that read nothing must fail closed',
  );

  // --live. Every rule here is a way the published address stops answering
  // while this tree stays green.
  const liveHealthy = () => ({
    assets,
    generated,
    fetched: new Map([
      ['versions.json', { status: 200, text: '{}\n' }],
      ['index.html', { status: 200, text: '<!doctype html>\n' }],
      ['install.sh', { status: 200, text: '#!/bin/sh\n' }],
      ['install.ps1', { status: 200, text: "Write-Output 'ptah'\n" }],
    ]),
    sources: new Map([
      ['install.sh', '#!/bin/sh\n'],
      ['install.ps1', "Write-Output 'ptah'\n"],
    ]),
  });

  assert(analyzeLive(liveHealthy()).length === 0, `a healthy site must pass: ${JSON.stringify(analyzeLive(liveHealthy()))}`);

  // The failure this mode exists for: the tree is intact, the assembly is
  // intact, and the deployed root has lost one file.
  const gone = liveHealthy();
  gone.fetched.set('install.sh', { status: 404, text: 'not found' });
  const goneProblems = analyzeLive(gone);
  assert(
    goneProblems.some((problem) => problem.includes('install.sh answered 404')),
    `a 404 at the published address must be reported: ${JSON.stringify(goneProblems)}`,
  );
  // And it must say what still answered, because "install.sh is 404" and "the
  // whole site is 404" send the reader to different files.
  assert(
    goneProblems.some((problem) => problem.includes('still answered 200')),
    'a partial outage must name the files that still answered',
  );

  const siteGone = liveHealthy();
  for (const name of [...generated, ...assets.map((asset) => asset.name)]) {
    siteGone.fetched.set(name, { status: 404, text: '' });
  }
  const siteGoneProblems = analyzeLive(siteGone);
  assert(siteGoneProblems.length === 4, `every root file must be reported: ${JSON.stringify(siteGoneProblems)}`);
  assert(
    !siteGoneProblems.some((problem) => problem.includes('still answered 200')),
    'a whole-site outage has no control to name',
  );

  const unreachable = liveHealthy();
  unreachable.fetched.set('install.ps1', { error: 'getaddrinfo ENOTFOUND' });
  assert(
    analyzeLive(unreachable).some((problem) => problem.includes('could not be requested')),
    'a request that never completed must be reported, not treated as a pass',
  );

  const emptyBody = liveHealthy();
  emptyBody.fetched.set('install.sh', { status: 200, text: '' });
  assert(
    analyzeLive(emptyBody).some((problem) => problem.includes('empty body')),
    'a 200 with nothing in it must be reported',
  );

  const stale = liveHealthy();
  stale.fetched.set('install.sh', { status: 200, text: '#!/bin/sh\n# something else\n' });
  assert(
    analyzeLive(stale).some((problem) => problem.includes('is not the bytes of')),
    'a served file that is not the tree\'s must be reported',
  );

  const notRequested = liveHealthy();
  notRequested.fetched.delete('install.ps1');
  assert(
    analyzeLive(notRequested).some((problem) => problem.includes('was not requested')),
    'a file nothing asked for must not read as a pass',
  );

  // The advertised address. It belongs to another deploy, so only the answer
  // is judged: each case below is a way the published one-liner fails.
  const advertisedHealthy = () => ({
    assets,
    fetched: new Map([
      ['install.sh', { status: 200, text: '#!/bin/sh\n# the installer\n' }],
      ['install.ps1', { status: 200, text: "Write-Output 'ptah'\n" }],
    ]),
  });
  assert(
    analyzeAdvertised(advertisedHealthy()).length === 0,
    `a site that serves both installers must pass: ${JSON.stringify(analyzeAdvertised(advertisedHealthy()))}`,
  );

  const advertisedGone = advertisedHealthy();
  advertisedGone.fetched.set('install.sh', { status: 404, text: 'not found' });
  assert(
    analyzeAdvertised(advertisedGone).some((problem) => problem.includes(`${InstallURL('install.sh')} answered 404`)),
    'a 404 at the advertised address must be reported',
  );

  const advertisedUnreachable = advertisedHealthy();
  advertisedUnreachable.fetched.set('install.ps1', { error: 'getaddrinfo ENOTFOUND' });
  assert(
    analyzeAdvertised(advertisedUnreachable).some((problem) => problem.includes('could not be requested')),
    'an advertised address that never answered must be reported',
  );

  const advertisedEmpty = advertisedHealthy();
  advertisedEmpty.fetched.set('install.ps1', { status: 200, text: '' });
  assert(
    analyzeAdvertised(advertisedEmpty).some((problem) => problem.includes('empty body')),
    'a 200 with nothing in it at the advertised address must be reported',
  );

  // A site that answers 200 with a page instead of the script -- a 404 page
  // served with the wrong status, a placeholder -- is the failure a `| sh`
  // turns into a shell parsing HTML.
  const advertisedNotAScript = advertisedHealthy();
  advertisedNotAScript.fetched.set('install.sh', { status: 200, text: '<!doctype html>\n' });
  assert(
    analyzeAdvertised(advertisedNotAScript).some((problem) => problem.includes('does not begin with #!/bin/sh')),
    'a 200 that is not a shell script must be reported',
  );

  const advertisedNotRequested = advertisedHealthy();
  advertisedNotRequested.fetched.delete('install.sh');
  assert(
    analyzeAdvertised(advertisedNotRequested).some((problem) => problem.includes('was not requested')),
    'an advertised address nothing asked for must not read as a pass',
  );

  // rootFileReferences reads root files and nothing else.
  const references = rootFileReferences(
    [
      `curl -fsSL ${RootURL('install.sh')} | sh`,
      `irm ${RootURL('install.ps1')} | iex`,
      `the site lives at ${Origin}/`,
      `a page at ${PageURL('edge', 'start/install/')}`,
      `the picker reads ${RootURL('versions.json')}`,
      `published at ${RootURL('install.ps1')}.`,
      `see ${RootURL('install.sh')}, which verifies`,
    ].join('\n'),
  );
  assert(references.has('install.sh'), 'the shell installer URL is a root file');
  assert(references.has('install.ps1'), 'the PowerShell installer URL is a root file');
  assert(references.has('versions.json'), 'the version index is a root file');
  assert(!references.has(''), 'the bare site URL names no root file');
  assert(!references.has('install.ps1.'), 'a sentence-ending period is not part of the file name');
  assert(!references.has('install.sh,'), 'a comma is not part of the file name either');
  assert(references.size === 3, `a versioned page URL names no root file: ${JSON.stringify([...references])}`);

  // The same scan under the project site's prefix reads the advertised
  // addresses and nothing under the documentation root. The root's host ends in
  // the site's host, which is why the prefix carries the scheme.
  const advertisedReferences = rootFileReferences(
    [
      `curl -fsSL ${InstallURL('install.sh')} | sh`,
      `irm ${InstallURL('install.ps1')} | iex`,
      `the site lives at ${SiteOrigin}/`,
      `the picker reads ${RootURL('versions.json')}`,
      `a page at ${PageURL('edge', 'start/install/')}`,
    ].join('\n'),
    SITE_PREFIX,
  );
  assert(advertisedReferences.has('install.sh'), 'the advertised shell installer is read');
  assert(advertisedReferences.has('install.ps1'), 'the advertised PowerShell installer is read');
  assert(
    advertisedReferences.size === 2,
    `a documentation-root URL is not an advertised one: ${JSON.stringify([...advertisedReferences])}`,
  );

  console.log('check-pages-root.mjs --selftest: OK');
}

async function main() {
  const argument = process.argv[2];
  if (argument === '--selftest') {
    selftest();
    return;
  }

  if (argument === '--live') {
    const names = [...GENERATED_ROOT_FILES, ...ROOT_ASSETS.map((asset) => asset.name)];
    const sources = new Map();
    for (const asset of ROOT_ASSETS) {
      const path = sourcePath(asset);
      if (existsSync(path)) sources.set(asset.name, readFileSync(path, 'utf8'));
    }
    const problems = [
      ...analyzeLive({
        assets: ROOT_ASSETS,
        generated: GENERATED_ROOT_FILES,
        fetched: await fetchRoot(names),
        sources,
      }),
      ...analyzeAdvertised({ assets: ROOT_ASSETS, fetched: await fetchAdvertised(ROOT_ASSETS) }),
    ];
    if (problems.length > 0) {
      console.error(`check-pages-root.mjs --live: ${problems.length} problem(s) at ${PAGES_PREFIX} or ${SITE_PREFIX}:`);
      for (const problem of problems) console.error(`- ${problem}`);
      process.exitCode = 1;
      return;
    }
    console.log(
      `check-pages-root.mjs --live: OK (${PAGES_PREFIX} serves ${names.join(', ')}; ` +
        `${SITE_PREFIX} serves ${ROOT_ASSETS.map((asset) => asset.name).join(', ')})`,
    );
    return;
  }

  if (argument === '--site') {
    const siteDir = process.argv[3];
    if (!siteDir) {
      console.error('usage: node scripts/check-pages-root.mjs --site <dir>');
      process.exitCode = 2;
      return;
    }
    const problems = checkAssembledSite(siteDir);
    if (problems.length > 0) {
      console.error(`check-pages-root.mjs: ${problems.length} problem(s) in the assembled Pages root:`);
      for (const problem of problems) console.error(`- ${problem}`);
      process.exitCode = 1;
      return;
    }
    console.log(`check-pages-root.mjs: OK (${siteDir} carries every root file)`);
    return;
  }

  const tracked = trackedPaths();
  const sources = new Map();
  for (const asset of ROOT_ASSETS) {
    const path = sourcePath(asset);
    if (!existsSync(path)) {
      sources.set(asset.name, { exists: false, tracked: false, text: '' });
      continue;
    }
    sources.set(asset.name, {
      exists: true,
      tracked: tracked.has(asset.source),
      text: readFileSync(path, 'utf8'),
    });
  }

  const files = trackedDocumentationFiles();
  const documented = new Map();
  const advertised = new Map();
  for (const file of files) {
    const text = readFileSync(join(repoRoot, file), 'utf8');
    for (const name of rootFileReferences(text)) {
      if (!documented.has(name)) documented.set(name, []);
      documented.get(name).push(file);
    }
    for (const name of rootFileReferences(text, SITE_PREFIX)) {
      if (!advertised.has(name)) advertised.set(name, []);
      advertised.get(name).push(file);
    }
  }

  const problems = analyze({
    assets: ROOT_ASSETS,
    generated: GENERATED_ROOT_FILES,
    sources,
    assembled: assembleFixture(),
    workflow: readFileSync(workflowPath, 'utf8'),
    documented,
    advertised,
    scanned: files.length,
  });

  if (problems.length > 0) {
    console.error(`check-pages-root.mjs: ${problems.length} problem(s) with the Pages root:`);
    for (const problem of problems) console.error(`- ${problem}`);
    process.exitCode = 1;
    return;
  }

  const names = [...GENERATED_ROOT_FILES, ...ROOT_ASSETS.map((asset) => asset.name)];
  console.log(`check-pages-root.mjs: OK (${names.join(', ')} across ${files.length} documentation files)`);
}

await main();
