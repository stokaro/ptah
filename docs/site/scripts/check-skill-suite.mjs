#!/usr/bin/env node
// Holds the documentation-maintenance skill's validation suite to the checks
// .github/workflows/docs.yml runs.
//
// The skill tells an agent to run "the complete suite" and that a local failure
// is a CI failure. That is only true while its list names what the workflow
// runs, and the two are edited by different changes: a new check lands in the
// workflow, and the list a contributor reads keeps naming the old set. Nothing
// fails, and the next change that breaks the new check is pushed green locally.
//
// So the comparison is between invocations, not text. Each `npm run <script>`
// is resolved through docs/site/package.json to the script file it runs, and
// each `node .../scripts/<file>.mjs` is taken as written. An invocation is its
// file and the names of the flags it passes, because `--selftest`, `--check`,
// `--write` and `--against` run different code of one script, and a flag's
// value -- a base commit, an output directory -- is the caller's. An npm
// script that runs no script file is compared by its name.
//
// Two directions fail. A check the workflow runs over the working tree and the
// skill does not list is the drift this exists for. A check the skill lists
// that the workflow does not run, or that no script backs, is a list naming
// something deleted, renamed, or never run.
//
// Some invocations belong to the deployment rather than to the tree: they
// assemble or publish the site, or read the results of other jobs. They are
// named in ciOnly with the reason, and an entry the workflow no longer runs
// fails, so the list cannot outlive what it describes.
//
// Only the suite block counts: the bash block that follows the
// `<!-- documentation-suite -->` marker in the skill. A command in another block
// is an example of something else, not part of what the skill says to run.
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repoRoot = join(siteRoot, '..', '..');
const workflowPath = join(repoRoot, '.github', 'workflows', 'docs.yml');
const skillPath = join(repoRoot, '.agents', 'skills', 'ptah-documentation-maintenance', 'SKILL.md');
const packagePath = join(siteRoot, 'package.json');

export const marker = '<!-- documentation-suite -->';

// The workflow runs far more than this; a pattern that stopped matching would
// find a handful and compare them successfully.
const minimumWorkflowChecks = 60;

const ciOnly = new Map([
  ['apply-docs-address.mjs --target', 'rewrites a checkout for the published address before a build'],
  ['apply-release-ui-overlay.mjs --target --version', 'overlays the current shell onto a released tag before its build'],
  ['check-deployment-candidate.mjs', 'compares the Pages candidate with the live deployment'],
  ['check-deployment-candidate.mjs --candidate-commit --candidate-versions --base-url', 'records the Pages candidate the deployment publishes'],
  ['check-documentation-gate.mjs', "reads the conclusions of the workflow's other jobs"],
  ['check-pages-root.mjs --site', 'reads the assembled Pages root, which only the deployment build writes'],
  ['check-public-docs.mjs', 'reads the published site after a deployment'],
  ['check-release-page-actions.mjs --dist --version --source-commit', 'reads a released tag built by the deployment'],
  ['gen-versions.mjs', 'writes the version index into the assembled Pages root'],
  ['publish-root-assets.mjs', 'copies the install scripts into the assembled Pages root'],
  ['release-doc-tags.mjs', 'chooses which released tags the deployment builds'],
  ['write-build-info.mjs --output --version --source-commit', 'records the build a deployment produced'],
]);

const npmRun = /\bnpm run ([a-z0-9:-]+)((?: --(?: [^\n|&;]*)?)?)/g;
const nodeScript = /\bnode (?:docs\/site\/)?scripts\/((?:lib\/)?[a-z0-9-]+\.mjs)([^\n|&;]*)/g;

// flagNames returns the identity suffix of an argument string: the names of the
// flags it passes, in order, without their values.
function flagNames(args) {
  const names = [];
  for (const word of args.split(/\s+/)) {
    const name = word.split('=')[0];
    if (/^--?[a-z][a-z0-9-]*$/.test(name) && name !== '--' && !names.includes(name)) names.push(name);
  }
  return names.length === 0 ? '' : ` ${names.join(' ')}`;
}

// joinContinuations folds a shell line continued with a trailing backslash,
// so a command written over several lines is read whole.
function joinContinuations(text) {
  return text.replace(/\\\n\s*/g, ' ');
}

// invocations returns the identities of the checks text runs, in order and
// without repeats, and the npm scripts it names that package.json lacks.
export function invocations(text, scripts) {
  const found = [];
  const unknown = [];
  const add = (key) => {
    if (!found.includes(key)) found.push(key);
  };
  const joined = joinContinuations(text);
  for (const match of joined.matchAll(npmRun)) {
    const script = scripts[match[1]];
    if (script === undefined) {
      if (!unknown.includes(match[1])) unknown.push(match[1]);
      continue;
    }
    const target = /^node scripts\/((?:lib\/)?[a-z0-9-]+\.mjs)(.*)$/.exec(script);
    add(target ? target[1] + flagNames(`${target[2]} ${match[2].replace(/^ --/, '')}`) : `npm run ${match[1]}`);
  }
  for (const match of joined.matchAll(nodeScript)) add(match[1] + flagNames(match[2]));
  return { found, unknown };
}

// suiteBlock returns the bash block that follows the marker, or null.
export function suiteBlock(markdown) {
  const lines = markdown.split('\n');
  const at = lines.findIndex((line) => line.trim() === marker);
  if (at === -1) return null;
  const open = lines.findIndex((line, index) => index > at && /^```(bash|sh)\b/.test(line.trim()));
  if (open === -1) return null;
  const body = [];
  for (const line of lines.slice(open + 1)) {
    if (line.trim().startsWith('```')) break;
    // A comment line is where the suite says how to run a check another way,
    // which is not an instruction to run that too.
    if (!line.trim().startsWith('#')) body.push(line);
  }
  return body.join('\n');
}

// compare returns the findings for a workflow, a skill and a package manifest.
export function compare(workflow, skill, scripts) {
  const findings = [];
  const suite = suiteBlock(skill);
  if (suite === null) return [`the skill has no bash block after ${marker}`];
  const ran = invocations(workflow, scripts);
  const listed = invocations(suite, scripts);
  if (ran.found.length < minimumWorkflowChecks) {
    findings.push(`the workflow yielded ${ran.found.length} checks, below the floor of ${minimumWorkflowChecks}; the patterns no longer read it`);
  }
  for (const key of ran.found) {
    if (!ciOnly.has(key) && !listed.found.includes(key)) findings.push(`the workflow runs ${key} and the skill's suite does not list it`);
  }
  for (const name of listed.unknown) findings.push(`the skill's suite runs npm script ${name}, which package.json does not define`);
  for (const name of ran.unknown) findings.push(`the workflow runs npm script ${name}, which package.json does not define`);
  for (const key of listed.found) {
    if (!ran.found.includes(key)) findings.push(`the skill's suite lists ${key}, which the workflow does not run`);
  }
  for (const key of ciOnly.keys()) {
    if (!ran.found.includes(key)) findings.push(`ciOnly names ${key}, which the workflow no longer runs`);
  }
  return findings;
}

function selftest() {
  const failures = [];
  const scripts = {
    'check:style': 'node scripts/check-style.mjs',
    'check:style:selftest': 'node scripts/check-style.mjs --selftest',
    'check:diagrams': 'node scripts/generate-journey-diagrams.mjs --check',
    'check:docroutes:selftest': 'node scripts/lib/docroutes.mjs --selftest',
    'check:examples': 'node scripts/check-examples.mjs',
    'examples:write': 'node scripts/check-examples.mjs --write',
    'check:snapshots': 'node scripts/check-visual-snapshots.mjs',
    build: 'astro build',
  };
  const filler = Array.from({ length: minimumWorkflowChecks }, (_, i) => `node docs/site/scripts/check-f${i}.mjs`).join('\n');
  const fillerSuite = Array.from({ length: minimumWorkflowChecks }, (_, i) => `node scripts/check-f${i}.mjs`).join('\n');
  const workflow = [
    'run: |',
    '  npm run check:style:selftest',
    '  npm run check:style',
    '  npm run check:diagrams',
    '  npm run check:docroutes:selftest',
    '  npm run check:examples',
    '  npm run build',
    '  npm run check:snapshots -- --output "$RUNNER_TEMP/out"',
    '  node docs/site/scripts/check-route-retirement.mjs --against "$base"',
    '  node docs/site/scripts/write-build-info.mjs \\',
    '    --output dist/build-info.json --version edge --source-commit "$SHA"',
    ...[...ciOnly.keys()].filter((key) => !key.startsWith('write-build-info')).map((key) => `  node docs/site/scripts/${key.replace(/ (--[a-z-]+)/g, ' $1 x')}`),
    filler,
  ].join('\n');
  const skill = (body, extra = '') => `Run:\n\n${marker}\n\n\`\`\`bash\ncd docs/site\nnpm ci\n${body}\n${fillerSuite}\n\`\`\`\n${extra}`;
  const complete = [
    'npm run check:style:selftest',
    'node scripts/check-style.mjs',
    'npm run check:diagrams',
    'npm run check:docroutes:selftest',
    'npm run check:examples',
    'npm run build',
    'npm run check:snapshots -- --output /tmp/out',
    'node scripts/check-route-retirement.mjs --against origin/master',
  ].join('\n');

  const clean = compare(workflow, skill(complete), scripts);
  if (clean.length > 0) failures.push(`a complete suite produced findings: ${clean.join('; ')}`);

  const cases = [
    ['a check the skill omits', skill(complete.replace('npm run check:diagrams\n', '')), 'generate-journey-diagrams.mjs --check'],
    ['a selftest counted as its check', skill(complete.replace('npm run check:style:selftest\n', '')), 'check-style.mjs --selftest'],
    ['a check under scripts/lib', skill(complete.replace('npm run check:docroutes:selftest\n', '')), 'lib/docroutes.mjs --selftest'],
    ['a writer in place of its check', skill(complete.replace('npm run check:examples', 'npm run examples:write')), 'check-examples.mjs --write'],
    ['another mode of the same script', skill(complete.replace('--against origin/master', '--forget /old/')), 'check-route-retirement.mjs --forget'],
    ['an npm script that runs no file', skill(complete.replace('npm run build\n', '')), 'npm run build'],
    ['an npm script nothing defines', skill(`${complete}\nnpm run check:gone`), 'check:gone'],
    ['a check the workflow does not run', skill(`${complete}\nnode scripts/check-extra.mjs`), 'check-extra.mjs'],
    ['a check named only in a comment', skill(complete.replace('\nnode scripts/check-route-retirement.mjs', '\n# node scripts/check-route-retirement.mjs')), 'check-route-retirement.mjs --against'],
    ['a check moved to another block', skill(complete.replace('npm run check:diagrams\n', ''), '\n```bash\nnpm run check:diagrams\n```\n'), 'generate-journey-diagrams.mjs --check'],
  ];
  for (const [name, text, needle] of cases) {
    if (!compare(workflow, text, scripts).some((finding) => finding.includes(needle))) failures.push(`${name} was not reported`);
  }
  if (!compare(workflow.replace('  node docs/site/scripts/gen-versions.mjs\n', ''), skill(complete), scripts).some((f) => f.includes('ciOnly names gen-versions.mjs'))) {
    failures.push('a ciOnly entry the workflow stopped running was not reported');
  }
  if (!compare(workflow, `\`\`\`bash\n${complete}\n\`\`\`\n`, scripts).some((f) => f.includes('no bash block after'))) {
    failures.push('a skill without the suite marker was not reported');
  }
  if (!compare('npm run check:style', skill(''), scripts).some((f) => f.includes('below the floor'))) {
    failures.push('a workflow below the floor was not reported');
  }

  if (failures.length > 0) {
    console.error('check-skill-suite.mjs --selftest: FAILED');
    for (const failure of failures) console.error(`- ${failure}`);
    process.exitCode = 1;
    return;
  }
  console.log(`check-skill-suite.mjs --selftest: OK (${cases.length + 4} assertions via compare())`);
}

function main() {
  if (process.argv[2] === '--selftest') {
    selftest();
    return;
  }
  const scripts = JSON.parse(readFileSync(packagePath, 'utf8')).scripts;
  const workflow = readFileSync(workflowPath, 'utf8');
  const findings = compare(workflow, readFileSync(skillPath, 'utf8'), scripts);
  if (findings.length > 0) {
    console.error('check-skill-suite.mjs: FAILED');
    for (const finding of findings) console.error(`- ${finding}`);
    console.error('Update the suite in .agents/skills/ptah-documentation-maintenance/SKILL.md.');
    process.exitCode = 1;
    return;
  }
  console.log(`check-skill-suite.mjs: OK (${invocations(workflow, scripts).found.length} workflow checks)`);
}

main();
