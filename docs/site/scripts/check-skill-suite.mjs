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
// each `node .../scripts/<file>.mjs` is taken as written. An invocation is
// identified by its file and its mode -- `--selftest`, `--check`, or neither --
// because those are different checks of one script, and the arguments a
// workflow step passes (`--against "$base"`, an output directory) are not.
//
// Two directions fail. A check the workflow runs over the working tree and the
// skill does not list is the drift this exists for. A check the skill lists
// that no script backs is a list naming something deleted or renamed.
//
// Some invocations belong to the deployment rather than to the tree: they
// assemble or publish the site, or read the results of other jobs. They are
// named in ciOnly with the reason, and an entry the workflow no longer runs
// fails, so the list cannot outlive what it describes.
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repoRoot = join(siteRoot, '..', '..');
const workflowPath = join(repoRoot, '.github', 'workflows', 'docs.yml');
const skillPath = join(repoRoot, '.agents', 'skills', 'ptah-documentation-maintenance', 'SKILL.md');
const packagePath = join(siteRoot, 'package.json');

// The workflow runs far more than this; a pattern that stopped matching would
// find a handful and compare them successfully.
const minimumWorkflowChecks = 60;

const ciOnly = new Map([
  ['apply-docs-address.mjs', 'rewrites a checkout for the published address before a build'],
  ['apply-release-ui-overlay.mjs', 'overlays the current shell onto a released tag before its build'],
  ['check-build-info.mjs', 'reads the build record write-build-info.mjs writes during a deployment build'],
  ['check-deployment-candidate.mjs', 'compares the Pages candidate with the live deployment'],
  ['check-documentation-gate.mjs', "reads the conclusions of the workflow's other jobs"],
  ['check-pages-root.mjs', 'reads the assembled Pages root, which only the deployment build writes'],
  ['check-public-docs.mjs', 'reads the published site after a deployment'],
  ['check-release-page-actions.mjs', 'reads a released tag built by the deployment'],
  ['gen-versions.mjs', 'writes the version index into the assembled Pages root'],
  ['publish-root-assets.mjs', 'copies the install scripts into the assembled Pages root'],
  ['release-doc-tags.mjs', 'chooses which released tags the deployment builds'],
  ['write-build-info.mjs', 'records the build a deployment produced'],
]);

const npmRun = /\bnpm run ([a-z0-9:-]+)/g;
const nodeScript = /\bnode (?:docs\/site\/)?scripts\/([a-z0-9-]+\.mjs)([^\n|&;]*)/g;

// mode is what part of a script an invocation runs.
function mode(args) {
  if (/(^|\s)--selftest\b/.test(args)) return ' --selftest';
  if (/(^|\s)--check\b/.test(args)) return ' --check';
  return '';
}

// invocations returns the identities of the checks text runs, in order and
// without repeats, and the npm scripts it names that package.json lacks.
export function invocations(text, scripts) {
  const found = [];
  const unknown = [];
  const add = (key) => {
    if (!found.includes(key)) found.push(key);
  };
  for (const match of text.matchAll(npmRun)) {
    const script = scripts[match[1]];
    if (script === undefined) {
      if (!['build', 'ci'].includes(match[1]) && !unknown.includes(match[1])) unknown.push(match[1]);
      continue;
    }
    const target = /^node scripts\/([a-z0-9-]+\.mjs)(.*)$/.exec(script);
    if (target) add(target[1] + mode(target[2]));
  }
  for (const match of text.matchAll(nodeScript)) add(match[1] + mode(match[2]));
  return { found, unknown };
}

// suiteBlocks returns the fenced bash blocks of the skill, which is where its
// commands are. Prose that mentions a check by name is not an instruction to
// run it, and neither is a comment line inside a block, which is where the
// suite says how to run a check another way.
export function suiteBlocks(markdown) {
  const blocks = [];
  let current = null;
  for (const line of markdown.split('\n')) {
    const trimmed = line.trim();
    if (current === null && /^```(bash|sh)\b/.test(trimmed)) {
      current = [];
    } else if (current !== null && trimmed.startsWith('```')) {
      blocks.push(current.join('\n'));
      current = null;
    } else if (current !== null && !trimmed.startsWith('#')) {
      current.push(line);
    }
  }
  return blocks.join('\n');
}

// compare returns the findings for a workflow, a skill and a package manifest.
export function compare(workflow, skill, scripts) {
  const findings = [];
  const ran = invocations(workflow, scripts);
  const listed = invocations(suiteBlocks(skill), scripts);
  if (ran.found.length < minimumWorkflowChecks) {
    findings.push(`the workflow yielded ${ran.found.length} checks, below the floor of ${minimumWorkflowChecks}; the patterns no longer read it`);
  }
  for (const key of ran.found) {
    const file = key.split(' ')[0];
    if (key === file && ciOnly.has(file)) continue;
    if (!listed.found.includes(key)) findings.push(`the workflow runs ${key} and the skill's suite does not list it`);
  }
  for (const name of listed.unknown) findings.push(`the skill's suite runs npm script ${name}, which package.json does not define`);
  for (const name of ran.unknown) findings.push(`the workflow runs npm script ${name}, which package.json does not define`);
  for (const key of listed.found) {
    if (!ran.found.includes(key)) findings.push(`the skill's suite lists ${key}, which the workflow does not run`);
  }
  for (const file of ciOnly.keys()) {
    if (!ran.found.includes(file)) findings.push(`ciOnly names ${file}, which the workflow no longer runs`);
  }
  return findings;
}

function selftest() {
  const failures = [];
  const scripts = {
    'check:style': 'node scripts/check-style.mjs',
    'check:style:selftest': 'node scripts/check-style.mjs --selftest',
    'check:diagrams': 'node scripts/generate-journey-diagrams.mjs --check',
  };
  const filler = Array.from({ length: minimumWorkflowChecks }, (_, i) => `node docs/site/scripts/check-f${i}.mjs`).join('\n');
  const fillerSuite = Array.from({ length: minimumWorkflowChecks }, (_, i) => `node scripts/check-f${i}.mjs`).join('\n');
  const workflow = [
    'run: |',
    '  npm run check:style:selftest',
    '  npm run check:style',
    '  npm run check:diagrams',
    '  node docs/site/scripts/check-route-retirement.mjs --against "$base"',
    ...[...ciOnly.keys()].map((file) => `  node docs/site/scripts/${file}`),
    filler,
  ].join('\n');
  const skill = (body) => `Run:\n\n\`\`\`bash\ncd docs/site\nnpm ci\n${body}\n${fillerSuite}\n\`\`\`\n`;
  const complete = 'npm run check:style:selftest\nnode scripts/check-style.mjs\nnpm run check:diagrams\nnode scripts/check-route-retirement.mjs';

  const clean = compare(workflow, skill(complete), scripts);
  if (clean.length > 0) failures.push(`a complete suite produced findings: ${clean.join('; ')}`);

  const cases = [
    ['a check the skill omits', skill(complete.replace('npm run check:diagrams\n', '')), 'generate-journey-diagrams.mjs --check'],
    ['a selftest counted as its check', skill(complete.replace('npm run check:style:selftest\n', '')), 'check-style.mjs --selftest'],
    ['an npm script nothing defines', skill(`${complete}\nnpm run check:gone`), 'check:gone'],
    ['a check the workflow does not run', skill(`${complete}\nnode scripts/check-extra.mjs`), 'check-extra.mjs'],
    ['a check named only in prose', `Run check-route-retirement.mjs.\n${skill(complete.replace('\nnode scripts/check-route-retirement.mjs', ''))}`, 'check-route-retirement.mjs'],
    ['a check named only in a comment', skill(complete.replace('\nnode scripts/check-route-retirement.mjs', '\n# node scripts/check-route-retirement.mjs')), 'check-route-retirement.mjs'],
  ];
  for (const [name, text, needle] of cases) {
    if (!compare(workflow, text, scripts).some((finding) => finding.includes(needle))) failures.push(`${name} was not reported`);
  }
  if (!compare(workflow.replace('  node docs/site/scripts/gen-versions.mjs\n', ''), skill(complete), scripts).some((f) => f.includes('ciOnly names gen-versions.mjs'))) {
    failures.push('a ciOnly entry the workflow stopped running was not reported');
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
  console.log(`check-skill-suite.mjs --selftest: OK (${cases.length + 3} assertions via compare())`);
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
