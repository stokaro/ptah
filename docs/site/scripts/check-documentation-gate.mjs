#!/usr/bin/env node

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const workflowPath = join(scriptDir, '..', '..', '..', '.github', 'workflows', 'docs.yml');

const jobContracts = [
  { changed: 'SITE_CHANGED', result: 'SITE_RESULT', label: 'versioned site' },
  { changed: 'STYLE_CHANGED', result: 'STYLE_RESULT', label: 'documentation style' },
  { changed: 'GENERATED_CHANGED', result: 'GENERATED_RESULT', label: 'generated documentation' },
  { changed: 'INVENTORY_CHANGED', result: 'INVENTORY_RESULT', label: 'feature inventory' },
  { changed: 'EXAMPLES_CHANGED', result: 'EXAMPLES_RESULT', label: 'example acceptance' },
  { changed: 'INFERENCE_CHANGED', result: 'INFERENCE_RESULT', label: 'inference quick start' },
  { changed: 'QUICKSTART_CHANGED', result: 'QUICKSTART_RESULT', label: 'quick-start acceptance' },
];

export function changeFilters(workflow, stepId = 'filter') {
  const filters = {};
  const lines = workflow.split(/\r?\n/);
  const step = lines.findIndex((line) => line.trim() === `- id: ${stepId}`);
  if (step === -1) throw new Error(`Docs workflow has no ${stepId} step`);
  const start = lines.findIndex((line, index) => index > step && /^\s+filters:\s*\|\s*$/.test(line));
  if (start === -1) throw new Error(`Docs workflow step ${stepId} has no paths-filter block`);
  const baseIndent = lines[start].search(/\S/);
  let group;
  for (const line of lines.slice(start + 1)) {
    if (line.trim() === '') continue;
    const indent = line.search(/\S/);
    if (indent <= baseIndent) break;
    const groupMatch = line.match(/^\s{12}([a-z][a-z0-9-]*):\s*$/);
    if (groupMatch) {
      group = groupMatch[1];
      filters[group] = [];
      continue;
    }
    const patternMatch = line.match(/^\s{14}-\s+['"]([^'"]+)['"]\s*$/);
    if (patternMatch && group) filters[group].push(patternMatch[1]);
  }
  return filters;
}

function globExpression(pattern) {
  let expression = '^';
  for (let index = 0; index < pattern.length;) {
    if (pattern.startsWith('**/', index)) {
      expression += '(?:.*/)?';
      index += 3;
    } else if (pattern.startsWith('**', index)) {
      expression += '.*';
      index += 2;
    } else if (pattern[index] === '*') {
      expression += '[^/]*';
      index += 1;
    } else {
      expression += pattern[index].replace(/[\\^$.*+?()[\]{}|]/g, '\\$&');
      index += 1;
    }
  }
  return new RegExp(`${expression}$`);
}

export function groupsForPaths(paths, filters) {
  return Object.entries(filters)
    .filter(([, patterns]) => paths.some((path) => patterns.some((pattern) => globExpression(pattern).test(path))))
    .map(([group]) => group)
    .sort();
}

export function aggregateGate(environment) {
  const problems = [];
  if (environment.CHANGES_RESULT !== 'success') {
    problems.push(`change detection concluded ${JSON.stringify(environment.CHANGES_RESULT)}, want success`);
  }

  for (const contract of jobContracts) {
    if (environment[contract.changed] !== 'true' && environment[contract.changed] !== 'false') {
      problems.push(
        `${contract.label} classification ${contract.changed} is ${JSON.stringify(environment[contract.changed])}, want "true" or "false"`,
      );
      continue;
    }
    const changed = environment[contract.changed] === 'true';
    const result = environment[contract.result];
    const wanted = changed ? 'success' : 'skipped';
    if (result !== wanted) {
      problems.push(`${contract.label} concluded ${JSON.stringify(result)}, want ${wanted} when ${contract.changed}=${JSON.stringify(environment[contract.changed])}`);
    }
  }
  return problems;
}

function fixture(overrides = {}) {
  const environment = { CHANGES_RESULT: 'success' };
  for (const contract of jobContracts) {
    environment[contract.changed] = 'false';
    environment[contract.result] = 'skipped';
  }
  return { ...environment, ...overrides };
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  assert(aggregateGate(fixture()).length === 0, 'unrelated change did not take the fast success path');
  assert(
    aggregateGate(fixture({ SITE_CHANGED: 'true', SITE_RESULT: 'success' })).length === 0,
    'relevant site change did not accept its successful validation',
  );
  assert(
    aggregateGate(fixture({ STYLE_CHANGED: 'true', STYLE_RESULT: 'failure' }))
      .some((problem) => problem.includes('documentation style concluded')),
    'failed relevant job did not fail the aggregate',
  );
  assert(
    aggregateGate(fixture({ QUICKSTART_CHANGED: 'false', QUICKSTART_RESULT: 'success' }))
      .some((problem) => problem.includes('want skipped')),
    'unexpectedly executed no-change job was not detected',
  );
  assert(
    aggregateGate(fixture({ SITE_CHANGED: '' }))
      .some((problem) => problem.includes('SITE_CHANGED') && problem.includes('"true" or "false"')),
    'empty path-classification output false-greened',
  );
  assert(
    aggregateGate(fixture({ STYLE_CHANGED: 'maybe' }))
      .some((problem) => problem.includes('STYLE_CHANGED') && problem.includes('"true" or "false"')),
    'invalid path-classification output false-greened',
  );

  const filters = changeFilters(readFileSync(workflowPath, 'utf8'));
  const pathCases = [
    {
      label: 'unrelated Go-only change',
      paths: ['internal/retry/backoff.go'],
      groups: ['examples', 'generated', 'inventory'],
    },
    {
      label: 'docs-site package-only bump',
      paths: ['docs/site/package.json', 'docs/site/package-lock.json'],
      groups: ['site', 'style'],
    },
    {
      label: 'top-level contributor Markdown',
      paths: ['PARSER_IMPLEMENTATION.md'],
      groups: ['style'],
    },
    {
      label: 'documentation-only change',
      paths: ['docs/site/src/content/docs/concepts/desired-schema-and-sources.md'],
      groups: ['inventory', 'site', 'style'],
    },
    {
      label: 'generated command-reference change',
      paths: ['docs/site/src/content/docs/reference/command-flags.md'],
      groups: ['generated', 'inventory', 'site', 'style'],
    },
    {
      label: 'example change',
      paths: ['examples/portable/README.md'],
      groups: ['examples', 'style'],
    },
    {
      label: 'inference quick-start change',
      paths: ['docs/site/fixtures/inference-quick-start/compose.yaml'],
      groups: ['inference', 'site', 'style'],
    },
    {
      label: 'default quick-start change',
      paths: ['docs/site/src/content/docs/start/quick-start.mdx'],
      groups: ['inventory', 'quickstart', 'site', 'style'],
    },
    {
      label: 'root Go dependency change',
      paths: ['go.mod'],
      groups: ['examples', 'generated', 'inference', 'inventory', 'quickstart'],
    },
    {
      label: 'nested documentation module source',
      paths: ['docs/site/fixtures/source-equivalence/models/schema.go'],
      groups: ['examples', 'generated', 'inventory', 'site', 'style'],
    },
    {
      label: 'Protobuf export fixture',
      paths: ['docs/site/fixtures/protobuf-export/schema.yaml'],
      groups: ['generated', 'site', 'style'],
    },
  ];
  for (const pathCase of pathCases) {
    const actual = groupsForPaths(pathCase.paths, filters);
    assert(
      JSON.stringify(actual) === JSON.stringify(pathCase.groups),
      `${pathCase.label} selected ${actual.join(', ') || '(none)'}, want ${pathCase.groups.join(', ') || '(none)'}`,
    );
    const environment = fixture();
    for (const group of actual) {
      const contract = jobContracts.find(({ changed }) => changed === `${group.toUpperCase()}_CHANGED`);
      assert(contract, `${pathCase.label} selected unknown validation group ${group}`);
      environment[contract.changed] = 'true';
      environment[contract.result] = 'success';
    }
    assert(aggregateGate(environment).length === 0, `${pathCase.label} did not produce a successful aggregate`);
  }

  const embeddedFilters = changeFilters(readFileSync(workflowPath, 'utf8'), 'embedded-filter');
  const embeddedCases = [
    { path: 'docs/site/package.json', selected: false },
    { path: 'README.md', selected: true },
    { path: 'docs/architecture_boundaries.json', selected: true },
    { path: 'docs/site/src/content/docs/start/quick-start.mdx', selected: true },
    { path: 'docs/site/scripts/data/feature-matrix-rows.json', selected: true },
  ];
  for (const embeddedCase of embeddedCases) {
    const selected = groupsForPaths([embeddedCase.path], embeddedFilters).includes('embedded');
    assert(
      selected === embeddedCase.selected,
      `${embeddedCase.path} embedded=${selected}, want ${embeddedCase.selected}`,
    );
  }

  console.log(`check-documentation-gate.mjs --selftest: OK (${pathCases.length} path shapes, ${embeddedCases.length} Go-contract scopes, and aggregate failures)`);
}

function main() {
  if (process.argv.includes('--selftest')) {
    selftest();
    return;
  }
  if (process.argv.length > 2) {
    console.error('usage: node scripts/check-documentation-gate.mjs [--selftest]');
    process.exitCode = 2;
    return;
  }

  const problems = aggregateGate(process.env);
  if (problems.length > 0) {
    console.error('Documentation gate: FAILED');
    for (const problem of problems) console.error(`  ${problem}`);
    process.exitCode = 1;
    return;
  }
  const applicable = jobContracts.filter((contract) => process.env[contract.changed] === 'true').length;
  console.log(`Documentation gate: OK (${applicable} applicable validation group(s))`);
}

main();
