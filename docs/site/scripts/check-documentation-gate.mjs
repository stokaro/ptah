#!/usr/bin/env node

const jobContracts = [
  { changed: 'SITE_CHANGED', result: 'SITE_RESULT', label: 'versioned site' },
  { changed: 'STYLE_CHANGED', result: 'STYLE_RESULT', label: 'documentation style' },
  { changed: 'GENERATED_CHANGED', result: 'GENERATED_RESULT', label: 'generated documentation' },
  { changed: 'EXAMPLES_CHANGED', result: 'EXAMPLES_RESULT', label: 'example acceptance' },
  { changed: 'INFERENCE_CHANGED', result: 'INFERENCE_RESULT', label: 'inference quick start' },
  { changed: 'QUICKSTART_CHANGED', result: 'QUICKSTART_RESULT', label: 'quick-start acceptance' },
];

export function aggregateGate(environment) {
  const problems = [];
  if (environment.CHANGES_RESULT !== 'success') {
    problems.push(`change detection concluded ${JSON.stringify(environment.CHANGES_RESULT)}, want success`);
  }

  for (const contract of jobContracts) {
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
  console.log('check-documentation-gate.mjs --selftest: OK (relevant and no-change paths)');
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
