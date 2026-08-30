#!/usr/bin/env node
// Enforce the declarative visual-output and visual-asset contracts. File names
// belong in the manifests; this gate derives its checks from those declarations.
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { manifestProblems, readVisualManifests } from './lib/visual-contract.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const repositoryRoot = join(siteRoot, '..', '..');
const contentRoot = join(siteRoot, 'src', 'content', 'docs');

function problemsFor(assetManifest, outputInventory) {
  return manifestProblems({ assetManifest, outputInventory, repositoryRoot, contentRoot });
}

function selftest() {
  const { assets, outputs } = readVisualManifests(siteRoot);
  const duplicate = structuredClone(assets);
  duplicate.assets.push(structuredClone(duplicate.assets[0]));
  const inaccessible = structuredClone(assets);
  const semantic = inaccessible.assets.find(({ type }) => type === 'semantic-diagram');
  semantic.descriptionRequired = true;
  semantic.path = 'docs/site/src/assets/logo.svg';
  const missingOutput = structuredClone(outputs);
  missingOutput.outputs = missingOutput.outputs.filter(({ id }) => id !== 'schema-viz-security');
  const unclassified = structuredClone(outputs);
  delete unclassified.outputs[0].fixtureClass;

  const findings = [
    ...problemsFor(duplicate, outputs),
    ...problemsFor(inaccessible, outputs),
    ...problemsFor(assets, missingOutput),
    ...problemsFor(assets, unclassified),
  ];
  for (const expected of ['duplicate asset id', 'SVG has no description', 'missing required output schema-viz-security', 'fixtureClass is required']) {
    if (!findings.some((finding) => finding.includes(expected))) {
      console.error(`check-visual-assets.mjs --selftest: FAILED (did not reject ${expected})`);
      process.exitCode = 1;
      return;
    }
  }
  console.log('check-visual-assets.mjs --selftest: OK (duplicates, inaccessible SVG, missing output, and unclassified fixtures rejected)');
}

if (process.argv.includes('--selftest')) {
  selftest();
} else {
  const { assets, outputs } = readVisualManifests(siteRoot);
  const problems = problemsFor(assets, outputs);
  if (problems.length > 0) {
    console.error('check-visual-assets.mjs: FAILED');
    for (const problem of problems) console.error(`- ${problem}`);
    process.exitCode = 1;
  } else {
    console.log(`check-visual-assets.mjs: OK (${assets.assets.length} declared assets, ${outputs.outputs.length} product-output capabilities, ${assets.proofs.length} enforced proof)`);
  }
}
