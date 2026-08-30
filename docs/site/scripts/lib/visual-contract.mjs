import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { basename, extname, join, relative } from 'node:path';

export const assetTypes = new Set([
  'semantic-diagram',
  'product-screenshot',
  'generated-report-preview',
  'generated-product-output',
  'decorative',
]);

export const deliveryStates = new Set(['verified', 'partial', 'phase4']);

const requiredOutputIds = new Set([
  'schema-viz',
  'schema-viz-security',
  'schema-document',
  'schema-serve',
  'migration-safety-report',
  'migration-test-report',
  'schema-test-report',
  'schema-lineage',
  'schema-stats',
  'api-contracts',
]);

export function readVisualManifests(siteRoot) {
  return {
    assets: JSON.parse(readFileSync(join(siteRoot, 'visual-assets.json'), 'utf8')),
    outputs: JSON.parse(readFileSync(join(siteRoot, 'visual-output-inventory.json'), 'utf8')),
  };
}

function filesBelow(root, extensions) {
  const files = [];
  const walk = (directory) => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const full = join(directory, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.isFile() && extensions.has(extname(entry.name))) files.push(full);
    }
  };
  walk(root);
  return files.sort();
}

function duplicateProblems(rows, key, label) {
  const seen = new Set();
  const problems = [];
  for (const row of rows) {
    if (seen.has(row[key])) problems.push(`duplicate ${label} ${row[key]}`);
    seen.add(row[key]);
  }
  return problems;
}

export function manifestProblems({ assetManifest, outputInventory, repositoryRoot, contentRoot }) {
  const problems = [];
  if (assetManifest.schemaVersion !== 1) problems.push('visual-assets.json: schemaVersion must be 1');
  if (outputInventory.schemaVersion !== 1) problems.push('visual-output-inventory.json: schemaVersion must be 1');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(outputInventory.verifiedAt ?? '')) {
    problems.push('visual-output-inventory.json: verifiedAt must be YYYY-MM-DD');
  }

  const assets = assetManifest.assets ?? [];
  const outputs = outputInventory.outputs ?? [];
  const proofs = assetManifest.proofs ?? [];
  const snapshots = assetManifest.snapshotRoutes ?? [];
  problems.push(...duplicateProblems(assets, 'id', 'asset id'));
  problems.push(...duplicateProblems(assets, 'path', 'asset path'));
  problems.push(...duplicateProblems(outputs, 'id', 'output id'));
  problems.push(...duplicateProblems(snapshots, 'name', 'snapshot name'));

  const assetIds = new Set(assets.map(({ id }) => id));
  const assetPaths = new Set(assets.map(({ path }) => path));
  const snapshotRoutes = new Set(snapshots.map(({ route }) => route));
  const content = filesBelow(contentRoot, new Set(['.md', '.mdx']))
    .map((file) => readFileSync(file, 'utf8'))
    .join('\n');

  for (const asset of assets) {
    const where = asset.id || '<asset without id>';
    if (!assetTypes.has(asset.type)) problems.push(`${where}: unknown asset type ${asset.type}`);
    if (!asset.path || !existsSync(join(repositoryRoot, asset.path))) {
      problems.push(`${where}: asset path does not exist: ${asset.path}`);
      continue;
    }
    const file = join(repositoryRoot, asset.path);
    if (!Number.isInteger(asset.sizeLimitBytes) || asset.sizeLimitBytes <= 0) {
      problems.push(`${where}: sizeLimitBytes must be a positive integer`);
    } else if (statSync(file).size > asset.sizeLimitBytes) {
      problems.push(`${where}: ${statSync(file).size} bytes exceeds ${asset.sizeLimitBytes}`);
    }
    if (!Array.isArray(asset.pages) || asset.pages.length === 0 || asset.pages.some((page) => !page.startsWith('/'))) {
      problems.push(`${where}: pages must contain reader routes`);
    }
    if (!asset.themeBehavior || !asset.fullSizeArtifact || typeof asset.versionedDownload !== 'boolean') {
      problems.push(`${where}: themeBehavior, fullSizeArtifact, and versionedDownload are required`);
    }
    if (asset.fullSizeArtifact && !existsSync(join(repositoryRoot, asset.fullSizeArtifact))) {
      problems.push(`${where}: full-size artifact does not exist: ${asset.fullSizeArtifact}`);
    }
    if (!content.includes(basename(asset.path)) && asset.id !== 'ptah-logo') {
      problems.push(`${where}: asset is not used by a reader page`);
    }

    const extension = extname(asset.path);
    if (extension === '.svg') {
      const source = readFileSync(file, 'utf8');
      if (asset.titleRequired && !source.includes('<title')) problems.push(`${where}: SVG has no title`);
      if (asset.descriptionRequired && !source.includes('<desc')) problems.push(`${where}: SVG has no description`);
    }
    if (extension === '.png' && asset.type !== 'product-screenshot' && asset.type !== 'generated-report-preview') {
      problems.push(`${where}: PNG is not declared as real product or report output`);
    }
    if (asset.type === 'semantic-diagram' && (!asset.editableSource || !existsSync(join(repositoryRoot, asset.editableSource)))) {
      problems.push(`${where}: semantic diagram has no committed editable source`);
    }
    if (asset.type === 'product-screenshot') {
      if (!asset.fixture || !existsSync(join(repositoryRoot, asset.fixture))) problems.push(`${where}: screenshot fixture is missing`);
      if (!asset.generator || !existsSync(join(repositoryRoot, asset.generator))) problems.push(`${where}: screenshot generator is missing`);
      if (asset.volatileDataNormalized !== true) problems.push(`${where}: screenshot does not declare volatile-data normalization`);
    }
  }

  const scannedAssets = filesBelow(join(repositoryRoot, 'docs', 'site', 'src', 'assets'), new Set(['.png', '.svg']));
  for (const file of scannedAssets) {
    const path = relative(repositoryRoot, file);
    if (!assetPaths.has(path)) problems.push(`${path}: visual asset is undeclared`);
  }

  const foundOutputIds = new Set(outputs.map(({ id }) => id));
  for (const id of requiredOutputIds) {
    if (!foundOutputIds.has(id)) problems.push(`visual-output-inventory.json: missing required output ${id}`);
  }
  for (const output of outputs) {
    const where = output.id || '<output without id>';
    for (const field of ['command', 'route', 'proofType', 'fixture', 'generator', 'themeBehavior', 'requiredPlacement', 'acceptanceTest', 'owner', 'deliveryState']) {
      if (!output[field]) problems.push(`${where}: ${field} is required`);
    }
    if (!output.route?.startsWith('/')) problems.push(`${where}: route must start with /`);
    if (!['product-output', 'explanatory'].includes(output.proofType)) problems.push(`${where}: invalid proofType ${output.proofType}`);
    if (!Array.isArray(output.variants) || !Array.isArray(output.downloads)) problems.push(`${where}: variants and downloads must be arrays`);
    if (!deliveryStates.has(output.deliveryState)) problems.push(`${where}: invalid deliveryState ${output.deliveryState}`);
    for (const field of ['fixture', 'generator', 'acceptanceTest', 'owner']) {
      if (output[field] && !existsSync(join(repositoryRoot, output[field]))) problems.push(`${where}: ${field} does not exist: ${output[field]}`);
    }
    if (output.primaryAsset !== null && !assetIds.has(output.primaryAsset)) {
      problems.push(`${where}: unknown primaryAsset ${output.primaryAsset}`);
    }
    if (output.deliveryState !== 'phase4' && output.primaryAsset === null) {
      problems.push(`${where}: delivered output has no primary asset`);
    }
  }

  for (const proof of proofs) {
    const where = `proof ${proof.route || '<route>'}/${proof.primaryVisualId || '<visual>'}`;
    if (!snapshotRoutes.has(proof.route)) problems.push(`${where}: route is not captured`);
    if (!assetIds.has(proof.primaryVisualId)) problems.push(`${where}: unknown visual id`);
    if (!proof.selector || !proof.expectedCaption || !proof.expectedArtifact) problems.push(`${where}: selector, caption, and artifact are required`);
    if (!assetPaths.has(proof.expectedArtifact)) problems.push(`${where}: expected artifact is undeclared`);
    if (!Number.isInteger(proof.maxVisibleWordsBeforeVisual) || proof.maxVisibleWordsBeforeVisual < 0) problems.push(`${where}: invalid word-distance limit`);
    if (!Number.isInteger(proof.minimumRenderedWidth) || !Number.isInteger(proof.minimumRenderedHeight)) problems.push(`${where}: rendered dimensions are required`);
    if (!Array.isArray(proof.requiredVariants) || !Array.isArray(proof.themes) || proof.themes.length === 0) problems.push(`${where}: variants and themes are required`);
  }

  return problems;
}
