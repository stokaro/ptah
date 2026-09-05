#!/usr/bin/env node

import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { execFileSync } from 'node:child_process';
import { dirname, isAbsolute, join, normalize, relative, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = join(scriptDir, '..', '..', '..');
const manifestPath = join(scriptDir, 'data', 'release-ui-overlay.json');
const requiredFiles = new Set([
  'docs/site/scripts/data/release-ui-overlay.json',
  'docs/site/src/components/PageActions.astro',
  'docs/site/src/components/PageTitle.astro',
  'docs/site/src/lib/page-context.mjs',
  'docs/site/src/lib/page-furniture.mjs',
  'docs/site/src/lib/source-context.mjs',
  'docs/site/src/lib/source-ref.mjs',
]);
const fullCommit = /^[0-9a-f]{40}$/;
const releaseVersion = /^v(\d+)\.(\d+)\.(\d+)$/;

function parseRelease(value, label) {
  const match = typeof value === 'string' ? value.match(releaseVersion) : undefined;
  if (!match) throw new Error(`${label} must use vMAJOR.MINOR.PATCH`);
  return match.slice(1).map(Number);
}

function compareReleases(left, right) {
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) return left[index] - right[index];
  }
  return 0;
}

export function supportsRelease(version, minimumRelease) {
  return compareReleases(
    parseRelease(version, 'documentation version'),
    parseRelease(minimumRelease, 'minimum release'),
  ) >= 0;
}

function repositoryRelativePathProblem(path) {
  if (typeof path !== 'string' || path.trim() === '') return 'must be a non-empty string';
  if (isAbsolute(path)) return 'must be repository-relative';
  const normalized = normalize(path);
  if (normalized === '..' || normalized.startsWith(`..${process.platform === 'win32' ? '\\' : '/'}`)) {
    return 'must not escape the repository';
  }
  if (normalized.includes(`${process.platform === 'win32' ? '\\' : '/'}src${process.platform === 'win32' ? '\\' : '/'}content${process.platform === 'win32' ? '\\' : '/'}`)) {
    return 'must not replace immutable release content';
  }
  return undefined;
}

export function overlayManifestProblems(manifest, { sourceRoot = repositoryRoot } = {}) {
  const problems = [];
  if (!manifest || typeof manifest !== 'object' || Array.isArray(manifest)) {
    return ['overlay manifest must be an object'];
  }
  if (manifest.schemaVersion !== 1) problems.push('schemaVersion must be 1');
  try {
    parseRelease(manifest.minimumRelease, 'minimum release');
  } catch (error) {
    problems.push(error.message);
  }
  if (!Array.isArray(manifest.files) || manifest.files.length === 0) {
    problems.push('files must be a non-empty array');
  } else {
    const seen = new Set();
    for (const path of manifest.files) {
      const pathProblem = repositoryRelativePathProblem(path);
      if (pathProblem) {
        problems.push(`${JSON.stringify(path)} ${pathProblem}`);
        continue;
      }
      if (seen.has(path)) problems.push(`duplicate overlay file ${path}`);
      seen.add(path);
      const source = join(sourceRoot, path);
      if (!existsSync(source) || !statSync(source).isFile()) problems.push(`overlay source does not exist: ${path}`);
    }
    for (const required of requiredFiles) {
      if (!seen.has(required)) problems.push(`required overlay file is missing: ${required}`);
    }
  }

  const generated = manifest.legacyGeneratedPages;
  if (!generated || typeof generated !== 'object' || Array.isArray(generated)) {
    problems.push('legacyGeneratedPages must be an object');
  } else {
    for (const [pageId, metadata] of Object.entries(generated)) {
      if (!/^[a-z0-9][a-z0-9./-]*$/.test(pageId)) problems.push(`invalid generated page id ${JSON.stringify(pageId)}`);
      for (const field of ['generator', 'editSource']) {
        const pathProblem = repositoryRelativePathProblem(metadata?.[field]);
        if (pathProblem) {
          problems.push(`${pageId}.${field} ${pathProblem}`);
        } else if (!existsSync(join(sourceRoot, metadata[field]))) {
          problems.push(`${pageId}.${field} does not exist: ${metadata[field]}`);
        }
      }
    }
  }
  return problems;
}

export function applyReleaseUiOverlay({ sourceRoot = repositoryRoot, targetRoot, manifest }) {
  const problems = overlayManifestProblems(manifest, { sourceRoot });
  if (problems.length > 0) throw new Error(problems.join('; '));
  const target = resolve(targetRoot);
  const relativeTarget = relative(target, join(target, 'docs', 'site', 'package.json'));
  if (!existsSync(join(target, relativeTarget))) {
    throw new Error(`${target} is not a Ptah documentation worktree`);
  }
  for (const path of manifest.files) {
    const destination = join(target, path);
    mkdirSync(dirname(destination), { recursive: true });
    cpSync(join(sourceRoot, path), destination);
  }
}

function gitCommit(root, arguments_) {
  const commit = execFileSync('git', arguments_, { cwd: root, encoding: 'utf8' }).trim();
  if (!fullCommit.test(commit)) throw new Error(`git returned an invalid commit: ${JSON.stringify(commit)}`);
  return commit;
}

function parseArguments(arguments_) {
  if (arguments_.includes('--selftest')) return { selftest: true };
  const value = (name) => {
    const index = arguments_.indexOf(name);
    return index === -1 ? undefined : arguments_[index + 1];
  };
  const targetRoot = value('--target');
  const version = value('--version');
  if (!targetRoot || !version || arguments_.length !== 4) {
    throw new Error('usage: node scripts/apply-release-ui-overlay.mjs --target <worktree> --version <vX.Y.Z>');
  }
  return { targetRoot, version };
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  assert(overlayManifestProblems(manifest).length === 0, 'committed overlay manifest is invalid');
  assert(supportsRelease('v0.3.0', manifest.minimumRelease), 'v0.3.0 is not supported');
  assert(supportsRelease('v1.0.0', manifest.minimumRelease), 'future release is not supported');
  assert(!supportsRelease('v0.2.0', manifest.minimumRelease), 'pre-v0.3.0 release is supported without evidence');
  const contentOverlay = structuredClone(manifest);
  contentOverlay.files.push('docs/site/src/content/docs/index.mdx');
  assert(
    overlayManifestProblems(contentOverlay).some((problem) => problem.includes('immutable release content')),
    'content replacement entered the UI overlay',
  );
  const missingEditSource = structuredClone(manifest);
  missingEditSource.legacyGeneratedPages['reference/command-flags'].editSource = 'internal/cmdref/missing.go';
  assert(
    overlayManifestProblems(missingEditSource).some((problem) => problem.includes('does not exist')),
    'missing legacy generator source passed',
  );

  const targetRoot = mkdtempSync(join(tmpdir(), 'ptah-release-ui-overlay-'));
  try {
    mkdirSync(join(targetRoot, 'docs', 'site', 'src', 'content', 'docs'), { recursive: true });
    writeFileSync(join(targetRoot, 'docs', 'site', 'package.json'), '{}\n', 'utf8');
    const sentinel = join(targetRoot, 'docs', 'site', 'src', 'content', 'docs', 'keep.md');
    writeFileSync(sentinel, 'immutable content\n', 'utf8');
    applyReleaseUiOverlay({ targetRoot, manifest });
    for (const path of manifest.files) {
      assert(
        readFileSync(join(targetRoot, path), 'utf8') === readFileSync(join(repositoryRoot, path), 'utf8'),
        `${path} was not copied exactly`,
      );
    }
    assert(readFileSync(sentinel, 'utf8') === 'immutable content\n', 'release content changed');
  } finally {
    rmSync(targetRoot, { recursive: true, force: true });
  }
  console.log('apply-release-ui-overlay.mjs --selftest: OK (manifest, version gate, exact copy, and immutable content)');
}

function main() {
  const options = parseArguments(process.argv.slice(2));
  if (options.selftest) {
    selftest();
    return;
  }

  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
  if (!supportsRelease(options.version, manifest.minimumRelease)) {
    console.log(
      `release UI overlay: unsupported ${options.version}; minimum structurally validated release is ${manifest.minimumRelease}`,
    );
    process.exitCode = 3;
    return;
  }
  const tagCommit = gitCommit(repositoryRoot, ['rev-list', '-n', '1', options.version]);
  const targetCommit = gitCommit(options.targetRoot, ['rev-parse', 'HEAD']);
  if (tagCommit !== targetCommit) {
    throw new Error(`target worktree is ${targetCommit}, but ${options.version} is ${tagCommit}`);
  }
  applyReleaseUiOverlay({ targetRoot: options.targetRoot, manifest });
  console.log(
    `release UI overlay: OK (${options.version} content at ${tagCommit}; ${manifest.files.length} current chrome files)`,
  );
}

try {
  main();
} catch (error) {
  console.error(`release UI overlay: FAILED: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
}
