#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

function npm(args, options = {}) {
  const result = spawnSync('npm', args, { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, ...options });
  if (result.error) {
    throw result.error;
  }
  return result;
}

function manifest(directory) {
  return JSON.parse(readFileSync(join(directory, 'package.json'), 'utf8'));
}

// distTag is the dist-tag a version is published under, or undefined for the
// default. A publish that names no tag moves `latest`, and npm refuses one for
// a prerelease, so a prerelease goes out as `next` and `latest` stays on the
// newest release. A release keeps the default, which also keeps npm's refusal
// of a version lower than the one `latest` names.
function distTag(version) {
  return /^\d+\.\d+\.\d+-/.test(version) ? 'next' : undefined;
}

function publish(directory, packed, dryRun) {
  const pkg = manifest(directory);
  const packedResult = npm(['pack', directory, '--json', '--pack-destination', packed]);
  if (packedResult.status !== 0) {
    throw new Error(`Could not pack ${pkg.name}: ${packedResult.stderr}`);
  }
  const [tarball] = JSON.parse(packedResult.stdout);
  const published = npm(['view', `${pkg.name}@${pkg.version}`, 'dist.integrity', '--json']);
  if (published.status === 0) {
    if (JSON.parse(published.stdout) !== tarball.integrity) {
      throw new Error(`${pkg.name}@${pkg.version} is already published with different contents`);
    }
    console.log(`${pkg.name}@${pkg.version} already matches the registry`);
    return;
  }
  if (!published.stderr.includes('E404')) {
    throw new Error(`Could not check ${pkg.name}@${pkg.version}: ${published.stderr}`);
  }

  const args = ['publish', join(packed, tarball.filename), '--access=public'];
  const tag = distTag(pkg.version);
  if (tag) {
    args.push(`--tag=${tag}`);
  }
  if (dryRun) {
    args.push('--dry-run');
  }
  const result = npm(args, { stdio: 'inherit' });
  if (result.status !== 0) {
    throw new Error(`Could not publish ${pkg.name}@${pkg.version}`);
  }
}

function main() {
  const dryRun = process.argv[4] === '--dry-run';
  if (process.argv.length !== (dryRun ? 5 : 4) || process.argv[2] !== '--staged') {
    throw new Error('Usage: publish.mjs --staged DIR [--dry-run]');
  }
  const staged = resolve(process.argv[3]);
  const root = manifest(join(staged, 'ptah'));
  const dependencies = Object.entries(root.optionalDependencies ?? {});
  if (root.name !== '@stokaro/ptah' || dependencies.length !== 6) {
    throw new Error('Expected @stokaro/ptah and six platform packages');
  }
  const packed = mkdtempSync(join(tmpdir(), 'ptah-npm-pack-'));
  try {
    for (const [name, version] of dependencies) {
      const directory = join(staged, name.slice('@stokaro/'.length));
      const pkg = manifest(directory);
      if (pkg.name !== name || pkg.version !== root.version || version !== root.version) {
        throw new Error(`Platform package ${name} does not match @stokaro/ptah@${root.version}`);
      }
      publish(directory, packed, dryRun);
    }
    publish(join(staged, 'ptah'), packed, dryRun);
    const compat = manifest(join(staged, 'ptah-compat'));
    if (compat.name !== '@stokaro/ptah-compat' || compat.version !== root.version || compat.dependencies?.['@stokaro/ptah'] !== root.version) {
      throw new Error('Compatibility package does not match @stokaro/ptah');
    }
    publish(join(staged, 'ptah-compat'), packed, dryRun);
  } finally {
    rmSync(packed, { recursive: true, force: true });
  }
}

main();
