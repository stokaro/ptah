#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { chmodSync, closeSync, copyFileSync, mkdirSync, openSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const source = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repository = resolve(source, '..');
const platforms = [
  { os: 'darwin', cpu: 'x64', archiveOS: 'darwin', archiveArch: 'amd64', format: 'tar.gz' },
  { os: 'darwin', cpu: 'arm64', archiveOS: 'darwin', archiveArch: 'arm64', format: 'tar.gz' },
  { os: 'linux', cpu: 'x64', archiveOS: 'linux', archiveArch: 'amd64', format: 'tar.gz' },
  { os: 'linux', cpu: 'arm64', archiveOS: 'linux', archiveArch: 'arm64', format: 'tar.gz' },
  { os: 'win32', cpu: 'x64', archiveOS: 'windows', archiveArch: 'amd64', format: 'zip' },
  { os: 'win32', cpu: 'arm64', archiveOS: 'windows', archiveArch: 'arm64', format: 'zip' },
];
const commands = ['ptah', 'ptah-compat', 'ptah-ls'];

function options(args) {
  const result = {};
  for (let index = 0; index < args.length; index += 2) {
    const name = args[index];
    const value = args[index + 1];
    if (!['--version', '--archive-version', '--assets', '--output'].includes(name) || !value || result[name]) {
      throw new Error('Usage: stage.mjs --version X.Y.Z [--archive-version VERSION] --assets DIR --output DIR');
    }
    result[name] = value;
  }
  if (!/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?$/.test(result['--version'] ?? '') || !result['--assets'] || !result['--output']) {
    throw new Error('A semantic package version, assets directory, and output directory are required');
  }
  if (result['--archive-version'] && !/^[0-9A-Za-z.-]+$/.test(result['--archive-version'])) {
    throw new Error('The archive version contains invalid characters');
  }
  return {
    version: result['--version'],
    archiveVersion: result['--archive-version'] ?? result['--version'],
    assets: resolve(result['--assets']),
    output: resolve(result['--output']),
  };
}

function checksums(path) {
  const values = new Map();
  for (const line of readFileSync(path, 'utf8').trim().split('\n')) {
    const match = /^([a-f0-9]{64})  (\S+)$/.exec(line.trim());
    if (!match || values.has(match[2])) {
      throw new Error(`Invalid checksum entry: ${line}`);
    }
    values.set(match[2], match[1]);
  }
  return values;
}

function verifyArchive(archivePath, expected) {
  const actual = createHash('sha256').update(readFileSync(archivePath)).digest('hex');
  if (actual !== expected) {
    throw new Error(`Checksum mismatch for ${basename(archivePath)}`);
  }
}

function extractBinary(archivePath, format, command, outputPath) {
  const member = `${command}${format === 'zip' ? '.exe' : ''}`;
  const descriptor = openSync(outputPath, 'w', 0o755);
  try {
    const result = format === 'zip'
      ? spawnSync('unzip', ['-p', archivePath, member], { stdio: ['ignore', descriptor, 'pipe'] })
      : spawnSync('tar', ['-xOzf', archivePath, member], { stdio: ['ignore', descriptor, 'pipe'] });
    if (result.error || result.status !== 0) {
      throw new Error(`Could not extract ${member} from ${basename(archivePath)}: ${result.error?.message ?? result.stderr.toString()}`);
    }
  } finally {
    closeSync(descriptor);
  }
  if (statSync(outputPath).size === 0) {
    throw new Error(`Empty binary ${member} in ${basename(archivePath)}`);
  }
}

function writeJSON(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function main() {
  const { version, archiveVersion, assets, output } = options(process.argv.slice(2));
  const hashes = checksums(join(assets, 'checksums.txt'));
  const archives = platforms.map((platform) => {
    const filename = `ptah_${archiveVersion}_${platform.archiveOS}_${platform.archiveArch}.${platform.format}`;
    const hash = hashes.get(filename);
    if (!hash) {
      throw new Error(`Missing checksum for ${filename}`);
    }
    const path = join(assets, filename);
    verifyArchive(path, hash);
    return { ...platform, path };
  });

  mkdirSync(output);
  const root = join(output, 'ptah');
  mkdirSync(root);
  mkdirSync(join(root, 'bin'));
  const rootPackage = JSON.parse(readFileSync(join(source, 'package.json'), 'utf8'));
  rootPackage.version = version;
  delete rootPackage.private;
  rootPackage.optionalDependencies = Object.fromEntries(platforms.map(({ os, cpu }) => [`@stokaro/ptah-${os}-${cpu}`, version]));
  writeJSON(join(root, 'package.json'), rootPackage);
  copyFileSync(join(source, 'README.md'), join(root, 'README.md'));
  copyFileSync(join(repository, 'LICENSE'), join(root, 'LICENSE'));
  for (const entry of ['ptah.mjs', 'ptah-compat.mjs', 'ptah-ls.mjs', 'run.mjs']) {
    copyFileSync(join(source, 'bin', entry), join(root, 'bin', entry));
    if (entry !== 'run.mjs') {
      chmodSync(join(root, 'bin', entry), 0o755);
    }
  }

  const compat = join(output, 'ptah-compat');
  mkdirSync(compat);
  mkdirSync(join(compat, 'bin'));
  const compatPackage = JSON.parse(readFileSync(join(source, 'compat', 'package.json'), 'utf8'));
  compatPackage.version = version;
  delete compatPackage.private;
  compatPackage.dependencies = { '@stokaro/ptah': version };
  writeJSON(join(compat, 'package.json'), compatPackage);
  copyFileSync(join(source, 'compat', 'README.md'), join(compat, 'README.md'));
  copyFileSync(join(repository, 'LICENSE'), join(compat, 'LICENSE'));
  copyFileSync(join(source, 'compat', 'bin', 'ptah-compat.mjs'), join(compat, 'bin', 'ptah-compat.mjs'));
  chmodSync(join(compat, 'bin', 'ptah-compat.mjs'), 0o755);

  for (const platform of archives) {
    const name = `ptah-${platform.os}-${platform.cpu}`;
    const directory = join(output, name);
    const binDirectory = join(directory, 'bin');
    mkdirSync(directory);
    mkdirSync(binDirectory);
    writeJSON(join(directory, 'package.json'), {
      name: `@stokaro/${name}`,
      version,
      description: `Ptah CLI binaries for ${platform.os}-${platform.cpu}`,
      license: 'MIT',
      repository: { type: 'git', url: 'git+https://github.com/stokaro/ptah.git', directory: 'npm' },
      os: [platform.os],
      cpu: [platform.cpu],
      files: ['bin', 'LICENSE'],
      publishConfig: { access: 'public' },
    });
    copyFileSync(join(repository, 'LICENSE'), join(directory, 'LICENSE'));
    for (const command of commands) {
      extractBinary(platform.path, platform.format, command, join(binDirectory, `${command}${platform.format === 'zip' ? '.exe' : ''}`));
    }
  }
  console.log(`Staged @stokaro/ptah@${version}, @stokaro/ptah-compat@${version}, and ${platforms.length} platform packages in ${output}`);
}

main();
