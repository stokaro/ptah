#!/usr/bin/env node

import { createHash } from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(scriptDir, '..');
const fixtureRoot = join(siteRoot, 'fixtures', 'inference-quick-start');
const publicSamples = join(siteRoot, 'public', 'samples');

export const archiveName = 'inference-quick-start.zip';
export const checksumName = `${archiveName}.sha256`;
export const archivePrefix = 'inference-quick-start/';

const requiredFiles = [
  'Dockerfile.embeddings',
  'Dockerfile.postgres',
  'README.md',
  'cleanup.ps1',
  'cleanup.sh',
  'compose.yaml',
  'embed.py',
  'init.sql',
  'run.ps1',
  'run.sh',
  'spec.yaml',
];

const crcTable = new Uint32Array(256);
for (let index = 0; index < crcTable.length; index += 1) {
  let value = index;
  for (let bit = 0; bit < 8; bit += 1) {
    value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
  }
  crcTable[index] = value >>> 0;
}

function crc32(bytes) {
  let value = 0xffffffff;
  for (const byte of bytes) value = crcTable[(value ^ byte) & 0xff] ^ (value >>> 8);
  return (value ^ 0xffffffff) >>> 0;
}

function u16(value) {
  const bytes = Buffer.alloc(2);
  bytes.writeUInt16LE(value, 0);
  return bytes;
}

function u32(value) {
  const bytes = Buffer.alloc(4);
  bytes.writeUInt32LE(value >>> 0, 0);
  return bytes;
}

function archiveMode(path) {
  return path.endsWith('.sh') ? 0o755 : 0o644;
}

export function fixtureFiles(root = fixtureRoot) {
  const found = [];
  const visit = (directory) => {
    const entries = readdirSync(directory, { withFileTypes: true }).sort((left, right) => {
      if (left.name < right.name) return -1;
      if (left.name > right.name) return 1;
      return 0;
    });
    for (const entry of entries) {
      if (entry.name.startsWith('.')) continue;
      const absolute = join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(absolute);
      } else if (entry.isFile()) {
        found.push(relative(root, absolute).split(sep).join('/'));
      }
    }
  };
  visit(root);
  return found.sort();
}

export function fixtureProblems(files) {
  const names = new Set(files);
  const problems = [];
  for (const required of requiredFiles) {
    if (!names.has(required)) problems.push(`fixture is missing ${required}`);
  }
  return problems;
}

// buildZip writes stored ZIP entries. Their order, timestamp, permissions, and
// header bytes are fixed so the same fixture produces the same archive on every
// supported host. Compression is deliberately omitted: this fixture is small,
// and a compressor version must not become part of the documentation contract.
export function buildZip(root = fixtureRoot) {
  const files = fixtureFiles(root);
  const problems = fixtureProblems(files);
  if (problems.length > 0) throw new Error(problems.join('\n'));

  const localParts = [];
  const centralParts = [];
  let localOffset = 0;
  const dosTime = 0;
  const dosDate = 33; // 1980-01-01, the earliest timestamp ZIP can represent.

  for (const path of files) {
    const name = Buffer.from(`${archivePrefix}${path}`, 'utf8');
    const contents = readFileSync(join(root, path));
    const checksum = crc32(contents);
    const flags = 0x0800; // File names are UTF-8.

    const localHeader = Buffer.concat([
      u32(0x04034b50),
      u16(20),
      u16(flags),
      u16(0),
      u16(dosTime),
      u16(dosDate),
      u32(checksum),
      u32(contents.length),
      u32(contents.length),
      u16(name.length),
      u16(0),
      name,
    ]);
    localParts.push(localHeader, contents);

    const unixMode = 0o100000 | archiveMode(path);
    const centralHeader = Buffer.concat([
      u32(0x02014b50),
      u16(0x031e), // ZIP 3.0, created on Unix.
      u16(20),
      u16(flags),
      u16(0),
      u16(dosTime),
      u16(dosDate),
      u32(checksum),
      u32(contents.length),
      u32(contents.length),
      u16(name.length),
      u16(0),
      u16(0),
      u16(0),
      u16(0),
      u32((unixMode << 16) >>> 0),
      u32(localOffset),
      name,
    ]);
    centralParts.push(centralHeader);
    localOffset += localHeader.length + contents.length;
  }

  const centralDirectory = Buffer.concat(centralParts);
  const end = Buffer.concat([
    u32(0x06054b50),
    u16(0),
    u16(0),
    u16(files.length),
    u16(files.length),
    u32(centralDirectory.length),
    u32(localOffset),
    u16(0),
  ]);
  return Buffer.concat([...localParts, centralDirectory, end]);
}

export function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

export function checksumFile(bytes) {
  return `${sha256(bytes)}  ${archiveName}\n`;
}

export function zipEntries(bytes) {
  if (bytes.length < 22 || bytes.readUInt32LE(bytes.length - 22) !== 0x06054b50) {
    throw new Error('archive has no ZIP end record');
  }
  const count = bytes.readUInt16LE(bytes.length - 12);
  let offset = bytes.readUInt32LE(bytes.length - 6);
  const names = [];
  for (let index = 0; index < count; index += 1) {
    if (bytes.readUInt32LE(offset) !== 0x02014b50) throw new Error(`archive central entry ${index + 1} is invalid`);
    const nameLength = bytes.readUInt16LE(offset + 28);
    const extraLength = bytes.readUInt16LE(offset + 30);
    const commentLength = bytes.readUInt16LE(offset + 32);
    names.push(bytes.subarray(offset + 46, offset + 46 + nameLength).toString('utf8'));
    offset += 46 + nameLength + extraLength + commentLength;
  }
  return names;
}

function writeBundle(outputDirectory, archive) {
  mkdirSync(outputDirectory, { recursive: true });
  writeFileSync(join(outputDirectory, archiveName), archive);
  writeFileSync(join(outputDirectory, checksumName), checksumFile(archive));
}

function checkCommitted(archive) {
  const expectedArchive = join(publicSamples, archiveName);
  const expectedChecksum = join(publicSamples, checksumName);
  const problems = [];
  if (!existsSync(expectedArchive)) {
    problems.push(`${relative(resolve(siteRoot, '..', '..'), expectedArchive)} is missing`);
  } else if (!readFileSync(expectedArchive).equals(archive)) {
    problems.push(`${relative(resolve(siteRoot, '..', '..'), expectedArchive)} is stale`);
  }
  if (!existsSync(expectedChecksum)) {
    problems.push(`${relative(resolve(siteRoot, '..', '..'), expectedChecksum)} is missing`);
  } else if (readFileSync(expectedChecksum, 'utf8') !== checksumFile(archive)) {
    problems.push(`${relative(resolve(siteRoot, '..', '..'), expectedChecksum)} is stale`);
  }
  if (problems.length > 0) {
    throw new Error(`${problems.join('\n')}\nrun node docs/site/scripts/build-inference-quick-start.mjs --write`);
  }
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const root = mkdtempSync(join(tmpdir(), 'ptah-inference-archive-'));
  try {
    for (const path of requiredFiles) {
      const absolute = join(root, path);
      mkdirSync(dirname(absolute), { recursive: true });
      writeFileSync(absolute, `${path}\n`);
    }
    const first = buildZip(root);
    const second = buildZip(root);
    assert(first.equals(second), 'the same fixture produced different archive bytes');
    assert(zipEntries(first).length === requiredFiles.length, 'the ZIP omitted a fixture file');
    assert(
      zipEntries(first).every((name) => name.startsWith(archivePrefix)),
      'a ZIP entry escaped the fixture directory',
    );
    assert(checksumFile(first) === `${sha256(first)}  ${archiveName}\n`, 'the checksum file named the wrong archive');

    writeFileSync(join(root, 'spec.yaml'), 'changed\n');
    assert(!first.equals(buildZip(root)), 'changing the fixture left the archive unchanged');
    rmSync(join(root, 'README.md'));
    assert(fixtureProblems(fixtureFiles(root)).includes('fixture is missing README.md'), 'a missing required file passed');
    console.log('build-inference-quick-start.mjs --selftest: OK (determinism, contents, checksum, and omissions)');
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

function usage() {
  return 'usage: node docs/site/scripts/build-inference-quick-start.mjs [--write | --output-dir <dir> | --selftest]';
}

function main(arguments_) {
  if (arguments_.length === 1 && arguments_[0] === '--selftest') {
    selftest();
    return;
  }
  const archive = buildZip();
  if (arguments_.length === 1 && arguments_[0] === '--write') {
    writeBundle(publicSamples, archive);
    console.log(`wrote ${archiveName} and ${checksumName}`);
    return;
  }
  if (arguments_.length === 2 && arguments_[0] === '--output-dir') {
    const outputDirectory = resolve(arguments_[1]);
    writeBundle(outputDirectory, archive);
    console.log(`wrote ${archiveName} and ${checksumName} to ${outputDirectory}`);
    return;
  }
  if (arguments_.length !== 0) throw new Error(usage());
  checkCommitted(archive);
  console.log(`inference quick-start archive: OK (${fixtureFiles().length} files; sha256 ${sha256(archive)})`);
}

try {
  main(process.argv.slice(2));
} catch (error) {
  console.error(`inference quick-start archive: FAILED: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
}
