// latest is a copy of the newest release build with its documentation URLs
// rebased. The tag's content, source links, and source commit stay attached to
// that release. Pagefind stores routes without the base; its browser bundle
// supplies the base and is rebased with the other assets.
import { cpSync, existsSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { extname, join } from 'node:path';
import { Origin } from '../../src/lib/docs-origin.mjs';
import { LATEST } from './doc-versions.mjs';
import { validateBuildInfo } from '../check-build-info.mjs';

const textExtensions = new Set(['.html', '.css', '.js', '.mjs', '.xml', '.json', '.svg', '.md', '.txt']);

export function rebaseLatestText(text, release) {
  const base = `/${release}`;
  const escapedBase = base.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return text
    .replaceAll(`${Origin}${base}/`, `${Origin}/${LATEST}/`)
    .replace(new RegExp(`(["'\x60]|\\burl=)${escapedBase}(?=/|["'\x60])`, 'g'), `$1/${LATEST}`)
    .replace(new RegExp(`(url\\(\\s*)${escapedBase}/`, 'g'), `$1/${LATEST}/`)
    .replace(/\bsrcset="([^"]*)"/g, (attribute, sources) =>
      `srcset="${sources.replace(new RegExp(`(^|,\\s*)${escapedBase}/`, 'g'), `$1/${LATEST}/`)}"`)
    .replace(`data-current="${release}"`, `data-current="${LATEST}"`)
    .replace(`<span class="ptah-version-picker__current">${release}</span>`, `<span class="ptah-version-picker__current">${LATEST}</span>`);
}

export function publishLatestAlias(siteDir, release) {
  const target = join(siteDir, LATEST);
  // This directory is derived output in the assembled deployment, not a tag.
  rmSync(target, { recursive: true, force: true });
  cpSync(join(siteDir, release), target, { recursive: true });
  function rebase(directory) {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const file = join(directory, entry.name);
      if (entry.isDirectory()) rebase(file);
      else if (entry.isFile() && textExtensions.has(extname(entry.name))) {
        const source = readFileSync(file, 'utf8');
        const rebased = rebaseLatestText(source, release);
        if (source !== rebased) writeFileSync(file, rebased);
      }
    }
  }
  rebase(target);
  const infoPath = join(target, 'build-info.json');
  if (existsSync(infoPath)) {
    const info = JSON.parse(readFileSync(infoPath, 'utf8'));
    if (info.documentation_version !== release || info.source_ref !== release) {
      throw new Error(`${release} has inconsistent build provenance`);
    }
    writeFileSync(infoPath, `${JSON.stringify({ ...info, documentation_version: LATEST }, null, 2)}\n`);
  }
}

export function latestAliasProblems(siteDir, release) {
  if (!release) return [];
  try {
    const source = JSON.parse(readFileSync(join(siteDir, release, 'build-info.json'), 'utf8'));
    const alias = JSON.parse(readFileSync(join(siteDir, LATEST, 'build-info.json'), 'utf8'));
    return validateBuildInfo(alias, { version: LATEST, sourceRef: release, commit: source.source_commit })
      .map((problem) => `latest: ${problem}`);
  } catch (error) {
    return [`latest build provenance cannot be read: ${error.message}`];
  }
}
