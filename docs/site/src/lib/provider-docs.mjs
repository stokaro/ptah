// Which provider repositories this site publishes pages from, and what the
// import step left in the content collection.
//
// A provider's documentation lives in the provider's repository, beside the
// code it describes, and the site renders it at build time. The declaration is
// `scripts/data/provider-docs.json`: one entry per provider, naming its
// repository and the release whose `docs/` folder the site shows.
// `scripts/import-provider-docs.mjs` fetches that folder and writes each page
// under `src/content/docs/_providers/<engine>/`, with a manifest the import
// writes last. This module reads the declaration and the manifests and nothing
// else, so the gates, the sidebar and the Astro config agree on one answer.
//
// The import directory starts with `_` on purpose. `docs/docs.go` embeds the
// content collection in every binary, and `go:embed` leaves out a directory
// whose name starts with `_`, so a binary carries the pages this repository
// holds and never the pages a build happened to fetch. Starlight still loads
// the files: its loader skips a basename starting with `_`, not a directory.
// Each imported page declares its route with `slug:`, under
// `extend/providers/<engine>/`.
//
// Two readers, two strictnesses. `importedProviders()` throws when the import
// is missing or does not match the declaration: a gate that enumerates pages
// must not report a pass over a site that lacks the provider pages. The
// sidebar reads `providerSidebarGroups()`, which describes what is on disk and
// never throws, because every gate imports the sidebar at load time, including
// the self-tests that run in a fresh checkout with nothing imported. The strict
// reader is what turns that state into a failure.
//
// Keep this module dependency-free: `src/sidebar.mjs` imports it, and that
// module is read by plain Node scripts.
import { createHash } from 'node:crypto';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const libDir = dirname(fileURLToPath(import.meta.url));
const siteRoot = join(libDir, '..', '..');

/** The declaration: which providers the site imports, and at which release. */
export const declarationPath = join(siteRoot, 'scripts', 'data', 'provider-docs.json');

/** The script that writes the import; its hash is part of every manifest. */
export const importerPath = join(siteRoot, 'scripts', 'import-provider-docs.mjs');

/** The content collection, the root every imported page is written under. */
export const contentRoot = join(siteRoot, 'src', 'content', 'docs');

/** The directory the import owns, relative to the content collection. */
export const importDirectoryName = '_providers';

/** The manifest the import writes into each provider's directory, last. */
export const manifestName = '.import.json';

/** The route prefix of a provider's pages, without slashes. */
export function routePrefix(engine) {
  return `extend/providers/${engine}`;
}

/** The host page that introduces a provider, relative to the content root. */
export function hostPageFor(engine) {
  return `${routePrefix(engine)}.md`;
}

const engineName = /^[a-z0-9][a-z0-9-]*$/;
const repositoryName = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;
const releaseName = /^v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/;
const fullCommit = /^[0-9a-f]{40}$/;

/** A declaration or import this module cannot accept. */
export class ProviderDocsError extends Error {
  constructor(message) {
    super(`provider-docs: ${message}`);
    this.name = 'ProviderDocsError';
  }
}

/**
 * The declared providers, validated, in declaration order.
 *
 * Each entry is `{ engine, name, repository, release, ref, refReason }`.
 * `release` is the provider release whose documentation the site shows. `ref`
 * is a full commit to import instead, allowed only with a `refReason`, for a
 * release that predates the provider's `docs/` folder.
 */
export function declaredProviders(path = declarationPath) {
  let declaration;
  try {
    declaration = JSON.parse(readFileSync(path, 'utf8'));
  } catch (cause) {
    throw new ProviderDocsError(`cannot read the declaration ${path}: ${cause.message}`);
  }
  return validateDeclaration(declaration);
}

/** Validates a parsed declaration; exported for the import's self-test. */
export function validateDeclaration(declaration) {
  if (declaration === null || typeof declaration !== 'object' || !Array.isArray(declaration.providers)) {
    throw new ProviderDocsError('the declaration must be an object with a "providers" array');
  }
  const seen = new Set();
  const problems = [];
  const providers = declaration.providers.map((entry, index) => {
    const where = `providers[${index}]`;
    if (entry === null || typeof entry !== 'object') {
      problems.push(`${where} is not an object`);
      return null;
    }
    const known = new Set(['engine', 'name', 'repository', 'release', 'ref', 'refReason']);
    for (const key of Object.keys(entry)) {
      if (!known.has(key)) problems.push(`${where} has the unknown key ${JSON.stringify(key)}`);
    }
    const { engine, name, repository, release, ref, refReason } = entry;
    if (typeof engine !== 'string' || !engineName.test(engine)) {
      problems.push(`${where}.engine must match ${engineName}`);
    } else if (seen.has(engine)) {
      problems.push(`${where}.engine ${engine} is declared twice`);
    } else {
      seen.add(engine);
    }
    if (typeof name !== 'string' || name.trim() === '') problems.push(`${where}.name must be a display name such as "YDB"`);
    if (typeof repository !== 'string' || !repositoryName.test(repository)) {
      problems.push(`${where}.repository must be a GitHub owner/name`);
    }
    if (typeof release !== 'string' || !releaseName.test(release)) {
      problems.push(`${where}.release must be a release tag such as v1.2.3`);
    }
    if (ref !== undefined && (typeof ref !== 'string' || !fullCommit.test(ref))) {
      problems.push(`${where}.ref must be a full lowercase commit, because a branch moves`);
    }
    if ((ref === undefined) !== (refReason === undefined)) {
      problems.push(`${where}.ref and ${where}.refReason go together: a ref that replaces the release says why`);
    }
    if (refReason !== undefined && (typeof refReason !== 'string' || refReason.trim() === '')) {
      problems.push(`${where}.refReason must say why the release itself cannot be imported`);
    }
    return { engine, name, repository, release, ref: ref ?? null, refReason: refReason ?? null };
  });
  if (problems.length > 0) {
    throw new ProviderDocsError(`the declaration is invalid:\n  ${problems.join('\n  ')}`);
  }
  return providers;
}

/** The SHA-256 of some bytes, as the manifest records a page. */
export function digest(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

/** The hash of the import step, so a changed transform invalidates every import. */
export function importerHash(path = importerPath) {
  return digest(readFileSync(path));
}

function readManifest(directory) {
  const path = join(directory, manifestName);
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, 'utf8'));
  } catch (cause) {
    throw new ProviderDocsError(`cannot read ${path}: ${cause.message}`);
  }
}

function pageFilesBelow(directory, prefix = '') {
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const relative = prefix === '' ? entry.name : `${prefix}/${entry.name}`;
    if (entry.isDirectory()) files.push(...pageFilesBelow(join(directory, entry.name), relative));
    else if (/\.(?:md|mdx)$/.test(entry.name)) files.push(relative);
  }
  return files.sort();
}

const remedy = 'run `node docs/site/scripts/import-provider-docs.mjs` (or `npm run providers:import` in docs/site)';

/**
 * Every declared provider and the pages its import wrote, checked against the
 * declaration. Throws a ProviderDocsError naming the remedy when a provider is
 * not imported, was imported for another release or by another version of the
 * import step, has a page on disk its manifest does not list or lists one that
 * is missing, or when the import directory holds a provider nothing declares.
 *
 * A manifest written from a local checkout (`--source`) matches any release of
 * its repository: that is how a provider's own pull request renders its pages.
 */
export function importedProviders({
  providers = declaredProviders(),
  root = contentRoot,
  hash = importerHash(),
} = {}) {
  const importRoot = join(root, importDirectoryName);
  const problems = [];
  const declared = new Set(providers.map((provider) => provider.engine));
  if (existsSync(importRoot)) {
    for (const entry of readdirSync(importRoot, { withFileTypes: true })) {
      if (entry.name.startsWith('.')) continue;
      if (!entry.isDirectory() || !declared.has(entry.name)) {
        problems.push(`${importDirectoryName}/${entry.name} is not a declared provider; the import removes it`);
      }
    }
  }

  const imported = [];
  for (const provider of providers) {
    const directory = join(importRoot, provider.engine);
    const manifest = existsSync(directory) ? readManifest(directory) : null;
    if (manifest === null) {
      problems.push(`${provider.engine}: not imported`);
      continue;
    }
    const mismatches = [];
    if (manifest.engine !== provider.engine) mismatches.push('engine');
    if (manifest.repository !== provider.repository) mismatches.push('repository');
    if (manifest.local !== true) {
      if (manifest.release !== provider.release) mismatches.push('release');
      if ((manifest.ref ?? null) !== provider.ref) mismatches.push('ref');
    }
    if (manifest.importer !== hash) mismatches.push('import step');
    if (mismatches.length > 0) {
      problems.push(`${provider.engine}: the import on disk is out of date (${mismatches.join(', ')} changed)`);
      continue;
    }
    const listed = (manifest.pages ?? []).map((page) => page.file).sort();
    const onDisk = pageFilesBelow(directory).map((file) => `${importDirectoryName}/${provider.engine}/${file}`);
    const missing = listed.filter((file) => !onDisk.includes(file));
    const extra = onDisk.filter((file) => !listed.includes(file));
    // A page edited where the import wrote it is an edit nobody will see again:
    // the next import replaces it, and its own repository never had it.
    const edited = (manifest.pages ?? [])
      .filter((page) => !missing.includes(page.file) && digest(readFileSync(join(root, page.file))) !== page.sha256)
      .map((page) => page.file);
    if (listed.length === 0) problems.push(`${provider.engine}: the import wrote no page`);
    for (const file of missing) problems.push(`${provider.engine}: ${file} is in the manifest and not on disk`);
    for (const file of extra) problems.push(`${provider.engine}: ${file} is on disk and not in the manifest`);
    for (const file of edited) {
      problems.push(`${provider.engine}: ${file} differs from what the import wrote; edit the page in ${provider.repository}`);
    }
    if (missing.length > 0 || extra.length > 0 || edited.length > 0 || listed.length === 0) continue;
    imported.push({ ...provider, ...manifestFacts(manifest) });
  }

  if (problems.length > 0) {
    throw new ProviderDocsError(`the provider documentation is not imported as declared; ${remedy}:\n  ${problems.join('\n  ')}`);
  }
  return imported;
}

function manifestFacts(manifest) {
  return {
    commit: manifest.commit,
    local: manifest.local === true,
    pages: manifest.pages.map((page) => ({ ...page })),
  };
}

/** Every imported page file, relative to the content root, in POSIX spelling. */
export function importedPageFiles(options) {
  return importedProviders(options).flatMap((provider) => provider.pages.map((page) => page.file)).sort();
}

/**
 * The sidebar subgroups of the imported providers, one per provider, after the
 * official providers' own pages. What is on disk, never a refusal; see the
 * header for why.
 */
export function providerSidebarGroups({ root = contentRoot, path = declarationPath } = {}) {
  let providers;
  try {
    providers = declaredProviders(path);
  } catch {
    return [];
  }
  const groups = [];
  for (const provider of providers) {
    const directory = join(root, importDirectoryName, provider.engine);
    let manifest;
    try {
      manifest = existsSync(directory) && statSync(directory).isDirectory() ? readManifest(directory) : null;
    } catch {
      manifest = null;
    }
    if (!manifest || !Array.isArray(manifest.pages) || manifest.pages.length === 0) continue;
    groups.push({
      label: `${provider.name} provider`,
      items: manifest.pages.map((page) => (page.label ? { slug: page.slug, label: page.label } : { slug: page.slug })),
    });
  }
  return groups;
}
