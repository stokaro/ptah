// The editorial vocabulary shared by Astro's content schema and the generated
// documentation inventory. Keep the conditional rules here so a page cannot
// pass the site build and fail the inventory for a different interpretation of
// the same metadata.

import { existsSync } from 'node:fs';
import { isAbsolute, relative, resolve, sep } from 'node:path';

export const pageTypes = [
  'landing',
  'tutorial',
  'how-to',
  'concept',
  'reference',
  'troubleshooting',
  'status',
  'contributor',
];

export const dispositions = ['keep', 'rewrite', 'split', 'merge', 'move', 'retire'];

export const sourceModes = [
  'source-neutral',
  'go-only',
  'static-file-only',
  'external-program-only',
  'oci-artifact-only',
  'live-database-only',
  'command-specific',
];

const verificationDate = /^\d{4}-\d{2}-\d{2}$/;
const externalURL = /^[a-z][a-z0-9+.-]*:\/\//i;
const githubIssue = /^[a-z0-9_.-]+\/[a-z0-9_.-]+#\d+$/i;
const githubRepository = /^github:[a-z0-9_.-]+\/[a-z0-9_.-]+$/i;
const namedEvidence = /^evidence:[a-z0-9_.-]+(?:\/[a-z0-9_.-]+)*$/i;

function stringArray(value, minimumLength = 0) {
  return Array.isArray(value) && value.length >= minimumLength &&
    value.every((item) => typeof item === 'string' && item.trim() !== '');
}

function currentUTCDate() {
  return new Date().toISOString().slice(0, 10);
}

function semanticDateProblem(value, today) {
  if (typeof value !== 'string' || !verificationDate.test(value)) return 'must use YYYY-MM-DD form';
  const parsed = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(parsed.valueOf()) || parsed.toISOString().slice(0, 10) !== value) {
    return 'must be a real calendar date';
  }
  if (value > today) return `must not be in the future (build date ${today})`;
  return null;
}

function isOutside(root, candidate) {
  const path = relative(root, candidate);
  return path === '..' || path.startsWith(`..${sep}`) || isAbsolute(path);
}

function referenceProblem(value, repositoryRoot, field) {
  if (typeof value !== 'string' || value.trim() === '') return null;
  const reference = value.trim();
  const requiresRepositoryPath = field === 'generator' || field === 'editSource';
  if (externalURL.test(reference) || githubIssue.test(reference)) {
    return requiresRepositoryPath ? 'must be a repository-relative path' : null;
  }
  if (field !== 'generator' && field !== 'editSource' &&
      (githubRepository.test(reference) || namedEvidence.test(reference))) return null;
  if (!repositoryRoot) return null;
  if (isAbsolute(reference)) return 'must be a repository-relative path';

  const candidate = resolve(repositoryRoot, reference);
  if (isOutside(repositoryRoot, candidate)) return 'must stay inside the repository';
  if (!existsSync(candidate)) return `references missing repository path ${JSON.stringify(reference)}`;
  return null;
}

/**
 * Return semantic metadata problems as `{ path, message }` objects.
 *
 * Zod owns scalar type validation in the Astro build. The generated inventory
 * uses this function too, after its deliberately small frontmatter reader has
 * parsed the fields it needs.
 */
export function validatePageMetadata(data, options = {}) {
  const problems = [];
  const add = (path, message) => problems.push({ path: [path], message });
  const repositoryRoot = options.repositoryRoot;
  const today = options.today ?? currentUTCDate();

  if (!pageTypes.includes(data.type)) add('type', `must be one of: ${pageTypes.join(', ')}`);
  if (!stringArray(data.audience, 1)) add('audience', 'must name at least one reader audience');
  if (typeof data.readerQuestion !== 'string' || data.readerQuestion.trim() === '') {
    add('readerQuestion', 'must state the primary question this page answers');
  } else if (!data.readerQuestion.trim().endsWith('?')) {
    add('readerQuestion', 'must be written as a question ending in ?');
  }
  if (typeof data.goal !== 'string' || data.goal.trim() === '') {
    add('goal', 'must state the result a reader can obtain after reading');
  } else if (typeof data.description === 'string' && data.goal.trim() === data.description.trim()) {
    add('goal', 'must state a reader outcome, not repeat the page description');
  }
  if (!stringArray(data.sourceOfTruth, 1)) {
    add('sourceOfTruth', 'must name at least one authoritative source');
  }
  if (!stringArray(data.overlaps)) {
    add('overlaps', 'must be an array of non-empty strings, including an explicit empty array');
  }
  for (const field of ['owns', 'searchAliases']) {
    if (data[field] !== undefined && !stringArray(data[field])) {
      add(field, 'must be an array of non-empty strings');
    }
  }
  if (data.evidence !== undefined && !stringArray(data.evidence, 1)) {
    add('evidence', 'must name at least one non-empty evidence source when present');
  }
  if (!dispositions.includes(data.disposition)) {
    add('disposition', `must be one of: ${dispositions.join(', ')}`);
  }
  if (typeof data.generated !== 'boolean') add('generated', 'must state whether the page is generated');
  if (data.lengthWaiver !== undefined) {
    add('lengthWaiver', 'is not accepted; use scripts/data/editorial-waivers.json');
  }
  if (data.sourceMode !== undefined && !sourceModes.includes(data.sourceMode)) {
    add('sourceMode', `must be one of: ${sourceModes.join(', ')}`);
  }

  if (data.generated === true) {
    if (typeof data.generator !== 'string' || data.generator.trim() === '') {
      add('generator', 'is required when generated is true');
    }
    if (typeof data.editSource !== 'string' || data.editSource.trim() === '') {
      add('editSource', 'is required when generated is true');
    }
  } else if (data.generated === false) {
    if (data.generator !== undefined) add('generator', 'is only accepted when generated is true');
    if (data.editSource !== undefined) add('editSource', 'is only accepted when generated is true');
  }

  if (data.type === 'status') {
    if (data.lastVerified === undefined) add('lastVerified', 'is required for status pages in YYYY-MM-DD form');
    if (data.evidence === undefined) {
      add('evidence', 'must name at least one evidence source for a status page');
    }
  }
  if (data.lastVerified !== undefined) {
    const problem = semanticDateProblem(data.lastVerified, today);
    if (problem) add('lastVerified', problem);
  }

  for (const field of ['sourceOfTruth', 'evidence']) {
    for (const reference of data[field] ?? []) {
      const problem = referenceProblem(reference, repositoryRoot, field);
      if (problem) add(field, problem);
    }
  }
  for (const field of ['generator', 'editSource']) {
    if (data[field] === undefined) continue;
    const problem = referenceProblem(data[field], repositoryRoot, field);
    if (problem) add(field, problem);
  }

  return problems;
}
