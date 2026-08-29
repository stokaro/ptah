// The editorial vocabulary shared by Astro's content schema and the generated
// documentation inventory. Keep the conditional rules here so a page cannot
// pass the site build and fail the inventory for a different interpretation of
// the same metadata.

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

const verificationDate = /^\d{4}-\d{2}-\d{2}$/;

function nonEmptyStrings(value) {
  return Array.isArray(value) && value.length > 0 && value.every((item) => typeof item === 'string' && item.trim() !== '');
}

/**
 * Return semantic metadata problems as `{ path, message }` objects.
 *
 * Zod owns scalar type validation in the Astro build. The generated inventory
 * uses this function too, after its deliberately small frontmatter reader has
 * parsed the fields it needs.
 */
export function validatePageMetadata(data) {
  const problems = [];
  const add = (path, message) => problems.push({ path: [path], message });

  if (!pageTypes.includes(data.type)) add('type', `must be one of: ${pageTypes.join(', ')}`);
  if (!nonEmptyStrings(data.audience)) add('audience', 'must name at least one reader audience');
  if (typeof data.readerQuestion !== 'string' || data.readerQuestion.trim() === '') {
    add('readerQuestion', 'must state the primary question this page answers');
  }
  if (typeof data.goal !== 'string' || data.goal.trim() === '') {
    add('goal', 'must state the result a reader can obtain after reading');
  } else if (typeof data.description === 'string' && data.goal.trim() === data.description.trim()) {
    add('goal', 'must state a reader outcome, not repeat the page description');
  }
  if (!nonEmptyStrings(data.sourceOfTruth)) {
    add('sourceOfTruth', 'must name at least one authoritative source');
  }
  if (!Array.isArray(data.overlaps)) add('overlaps', 'must be an array, including an explicit empty array');
  if (!dispositions.includes(data.disposition)) {
    add('disposition', `must be one of: ${dispositions.join(', ')}`);
  }
  if (typeof data.generated !== 'boolean') add('generated', 'must state whether the page is generated');

  if (data.generated === true) {
    if (typeof data.generator !== 'string' || data.generator.trim() === '') {
      add('generator', 'is required when generated is true');
    }
    if (typeof data.editSource !== 'string' || data.editSource.trim() === '') {
      add('editSource', 'is required when generated is true');
    }
  }

  if (data.type === 'status') {
    if (typeof data.lastVerified !== 'string' || !verificationDate.test(data.lastVerified)) {
      add('lastVerified', 'is required for status pages in YYYY-MM-DD form');
    }
    if (!nonEmptyStrings(data.evidence)) {
      add('evidence', 'must name at least one evidence source for a status page');
    }
  } else if (data.lastVerified !== undefined &&
      (typeof data.lastVerified !== 'string' || !verificationDate.test(data.lastVerified))) {
    add('lastVerified', 'must use YYYY-MM-DD form');
  }

  return problems;
}
