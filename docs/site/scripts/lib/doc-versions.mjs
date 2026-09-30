// The documentation versions a deployment carries: edge and every release tag.
//
// The Docs workflow selects tags with release-doc-tags.mjs, gen-versions.mjs
// orders the version picker, and check-deployment-candidate.mjs validates the
// deployed version set. They share the release grammar so a built release
// cannot disappear from the picker or the deployment guard.

export const EDGE = 'edge';

const RELEASE = /^v(\d+)\.(\d+)(?:\.(\d+))?$/;

export function isRelease(name) {
  return RELEASE.test(name);
}

export function isVersionFolder(name) {
  return name === EDGE || isRelease(name);
}

export function parseSemver(name) {
  const match = RELEASE.exec(name);
  if (!match) return null;
  return [Number(match[1]), Number(match[2]), match[3] === undefined ? 0 : Number(match[3])];
}

// Numeric, not lexical: v1.10.0 is newer than v1.2.0. A tag without a patch
// number compares as .0, and the name breaks that tie, so the order is total
// and v1.2 sorts before v1.2.0, as `sort -V` puts them.
export function compareReleases(a, b) {
  const left = parseSemver(a);
  const right = parseSemver(b);
  if (!left || !right) throw new Error(`not a release version: ${left ? b : a}`);
  for (let i = 0; i < 3; i += 1) {
    if (left[i] !== right[i]) return left[i] - right[i];
  }
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

// Every release among `names`, oldest first. The workflow hands over every
// `v*` tag, including names that are not releases.
export function releaseVersions(names) {
  return [...new Set(names)].filter(isRelease).sort(compareReleases);
}
