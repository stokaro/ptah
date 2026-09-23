// The documentation versions a deployment carries: edge, and the release tags
// the retention window keeps.
//
// Three programs answer questions about the same set. The Docs workflow picks
// which tags to build (retained-doc-tags.mjs), gen-versions.mjs orders the
// version picker, and check-deployment-candidate.mjs decides whether a served
// version may leave the site. All three read the order from this module. With
// a second ordering, the build could keep a tag that the guard counts as
// retired: the guard would then refuse every later deploy, or accept one that
// drops a version the window still holds.

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

// The newest `keep` releases among `names`, oldest first. A name that is not a
// release tag is ignored, because the workflow hands over every `v*` tag.
export function retainedReleases(names, keep) {
  if (!Number.isInteger(keep) || keep < 1) {
    throw new Error(`the number of retained releases must be a positive integer, got ${keep}`);
  }
  return [...new Set(names)].filter(isRelease).sort(compareReleases).slice(-keep);
}
