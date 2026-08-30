/** Return the Git ref whose source corresponds to one documentation version. */
export function sourceRefForVersion(version) {
  if (typeof version !== 'string' || version.trim() === '') {
    throw new Error('documentation version must be a non-empty string');
  }
  return version === 'edge' ? 'master' : version;
}

/** Return whether a documentation version is the mutable edge version. */
export function isEdgeVersion(version) {
  return version === 'edge';
}
