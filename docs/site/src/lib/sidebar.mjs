/**
 * Return the landing-page link carried as a group's first `Overview` entry.
 *
 * Starlight groups cannot carry a link of their own. Ptah keeps the landing
 * page as a normal entry so route data, pagination, breadcrumbs, and the docs
 * gates all read one navigation tree. The sidebar renderer promotes that entry
 * onto the expandable group row instead of showing a duplicate `Overview`
 * child.
 */
export function groupLandingPage(entry) {
  if (entry?.type !== 'group') return undefined;

  const first = entry.entries[0];
  return first?.type === 'link' && first.label === 'Overview' ? first : undefined;
}

/** Return the visible children below a group row. */
export function groupChildEntries(entry) {
  return groupLandingPage(entry) ? entry.entries.slice(1) : entry.entries;
}

/** Whether the current page is anywhere inside a sidebar branch. */
export function branchContainsCurrent(entries) {
  return entries.some((entry) =>
    entry.type === 'link' ? entry.isCurrent : branchContainsCurrent(entry.entries),
  );
}
