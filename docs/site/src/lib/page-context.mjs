/**
 * Return the navigation path to the current page.
 *
 * Starlight gives page components the rendered sidebar tree, including the
 * `isCurrent` marker. Deriving breadcrumbs from that tree keeps labels and
 * hierarchy tied to the navigation instead of maintaining a second map.
 */
export function breadcrumbTrail(entries, landingHrefs = new Set()) {
  for (const entry of entries) {
    if (entry.type === 'link' && entry.isCurrent) {
      return [{ label: entry.label, href: entry.href, current: true }];
    }

    if (entry.type !== 'group') continue;
    const childTrail = breadcrumbTrail(entry.entries, landingHrefs);
    if (childTrail.length > 0) {
      const first = entry.entries[0];
      const href = first?.type === 'link' && landingHrefs.has(first.href) ? first.href : undefined;
      return [{ label: entry.label, href, current: false }, ...childTrail];
    }
  }

  return [];
}

/**
 * Infer section landings from the rendered sidebar when release content
 * predates Ptah's `type: landing` frontmatter contract.
 *
 * An explicit Overview child is the release-compatible signal. Do not treat
 * every group's first task as a landing: older sidebars contain groups whose
 * first item is only the first task in that section.
 */
export function sidebarOverviewHrefs(entries) {
  const hrefs = new Set();
  for (const entry of entries) {
    if (entry.type !== 'group') continue;
    const first = entry.entries[0];
    if (
      first?.type === 'link' &&
      (first.label === 'Overview' || /\/overview\/?$/.test(first.href))
    ) {
      hrefs.add(first.href);
    }
    for (const href of sidebarOverviewHrefs(entry.entries)) hrefs.add(href);
  }
  return hrefs;
}

/** Remove a Markdown or MDX frontmatter block without parsing its contents. */
export function stripFrontmatter(source) {
  if (!source.startsWith('---')) return source.trim();

  const match = source.match(/^---\r?\n[\s\S]*?\r?\n---\r?\n?/);
  return match ? source.slice(match[0].length).trim() : source.trim();
}

/** Build the text copied by the page action. */
export function pageMarkdown({ title, description, canonicalUrl, source }) {
  const sections = [`# ${title}`];
  if (description) sections.push(description.trim());
  sections.push(`Source: ${canonicalUrl}`, stripFrontmatter(source));
  return `${sections.filter(Boolean).join('\n\n')}\n`;
}
