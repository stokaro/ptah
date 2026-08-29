import { groupLandingPage } from './sidebar.mjs';

/**
 * Return the navigation path to the current page.
 *
 * Starlight gives page components the rendered sidebar tree, including the
 * `isCurrent` marker. Deriving breadcrumbs from that tree keeps labels and
 * hierarchy tied to the navigation instead of maintaining a second map.
 */
export function breadcrumbTrail(entries) {
  for (const entry of entries) {
    if (entry.type === 'link' && entry.isCurrent) {
      return [{ label: entry.label, href: entry.href, current: true }];
    }

    if (entry.type !== 'group') continue;
    const landingPage = groupLandingPage(entry);
    if (landingPage?.isCurrent) {
      return [{ label: entry.label, href: landingPage.href, current: true }];
    }

    const childTrail = breadcrumbTrail(entry.entries);
    if (childTrail.length > 0) {
      return [{ label: entry.label, current: false }, ...childTrail];
    }
  }

  return [];
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
