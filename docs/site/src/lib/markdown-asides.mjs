/**
 * Tells an aside's default title (its type name) from an author's own, so the
 * page can draw the type as a label and the title as a heading
 * (stokaro/ptah#2893).
 *
 * A Sätteri hast plugin, registered on `markdown.processor` in
 * astro.config.mjs. Starlight's asides plugin runs on the mdast, so by the time
 * this visitor sees an `<aside>` it already has Starlight's shape:
 *
 *   <aside class="starlight-aside starlight-aside--caution" aria-label="TITLE">
 *     <p class="starlight-aside__title" aria-hidden="true">
 *       <svg class="starlight-aside__icon" …/>TITLE
 *     </p>
 *     <div class="starlight-aside__content">…</div>
 *   </aside>
 *
 * TITLE is the type name ("Note", "Tip", "Caution", "Danger") when the author
 * wrote `:::caution`, and the author's words when they wrote
 * `:::caution[Tags are not immutable]`. The markup does not say which. The
 * design draws the type as a small uppercase label in a column beside the text
 * (content.css), so a default title would repeat that label, while an author's
 * title is a sentence the reader needs. This plugin decides, and leaves the
 * decision on the aside as `data-ptah-title="default"` or `"custom"`.
 *
 * The comparison ignores the icon. In a `.md` file the icon is an `html` mdast
 * node that becomes a `raw` hast node; in an `.mdx` file it is an
 * `mdxJsxTextElement` (a `<Fragment set:html>`). Neither carries text nodes,
 * but the text is gathered by hand rather than through `ctx.textContent` so an
 * SVG that ever did carry a `<title>` cannot turn a default into a custom one.
 * Only the English labels are compared: the site has no other locale. The
 * `aria-label` Starlight put on the aside is left alone, so the accessible name
 * is the same whichever way the decision goes.
 */

/** Starlight's English default titles, by variant (its `aside.<variant>` strings). */
const DEFAULT_TITLES = {
  note: 'Note',
  tip: 'Tip',
  caution: 'Caution',
  danger: 'Danger',
};

const VARIANT_CLASS = /^starlight-aside--([a-z]+)$/;

/** The class names of a hast element, whichever shape the property takes. */
function classNames(node) {
  const value = node.properties?.className ?? node.properties?.class;
  if (Array.isArray(value)) return value.map(String);
  if (typeof value === 'string') return value.split(/\s+/).filter(Boolean);
  return [];
}

/**
 * The words of a node: its text nodes, descending through inline elements
 * such as `<strong>` or `<code>` in a title, and skipping raw HTML, SVG and
 * MDX JSX nodes, which is where the icon lives in each source format.
 */
function ownText(node) {
  if (node.type === 'text') return node.value ?? '';
  if (node.type === 'element' && node.tagName === 'svg') return '';
  if (node.type !== 'element' && node.type !== 'root') return '';
  let text = '';
  for (const child of node.children ?? []) text += ownText(child);
  return text;
}

export default function markdownAsides() {
  return {
    name: 'ptah-asides',
    element: {
      filter: ['aside'],
      visit(node, ctx) {
        const classes = classNames(node);
        if (!classes.includes('starlight-aside')) return;
        const variant = classes.map((name) => VARIANT_CLASS.exec(name)?.[1]).find(Boolean);
        const defaultTitle = variant ? DEFAULT_TITLES[variant] : undefined;
        const title = (node.children ?? []).find(
          (child) =>
            child.type === 'element' &&
            child.tagName === 'p' &&
            classNames(child).includes('starlight-aside__title'),
        );
        if (!title) return;
        const text = ownText(title).replace(/\s+/g, ' ').trim();
        const isDefault =
          defaultTitle !== undefined && text.toLowerCase() === defaultTitle.toLowerCase();
        ctx.setProperty(node, 'data-ptah-title', isDefault ? 'default' : 'custom');
      },
    },
  };
}
