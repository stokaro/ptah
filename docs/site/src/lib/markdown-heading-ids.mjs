/**
 * Lets a heading declare its own anchor, as `## Question text {#explicit-id}`.
 *
 * A Sätteri hast plugin, registered on `markdown.processor` in
 * astro.config.mjs beside `markdownAsides()`.
 *
 * Astro 7 renders Markdown through Sätteri, which slugs a heading from its text
 * and has no syntax for an author-chosen id: written without this plugin,
 * `## Question {#q-1}` renders the braces as visible text and takes
 * `question-q-1` as its id. Sätteri's own `heading-ids` plugin does honour an
 * id already on the node (`node.properties.id`), which is the seam this uses.
 *
 * The anchor matters wherever a URL is shared rather than followed from the
 * page: a slug derived from the wording changes the moment the wording is
 * edited, and every link anyone saved breaks silently. An explicit id survives
 * a rewrite of the sentence above it.
 *
 * The suffix is read off the heading's last text node, so `{#id}` written
 * anywhere else in the line is left alone as prose.
 */

const SUFFIX = /\s*\{#([a-z0-9]+(?:-[a-z0-9]+)*)\}\s*$/;

export default function markdownHeadingIds() {
  return {
    name: 'ptah-heading-ids',
    element: {
      filter: ['h1', 'h2', 'h3', 'h4', 'h5', 'h6'],
      visit(node, ctx) {
        const children = node.children;
        if (!children || !children.length) return;

        // Only a trailing text node can carry the suffix. Anything else at the
        // end of the heading (a code span, emphasis) means the author did not
        // write one.
        const last = children[children.length - 1];
        if (!last || last.type !== 'text' || typeof last.value !== 'string') return;

        const match = last.value.match(SUFFIX);
        if (!match) return;

        // The visitor hands out read-only child stubs, so the text is edited
        // through the context rather than assigned.
        const trimmed = last.value.slice(0, last.value.length - match[0].length);
        if (trimmed === '') {
          // A heading that was only an anchor would leave an empty text node.
          ctx.removeChildAt(node, children.length - 1);
        } else {
          ctx.setProperty(last, 'value', trimmed);
        }

        ctx.setProperty(node, 'id', match[1]);
      },
    },
  };
}
