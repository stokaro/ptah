/**
 * Lifts the word an FAQ answer commits to into a tag beside it.
 *
 * A Sätteri hast plugin, registered on `markdown.processor` in astro.config.mjs.
 *
 * Half the answers on the FAQ open by committing: `Yes.`, `No.`, `Partly.` The
 * word is the most useful thing on the line and the easiest to miss inside a
 * paragraph, so it is drawn as a tag. It stays a word in the Markdown source,
 * which is where the documentation gates read the prose and where an author
 * writes it; nothing here is a second place to keep it.
 *
 * Only a paragraph that is the answer to a question is touched: the FAQ's
 * answers are the paragraphs whose first text node opens with one of the three
 * words and a full stop. Prose that merely contains them is left alone, and so
 * is every other page -- the plugin reads the page's `faqVerdicts` frontmatter
 * flag and does nothing without it.
 */

// Only the full-stop form. `Yes, for constructs both engines can express` is a
// sentence whose first word carries a clause, and lifting it would leave the
// clause without its subject.
const VERDICTS = new Map([
  ['Yes.', 'yes'],
  ['No.', 'no'],
  ['Partly.', 'partly'],
]);

const LABEL = { yes: 'Yes', no: 'No', partly: 'Partly' };

export default function markdownFaqVerdict() {
  return {
    name: 'ptah-faq-verdict',
    element: {
      filter: ['p'],
      visit(node, ctx) {
        if (!ctx.data?.astro?.frontmatter?.faqVerdicts) return;

        const first = node.children?.[0];
        if (!first || first.type !== 'text' || typeof first.value !== 'string') return;

        const [word] = first.value.split(/(?<=\.)\s/, 1);
        const verdict = VERDICTS.get(word);
        if (!verdict) return;

        const kept = first.value.slice(word.length).replace(/^\s+/, '');
        if (kept === '' && node.children.length === 1) return;
        ctx.setProperty(first, 'value', kept);
        ctx.insertBefore(first, {
          type: 'element',
          tagName: 'span',
          properties: { className: ['faq-verdict'], 'data-verdict': verdict },
          children: [{ type: 'text', value: LABEL[verdict] }],
        });
      },
    },
  };
}
