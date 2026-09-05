/**
 * Labels the code blocks that show what a command prints, so the page can draw
 * them apart from the commands (stokaro/ptah#2893). A Sätteri mdast plugin,
 * registered on `markdown.processor` in astro.config.mjs; it runs for `.md`
 * and `.mdx` alike, before Starlight's own plugins.
 *
 * The rule. A fence with no language, or with `text`, that directly follows a
 * paragraph announcing output -- the paragraph says "output", "prints",
 * "stderr", "stdout", "standard output" or "standard error", "exit code" or
 * "exits" -- is an expected-output block. It gets `title="expected output
 * includes"` when that paragraph says "includes", and `title="expected output"`
 * otherwise. Expressive Code reads the fence's meta for `title="..."` and
 * renders it in the frame's header, which src/styles/ptah/code.css draws as
 * the dashed bar the concept gives an output block; a `text` fence without a
 * title gets the dashed frame and no bar. A fence that already carries a
 * title is left alone, and existing meta (`wrap`, `frame=`, markers) is kept.
 *
 * The measurement behind the rule: of the 261 `text` fences in the content,
 * 87 follow a paragraph containing "output", and 61 of those paragraphs are
 * exactly "Expected output includes:"; another 13 follow a paragraph that
 * announces the block with stderr, stdout or an exit code ("The receipt naming
 * the approver goes to stderr:"), which is what the wider cue catches so one
 * page does not mix barred and bare output frames. The other `text` fences are
 * notes, file layouts and snippets that follow no such paragraph, and stay
 * unlabeled.
 *
 * `ctx.textContent` concatenates the paragraph's text nodes, inline code
 * included, so "output" inside backticks counts as well. In MDX a paragraph
 * and the fence after it are siblings under the same parent whether that is
 * the root or a `<TabItem>` (an `mdxJsxFlowElement`), so the rule holds inside
 * tabbed blocks too.
 */

const announcesOutput = /\b(?:output|prints?|stderr|stdout|standard (?:output|error)|exit code|exits?)\b/i;
const saysIncludes = /\bincludes\b/i;
const hasTitle = /(?:^|\s)title\s*=/;

export default function markdownExpectedOutput() {
  return {
    name: 'ptah-expected-output',
    code(node, ctx) {
      if (node.lang && node.lang !== 'text') return;
      if (node.meta && hasTitle.test(node.meta)) return;

      const parent = ctx.parent(node);
      const index = ctx.indexOf(node);
      if (!parent || index === undefined || index < 1) return;

      const previous = parent.children[index - 1];
      if (!previous || previous.type !== 'paragraph') return;

      const text = ctx.textContent(previous);
      if (!announcesOutput.test(text)) return;

      const title = saysIncludes.test(text) ? 'expected output includes' : 'expected output';
      const meta = node.meta ? `${node.meta} title="${title}"` : `title="${title}"`;
      ctx.setProperty(node, 'meta', meta);
    },
  };
}
