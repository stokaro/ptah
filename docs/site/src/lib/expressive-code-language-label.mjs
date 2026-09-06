/**
 * Puts the fence's own language on the frame header, so the bar over an
 * untitled code block can name it without anything enumerating the languages
 * the content happens to use (stokaro/ptah#2930).
 *
 * src/styles/ptah/code.css drew that bar from nine `:has(pre[data-language=...])`
 * rules, one per language in the content. The list was accurate when it was
 * written and had nothing keeping it so: a page in a tenth language kept the
 * bar -- the `:not(.is-terminal):not(.has-title)` rule brings it back -- and
 * filled it with a non-breaking space, shipping a blank strip with a Copy
 * button in it. CSS cannot read a descendant's attribute, and the language
 * lives on `<pre>` while the bar is `<figcaption>`, so the value is copied up
 * here and the stylesheet reads it with `attr()`.
 *
 * The frame markup is Expressive Code's: `<figure class="frame"><figcaption
 * class="header">…</figcaption><pre data-language="…">`. Only the header is
 * touched; nothing else about the block changes.
 */
function headerOf(node) {
  if (!node || typeof node !== 'object') return null;
  const classes = node.properties?.className;
  const names = Array.isArray(classes) ? classes : typeof classes === 'string' ? classes.split(/\s+/) : [];
  if (node.tagName === 'figcaption' && names.includes('header')) return node;
  for (const child of node.children ?? []) {
    const found = headerOf(child);
    if (found) return found;
  }
  return null;
}

export function pluginLanguageLabel() {
  return {
    name: 'ptah-language-label',
    hooks: {
      postprocessRenderedBlock: ({ codeBlock, renderData }) => {
        const language = codeBlock.language;
        if (!language) return;
        const header = headerOf(renderData.blockAst);
        if (!header) return;
        header.properties = { ...header.properties, 'data-language': language };
      },
    },
  };
}
