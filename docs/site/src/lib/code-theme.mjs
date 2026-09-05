/**
 * The two syntax themes the code blocks are highlighted with, one per color
 * scheme. Four colors and no more, the same scopes in both schemes, as the
 * docs concept draws them (stokaro/ptah#2893): keywords and command-line
 * flags in the brand blue, quoted strings in green, numbers, types and
 * booleans in amber, comments in the muted gray that labels use. Everything
 * else -- identifiers, a bash command name and its arguments, operators,
 * punctuation -- stays in the strongest text color, so a command reads as one
 * thing a reader types. Two scopes outside that set come from the `console`
 * grammar: a shell prompt's `$` is amber and the lines a command printed sit
 * one step below it in the secondary gray.
 *
 * The values are the hex forms of the tokens in src/styles/ptah/tokens.css.
 * Expressive Code puts a token's color into the page as an inline custom
 * property and mixes it for contrast, so a `var()` would not survive; the
 * theme has to spell the colors out. When a token changes there, change it
 * here as well. The oklch tokens convert to: accent oklch(0.47 0.16 238) =
 * #0062a8 and oklch(0.78 0.11 225) = #5ec6ec; green oklch(0.52 0.12 150) =
 * #287c42 and oklch(0.72 0.12 150) = #69ba7c; amber oklch(0.55 0.15 62) =
 * #ad5800 and oklch(0.8 0.15 70) = #faab3f.
 *
 * Every color clears WCAG AA 4.5:1 against the code surface it sits on
 * (`--sl-color-bg-inline-code`: #f3f4f4 light, #1d1916 dark), which
 * check-accessibility.mjs measures on the built pages:
 *
 *   role      light    on #f3f4f4    dark     on #1d1916
 *   ink       #111417  16.77         #ede7de  14.20
 *   accent    #0062a8   5.75         #5ec6ec   8.95
 *   green     #287c42   4.70         #69ba7c   7.41
 *   amber     #ad5800   4.57         #faab3f   9.11
 *   gray-3    #666d76   4.75         #8d8377   4.69
 *   gray-2    #474d54   7.75         #b6ada0   7.88
 *
 * An expected-output block has no fill, so its text sits on the page
 * background (#fbfbfa / #161311) instead; gray-2 measures 8.25 and 8.34
 * there, and the stylesheet (src/styles/ptah/code.css) forces every token of
 * a `text` block to gray-2 regardless of what the theme says.
 *
 * `minSyntaxHighlightingColorContrast` is 0 in astro.config.mjs so Expressive
 * Code leaves these colors as written instead of pushing the three near the
 * threshold (green, amber, gray-3 in light) toward its own 5.5:1 default.
 *
 * The scopes are TextMate scopes, taken from a probe of Shiki's grammars for
 * every language the content uses (bash, sh, console, powershell, sql, yaml,
 * hcl, go, json, proto, graphql, markdown). TextMate resolves a token by the
 * deepest matching scope, so a more specific selector wins over a broader one
 * regardless of order; the rules below rely on that where a scope has to be
 * pulled back out of a broader family:
 *
 * - `constant.other.option` is a bash flag (`--to`, `-fsSL`). The same token
 *   also carries `string.unquoted.argument`, as every bash argument does, so
 *   arguments are ruled ink by name and the deeper flag scope wins for flags.
 *   Bash colors nothing else on a command line: command names are
 *   `entity.name.command`, left alone. PowerShell's grammar scopes only the
 *   dash of a flag (`keyword.operator.assignment`) and nothing of its name,
 *   so PowerShell flags stay ink rather than showing a lone blue dash.
 * - `keyword.operator` is ruled ink: `=`, `|`, `>`, `<<`, `:=` are
 *   punctuation to the eye, and keeping them ink is what keeps a command one
 *   color. `storage.modifier` (SQL `PRIMARY KEY`, `DEFAULT`, `NOT NULL`;
 *   bash `export`; proto `repeated`) and `storage.type.function` (bash
 *   `function`) read as keywords and join the accent.
 * - `storage.type` is a type name (SQL `INTEGER`, Go `int64`, proto
 *   `string`) and joins numbers and booleans in amber, with GraphQL's
 *   `support.type.builtin` (`ID`, `String`).
 * - A YAML key is `entity.name.tag`, a JSON key `support.type.property-name`:
 *   both are ruled accent, as the concept draws YAML keys, and both are deeper
 *   than the `string` scope the same token carries. HCL block types
 *   (`table`, `column`) are `entity.name.type.hcl`; the grammar puts a block's
 *   quoted label in `variable.other.enummember.hcl`, ruled green so the label
 *   reads as the string it is. HCL's bare type values (`integer`, `text`)
 *   reach the grammar as block punctuation and cannot be colored.
 * - `string.unquoted.shell` (an unquoted assignment value or a glob) stays
 *   ink with the arguments; only quoted strings and heredoc bodies go green.
 *   A heredoc's delimiter (`'YAML'`) is `punctuation.definition.string.heredoc`
 *   and goes green with the body it opens. A number given as an argument
 *   (`sleep 2`, `--to 20240101000002`) carries `constant.numeric` under
 *   `string.unquoted.argument`; the parent-scope selector keeps it ink with
 *   the other arguments, while `export FOO=1` keeps its amber `1`. Inside a
 *   quoted string, a `$(...)` subshell brings the argument and flag scopes
 *   back; the two `string.quoted ...` selectors keep the whole string green.
 * - `punctuation.separator.prompt` and `entity.other.prompt-prefix` are what
 *   the `console` grammar gives a prompt's `$` and its `user@host` prefix;
 *   `meta.output` is what it gives the lines after a command. That grammar
 *   hands the command to `source.shell` as one capture, so bash's statement
 *   rules never run on it: a console command is one flat token, its flags are
 *   not colored (only a bash fence colors flags), and a stray keyword-looking
 *   word (`--env local`) would come out accent; the `text.shell-session`
 *   selectors rule those ink so a console command is one color. The grammar
 *   also treats a `#` at the start of a console line as a prompt, so a comment
 *   in a console block starts with an amber `#` and the rest of the line is
 *   colored as a command (quoted words green); write such notes in prose or
 *   as a bash fence.
 */

function theme({ name, type, background, foreground, muted, output, prompt, keyword, string, number }) {
  return {
    name,
    type,
    colors: {
      'editor.background': background,
      'editor.foreground': foreground,
    },
    tokenColors: [
      {
        scope: ['comment', 'punctuation.definition.comment', 'entity.other.prompt-prefix'],
        settings: { foreground: muted, fontStyle: '' },
      },
      {
        scope: ['punctuation.separator.prompt'],
        settings: { foreground: prompt },
      },
      {
        scope: ['meta.output'],
        settings: { foreground: output },
      },
      {
        scope: [
          'keyword',
          'storage.modifier',
          'storage.type.function',
          'constant.other.option',
          'entity.name.tag',
          'entity.name.type.hcl',
          'support.type.property-name',
        ],
        settings: { foreground: keyword, fontStyle: '' },
      },
      {
        scope: ['keyword.operator'],
        settings: { foreground },
      },
      {
        scope: [
          'string',
          'punctuation.definition.string.heredoc',
          'variable.other.enummember.hcl',
          'string.quoted string.unquoted.argument',
          'string.quoted constant.other.option',
        ],
        settings: { foreground: string, fontStyle: '' },
      },
      {
        scope: ['string.unquoted.argument', 'string.unquoted.shell', 'string.unquoted.argument constant.numeric'],
        settings: { foreground },
      },
      {
        scope: ['constant.numeric', 'constant.language', 'storage.type', 'support.type.builtin'],
        settings: { foreground: number, fontStyle: '' },
      },
      {
        scope: ['text.shell-session keyword', 'text.shell-session storage.modifier'],
        settings: { foreground },
      },
    ],
  };
}

export const codeThemeDark = theme({
  name: 'ptah-dark',
  type: 'dark',
  background: '#1d1916',
  foreground: '#ede7de',
  muted: '#8d8377',
  output: '#b6ada0',
  prompt: '#faab3f',
  keyword: '#5ec6ec',
  string: '#69ba7c',
  number: '#faab3f',
});

export const codeThemeLight = theme({
  name: 'ptah-light',
  type: 'light',
  background: '#f3f4f4',
  foreground: '#111417',
  muted: '#666d76',
  output: '#474d54',
  prompt: '#ad5800',
  keyword: '#0062a8',
  string: '#287c42',
  number: '#ad5800',
});
