# README product-flow diagram source

`readme-product-flow.png` is an illustrative product diagram. It does not
represent output produced by a Ptah command.

- Tool: OpenAI image generation
- Generated: 2026-08-28
- Semantic reference: the README product model and the earlier README hero
- Future visual reference: use the committed PNG
- Manual editing: none
- Post-processing: color reduction, metadata removal, and lossless RGBA output
  with ImageMagick; keeping full alpha preserves labels in GitHub's dark theme

```bash
magick generated.png -strip -colors 256 -define png:compression-level=9 \
  PNG32:docs/site/src/assets/readme-product-flow.png
```

## Generation brief

```text
Create a clean, professional architecture diagram for the Ptah README.

Show two inputs on the left: "Schema sources" above "Current database". Route
both into a central "Compare + plan" step. From that step, split into two
parallel paths:

- a blue upper path labeled "Versioned migrations" with the subtitle
  "generate · review · commit · apply";
- an orange lower path labeled "Direct schema changes" with the subtitle
  "review · approve · apply".

Join both paths at a "Target database" card on the right.

Use a wide left-to-right composition, transparent background, rounded cards,
blue and orange flow lines, flat document and database icons, strong alignment,
and highly legible text. Keep the style suitable for an official open-source
README. Do not add a title, legend, decorative elements, or extra stages.
```
