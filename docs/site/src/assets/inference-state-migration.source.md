# Inference-state migration diagram source

`inference-state-migration.png` is an illustrative architecture diagram. It
does not represent output produced by a Ptah command.

- Tool: OpenAI image generation
- Generated: 2026-08-28
- Reference: an earlier Ptah inference-flow draft from the same design pass
- Future reference: use the committed PNG to preserve its visual language
- Manual editing: none
- Post-processing: palette reduction and metadata removal with ImageMagick

```bash
magick generated.png -strip -colors 256 PNG8:docs/site/src/assets/inference-state-migration.png
```

## Generation prompt

```text
Use case: infographic-diagram.

Create a brand-new clean, professional architecture diagram for Ptah
inference-state migration. Use the supplied image only as a visual reference
for its general visual language and left-to-right composition; do not edit or
copy it literally.

Purpose and core message:
Ptah does not run inference itself. Ptah orchestrates migration of persistent
inference state. An external embedding endpoint computes embeddings.

Canvas and style:
- Wide landscape composition suitable for an official open-source GitHub
  README and documentation site.
- Solid clean white background, not transparent and not a checkerboard.
- Rounded rectangular cards, strong alignment, even spacing, highly legible
  text.
- Blue primary migration flow.
- Orange generation-lifecycle and rollback flow.
- Simple flat technical icons: document/spec, source rows/database,
  plan/checklist, candidate database, verification magnifier/checkmark, cutover
  switch, active/previous databases, external API/code endpoint.
- Minimal polished software-documentation style, no decorative elements, no
  visual noise, no gradients except an extremely subtle one if needed.
- Keep all text horizontal and crisp.

Main blue flow, left to right:
Two compact input cards at the far left, labeled exactly:
"Generation spec"
"Source rows"
They converge into:
"Plan + prepare"
then:
"Build candidate generation"
with a smaller subtitle exactly:
"backfill · catch up concurrent changes"
then:
"Verify + evaluate"
then:
"Cutover"
then:
"Active generation"

Semantics the composition must make clear:
- Candidate generation is built alongside the currently active generation,
  never by overwriting it.
- Catch-up covers concurrent inserts, updates, and deletes before verification
  and cutover.
- Verification is visibly before cutover.
- Cutover switches which generation consumers use; it is a pointer/switch
  operation, not a data-copy operation.

External embedding component:
- Place a separate rounded card directly below "Build candidate generation",
  labeled exactly:
"Embedding endpoint"
- Make it unmistakably external to Ptah by visually detaching it from the main
  flow and using a restrained dashed outline or small external/API visual cue.
- Connect only "Build candidate generation" and "Embedding endpoint" with a
  dashed two-way vertical request/response interaction: two arrowheads, one
  down for the request and one up for the response.
- The interaction must read visually as: Ptah reads source row, calls embedding
  endpoint, receives embedding, then Ptah writes candidate state.
- Do not connect the embedding endpoint directly to any database card. Do not
  imply the endpoint writes persistent state.

Generation lifecycle on the right:
- Keep "Active generation" above "Previous generation".
- Label the lower card exactly:
"Previous generation"
- Use orange secondary flow to show that after cutover the former active
  generation becomes the previous generation.
- Add the exact label:
"retain for rollback"
- Show a clear orange rollback path from "Previous generation" back to "Active
  generation".
- Optionally show a very small, visually secondary orange continuation from
  "Previous generation" to:
"Retire later"
- Ensure blue cutover flow and orange lifecycle flow are distinct and easy to
  follow.

Text constraints:
Use the exact labels above with no spelling changes. Do not add a title, legend,
paragraphs, branding, or unrelated labels. Avoid excessive arrows.
```
