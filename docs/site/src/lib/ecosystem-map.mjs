// Turns the ecosystem declaration (src/ecosystem.mjs) and its drawing
// (src/ecosystem-layout.mjs) into what the page renders: the model the
// interactive map and the text views read, and the map as SVG markup.
//
// The same function draws the map inline on the page and as the full-size
// file under src/pages/diagrams/. Inline, the drawing carries classes and the
// site's stylesheet colors it, so it follows the theme the reader picked; the
// full-size file has no stylesheet around it, so it embeds the colors of one
// theme. Nothing else differs, which is what lets the full-size link stand in
// for the inline map.
//
// It reads no file and imports nothing from node_modules, so a plain Node
// script imports it the same way Astro does.
import * as definition from '../ecosystem.mjs';
import { wideLayout } from '../ecosystem-layout.mjs';

const NODE_KINDS = new Set(['repository', 'entry', 'infrastructure']);
const SIDES = new Set(['top', 'right', 'bottom', 'left']);

/**
 * Every problem with the declaration and the layout, as sentences. An empty
 * list means the map can be drawn and every view agrees with it.
 */
export function ecosystemProblems(source = definition, layout = wideLayout) {
  const problems = [];
  const ids = (list) => new Set(list.map(({ id }) => id));
  const duplicate = (list, what) => {
    const seen = new Set();
    for (const { id } of list) {
      if (seen.has(id)) problems.push(`duplicate ${what} id ${id}`);
      seen.add(id);
    }
  };
  duplicate(source.groups, 'group');
  duplicate(source.components, 'component');
  duplicate(source.relationships, 'relationship');
  duplicate(source.paths, 'path');

  const groupIds = ids(source.groups);
  const componentIds = ids(source.components);
  const kindIds = ids(source.relationshipKinds);
  const stateIds = ids(source.relationshipStates);
  const statusIds = ids(source.statuses);
  const relationshipIds = ids(source.relationships);

  for (const component of source.components) {
    const where = `component ${component.id}`;
    if (!groupIds.has(component.group)) problems.push(`${where}: unknown group ${component.group}`);
    if (!NODE_KINDS.has(component.kind)) problems.push(`${where}: unknown kind ${component.kind}`);
    for (const field of ['name', 'role', 'purpose', 'whenNeeded']) {
      if (typeof component[field] !== 'string' || component[field].trim() === '') problems.push(`${where}: ${field} is required`);
    }
    if (component.kind === 'repository') {
      if (!statusIds.has(component.status)) problems.push(`${where}: unknown status ${component.status}`);
      const repository = component.links?.find(({ label }) => label === 'Repository');
      if (!repository?.href?.startsWith('https://github.com/stokaro/')) {
        problems.push(`${where}: a repository links to its public GitHub repository`);
      }
    } else if (component.status) {
      problems.push(`${where}: only a repository has a release status`);
    }
    if (!Array.isArray(component.links) || component.links.length === 0) problems.push(`${where}: links are required`);
    for (const link of component.links ?? []) {
      if (Boolean(link.href) === Boolean(link.doc)) problems.push(`${where}: link ${link.label} needs exactly one of href and doc`);
      if (link.href && !link.href.startsWith('https://')) problems.push(`${where}: link ${link.label} is not an https address`);
      if (link.doc && (link.doc.startsWith('/') || !link.doc.endsWith('/'))) {
        problems.push(`${where}: link ${link.label} names a route as section/page/, without a leading slash`);
      }
    }
  }

  for (const relationship of source.relationships) {
    const where = `relationship ${relationship.id}`;
    if (!componentIds.has(relationship.from)) problems.push(`${where}: unknown component ${relationship.from}`);
    if (!componentIds.has(relationship.to)) problems.push(`${where}: unknown component ${relationship.to}`);
    if (!kindIds.has(relationship.kind)) problems.push(`${where}: unknown kind ${relationship.kind}`);
    if (relationship.state && !stateIds.has(relationship.state)) problems.push(`${where}: unknown state ${relationship.state}`);
    if (!relationship.label?.trim()) problems.push(`${where}: label is required`);
  }

  for (const path of source.paths) {
    for (const id of path.relationships) {
      if (!relationshipIds.has(id)) problems.push(`path ${path.id}: unknown relationship ${id}`);
    }
  }

  const connected = new Set(source.relationships.flatMap(({ from, to }) => [from, to]));
  for (const component of source.components) {
    if (!connected.has(component.id)) problems.push(`component ${component.id}: no relationship reaches it`);
  }

  for (const id of componentIds) {
    if (!layout.nodes[id]) problems.push(`layout: component ${id} is not placed`);
  }
  for (const id of Object.keys(layout.nodes)) {
    if (!componentIds.has(id)) problems.push(`layout: places unknown component ${id}`);
  }
  for (const id of relationshipIds) {
    if (!layout.edges[id]) problems.push(`layout: relationship ${id} is not routed`);
  }
  for (const [id, edge] of Object.entries(layout.edges)) {
    if (!relationshipIds.has(id)) {
      problems.push(`layout: routes unknown relationship ${id}`);
      continue;
    }
    try {
      const points = routePoints(edge.route, layout);
      if (points.length < 2) problems.push(`layout: relationship ${id} needs at least two points`);
    } catch (error) {
      problems.push(`layout: relationship ${id}: ${error.message}`);
    }
  }
  for (const id of Object.keys(layout.frames)) {
    if (!groupIds.has(id)) problems.push(`layout: frames unknown group ${id}`);
  }
  return problems;
}

/** Throws with every problem at once, so one build reports all of them. */
export function assertEcosystem(source = definition, layout = wideLayout) {
  const problems = ecosystemProblems(source, layout);
  if (problems.length > 0) {
    throw new Error(`The ecosystem declaration is inconsistent:\n- ${problems.join('\n- ')}`);
  }
}

/**
 * The declaration indexed for the views: components in group order, each with
 * the relationships that touch it, and lookups by id.
 */
export function ecosystemModel(source = definition) {
  assertEcosystem(source);
  const groupOrder = new Map(source.groups.map(({ id }, index) => [id, index]));
  const components = [...source.components].sort(
    (left, right) => groupOrder.get(left.group) - groupOrder.get(right.group),
  );
  const byId = new Map(components.map((component) => [component.id, component]));
  const kinds = new Map(source.relationshipKinds.map((kind) => [kind.id, kind]));
  const states = new Map(source.relationshipStates.map((state) => [state.id, state]));
  const statuses = new Map(source.statuses.map((status) => [status.id, status]));
  const relationships = source.relationships.map((relationship) => ({
    ...relationship,
    state: relationship.state ?? 'available',
  }));
  const touching = (id) => relationships.filter(({ from, to }) => from === id || to === id);
  return {
    groups: source.groups.map((group) => ({ ...group, components: components.filter((component) => component.group === group.id) })),
    components,
    byId,
    kinds,
    states,
    statuses,
    relationships,
    touching,
    paths: source.paths,
  };
}

// ---------------------------------------------------------------- geometry

function anchor(spec, layout) {
  if (Array.isArray(spec)) return spec;
  const [id, side, offsetText] = String(spec).split(':');
  const box = layout.nodes[id];
  if (!box) throw new Error(`anchor names unknown component ${id}`);
  if (!SIDES.has(side)) throw new Error(`anchor ${spec} names unknown side ${side}`);
  const offset = Number(offsetText ?? 0);
  const [x, y, width, height] = box;
  switch (side) {
    case 'top':
      return [x + width / 2 + offset, y];
    case 'bottom':
      return [x + width / 2 + offset, y + height];
    case 'left':
      return [x, y + height / 2 + offset];
    default:
      return [x + width, y + height / 2 + offset];
  }
}

/**
 * A route is a list of points. The first and the last may name a side of a
 * component (`ptah:top`, `ptah:top:-40`); an intermediate point may give only
 * one coordinate (`{ x: 206 }`), and takes the other from the point before it,
 * which keeps an orthogonal route to the numbers that matter.
 */
export function routePoints(route, layout) {
  const points = [];
  for (const spec of route) {
    if (spec && typeof spec === 'object' && !Array.isArray(spec)) {
      const previous = points.at(-1);
      if (!previous) throw new Error('a route cannot start with a partial point');
      points.push([spec.x ?? previous[0], spec.y ?? previous[1]]);
    } else {
      points.push(anchor(spec, layout));
    }
  }
  // A partial point may also take its missing coordinate from the point after
  // it: `{ y: 136 }` right before `ptah:top` means "turn at y=136, above the
  // anchor". Resolve those in a second pass, now that the end is known.
  for (let index = 1; index < route.length - 1; index += 1) {
    const spec = route[index];
    if (spec && typeof spec === 'object' && !Array.isArray(spec) && spec.align === 'next') {
      const next = points[index + 1];
      points[index] = [spec.x ?? next[0], spec.y ?? next[1]];
    }
  }
  return points;
}

const round = (value) => Math.round(value * 10) / 10;

const MONO = "'IBM Plex Mono', ui-monospace, monospace";

// The line pattern of each kind, and of a relationship that is not implemented.
// The written label carries the same information, so the patterns only make
// the drawing quicker to read. The legend draws its samples from this table.
export const LINE_PATTERNS = {
  runs: { width: 1.5 },
  installs: { width: 1.5, dash: '8 4' },
  checks: { width: 1.75, dash: '1.5 4', cap: 'round' },
  links: { width: 1.5, dash: '10 3 2 3' },
  'not-implemented': { width: 1.25, dash: '4 6' },
};

function lineAttributes(relationship) {
  const pattern = LINE_PATTERNS[relationship.state === 'not-implemented' ? 'not-implemented' : relationship.kind];
  const parts = ['fill="none"', `stroke-width="${pattern.width}"`];
  if (pattern.dash) parts.push(`stroke-dasharray="${pattern.dash}"`);
  if (pattern.cap) parts.push(`stroke-linecap="${pattern.cap}"`);
  return parts.join(' ');
}

/** Straight segments with a small radius at every turn. */
function pathData(points, radius = 6) {
  let data = `M${round(points[0][0])} ${round(points[0][1])}`;
  for (let index = 1; index < points.length - 1; index += 1) {
    const [px, py] = points[index - 1];
    const [cx, cy] = points[index];
    const [nx, ny] = points[index + 1];
    const inLength = Math.hypot(cx - px, cy - py);
    const outLength = Math.hypot(nx - cx, ny - cy);
    const r = Math.min(radius, inLength / 2, outLength / 2);
    const ax = cx - ((cx - px) / inLength) * r;
    const ay = cy - ((cy - py) / inLength) * r;
    const bx = cx + ((nx - cx) / outLength) * r;
    const by = cy + ((ny - cy) / outLength) * r;
    data += ` L${round(ax)} ${round(ay)} Q${round(cx)} ${round(cy)} ${round(bx)} ${round(by)}`;
  }
  const last = points.at(-1);
  return `${data} L${round(last[0])} ${round(last[1])}`;
}

/** The arrowhead as a polygon, so it needs no marker and takes the line's class. */
function arrowhead(points, size = 7) {
  const [tx, ty] = points.at(-1);
  const [fx, fy] = points.at(-2);
  const length = Math.hypot(tx - fx, ty - fy) || 1;
  const ux = (tx - fx) / length;
  const uy = (ty - fy) / length;
  const bx = tx - ux * size;
  const by = ty - uy * size;
  const half = size * 0.55;
  return [
    [tx, ty],
    [bx - uy * half, by + ux * half],
    [bx + uy * half, by - ux * half],
  ]
    .map(([x, y]) => `${round(x)},${round(y)}`)
    .join(' ');
}

/** Breaks a label into lines of at most `width` characters, at spaces. */
export function wrapLabel(text, width) {
  const lines = [];
  let line = '';
  for (const word of text.split(' ')) {
    if (line && `${line} ${word}`.length > width) {
      lines.push(line);
      line = word;
    } else {
      line = line ? `${line} ${word}` : word;
    }
  }
  if (line) lines.push(line);
  return lines;
}

// ---------------------------------------------------------------- drawing

const escape = (value) =>
  String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');

// The two themes the full-size file embeds. The values are the site's tokens
// (src/styles/ptah/tokens.css) in a form an SVG viewer without the site's
// stylesheet resolves.
const THEMES = {
  light: {
    background: '#f3f4f4',
    surface: '#ffffff',
    ink: '#111417',
    muted: '#474d54',
    faint: '#666d76',
    rule: '#9aa1a8',
    hairline: '#d9dcdf',
  },
  dark: {
    background: '#1d1916',
    surface: '#26211d',
    ink: '#ede7de',
    muted: '#d8d1c6',
    faint: '#b6ada0',
    rule: '#8d8377',
    hairline: '#5c554d',
  },
};

function standaloneStyle(theme) {
  const c = THEMES[theme];
  return `
    svg { font-family: 'Instrument Sans', ui-sans-serif, system-ui, sans-serif; }
    .eco-background { fill: ${c.background}; }
    .eco-frame { fill: none; stroke: ${c.hairline}; }
    .eco-frame__label { fill: ${c.faint}; }
    .eco-node__box { fill: ${c.surface}; stroke: ${c.rule}; }
    .eco-node--entry .eco-node__box, .eco-node--infrastructure .eco-node__box { fill: ${c.background}; stroke-dasharray: 4 3; }
    .eco-node--core .eco-node__box { stroke: ${c.ink}; stroke-width: 1.5; }
    .eco-node__name { fill: ${c.ink}; }
    .eco-node__role { fill: ${c.muted}; }
    .eco-node__badge rect { fill: ${c.surface}; stroke: ${c.rule}; }
    .eco-node__badge text { fill: ${c.ink}; }
    .eco-node__focus { display: none; }
    .eco-edge__line { stroke: ${c.faint}; }
    .eco-edge__head { fill: ${c.faint}; stroke: ${c.faint}; }
    .eco-edge--not-implemented .eco-edge__head { fill: ${c.background}; }
    .eco-edge--experimental .eco-edge__head { fill: ${c.background}; }
    .eco-edge__label { fill: ${c.muted}; stroke: ${c.background}; }
  `;
}

/**
 * The map as an `<svg>` element. `theme` is `site` for the inline map, whose
 * colors come from the page's stylesheet, or `light` / `dark` for a
 * standalone file.
 */
export function renderEcosystemSvg({ theme = 'site', idPrefix = 'ptah-ecosystem', model = ecosystemModel(), layout = wideLayout } = {}) {
  const titleId = `${idPrefix}-title`;
  const descId = `${idPrefix}-desc`;
  const { width, height } = layout;
  const out = [];
  out.push(
    `<svg xmlns="http://www.w3.org/2000/svg" class="eco-svg" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="img" aria-labelledby="${titleId} ${descId}">`,
  );
  out.push(`<title id="${titleId}">Ptah ecosystem map</title>`);
  out.push(`<desc id="${descId}">${escape(describe(model))}</desc>`);
  if (theme !== 'site') out.push(`<style>${standaloneStyle(theme)}</style>`);
  out.push(`<rect class="eco-background" x="0" y="0" width="${width}" height="${height}"/>`);

  out.push('<g class="eco-frames">');
  for (const group of model.groups) {
    const frame = layout.frames[group.id];
    if (!frame) continue;
    const [x, y, w, h] = frame;
    out.push(
      `<g class="eco-frame-group" data-group="${group.id}"><rect class="eco-frame" x="${x}" y="${y}" width="${w}" height="${h}" rx="2" fill="none" stroke-width="1"/>` +
        `<text class="eco-frame__label" x="${x + 10}" y="${y + 16}" font-size="10" font-weight="500" letter-spacing="0.8" font-family="${MONO}">${escape(group.label.toUpperCase())}</text></g>`,
    );
  }
  out.push('</g>');

  const edges = model.relationships.map((relationship) => {
    const edge = layout.edges[relationship.id];
    return { relationship, edge, points: routePoints(edge.route, layout) };
  });

  out.push('<g class="eco-edges">');
  for (const { relationship, points } of edges) {
    const classes = `eco-edge eco-edge--${relationship.kind} eco-edge--${relationship.state}`;
    out.push(
      `<g class="${classes}" data-eco-edge="${relationship.id}" data-from="${relationship.from}" data-to="${relationship.to}">` +
        `<path class="eco-edge__line" d="${pathData(points)}" ${lineAttributes(relationship)}/>` +
        `<polygon class="eco-edge__head" points="${arrowhead(points)}" stroke-width="1.25" stroke-linejoin="round"/></g>`,
    );
  }
  out.push('</g>');

  out.push('<g class="eco-nodes">');
  for (const component of model.components) {
    const [x, y, w, h] = layout.nodes[component.id];
    const core = component.id === 'ptah';
    const classes = ['eco-node', `eco-node--${component.kind}`, core ? 'eco-node--core' : ''].filter(Boolean).join(' ');
    const radius = component.kind === 'entry' ? h / 2 : 2;
    const inset = component.kind === 'entry' ? 20 : 12;
    const nameY = y + h / 2 - (core ? 4 : 3);
    const roleY = nameY + (core ? 19 : 16);
    const textX = core ? x + w / 2 : x + inset;
    const anchorAttribute = core ? ' text-anchor="middle"' : '';
    let badge = '';
    if (component.badge) {
      const label = component.badge.toUpperCase();
      const badgeWidth = Math.round(label.length * 6.1 + 10);
      const bx = x + w - badgeWidth - 8;
      const by = y - 8;
      badge =
        `<g class="eco-node__badge"><rect x="${bx}" y="${by}" width="${badgeWidth}" height="15" rx="2" stroke-width="1"/>` +
        `<text x="${bx + badgeWidth / 2}" y="${by + 11}" text-anchor="middle" font-size="9" font-weight="500" letter-spacing="0.6" font-family="${MONO}">${escape(label)}</text></g>`;
    }
    out.push(
      `<g class="${classes}" data-eco-node="${component.id}" data-eco-description="${escape(nodeDescription(component, model))}">` +
        `<rect class="eco-node__focus" x="${x - 4}" y="${y - 4}" width="${w + 8}" height="${h + 8}" rx="${component.kind === 'entry' ? (h + 8) / 2 : 4}" fill="none" stroke-width="2"/>` +
        `<rect class="eco-node__box" x="${x}" y="${y}" width="${w}" height="${h}" rx="${radius}" stroke-width="1"${component.kind === 'repository' ? '' : ' stroke-dasharray="4 3"'}/>` +
        `<text class="eco-node__name" x="${textX}" y="${nameY}"${anchorAttribute} font-size="${core ? 17 : 14}" font-weight="600">${escape(component.name)}</text>` +
        `<text class="eco-node__role" x="${textX}" y="${roleY}"${anchorAttribute} font-size="12">${escape(component.role)}</text>` +
        `${badge}</g>`,
    );
  }
  out.push('</g>');

  out.push('<g class="eco-labels">');
  for (const { relationship, edge, points } of edges) {
    const [lx, ly] = edge.label ?? points[Math.floor(points.length / 2)];
    const lines = wrapLabel(labelText(relationship, model), edge.wrap ?? 20);
    const align = edge.align ?? 'middle';
    const top = ly - ((lines.length - 1) * 13) / 2 + 4;
    const anchorAttribute = align === 'middle' ? 'middle' : align;
    const tspans = lines.map((line, index) => `<tspan x="${lx}" y="${round(top + index * 13)}">${escape(line)}</tspan>`).join('');
    out.push(
      `<text class="eco-edge__label eco-edge__label--${relationship.state}" data-eco-edge-label="${relationship.id}" text-anchor="${anchorAttribute}" font-size="11.5" stroke-width="5" stroke-linejoin="round" paint-order="stroke">${tspans}</text>`,
    );
  }
  out.push('</g>');

  out.push('</svg>');
  return `${out.join('\n')}\n`;
}

/** What the map's label says: the relationship, and its state when it is not available. */
export function labelText(relationship, model) {
  const state = relationship.state ?? 'available';
  if (state === 'available') return relationship.label;
  return `${relationship.label} (${model.states.get(state).label.toLowerCase()})`;
}

/**
 * What the map says about a component beyond the text on its node, as a sentence
 * for its accessible description. The node's name is that text.
 */
export function nodeDescription(component, model) {
  if (component.kind === 'entry') return 'Entry point, not a repository.';
  if (component.kind === 'infrastructure') return 'Infrastructure, not a repository.';
  return `Status: ${model.statuses.get(component.status).label}.`;
}

/** The SVG's description: the structure in one paragraph, for a reader who cannot see it. */
export function describe(model) {
  const groups = model.groups
    .filter((group) => group.components.length > 0)
    .map((group) => `${group.label}: ${group.components.map(({ name }) => name).join(', ')}.`)
    .join(' ');
  return `The Ptah ecosystem grouped by role, with ptah at the center. ${groups} The repository table and the relationship list below the map give every component and connection as text.`;
}

// ------------------------------------------------------------- text views

/** The declaration's prose as HTML: escaped, with `code` spans marked up. */
export function inlineMarkup(text) {
  return String(text)
    .split('`')
    .map((part, index) => (index % 2 === 1 ? `<code>${escape(part)}</code>` : escape(part)))
    .join('');
}

const componentMarkup = (component) =>
  component.kind === 'repository' ? `<code>${escape(component.name)}</code>` : escape(component.name);

/** One relationship as text: `from → to: label`, as HTML. */
export function relationshipSentence(relationship, model) {
  const from = componentMarkup(model.byId.get(relationship.from));
  const to = componentMarkup(model.byId.get(relationship.to));
  return `${from} → ${to}: ${escape(labelText(relationship, model))}`;
}

// The slug Starlight gives a heading, for the headings the declaration links
// to. Astro renders the page after this module runs, so the check reads the
// page's source rather than the page.
const headingSlug = (heading) =>
  heading
    .toLowerCase()
    .replaceAll(/[`*_]/g, '')
    .replaceAll(/[^a-z0-9 -]/g, '')
    .trim()
    .replaceAll(' ', '-');

/**
 * Resolves a declared link for a page at `pathname`. An address is returned as
 * written. A route on this site is returned relative to the page, so every
 * documentation version keeps its own copy of the link, and a route or heading
 * no page has fails the build: the links here are outside what
 * scripts/check-links.mjs reads.
 */
export function docLinkResolver(entries, pathname, base) {
  const normalizedBase = base.endsWith('/') ? base : `${base}/`;
  const route = pathname.startsWith(normalizedBase) ? pathname.slice(normalizedBase.length) : pathname.replace(/^\//, '');
  const depth = route.split('/').filter(Boolean).length;
  const prefix = '../'.repeat(depth);
  const byId = new Map(entries.map((entry) => [entry.id, entry]));
  const resolve = (link) => {
    if (link.href) return link.href;
    const id = link.doc.replace(/\/$/, '');
    const entry = byId.get(id);
    if (!entry) throw new Error(`ecosystem link ${link.label}: no page has the route ${link.doc}`);
    if (link.anchor) {
      const headings = [...(entry.body ?? '').matchAll(/^#{2,6}\s+(.+?)\s*$/gm)].map((match) => headingSlug(match[1]));
      if (!headings.includes(link.anchor)) throw new Error(`ecosystem link ${link.label}: ${link.doc} has no heading #${link.anchor}`);
    }
    return `${prefix}${link.doc}${link.anchor ? `#${link.anchor}` : ''}`;
  };
  resolve.prefix = prefix;
  return resolve;
}
