// The YDB walkthrough as data: the components, the objects, the workflow
// steps, the scenarios and the connections of the interactive view on
// extend/write-a-provider/ydb-walkthrough-explore.mdx. One declaration feeds
// both views of it: the interactive figure (src/components/YdbWalkthrough.astro)
// and the text equivalent that scripts/build-ydb-walkthrough.mjs writes into
// the page, which a reader without JavaScript and the page's Markdown copy
// both read. A second copy of any of these facts is how the two views would
// come to disagree, so none exists.
//
// It is plain JavaScript with no imports, so a Node script reads it the same
// way Astro does. walkthroughModel() validates it: a step, a scenario result
// or a connection that names something not declared here fails the build.
//
// The facts come from the walkthrough the run page performs and from the tests
// the scenario page cites. A scenario result states what a test measured; the
// evidence link names the test.

/** The four components a reader meets, each in one layer. */
export const components = [
  {
    id: 'core',
    name: 'Ptah Core',
    layer: 'Host: the ptah binary',
    responsibility: 'Loads the declaration, compares common tables, routes provider models to their owners, orders the whole plan, prints and applies it.',
    understands: 'Common tables, identities and coverage. A provider payload only as bytes.',
    doesNotOwn: 'What a transfer means, or whether its source will exist after the plan.',
  },
  {
    id: 'contract',
    name: 'Contract and SDK',
    layer: 'Boundary between Core and the provider',
    responsibility: 'Defines the messages and features both sides agree on. The SDK checks each request and answer against them.',
    understands: 'Message shapes, identities, representations and coverage records.',
    doesNotOwn: 'YDB meaning: a well-formed request can still describe a plan YDB would refuse.',
  },
  {
    id: 'provider',
    name: 'YDB provider',
    layer: 'Provider: the ptah-plugin-ydb executable',
    responsibility: 'Reads the YDB catalog, compares the models it owns, proposes plan steps, renders and runs YQL.',
    understands: 'YDB semantics: changefeeds, topics, transfers and what each one needs.',
    doesNotOwn: 'Common tables, the order of the whole plan, and migration history policy.',
  },
  {
    id: 'ydb',
    name: 'YDB server',
    layer: 'The database',
    responsibility: 'Stores the tables, keeps the changefeed and its topic, and runs the transfer after Ptah exits.',
    understands: 'YQL statements and its own catalog.',
    doesNotOwn: 'The declaration, the comparison and the plan.',
  },
];

/** The four YDB objects of the example, in the order the data flows. */
export const objects = [
  {
    id: 'orders',
    name: 'orders',
    ydb: 'Row table shop/orders',
    ptah: 'A common table; its YDB settings are a row-table-properties facet',
    comparedBy: 'Ptah Core for the table; ydb-row-table-properties for the facet',
    request: 'declaredRelations, as a declared table',
    plan: 'CREATE TABLE `shop/orders`',
  },
  {
    id: 'feed',
    name: 'orders/feed',
    ydb: 'Changefeed of orders, with its topic shop/orders/feed',
    ptah: 'One entry in the table-changefeeds facet of orders',
    comparedBy: 'ydb-row-table-properties',
    request: 'declaredFacets, as a changefeed orders declares; currentContext, once a read finds it',
    plan: 'ALTER TABLE `shop/orders` ADD CHANGEFEED `feed`, then ALTER TOPIC `shop/orders/feed` ADD CONSUMER `archiver`',
  },
  {
    id: 'transfer',
    name: 'ingest_orders',
    ydb: 'Transfer shop/ingest_orders',
    ptah: 'A provider object of kind transfer',
    comparedBy: 'ydb-transfers',
    request: 'desired, as the declared transfer; current, once it exists',
    plan: 'CREATE TRANSFER `shop/ingest_orders` FROM `shop/orders/feed` TO `shop/orders_archive`',
  },
  {
    id: 'archive',
    name: 'orders_archive',
    ydb: 'Row table shop/orders_archive',
    ptah: 'A common table',
    comparedBy: 'Ptah Core',
    request: 'declaredRelations, as a declared table',
    plan: 'CREATE TABLE `shop/orders_archive`',
  },
];

/**
 * What a connection means. The kind is written next to every connection, so
 * nothing depends on telling line styles or colors apart.
 */
export const connectionKinds = [
  { id: 'data', label: 'Data flow in YDB', description: 'Data YDB moves at run time, after Ptah exits.' },
  { id: 'call', label: 'Call', description: 'A contract call between Core and the provider, or a YDB API call the provider makes.' },
  { id: 'dependency', label: 'Model dependency', description: 'One object needs another for the plan to be valid.' },
  { id: 'order', label: 'Execution order', description: 'One statement has to run before another.' },
];

/** The connections. `from` and `to` name a component or an object. */
export const connections = [
  { id: 'flow-changes', kind: 'data', from: 'orders', to: 'feed', label: 'row changes, written by YDB' },
  { id: 'flow-messages', kind: 'data', from: 'feed', to: 'transfer', label: 'messages, read by the transfer' },
  { id: 'flow-rows', kind: 'data', from: 'transfer', to: 'archive', label: 'rows, written by YDB' },
  { id: 'call-decode', kind: 'call', from: 'core', to: 'provider', label: 'DecodeSource and NormalizeValues' },
  { id: 'call-describe', kind: 'call', from: 'core', to: 'provider', label: 'Describe and Open' },
  { id: 'call-inspect', kind: 'call', from: 'core', to: 'provider', label: 'Inspect' },
  { id: 'call-compare', kind: 'call', from: 'core', to: 'provider', label: 'CompareModels' },
  { id: 'call-plan', kind: 'call', from: 'core', to: 'provider', label: 'PlanFeatures' },
  { id: 'call-execute', kind: 'call', from: 'core', to: 'provider', label: 'Render and Execute' },
  { id: 'api-catalog', kind: 'call', from: 'provider', to: 'ydb', label: 'catalog reads' },
  { id: 'api-statements', kind: 'call', from: 'provider', to: 'ydb', label: 'YQL statements' },
  { id: 'needs-source', kind: 'dependency', from: 'transfer', to: 'feed', label: 'reads it; the transfer names its topic as a required object' },
  { id: 'needs-target', kind: 'dependency', from: 'transfer', to: 'archive', label: 'writes it; it has to be a declared table' },
  { id: 'needs-table', kind: 'dependency', from: 'feed', to: 'orders', label: 'is a facet of it and exists only with it' },
  { id: 'order-feed', kind: 'order', from: 'orders', to: 'feed', label: 'the table is created before its changefeed' },
  { id: 'order-transfer', kind: 'order', from: 'feed', to: 'transfer', label: 'the changefeed is created before the transfer' },
  { id: 'order-target', kind: 'order', from: 'archive', to: 'transfer', label: 'the target table is created before the transfer' },
];

/**
 * The workflow, in order. `active` names the components and `connections` the
 * calls a step uses; `result` is what the step produces in a normal creation.
 */
export const steps = [
  {
    id: 'declaration',
    label: 'Declaration',
    active: ['core', 'contract', 'provider'],
    connections: ['call-decode'],
    explanation: 'Core loads schema.yaml. The provider decodes the YDB extensions, the changefeed and the transfer, and the SDK checks their values.',
    result: 'The declared tables in Core\'s common model, and normalized values of the changefeed set and the transfer.',
  },
  {
    id: 'selection',
    label: 'Selection',
    active: ['core', 'contract', 'provider'],
    connections: ['call-describe'],
    explanation: 'Core starts the provider and asks what it serves. Both agree on features, such as models.current-context, and the provider opens a session.',
    result: 'A session on the database and the list of features both sides negotiated.',
  },
  {
    id: 'reading',
    label: 'Reading',
    active: ['core', 'provider', 'ydb'],
    connections: ['call-inspect', 'api-catalog'],
    explanation: 'The provider reads the selected directory, shop, and says what it established about each kind.',
    result: 'Nothing in shop, and coverage that says shop was read completely.',
  },
  {
    id: 'comparison',
    label: 'Comparison',
    active: ['core', 'contract', 'provider'],
    connections: ['call-compare'],
    explanation: 'Core compares the common tables and sends each owner a request. ydb-transfers receives the declared transfer with the facts it needs about other owners.',
    result: 'A request with both tables in declaredRelations and the changefeed in declaredFacets.',
  },
  {
    id: 'decision',
    label: 'Changes or refusal',
    active: ['provider'],
    connections: ['call-compare'],
    explanation: 'Each owner returns changes or refuses one and says why. ydb-transfers checks the transfer\'s source and target first.',
    result: 'Two new tables from Core, a new changefeed from ydb-row-table-properties and a new transfer from ydb-transfers.',
  },
  {
    id: 'planning',
    label: 'Planning',
    active: ['core', 'contract', 'provider'],
    connections: ['call-plan'],
    explanation: 'The provider turns its changes into steps and says which steps depend on which. Core checks them and orders the whole plan.',
    result: 'Four operations, the transfer last, after its changefeed and both tables.',
  },
  {
    id: 'execution',
    label: 'Review and execution',
    active: ['core', 'provider', 'ydb'],
    connections: ['call-execute', 'api-statements'],
    explanation: 'Core prints the plan. On apply, the provider renders YQL and runs it, and YDB starts the transfer.',
    result: 'The statements run, and a row written to orders reaches orders_archive.',
  },
  {
    id: 'reinspection',
    label: 'Reinspection',
    active: ['core', 'provider', 'ydb'],
    connections: ['call-inspect', 'call-compare'],
    explanation: 'The next run reads and compares again.',
    result: 'Schema is synced, no changes to be made.',
  },
];

const notReached = 'Not reached: the run stops at the refusal.';

/**
 * The scenarios. `results` overrides the result of a step; a step it does not
 * name produces the same result as in a normal creation.
 */
export const scenarios = [
  {
    id: 'create',
    label: 'Normal creation',
    fact: 'Every object is declared, and shop is empty.',
    decidedBy: 'Each owner reports its own changes; Core orders them.',
    outcome: 'Four operations; the transfer is created last.',
    evidence: { test: 'TestHostCreatesATransferAfterItsChangefeed_HappyPath', file: 'integration/host_transfer_order_test.go' },
    results: {},
  },
  {
    id: 'noop',
    label: 'Valid no-op',
    fact: 'Every object is declared, and the database already holds all four as declared.',
    decidedBy: 'ydb-transfers runs its dependency checks, and they pass.',
    outcome: 'Schema is synced, no changes to be made.',
    evidence: { test: 'TestHostCreatesATransferAfterItsChangefeed_HappyPath', file: 'integration/host_transfer_order_test.go' },
    results: {
      reading: 'All four objects, as the read found them, and complete coverage of shop.',
      comparison: 'A request with the transfer in desired and in current, and the changefeed set of orders in currentContext.',
      decision: 'No change: the source and the target are declared, and the transfer matches its declaration.',
      planning: 'Nothing to plan.',
      execution: 'Nothing runs: Schema is synced, no changes to be made.',
    },
  },
  {
    id: 'missing',
    label: 'Missing source',
    fact: 'The schema leaves out the changefeed feed of orders, and the database has none.',
    decidedBy: 'ydb-transfers, before anything is planned.',
    outcome: 'Refused: the transfer reads a topic the plan leaves absent.',
    evidence: { test: 'TestHostRefusesATransferOfAnUndeclaredChangefeed_FailurePath', file: 'integration/host_replication_test.go' },
    results: {
      comparison: 'A request in which declaredFacets holds no changefeed of orders, and currentContext holds none.',
      decision: 'Refused: it reads topic shop/orders/feed, which the schema declares neither as a topic nor as a changefeed, so the plan leaves no such topic.',
      planning: notReached,
      execution: notReached,
      reinspection: notReached,
    },
  },
  {
    id: 'recorded',
    label: 'Unrepresentable source, current context',
    fact: 'The tables exist, and orders holds a changefeed whose consumer names a codec Ptah does not declare. The schema leaves the changefeed out. The host and the provider negotiate models.current-context.',
    decidedBy: 'ydb-transfers, from the current context.',
    outcome: 'The transfer is created; the plan neither drops nor changes the changefeed.',
    evidence: { test: 'TestHostCreatesATransferOfAChangefeedItCannotDescribe_HappyPath', file: 'integration/host_transfer_recorded_source_test.go' },
    results: {
      reading: 'Both tables, and the changefeed set of orders recorded as unrepresentable, with no value.',
      comparison: 'A request whose currentContext carries that coverage record of orders.',
      decision: 'One new transfer. Its source stays after the plan, because the plan does not drop what the read could not describe.',
      planning: 'One operation: the transfer.',
      execution: 'CREATE TRANSFER runs. Nothing checked the changefeed\'s definition.',
      reinspection: 'Nothing to plan for the transfer.',
    },
  },
  {
    id: 'no-context',
    label: 'Unrepresentable source, no current context',
    fact: 'The same database and schema, with a host or a provider that does not negotiate models.current-context, such as Ptah v0.24.1 or YDB plugin v0.7.0.',
    decidedBy: 'ydb-transfers, without the read\'s facts about changefeeds.',
    outcome: 'Refused as a transfer of a missing topic.',
    evidence: { test: 'TestTransferComparisonAcceptsARecordedSource_FailurePath', file: 'internal/backend/replication_test.go' },
    results: {
      selection: 'A session, without models.current-context among the negotiated features.',
      reading: 'Both tables, and the changefeed set of orders recorded as unrepresentable, with no value.',
      comparison: 'A request without currentContext.',
      decision: 'Refused: it reads topic shop/orders/feed, which the schema declares neither as a topic nor as a changefeed.',
      planning: notReached,
      execution: notReached,
      reinspection: notReached,
    },
  },
];

/** The plugin commit the evidence links name, so a link does not move. */
export const evidenceCommit = '24d982d5c88aaa0e474e40a840dc5d4074a9b54e';

/** The address of a scenario's evidence. */
export function evidenceHref(evidence) {
  return `https://github.com/stokaro/ptah-ydb-plugin/blob/${evidenceCommit}/${evidence.file}`;
}

/** The result of a step under a scenario. */
export function stepResult(scenario, step) {
  return scenario.results[step.id] ?? step.result;
}

/** A component or an object by id. */
function nodeName(id) {
  return (components.find((node) => node.id === id) ?? objects.find((node) => node.id === id)).name;
}

/** The sentence a connection reads as. */
export function connectionText(connection) {
  return `${nodeName(connection.from)} → ${nodeName(connection.to)}: ${connection.label}`;
}

/**
 * The validated model. Throws when a reference names nothing declared here, so
 * a broken walkthrough fails the build rather than rendering.
 */
export function walkthroughModel() {
  const problems = [];
  const nodes = new Set([...components, ...objects].map(({ id }) => id));
  const kinds = new Set(connectionKinds.map(({ id }) => id));
  const connectionIds = new Set(connections.map(({ id }) => id));
  const stepIds = new Set(steps.map(({ id }) => id));
  const unique = (list, what) => {
    const seen = new Set();
    for (const { id } of list) {
      if (seen.has(id)) problems.push(`${what} ${id} is declared twice`);
      seen.add(id);
    }
  };
  unique([...components, ...objects], 'node');
  unique(connections, 'connection');
  unique(steps, 'step');
  unique(scenarios, 'scenario');
  for (const connection of connections) {
    if (!kinds.has(connection.kind)) problems.push(`connection ${connection.id} has unknown kind ${connection.kind}`);
    for (const end of [connection.from, connection.to]) {
      if (!nodes.has(end)) problems.push(`connection ${connection.id} names unknown node ${end}`);
    }
  }
  for (const step of steps) {
    for (const id of step.active) {
      if (!components.some((component) => component.id === id)) problems.push(`step ${step.id} names unknown component ${id}`);
    }
    for (const id of step.connections) {
      if (!connectionIds.has(id)) problems.push(`step ${step.id} names unknown connection ${id}`);
    }
  }
  for (const scenario of scenarios) {
    for (const id of Object.keys(scenario.results)) {
      if (!stepIds.has(id)) problems.push(`scenario ${scenario.id} has a result for unknown step ${id}`);
    }
  }
  if (problems.length > 0) throw new Error(`ydb-walkthrough.mjs: ${problems.join('; ')}`);
  return { components, objects, connectionKinds, connections, steps, scenarios };
}

const cell = (text) => text.replaceAll('|', '\\|');
const row = (cells) => `| ${cells.map(cell).join(' | ')} |`;
const table = (head, rows) => [row(head), row(head.map(() => '---')), ...rows.map(row)].join('\n');

/**
 * The text equivalent of the interactive view, as Markdown: every fact the
 * figure shows, in tables. scripts/build-ydb-walkthrough.mjs writes it into
 * the page between its markers.
 */
export function walkthroughMarkdown() {
  const model = walkthroughModel();
  const name = (id) => nodeName(id);
  const sections = [
    '### Components',
    table(['Component', 'Responsible for', 'Understands', 'Does not own'],
      model.components.map((c) => [`${c.name} (${c.layer})`, c.responsibility, c.understands, c.doesNotOwn])),
    '### Objects',
    table(['Object', 'In YDB', 'In Ptah', 'Compared by'], model.objects.map((o) => [`\`${o.name}\``, o.ydb, o.ptah, o.comparedBy])),
    table(['Object', 'In the ydb-transfers request', 'Plan step'], model.objects.map((o) => [`\`${o.name}\``, o.request, o.plan])),
    '### Workflow steps',
    table(['Step', 'Who acts', 'What happens', 'Result in a normal creation'],
      model.steps.map((s, index) => [`${index + 1}. ${s.label}`, s.active.map(name).join(', '), s.explanation, s.result])),
    '### Scenarios',
    table(['Scenario', 'Changed fact', 'Decided by', 'Outcome'], model.scenarios.map((s) => [s.label, s.fact, s.decidedBy, s.outcome])),
    'Steps whose result differs from a normal creation:',
    table(['Scenario', 'Step', 'Result'], model.scenarios.flatMap((s) =>
      model.steps.filter((step) => s.results[step.id]).map((step) => [s.label, step.label, s.results[step.id]]))),
    'The test that measures each scenario:',
    table(['Scenario', 'Test'], model.scenarios.map((s) => [s.label, `[\`${s.evidence.test}\`](${evidenceHref(s.evidence)})`])),
    '### Connections',
    table(['Connection', 'Kind'], model.connections.map((c) => [connectionText(c), model.connectionKinds.find(({ id }) => id === c.kind).label])),
  ];
  return `${sections.join('\n\n')}\n`;
}
