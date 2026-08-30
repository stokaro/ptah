import { execFileSync } from 'node:child_process';
import { isEdgeVersion, sourceRefForVersion } from './source-ref.mjs';

const defaultRepositoryUrl = 'https://github.com/stokaro/ptah';
const fullCommit = /^[0-9a-f]{40}$/;
let cachedCheckoutCommit;
let checkoutCommitRead = false;

function requiredString(value, label) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value.trim();
}

function repositoryFileUrl(repositoryUrl, operation, ref, sourcePath) {
  const encodedPath = sourcePath.split('/').map(encodeURIComponent).join('/');
  return `${repositoryUrl}/${operation}/${encodeURIComponent(ref)}/${encodedPath}`;
}

/** Resolve the exact commit used by a build, with `master` as the UI fallback. */
export function sourceCommitForBuild(supplied = process.env.DOCS_SOURCE_COMMIT, cwd = process.cwd()) {
  const explicit = supplied?.trim();
  if (explicit) {
    if (!fullCommit.test(explicit)) throw new Error('DOCS_SOURCE_COMMIT must be a full lowercase Git SHA');
    return explicit;
  }
  const cacheable = cwd === process.cwd();
  if (cacheable && checkoutCommitRead) return cachedCheckoutCommit;
  try {
    const commit = execFileSync('git', ['rev-parse', 'HEAD'], { cwd, encoding: 'utf8' }).trim();
    const value = fullCommit.test(commit) ? commit : undefined;
    if (cacheable) {
      cachedCheckoutCommit = value;
      checkoutCommitRead = true;
    }
    return value;
  } catch {
    if (cacheable) checkoutCommitRead = true;
    return undefined;
  }
}

/**
 * Resolve the source represented by one rendered documentation page and the
 * latest source a contributor should edit.
 */
export function resolveSourceContext({
  documentationVersion,
  sourceCommit,
  renderedSourcePath,
  generated = false,
  generator,
  editSource,
}) {
  const version = requiredString(documentationVersion, 'documentation version');
  const renderedPath = requiredString(renderedSourcePath, 'rendered source path');
  const commit = sourceCommit?.trim() || undefined;
  if (commit && !fullCommit.test(commit)) {
    throw new Error('source commit must be a full lowercase Git SHA');
  }
  if (typeof generated !== 'boolean') throw new Error('generated must be a boolean');

  const generatorPath = generator?.trim() || undefined;
  const declaredEditSource = editSource?.trim() || undefined;
  if (generated && !generatorPath) throw new Error('generated pages must declare their generator');
  if (generated && !declaredEditSource) throw new Error('generated pages must declare editSource');

  return {
    documentationVersion: version,
    viewRef: isEdgeVersion(version) ? (commit ?? 'master') : sourceRefForVersion(version),
    editRef: 'master',
    renderedSourcePath: renderedPath,
    editSourcePath: generated ? declaredEditSource : renderedPath,
    generated,
    generator: generatorPath,
    sourceCommit: commit,
  };
}

/** Build the visible page actions and the reproducible issue-report context. */
export function pageActionsForSource(context, {
  pageUrl,
  title,
  repositoryUrl = defaultRepositoryUrl,
} = {}) {
  const renderedPageUrl = requiredString(pageUrl, 'rendered page URL');
  const pageTitle = requiredString(title, 'page title');
  const edge = isEdgeVersion(context.documentationVersion);
  const sourceUrl = repositoryFileUrl(
    repositoryUrl,
    'blob',
    context.viewRef,
    context.renderedSourcePath,
  );
  const editUrl = repositoryFileUrl(
    repositoryUrl,
    'edit',
    context.editRef,
    context.editSourcePath,
  );

  const source = {
    kind: 'source',
    url: sourceUrl,
    label: context.generated
      ? `View generated source${edge ? '' : ` at ${context.documentationVersion}`}`
      : `View source${edge ? '' : ` at ${context.documentationVersion}`}`,
    description: edge
      ? 'Open the source used for this build'
      : 'Open the source for this released documentation',
  };
  const edit = {
    kind: 'edit',
    url: editUrl,
    label: context.generated
      ? `Edit generator source${edge ? '' : ' in latest documentation'}`
      : edge ? 'Edit this page' : 'Edit latest documentation',
    description: context.generated
      ? 'Propose a change to the editable generator source'
      : edge ? 'Propose a change on GitHub' : 'Propose a change to the latest documentation',
  };

  const reportLines = [
    `Page: ${renderedPageUrl}`,
    `Documentation version: ${context.documentationVersion}`,
    `Source ref: ${context.viewRef}`,
    ...(context.sourceCommit ? [`Source commit: ${context.sourceCommit}`] : []),
    `Rendered source: ${context.renderedSourcePath}`,
    ...(context.generator ? [`Generator: ${context.generator}`] : []),
    ...(context.generated ? [`Edit source: ${context.editSourcePath}`] : []),
    '',
    'What needs to change?',
    '',
  ];
  const reportParams = new URLSearchParams({
    title: `docs: ${pageTitle}`,
    body: reportLines.join('\n'),
  });
  const report = {
    kind: 'report',
    url: `${repositoryUrl}/issues/new?${reportParams}`,
    label: 'Report a documentation issue',
    description: 'Open a prefilled GitHub issue',
  };

  return {
    markdownLabel: 'Copy page as Markdown',
    actions: edge ? [edit, source, report] : [source, edit, report],
    sourceUrl,
    editUrl,
    reportUrl: report.url,
  };
}
