#!/usr/bin/env node

import { appendFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { EDGE, compareReleases, isRelease, retainedReleases } from './lib/doc-versions.mjs';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = join(scriptDir, '..', '..', '..');
const fullCommit = /^[0-9a-f]{40}$/;

function requiredCommit(value, label) {
  if (typeof value !== 'string' || !fullCommit.test(value)) {
    throw new Error(`${label} must be a full lowercase Git SHA`);
  }
  return value;
}

function normalizedVersions(value, label) {
  const versions = Array.isArray(value) ? value : String(value ?? '').split(',');
  const normalized = versions.map((version) => version.trim()).filter(Boolean);
  if (!normalized.includes(EDGE)) throw new Error(`${label} must contain edge`);
  if (normalized.some((version) => version !== EDGE && !isRelease(version))) {
    throw new Error(`${label} contains an invalid documentation version`);
  }
  if (new Set(normalized).size !== normalized.length) throw new Error(`${label} contains a duplicate`);
  return normalized.sort();
}

function isSuperset(candidate, deployed) {
  const candidateSet = new Set(candidate);
  return deployed.every((version) => candidateSet.has(version));
}

function positiveInteger(value, label) {
  if (!Number.isInteger(value) || value < 1) throw new Error(`${label} must be a positive integer`);
  return value;
}

// A served version may leave only because the retention window moved past it.
// The candidate then holds exactly the window over every release either side
// knows: the newest `keep` of them. A candidate built before a newer tag
// existed misses that tag, and one whose historical build failed misses a
// version inside the window; neither equals the window, so both are refused.
function removalIsRetention(candidateSet, deployedSet, keep) {
  const window = retainedReleases([...candidateSet, ...deployedSet], keep);
  const candidateReleases = candidateSet.filter(isRelease).sort(compareReleases);
  return candidateReleases.join(',') === window.join(',');
}

function commitDecision({
  candidateCommit,
  deployedCommit,
  candidateSet,
  deployedSet,
  deployedIsAncestorOfCandidate,
  candidateIsAncestorOfDeployed,
  keep,
}) {
  const removed = deployedSet.filter((version) => !candidateSet.includes(version));
  if (removed.length && !removalIsRetention(candidateSet, deployedSet, keep)) {
    return {
      action: 'skip',
      reason: `candidate would remove ${removed.join(', ')}, which the site serves and the retention window of ${keep} still holds`,
    };
  }
  const added = candidateSet.filter((version) => !deployedSet.includes(version));
  if (candidateCommit === deployedCommit) {
    return added.length
      ? { action: 'deploy', reason: `candidate adds ${added.join(', ')} to the deployed edge source` }
      : { action: 'skip', reason: `candidate ${candidateCommit} is already served` };
  }
  if (candidateIsAncestorOfDeployed) {
    return { action: 'skip', reason: `candidate ${candidateCommit} is older than deployed ${deployedCommit}` };
  }
  if (!deployedIsAncestorOfCandidate) {
    return {
      action: 'fail',
      reason: `candidate ${candidateCommit} and deployed ${deployedCommit} have divergent histories`,
    };
  }
  return { action: 'deploy', reason: `candidate ${candidateCommit} advances deployed ${deployedCommit}` };
}

// `release` is the tag a tag run documents. That run succeeds only when the
// served site ends up listing it: a candidate that lacks it fails, and so does
// a skip over a site that does not carry it, because the release process reads
// a green tag run as a published release.
export function deploymentCandidateDecision({
  candidateCommit,
  deployedCommit,
  candidateVersions,
  deployedVersions,
  deployedIsAncestorOfCandidate,
  candidateIsAncestorOfDeployed,
  retainedReleases: keep,
  release,
}) {
  requiredCommit(candidateCommit, 'candidate commit');
  requiredCommit(deployedCommit, 'deployed commit');
  const candidateSet = normalizedVersions(candidateVersions, 'candidate versions');
  const deployedSet = normalizedVersions(deployedVersions, 'deployed versions');
  if (typeof deployedIsAncestorOfCandidate !== 'boolean') {
    throw new Error('deployedIsAncestorOfCandidate must be a boolean');
  }
  if (typeof candidateIsAncestorOfDeployed !== 'boolean') {
    throw new Error('candidateIsAncestorOfDeployed must be a boolean');
  }
  positiveInteger(keep, 'retained releases');
  if (release !== undefined && !isRelease(release)) throw new Error(`release ${release} is not a release version`);
  if (release !== undefined && !candidateSet.includes(release)) {
    return { action: 'fail', reason: `candidate does not carry ${release}, the release this run documents` };
  }
  const decision = commitDecision({
    candidateCommit,
    deployedCommit,
    candidateSet,
    deployedSet,
    deployedIsAncestorOfCandidate,
    candidateIsAncestorOfDeployed,
    keep,
  });
  if (release !== undefined && decision.action === 'skip' && !deployedSet.includes(release)) {
    return { action: 'fail', reason: `${decision.reason}, and the served site does not list ${release}` };
  }
  return decision;
}

function gitIsAncestor(older, newer) {
  const result = spawnSync('git', ['merge-base', '--is-ancestor', older, newer], {
    cwd: repositoryRoot,
    encoding: 'utf8',
  });
  if (result.status === 0) return true;
  if (result.status === 1) return false;
  throw new Error(`git merge-base failed: ${(result.stderr || result.stdout).trim()}`);
}

async function readPublicState(baseUrl, { attempts, delayMilliseconds }) {
  let lastProblem = 'not requested';
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const suffix = `candidate=${Date.now()}-${attempt}`;
      const infoResponse = await fetch(new URL(`build-info.json?${suffix}`, baseUrl), { cache: 'no-store' });
      const versionsResponse = await fetch(new URL(`../versions.json?${suffix}`, baseUrl), { cache: 'no-store' });
      if (!infoResponse.ok || !versionsResponse.ok) {
        lastProblem = `build-info HTTP ${infoResponse.status}; versions HTTP ${versionsResponse.status}`;
      } else {
        const info = await infoResponse.json();
        const versions = await versionsResponse.json();
        if (
          info.documentation_version !== 'edge' ||
          info.source_ref !== 'master' ||
          !fullCommit.test(info.source_commit)
        ) {
          lastProblem = 'public build-info.json has invalid edge provenance';
        } else {
          return {
            commit: info.source_commit,
            versions: normalizedVersions(
              versions.versions?.map(({ slug }) => slug),
              'public versions',
            ),
          };
        }
      }
    } catch (error) {
      lastProblem = error instanceof Error ? error.message : String(error);
    }
    if (attempt < attempts) await delay(delayMilliseconds);
  }
  throw new Error(`public deployment state was unavailable after ${attempts} attempts: ${lastProblem}`);
}

const DECISION_OPTIONS = ['--candidate-commit', '--candidate-versions', '--base-url', '--retained-releases'];
const WAIT_OPTIONS = ['--candidate-commit', '--candidate-versions', '--base-url'];

function parseArguments(arguments_) {
  if (arguments_.length === 1 && arguments_[0] === '--selftest') return { selftest: true };
  const wait = arguments_[0] === '--wait-for-deployment';
  const pairs = wait ? arguments_.slice(1) : arguments_;
  const required = wait ? WAIT_OPTIONS : DECISION_OPTIONS;
  const allowed = wait ? WAIT_OPTIONS : [...DECISION_OPTIONS, '--tag'];
  const values = new Map();
  for (let index = 0; index < pairs.length; index += 2) {
    const [name, value] = [pairs[index], pairs[index + 1]];
    if (!allowed.includes(name) || values.has(name) || value === undefined) values.set('invalid', true);
    values.set(name, value);
  }
  if (values.has('invalid') || required.some((name) => !values.get(name))) {
    throw new Error(
      'usage: node scripts/check-deployment-candidate.mjs --candidate-commit <full-sha> --candidate-versions <csv> --base-url <edge-url> --retained-releases <n> [--tag <tag>]\n' +
        '       node scripts/check-deployment-candidate.mjs --wait-for-deployment --candidate-commit <full-sha> --candidate-versions <csv> --base-url <edge-url>',
    );
  }
  const baseUrl = values.get('--base-url');
  const options = {
    candidateCommit: requiredCommit(values.get('--candidate-commit'), 'candidate commit'),
    candidateVersions: normalizedVersions(values.get('--candidate-versions'), 'candidate versions'),
    baseUrl: new URL(baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`),
    wait,
  };
  if (!wait) {
    const keep = values.get('--retained-releases');
    options.retainedReleases = positiveInteger(/^\d+$/.test(keep) ? Number(keep) : Number.NaN, '--retained-releases');
    options.tag = values.get('--tag');
  }
  return options;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function selftest() {
  const deployed = '1111111111111111111111111111111111111111';
  const candidate = '2222222222222222222222222222222222222222';
  const versions = ['edge', 'v0.3.0'];
  const base = {
    candidateCommit: candidate,
    deployedCommit: deployed,
    candidateVersions: versions,
    deployedVersions: versions,
    deployedIsAncestorOfCandidate: true,
    candidateIsAncestorOfDeployed: false,
    retainedReleases: 10,
  };
  assert(
    deploymentCandidateDecision(base).action === 'deploy',
    'successful A was suppressed because a later B might fail before deployment',
  );
  assert(
    deploymentCandidateDecision({
      ...base,
      candidateCommit: deployed,
      deployedCommit: candidate,
      deployedIsAncestorOfCandidate: false,
      candidateIsAncestorOfDeployed: true,
    }).action === 'skip',
    'A was allowed to overwrite already deployed B',
  );
  assert(
    deploymentCandidateDecision({
      ...base,
      candidateCommit: deployed,
      deployedCommit: deployed,
      candidateVersions: [...versions, 'v0.4.0'],
      deployedIsAncestorOfCandidate: true,
      candidateIsAncestorOfDeployed: true,
    }).action === 'deploy',
    'release-only candidate with the same edge source was rejected',
  );
  assert(
    deploymentCandidateDecision({ ...base, candidateVersions: ['edge'] }).action === 'skip',
    'candidate that removes a released version was accepted',
  );
  assert(
    deploymentCandidateDecision({
      ...base,
      deployedIsAncestorOfCandidate: false,
      candidateIsAncestorOfDeployed: false,
    }).action === 'fail',
    'divergent history was accepted',
  );

  // A window of three over v0.1.0 to v0.4.0: v0.4.0 is the new release.
  const served = ['edge', 'v0.1.0', 'v0.2.0', 'v0.3.0'];
  const window = { ...base, deployedVersions: served, retainedReleases: 3 };
  assert(
    deploymentCandidateDecision({ ...window, candidateVersions: ['edge', 'v0.2.0', 'v0.3.0', 'v0.4.0'] })
      .action === 'deploy',
    'a release that pushed the oldest version out of the retention window was refused',
  );
  assert(
    deploymentCandidateDecision({
      ...window,
      candidateCommit: deployed,
      deployedCommit: deployed,
      candidateVersions: ['edge', 'v0.2.0', 'v0.3.0', 'v0.4.0'],
      candidateIsAncestorOfDeployed: true,
    }).action === 'deploy',
    'a release that swapped the oldest version on the served edge source was counted as already served',
  );
  assert(
    deploymentCandidateDecision({
      ...window,
      deployedVersions: ['edge', 'v0.2.0', 'v0.3.0', 'v0.4.0'],
      candidateVersions: ['edge', 'v0.1.0', 'v0.2.0', 'v0.3.0'],
    }).action === 'skip',
    'a candidate built before the newest tag was allowed to remove it',
  );
  assert(
    deploymentCandidateDecision({ ...window, candidateVersions: ['edge', 'v0.1.0', 'v0.3.0', 'v0.4.0'] })
      .action === 'skip',
    'a candidate missing a version inside the window was accepted',
  );
  assert(
    deploymentCandidateDecision({ ...window, candidateVersions: ['edge', 'v0.3.0', 'v0.4.0'] }).action ===
      'skip',
    'a candidate whose oldest retained build is missing was accepted',
  );
  assert(
    deploymentCandidateDecision({ ...window, retainedReleases: 2, candidateVersions: ['edge', 'v0.2.0', 'v0.3.0'] })
      .action === 'deploy',
    'a narrower retention window could not retire the versions it no longer holds',
  );

  // A tag run documents one release, and a green run means the site serves it.
  const tagRun = { ...window, release: 'v0.4.0' };
  assert(
    deploymentCandidateDecision({ ...tagRun, candidateVersions: ['edge', 'v0.1.0', 'v0.2.0', 'v0.3.0'] })
      .action === 'fail',
    'a tag run whose candidate lacks its own release was allowed to pass',
  );
  assert(
    deploymentCandidateDecision({
      ...tagRun,
      candidateVersions: ['edge', 'v0.2.0', 'v0.3.0', 'v0.4.0'],
      deployedVersions: ['edge', 'v0.2.0', 'v0.3.0', 'v0.4.0'],
      candidateIsAncestorOfDeployed: true,
      deployedIsAncestorOfCandidate: false,
    }).action === 'skip',
    'a tag run was failed although a newer deploy already serves its release',
  );
  assert(
    deploymentCandidateDecision({
      ...tagRun,
      candidateVersions: ['edge', 'v0.2.0', 'v0.3.0', 'v0.4.0'],
      candidateIsAncestorOfDeployed: true,
      deployedIsAncestorOfCandidate: false,
    }).action === 'fail',
    'a tag run skipped over a site that does not serve its release, and passed',
  );
  console.log(
    'check-deployment-candidate.mjs --selftest: OK (later failure, B-before-A, release set, divergence, retention window, tag run)',
  );
}

async function waitForCandidate(options) {
  let lastState;
  for (let attempt = 1; attempt <= 30; attempt += 1) {
    try {
      const state = await readPublicState(options.baseUrl, { attempts: 1, delayMilliseconds: 0 });
      lastState = `${state.commit}; ${state.versions.join(',')}`;
      if (
        state.commit === options.candidateCommit &&
        state.versions.length === options.candidateVersions.length &&
        isSuperset(state.versions, options.candidateVersions)
      ) {
        console.log(`deployment candidate: OBSERVED (${lastState})`);
        return;
      }
    } catch (error) {
      lastState = error instanceof Error ? error.message : String(error);
    }
    if (attempt < 30) await delay(10_000);
  }
  throw new Error(`candidate was not publicly observable after 30 attempts: ${lastState}`);
}

async function main() {
  const options = parseArguments(process.argv.slice(2));
  if (options.selftest) {
    selftest();
    return;
  }
  if (options.wait) {
    await waitForCandidate(options);
    return;
  }

  // A tag run documents its tag only when the tag has the release form the
  // build selects. A prerelease tag such as v1.0.0-rc.1 builds no version of
  // its own, so there is nothing to require of the served site.
  const release = options.tag !== undefined && isRelease(options.tag) ? options.tag : undefined;
  if (options.tag !== undefined && release === undefined) {
    console.log(`deployment candidate: ${options.tag} is not a release version, so no served version is required`);
  }
  const deployed = await readPublicState(options.baseUrl, { attempts: 12, delayMilliseconds: 5_000 });
  const decision = deploymentCandidateDecision({
    candidateCommit: options.candidateCommit,
    deployedCommit: deployed.commit,
    candidateVersions: options.candidateVersions,
    deployedVersions: deployed.versions,
    deployedIsAncestorOfCandidate: gitIsAncestor(deployed.commit, options.candidateCommit),
    candidateIsAncestorOfDeployed: gitIsAncestor(options.candidateCommit, deployed.commit),
    retainedReleases: options.retainedReleases,
    release,
  });
  if (decision.action === 'fail') throw new Error(decision.reason);
  const allowed = decision.action === 'deploy';
  const output = process.env.GITHUB_OUTPUT?.trim();
  if (output) appendFileSync(output, `allowed=${allowed}\n`, 'utf8');
  console.log(`deployment candidate: ${allowed ? 'OK' : 'SKIP'}: ${decision.reason}`);
}

main().catch((error) => {
  console.error(`deployment candidate: FAILED: ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
});
