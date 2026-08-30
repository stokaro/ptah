#!/usr/bin/env node

import { appendFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = join(scriptDir, '..', '..', '..');
const fullCommit = /^[0-9a-f]{40}$/;
const releaseVersion = /^v\d+\.\d+(?:\.\d+)?$/;

function requiredCommit(value, label) {
  if (typeof value !== 'string' || !fullCommit.test(value)) {
    throw new Error(`${label} must be a full lowercase Git SHA`);
  }
  return value;
}

function normalizedVersions(value, label) {
  const versions = Array.isArray(value) ? value : String(value ?? '').split(',');
  const normalized = versions.map((version) => version.trim()).filter(Boolean);
  if (!normalized.includes('edge')) throw new Error(`${label} must contain edge`);
  if (normalized.some((version) => version !== 'edge' && !releaseVersion.test(version))) {
    throw new Error(`${label} contains an invalid documentation version`);
  }
  if (new Set(normalized).size !== normalized.length) throw new Error(`${label} contains a duplicate`);
  return normalized.sort();
}

function isSuperset(candidate, deployed) {
  const candidateSet = new Set(candidate);
  return deployed.every((version) => candidateSet.has(version));
}

export function deploymentCandidateDecision({
  candidateCommit,
  deployedCommit,
  candidateVersions,
  deployedVersions,
  deployedIsAncestorOfCandidate,
  candidateIsAncestorOfDeployed,
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
  if (!isSuperset(candidateSet, deployedSet)) {
    return { action: 'skip', reason: 'candidate would remove a documentation version already served' };
  }
  if (candidateCommit === deployedCommit) {
    return candidateSet.length > deployedSet.length
      ? { action: 'deploy', reason: 'candidate adds a release while preserving the deployed edge source' }
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

function parseArguments(arguments_) {
  if (arguments_.includes('--selftest')) return { selftest: true };
  const value = (name) => {
    const index = arguments_.indexOf(name);
    return index === -1 ? undefined : arguments_[index + 1];
  };
  const candidateCommit = value('--candidate-commit');
  const candidateVersions = value('--candidate-versions');
  const baseUrl = value('--base-url');
  const wait = arguments_.includes('--wait-for-deployment');
  const expectedLength = wait ? 7 : 6;
  if (!candidateCommit || !candidateVersions || !baseUrl || arguments_.length !== expectedLength) {
    throw new Error(
      'usage: node scripts/check-deployment-candidate.mjs [--wait-for-deployment] --candidate-commit <full-sha> --candidate-versions <csv> --base-url <edge-url>',
    );
  }
  return {
    candidateCommit: requiredCommit(candidateCommit, 'candidate commit'),
    candidateVersions: normalizedVersions(candidateVersions, 'candidate versions'),
    baseUrl: new URL(baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`),
    wait,
  };
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
  console.log('check-deployment-candidate.mjs --selftest: OK (later failure, B-before-A, release set, and divergence)');
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

  const deployed = await readPublicState(options.baseUrl, { attempts: 12, delayMilliseconds: 5_000 });
  const decision = deploymentCandidateDecision({
    candidateCommit: options.candidateCommit,
    deployedCommit: deployed.commit,
    candidateVersions: options.candidateVersions,
    deployedVersions: deployed.versions,
    deployedIsAncestorOfCandidate: gitIsAncestor(deployed.commit, options.candidateCommit),
    candidateIsAncestorOfDeployed: gitIsAncestor(options.candidateCommit, deployed.commit),
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
