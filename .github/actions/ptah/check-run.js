const fs = require("fs");

module.exports = async ({ github, context, core }) => {
  const { owner, repo } = context.repo;
  const headSha = context.payload.pull_request
    ? context.payload.pull_request.head.sha
    : context.sha;

  if (!headSha) {
    core.warning("Skipping Ptah check run because no commit SHA is available.");
    return;
  }

  const check = buildCheckRun(headSha);
  try {
    await github.rest.checks.create({
      owner,
      repo,
      ...check,
    });
  } catch (error) {
    core.warning(`Could not create Ptah check run: ${error.message}`);
  }
};

function buildCheckRun(headSha) {
  const planExitCode = process.env.PTAH_PLAN_EXIT_CODE || "unknown";
  const safetyExitCode = process.env.PTAH_SAFETY_EXIT_CODE || "unknown";
  const lintExitCode = process.env.PTAH_LINT_EXIT_CODE || "unknown";
  const safety = readJSON(process.env.PTAH_SAFETY_PATH);
  const lint = readJSON(process.env.PTAH_LINT_PATH);
  const destructive = safety ? safety.destructive === true : process.env.PTAH_DESTRUCTIVE === "true";
  const highest = safety && safety.highest ? safety.highest : "unknown";
  const lintFindings = lint && Array.isArray(lint.findings) ? lint.findings.length : "unknown";

  return {
    name: "Ptah destructive-change verdict",
    head_sha: headSha,
    status: "completed",
    conclusion: conclusion({ planExitCode, safetyExitCode, lintExitCode, destructive }),
    output: {
      title: title({ destructive, highest }),
      summary: [
        `Safety verdict: ${highest}.`,
        `Destructive changes: ${destructive ? "yes" : "no"}.`,
        `Plan command exit code: ${planExitCode}.`,
        `Safety command exit code: ${safetyExitCode}.`,
        `Lint command exit code: ${lintExitCode}.`,
        `Lint findings: ${lintFindings}.`,
      ].join("\n"),
    },
  };
}

function conclusion({ planExitCode, safetyExitCode, lintExitCode, destructive }) {
  if (planExitCode !== "0" || lintExitCode !== "0") {
    return "failure";
  }
  if (safetyExitCode !== "0") {
    return "failure";
  }
  return destructive ? "failure" : "success";
}

function title({ destructive, highest }) {
  if (destructive) {
    return "Ptah found destructive migration statements";
  }
  return `Ptah migration safety: ${highest}`;
}

function readJSON(path) {
  if (!path) {
    return null;
  }
  try {
    return JSON.parse(fs.readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}
