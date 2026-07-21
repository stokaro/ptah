const fs = require("fs");

const marker = "<!-- ptah-action-comment -->";
const maxBlockBytes = 18000;
const maxCommentBytes = 60000;

module.exports = async ({ github, context, core }) => {
  const pullRequest = context.payload.pull_request;
  if (!pullRequest) {
    core.info("Skipping Ptah PR comment outside pull_request events.");
    return;
  }

  const body = buildComment();
  const { owner, repo } = context.repo;
  const issue_number = pullRequest.number;
  try {
    const comments = await github.paginate(github.rest.issues.listComments, {
      owner,
      repo,
      issue_number,
      per_page: 100,
    });
    const existing = comments.find(
      (comment) =>
        comment.body &&
        comment.body.includes(marker) &&
        comment.user &&
        comment.user.type === "Bot",
    );

    if (existing) {
      await github.rest.issues.updateComment({
        owner,
        repo,
        comment_id: existing.id,
        body,
      });
      return;
    }

    await github.rest.issues.createComment({
      owner,
      repo,
      issue_number,
      body,
    });
  } catch (error) {
    if (error.status === 403 || error.status === 404 || error.status === 422) {
      core.warning(`Skipping Ptah PR comment: ${error.message}`);
      return;
    }
    throw error;
  }
};

function buildComment() {
  const plan = readText(process.env.PTAH_PLAN_PATH);
  const safetyText = readText(process.env.PTAH_SAFETY_PATH);
  const safetyErrorText = readText(process.env.PTAH_SAFETY_ERROR_PATH);
  const lintText = readText(process.env.PTAH_LINT_PATH);
  const lintErrorText = readText(process.env.PTAH_LINT_ERROR_PATH);
  const safety = readJSON(safetyText);
  const lint = readJSON(lintText);

  const lines = [
    marker,
    "## Ptah migration plan",
    "",
    `Safety: ${formatSafety(safety)}.`,
    `Plan command: exit ${process.env.PTAH_PLAN_EXIT_CODE || "unknown"}.`,
    `Safety command: exit ${process.env.PTAH_SAFETY_EXIT_CODE || "unknown"}.`,
    `Lint command: exit ${process.env.PTAH_LINT_EXIT_CODE || "unknown"}.`,
    "",
    details("Migration SQL and safety text", "sql", plan),
    "",
    details("Safety JSON", "json", safetyText),
  ];

  if (safetyErrorText.trim()) {
    lines.push("", details("Safety command stderr", "text", safetyErrorText));
  }

  if (lintText.trim()) {
    lines.push("", details(`Lint JSON (${lintSummary(lint)})`, "json", lintText));
  }

  if (lintErrorText.trim()) {
    lines.push("", details("Lint command stderr", "text", lintErrorText));
  }

  return truncateComment(lines.join("\n"));
}

function readText(path) {
  if (!path) {
    return "";
  }
  try {
    return fs.readFileSync(path, "utf8");
  } catch (error) {
    return `Could not read ${path}: ${error.message}`;
  }
}

function readJSON(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function formatSafety(report) {
  if (!report) {
    return `unknown (destructive output: ${process.env.PTAH_DESTRUCTIVE || "unknown"})`;
  }
  return `${report.highest || "unknown"}${report.destructive ? ", destructive" : ""}`;
}

function lintSummary(report) {
  if (!report || !Array.isArray(report.findings)) {
    return "unavailable";
  }
  return `${report.findings.length} finding(s)`;
}

function details(summary, language, content) {
  return [
    `<details><summary>${summary}</summary>`,
    "",
    `\`\`\`${language}`,
    truncate(content),
    "```",
    "",
    "</details>",
  ].join("\n");
}

function truncate(content) {
  const buffer = Buffer.from(content || "", "utf8");
  if (buffer.length <= maxBlockBytes) {
    return content || "";
  }
  return `${buffer.subarray(0, maxBlockBytes).toString("utf8")}\n\n... truncated ...`;
}

function truncateComment(content) {
  const buffer = Buffer.from(content || "", "utf8");
  if (buffer.length <= maxCommentBytes) {
    return content || "";
  }
  return `${buffer.subarray(0, maxCommentBytes).toString("utf8")}\n\n... comment truncated ...`;
}
