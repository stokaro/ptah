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

  const generated = generatedFiles();
  if (generated.length) {
    lines.push("", `Generated ${generated.length} migration file(s) in this run:`, "");
    for (const file of generated) {
      lines.push(`- \`${file}\``);
    }
    lines.push("", details("Generated migration SQL", "sql", generatedSQL(generated)));
  }

  return truncateComment(lines.join("\n"));
}

// generatedFiles reads the list run.sh wrote by comparing the migration
// directory before and after generation. An empty or missing list means
// generation was off or produced nothing, and both render as no section rather
// than as an empty one.
function generatedFiles() {
  const listed = readText(process.env.PTAH_GENERATED_LIST_PATH);
  return listed
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

// generatedSQL concatenates the files so a reviewer reads what would be
// committed rather than a description of it. Each file keeps its name above
// it: an up and a down migration are easy to confuse once the SQL runs
// together.
function generatedSQL(files) {
  return files
    .map((file) => `-- ${file}\n${readText(file).trimEnd()}`)
    .join("\n\n");
}

// buildComment is exported so a documented comment body can be produced by
// running the same code the Action runs, rather than transcribed by hand into
// a page that then drifts from it.
module.exports.buildComment = buildComment;

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
