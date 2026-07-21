const vscode = require("vscode");
const { LanguageClient } = require("vscode-languageclient/node");

let client;

async function activate(context) {
  if (!vscode.workspace.isTrusted) {
    return;
  }

  const config = vscode.workspace.getConfiguration("ptah");
  const command = config.get("languageServer.path", "ptah-ls");
  client = new LanguageClient(
    "ptahAnnotations",
    "Ptah Annotations",
    {
      command,
      args: [],
      options: {}
    },
    {
      documentSelector: [{ scheme: "file", language: "go" }]
    }
  );
  context.subscriptions.push(client);
  await client.start();
}

async function deactivate() {
  if (!client) {
    return undefined;
  }
  await client.stop();
  client = undefined;
  return undefined;
}

module.exports = {
  activate,
  deactivate
};
