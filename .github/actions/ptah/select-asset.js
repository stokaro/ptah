const fs = require("fs");

const [releasePath, os, arch] = process.argv.slice(2);
const release = JSON.parse(fs.readFileSync(releasePath, "utf8"));
const suffix = `_${os}_${arch}.tar.gz`;
const asset = (release.assets || []).find((candidate) => candidate.name.endsWith(suffix));

if (!asset) {
  const names = (release.assets || []).map((candidate) => candidate.name).join(", ");
  throw new Error(`no Ptah release asset ending with ${suffix}; available assets: ${names}`);
}

console.log(asset.browser_download_url);
