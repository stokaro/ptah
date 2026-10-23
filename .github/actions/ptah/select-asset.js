// Picks the release archive for one platform and the checksums.txt that
// verifies it. Prints the archive name, the archive URL and the checksums URL,
// one per line.
//
// Exit status:
//   0  both were found
//   2  the release has no archive for this platform, so the caller may build
//      from source instead
//   3  the release has an archive but no checksums.txt; the caller must not
//      install an archive it cannot verify, and must not fall back to a build
//      from source to get around that
const fs = require("fs");

const [releasePath, os, arch] = process.argv.slice(2);
const release = JSON.parse(fs.readFileSync(releasePath, "utf8"));
const assets = release.assets || [];
const suffix = `_${os}_${arch}.tar.gz`;
const archive = assets.find((candidate) => candidate.name.endsWith(suffix));

if (!archive) {
  const names = assets.map((candidate) => candidate.name).join(", ");
  console.error(`no Ptah release asset ending with ${suffix}; available assets: ${names}`);
  process.exit(2);
}

const checksums = assets.find((candidate) => candidate.name === "checksums.txt");
if (!checksums) {
  console.error(`release has ${archive.name} but no checksums.txt to verify it against`);
  process.exit(3);
}

console.log(archive.name);
console.log(archive.browser_download_url);
console.log(checksums.browser_download_url);
