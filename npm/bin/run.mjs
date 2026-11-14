import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';

const require = createRequire(import.meta.url);
const commands = new Set(['ptah', 'ptah-compat', 'ptah-ls']);
const platforms = new Set(['darwin-x64', 'darwin-arm64', 'linux-x64', 'linux-arm64', 'win32-x64', 'win32-arm64']);

function launch(command) {
  if (!commands.has(command)) {
    throw new Error(`Unknown Ptah command: ${command}`);
  }

  const platform = `${process.platform}-${process.arch}`;
  if (!platforms.has(platform)) {
    throw new Error(`Ptah has no binary for ${platform}. Use a supported Linux, macOS, or Windows x64/arm64 host.`);
  }

  const packageName = `@stokaro/ptah-${platform}`;
  let packagePath;
  try {
    packagePath = require.resolve(`${packageName}/package.json`);
  } catch (error) {
    if (error.code !== 'MODULE_NOT_FOUND') {
      throw error;
    }
    throw new Error(`The ${packageName} binary package is missing. Reinstall @stokaro/ptah with optional dependencies enabled.`, { cause: error });
  }

  const binary = join(dirname(packagePath), 'bin', `${command}${process.platform === 'win32' ? '.exe' : ''}`);
  const result = spawnSync(binary, process.argv.slice(2), { stdio: 'inherit' });
  if (result.error) {
    throw result.error;
  }
  if (result.signal) {
    process.kill(process.pid, result.signal);
  }
  process.exit(result.status ?? 1);
}

export function run(command) {
  try {
    launch(command);
  } catch (error) {
    console.error(`ptah: ${error.message}`);
    process.exitCode = 1;
  }
}
