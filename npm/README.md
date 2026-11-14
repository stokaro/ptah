# Ptah on npm

Run the native Ptah CLI without a global install:

```sh
npx @stokaro/ptah version
```

Pin a version in scripts and CI:

```sh
npx @stokaro/ptah@X.Y.Z version
npx --package=@stokaro/ptah@X.Y.Z ptah-compat version
```

The package exposes `ptah`, `ptah-compat`, and `ptah-ls`. It installs the matching platform binary from npm as an optional dependency. No install script or runtime download is required. For a project dependency, run `npm install --save-dev @stokaro/ptah` and invoke the commands in npm scripts.

Supported hosts are Linux, macOS, and Windows on x64 and arm64. Ptah's database provider plugins are installed separately through the Ptah CLI.

See the [Ptah documentation](https://docs.ptah.run/) for commands and other installation methods.
