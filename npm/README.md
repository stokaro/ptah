# Ptah on npm

Run the native Ptah CLI without a global install:

```sh
npx @stokaro/ptah version
```

Pin a version in scripts and CI:

```sh
npx @stokaro/ptah@X.Y.Z version
npx @stokaro/ptah-compat@X.Y.Z version
```

The `@stokaro/ptah` package exposes `ptah`, `ptah-compat`, and `ptah-ls`. Use the separate `@stokaro/ptah-compat` package for a direct `npx` command or an `atlas` executable in an existing project. Both packages use the same platform binaries. No install script or runtime download is required.

Supported hosts are Linux, macOS, and Windows on x64 and arm64. Ptah's database provider plugins are installed separately through the Ptah CLI.

See the [Ptah documentation](https://docs.ptah.run/) for commands and other installation methods.
