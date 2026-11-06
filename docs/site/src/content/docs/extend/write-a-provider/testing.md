---
title: Test a provider
description: Test a provider's services with the SDK's validators, through its own process, and through a Ptah binary, including against host changes that have not been released.
type: how-to
sourceMode: source-neutral
audience:
  - "go-developer"
  - "integration-developer"
readerQuestion: "How do I test a provider before Ptah depends on it?"
goal: "Cover a provider with service tests that use the SDK's validators, process tests through the SDK client, and tests that drive a ptah binary."
sourceOfTruth:
  - "examples/minimal-provider/backend"
  - "examples/minimal-provider/integration"
  - "provider-sdk/provider"
  - "provider-sdk/runtime"
  - "provider-sdk/integration"
  - "scripts/check-examples.sh"
  - "github:stokaro/ptah-ydb-plugin"
generated: false
searchAliases:
  - "provider tests"
  - "provider conformance"
  - "host-ref"
overlaps: []
disposition: keep
---

Test a provider at three distances from Ptah: each service called directly,
the executable driven through the SDK client, and the executable driven by a
`ptah` binary. The examples below come from the
[minimal provider example (latest development source)](https://github.com/stokaro/ptah/blob/master/examples/minimal-provider/README.md)
that [Build a minimal provider](../tutorial/) builds. You need a provider
module of your own built with the SDK.

## What a passing provider does

Ptah has no separate conformance suite for providers. The contract is enforced
while Ptah runs: the SDK validates every request and every reply on both sides
of the process boundary, and Ptah checks the description and what it plans
with. A reply that fails a check is an error, or an unknown outcome, in the
operator's command. Tests catch the same failures earlier. Cover at least
these behaviors:

- A read of an empty database is complete, and an object the provider cannot
  describe fails the read rather than disappearing from it.
- `Render` returns one unit per change, in order, and one unsupported change
  refuses the whole batch.
- `Execute` reports the prefix it confirmed. A refused statement and a
  canceled call give different outcomes, and nothing is ever run twice.
- A lock excludes a second session in another process, a lock ID is used once,
  releasing an unused ID fences it, and closing a session releases its locks.
- A reason never quotes an address, a value, a statement or a credential.
- Every feature the description lists has a service, and every service has its
  feature.

## Check each service with the SDK's validators

For each service, the SDK has the validator its client and server run. Call
the service directly in a unit test and check the reply with that validator,
then assert what the reply means. A validator that takes both the request and
the reply checks the request first; a name ending in `Reply` checks the reply
alone, for a request already checked.

| Service | Validator |
| --- | --- |
| `Describe` | `runtime.Server.Validate`, `provider.ValidateDescription` |
| `Open` | `provider.ValidateSession` |
| `Inspect` | `provider.InspectionValues` |
| `Render` | `provider.ValidateRenderedBatch` |
| `Execute` | `provider.CheckExecuted` |
| `AnalyzeSQL` | `provider.ValidateSQLAnalysis` |
| `TryLock`, `CheckLock`, `ReleaseLock` | `provider.ValidateLockResult` |
| `CompareModels` | `provider.ValidateComparedModels` |
| `PlanFeatures` | `provider.ValidatePlannedFeatures` |
| `DecodeSource`, `EncodeSource` | `provider.ValidateDecodedSource`, `provider.ValidateEncodedSource` |

The other services follow the same naming in the `provider` package. Start
with the description, because `runtime.Serve` refuses a server whose features
and services differ:

```go title="backend/description_test.go"
func TestServer_HappyPath(t *testing.T) {
	c := qt.New(t)
	server := backend.Server()
	c.Assert(server.Validate(), qt.IsNil)
	c.Assert(provider.ValidateDescription(server.Description, hostRequires), qt.IsNil)
	c.Assert(server.Description.GetProviderId(), qt.Equals, backend.ProviderID)
	c.Assert(server.Description.GetEngines(), qt.HasLen, 1)
	c.Assert(server.Description.GetEngines()[0].GetId(), qt.Equals, backend.Engine)
}

// A service the description offers without an implementation is a false
// advertisement, and Serve refuses it before the process answers anything.
func TestServer_FailurePath(t *testing.T) {
	c := qt.New(t)
	server := backend.Server()
	server.Locks = nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}
```

A rendering test checks the batch with the validator and then the statement:

```go title="backend/render_test.go"
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := &protocol.RenderRequest{Engine: backend.Engine, Changes: []*protocol.Change{test.change}}
			result, err := (&backend.Provider{}).Render(c.Context(), request)
			c.Assert(err, qt.IsNil)
			// The validator the SDK runs on both sides of the process boundary.
			c.Assert(provider.ValidateRenderedBatch(backend.Description(), request, result), qt.IsNil)
			c.Assert(result.GetUnits(), qt.HasLen, 1)
			c.Assert(result.GetUnits()[0].GetChangeId(), qt.Equals, "1")
			c.Assert(result.GetUnits()[0].GetSql(), qt.Equals, test.want)
		})
```

A failure path asserts the gRPC code and the reason. `runtime.ErrorWithReason`
makes the reason the status message, so `status.Convert(err).Message()` reads
it in a test:

```go title="backend/render_test.go"
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := &protocol.RenderRequest{Engine: backend.Engine, Changes: []*protocol.Change{test.change}}
			result, err := (&backend.Provider{}).Render(c.Context(), request)
			c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
			c.Assert(status.Convert(err).Message(), qt.Equals, test.reason)
			c.Assert(result, qt.IsNil)
		})
```

Two helpers cover what the host negotiates. `provider.NegotiatedDescription`
returns the description a given host receives, and
`provider.ContextWithHostFeatures` builds the context a service reads with
`provider.HostFeatures`:

```go title="backend/description_test.go"
// Negotiation keeps the reasons feature for a host that lists it and drops it
// for one that does not, so a reason never reaches a host that cannot show it.
func TestDescription_Negotiation(t *testing.T) {
	c := qt.New(t)
	withReasons := provider.NegotiatedDescription(backend.Description(), []string{provider.ErrorReasonsFeature})
	c.Assert(withReasons.GetFeatures(), qt.Contains, provider.ErrorReasonsFeature)
	without := provider.NegotiatedDescription(backend.Description(), nil)
	c.Assert(without.GetFeatures(), qt.Not(qt.Contains), provider.ErrorReasonsFeature)
}
```

A test that opens a database file or a server crosses a boundary that a unit
test does not. The example keeps such tests under `integration/` behind the
`integration` build tag, and calls the services in process with the same
validators:

```go title="integration/backend_fs_test.go"
// inspect reads the database and checks the snapshot as the SDK does.
func inspect(c *qt.C, tutorial *backend.Provider, session *protocol.Session) *protocol.Schema {
	c.Helper()
	request := &protocol.InspectRequest{SessionId: session.GetId()}
	snapshot, err := tutorial.Inspect(c.Context(), request)
	c.Assert(err, qt.IsNil)
	_, err = provider.InspectionValues(backend.Description(), request, snapshot)
	c.Assert(err, qt.IsNil)
	return snapshot
}
```

## Drive the provider through its process

A unit test calls a service as a Go method. A process test builds the
executable and starts it the way Ptah does, through `runtime.Start`, so it also
catches what only the boundary shows: a description the negotiation refuses, a
reply the SDK client refuses, a reason the SDK removes, and anything the
process writes to standard output that breaks the bootstrap. Build the
executable in the test and compute its digest:

```go title="integration/provider_e2e_test.go"
// build compiles the provider executable from the module root. The name ends
// in .exe on every platform, which Windows needs and others ignore.
func build(c *qt.C) (executable, digest string) {
	c.Helper()
	executable = filepath.Join(c.TempDir(), "ptah-tutorial-provider.exe")
	command := exec.CommandContext(c.Context(), "go", "build", "-o", executable, ".")
	command.Dir = ".."
	output, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go build: %s", output))
	content, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	sum := sha256.Sum256(content)
	return executable, hex.EncodeToString(sum[:])
}

// start starts the provider as Ptah does: the executable is named by path and
// digest, the identity is checked, and the features are negotiated before any
// session opens.
func start(c *qt.C, executable, digest string, hostFeatures []string) *runtime.Client {
	c.Helper()
	client, err := runtime.Start(c.Context(), runtime.Options{
		Executable:       executable,
		SHA256:           digest,
		ProviderID:       backend.ProviderID,
		RequiredFeatures: hostRequires,
		HostFeatures:     hostFeatures,
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(client.Close)
	return client
}
```

`RequiredFeatures` lists what Ptah requires of every provider. `HostFeatures`
decides which two-way features the host acts on: `provider.TwoWayFeatures()`
for a host that acts on all of them, and none for a host of the first minor of
the contract. Test both, because a provider built from a newer SDK has to keep
working with an older host:

```go title="integration/provider_e2e_test.go"
// A host that lists errors.reasons sees the provider's reason after the
// code; a host that does not sees the code alone, because the SDK server
// removes the reason before the reply leaves the process.
func TestProviderProcess_FailurePath(t *testing.T) {
	c := qt.New(t)
	executable, digest := build(c)
	for _, test := range []struct {
		name         string
		hostFeatures []string
		want         string
	}{
		{"with reasons", []string{provider.ErrorReasonsFeature}, `provider render failed \(InvalidArgument\): the tutorial engine stores the column types INTEGER, TEXT, REAL and BLOB only`},
		{"without reasons", nil, `provider render failed \(InvalidArgument\)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			client := start(c, executable, digest, test.hostFeatures)
			result, err := client.Render(c.Context(), &protocol.RenderRequest{Engine: backend.Engine, Changes: []*protocol.Change{unsupportedColumn}})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}
```

On Windows, a test closes the client before `TempDir` removes its directory,
since an open database file cannot be removed. Registering `client.Close` with
`Cleanup` after the directory gives that order.

Ptah discards the provider's standard output and standard error. To trace a
provider while you debug it, have it write a log file that an environment
variable names, and pass the variable through `runtime.Options.Environment`
in a test, or through the manifest entry's `environment` list under Ptah. The
process receives no other variable from the command that starts it.

## Drive the provider through Ptah

The last distance runs the commands an operator runs: a manifest that names
the build, then `ptah providers check`, a read, a plan and an apply. The
example's README carries that sequence, and Ptah's example gate runs it on each
change:

```bash
ptah providers check --provider-config providers.json --provider-engine tutorial
ptah schema plan --provider-config providers.json \
  --db-url "$DATABASE_URL" --schema-file schema.yaml --output change.plan.json
ptah schema apply --provider-config providers.json \
  --db-url "$DATABASE_URL" --plan change.plan.json --tx-mode none --auto-approve
```

In a provider's own repository, write these as tests that run a `ptah`
binary. The official
[YDB provider](https://github.com/stokaro/ptah-ydb-plugin) shows the shape:

- The test reads the path of a host binary from an environment variable,
  `PTAH_HOST_TEST_BIN`, and a live database address from another,
  `PTAH_YDB_TEST_URL`.
- A helper builds the provider, writes a manifest with its path and digest to
  a temporary directory, and passes the manifest with `--provider-config`.
  `ptah-compat` reads the same manifest from `PTAH_PROVIDER_CONFIG`.
- A test asserts on what the command printed and on the database read back,
  not on the statements the provider rendered.

Check a command that writes with an exact expectation. A plan that reports
`no-changes` after an apply is the usual proof that a read and a render agree.

## Test against a host that has not been released

A provider feature often needs a host change, and the two land in different
repositories. Build the host from source for those tests rather than waiting
for a release. The YDB provider's continuous integration runs its live tests
against three hosts:

- the newest released Ptah, which blocks the pull request;
- Ptah's `master`, which does not block, so a host change that breaks the
  provider shows early;
- a host branch or commit named in `.github/host-ref`, which blocks when it is
  present.

A pull request that needs an unmerged host change commits `.github/host-ref`
with the host branch in it. A separate check refuses the file on `master` and
on a pull request that is ready for review, so the file never outlives the host
change it names. Each leg checks out the host at its reference, builds `ptah`
and `ptah-compat`, and points `PTAH_HOST_TEST_BIN` at the result.

## Keep the tests portable

- Build the executable with `go build` in the test and name it with `.exe` on
  every platform; Windows needs the suffix and other systems ignore it.
- Build with `GOWORK=off` before you publish, so the tests use the SDK version
  `go.mod` requires and not a local checkout.
- Address a database file as `scheme:///` followed by its slash-separated path,
  with `filepath.ToSlash`, so a Windows drive letter survives the URL.

## Next steps

- To publish a build that Ptah can install and verify, follow
  [Package and release a provider](../release/).
- To find the validator and the decision record of a service, use the
  [Provider SDK reference](../sdk-reference/).
