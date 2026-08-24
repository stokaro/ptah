# ADR 0006: One policy-bearing agent runtime, and an operator-bound database target

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1491](https://github.com/stokaro/ptah/issues/1491), under [#1483](https://github.com/stokaro/ptah/issues/1483)
- Supersedes: section 2.3 of
  [ADR 0002](0002-read-only-agent-mvp-scope-and-transport.md), the arrangement
  that made the four read operations free functions reachable without a
  session; section 3.4 of
  [ADR 0003](0003-agent-surface-trust-boundaries.md), which recorded the read
  operations as reading what the caller names with the process's own
  permissions; and section 2.1 of
  [ADR 0004](0004-constrained-artifact-mutation.md), which scoped the capability
  broker to artifact operations. The frozen operation set, the naming rule, the
  transport decision, the digest binding and the gate design all stand.

## 1. Context

ADR 0004 introduced a capability broker and a policy table with sixteen
capabilities. ADR 0003 recorded the trust boundary the agent surface stands on.
Both were measured, and both were accurate about what they measured.

What neither recorded is that only four of the sixteen capabilities were ever
requested at a call site.

### 1.1 The bypass, as it was found

`describe_workspace` returned the whole resolved policy. Among its rows:

```text
schema.validate               allow
schema.render                 allow
schema.lineage                allow
database.inspect:unclassified deny
```

The four read operations were registered directly against package-level
functions in `internal/agentapi`, outside the session that carries the broker:

```go
func register(server *mcp.Server, cfg Config) {
	registerReadTools(server)                       // no session
	if cfg.Session != nil {
		registerArtifactTools(server, cfg.Session)  // session
	}
}
```

`grep` over the tree at that commit found four `Authorize` call sites, all for
artifact work: `project.read`, `artifact.read`, `artifact.write`,
`artifact.delete`. `internal/agentapi/agentapi.go` contained no reference to a
broker at all.

The consequence was measured over stdio against the built binary. A server
started with no flags published `database.inspect:unclassified deny` and then:

```text
tools/call read_database {"database_url": "postgres://u:p@host/db"}
→ connected
```

A caller reading the table before letting an agent loose was reading a refusal
that was not enforced. Twelve of the sixteen rows were in that state; four of
them corresponded to operations that existed.

### 1.2 Two further routes, found while measuring the first

`read_database` took the connection URL as a tool argument. The model therefore
chose the resource its own authorization was decided about — and since
`ClassUnclassified` is the class of any URL Ptah was told nothing about, the
answer was the same whatever it named. The argument also *was* the credential:
it arrived from the model, was written verbatim into the session file, and was
replayed to the provider on every later turn.

`schema_files` reached `internal/schemaload`, which resolves `oci://` through
`internal/ociartifact`. `network.arbitrary` is hard-denied — no layer may grant
it — so a schema operation was a way to perform an operation the policy says is
ungrantable. `root_dirs` was an arbitrary local read under the same name, and
`filesystem.arbitrary_read` is hard-denied too.

(`env://` was checked and is not a third route: `schemaload` rejects it with a
diagnostic rather than resolving it.)

## 2. Decision

### 2.1 A session is the runtime, and a workspace is state inside it

Every MCP and Assist run has one resolved policy, one broker, one approval path
and one audit path. A workspace is optional state; it is not what decides
whether authorization exists.

`mcpserver.New` returns an error when given no session. A server whose tools
reach no broker is not a configuration to be handled — it is the defect this
record is about, and the signature is where it stops being expressible.

### 2.2 Every exposed operation authorizes before it does anything observable

The four read operations moved onto the session. Each asks the broker before it
stats a path, parses a file, resolves a name or opens a socket.

| operation | capability |
| --- | --- |
| `validate_schema` | `schema.validate` |
| `render_schema` | `schema.render` |
| `schema_lineage` | `schema.lineage` |
| `read_database` | `database.inspect:<class of the configured target>` |

The package-level functions still exist beneath them and are now unexported.
That is the point: an adapter cannot call what it cannot name. The canonical
path is MCP or Assist → the versioned agent API → the broker → the deterministic
operation owner.

A denied call returns the policy refusal even when its source or target would
have produced some other error. That ordering is what makes "authorization ran
first" observable rather than asserted, and it is tested by denying an operation
whose source is also invalid and requiring the policy error to win.

### 2.3 The database is the operator's, named by the model

`read_database` no longer accepts a URL. It accepts the *name* of a target the
operator configured, and omitting the name selects the only one when a process
has exactly one.

A target carries an immutable identity (a digest over its name and URL), a
non-secret display string, the connection URL, and an `agentpolicy.DatabaseClass`
the operator set. Nothing infers a class from a URL, a host, a database name or
a label: a target called `production` on a host called `prod` is
`ClassUnclassified` until an operator says otherwise, and `ClassUnclassified` is
denied by the builtin table.

`ClassEphemeral` remains for a database Ptah created and owns.

### 2.4 Approvals are bound to the target, not to its class

The broker's session-grant key is the policy cell plus the target identity.
"Allow for this session" on one dev database does not authorize the next one,
and repointing a name at a different URL changes the identity, so a grant made
before the change does not survive it.

The prompt is composed by Ptah from the target's name, class and sanitized
display. The URL never appears in a prompt, an audit record, a discovery
response, a tool schema or an error.

### 2.5 Inspection is its own operator decision

A configured database and permission to read it are separate. `--allow-database-inspect`
takes `ask` or `allow` and is scoped to the class of the configured target.

It is a distinct flag rather than a reuse of `--auto-approve`, which is named
for patch approval. An operator enabling unattended patching should not thereby
grant unattended database access without having read a word about it.

### 2.6 A declared schema is read from configured directories only

Schema sources resolve inside roots the operator named — the workspace when
there is one, plus `--schema-source-root`. No configured root permits nothing.

Sources that would be fetched rather than opened are refused by shape: any
`scheme://` prefix, not a list of known schemes, because the question is whether
the loader would go somewhere and an unrecognized scheme is not evidence that it
would not.

**The limit, stated rather than implied.** The loaders take plain paths, so
containment here is a check on the path and not an `os.Root` handle as it is for
artifacts. Symlinks are resolved as far as the path exists before the comparison,
which closes the link-inside-the-scope escape; a directory swapped between the
check and the read is not closed by this and would need the loaders to take a
root handle.

### 2.7 Reporting separates authority from reachability

`describe_workspace` became `describe_session`. It works without a workspace,
and it reports:

- **authority** — the whole resolved table, refusals included;
- **reachability** — the configured schema source roots, the configured
  databases by name and class, and the workspace when there is one.

`database.inspect:dev ask` beside an empty database list is not a contradiction:
one says what policy would permit, the other says nothing is there. Filtering
the table to what is reachable was rejected — a report listing only grants
answers "nothing was granted" the same way a broken report does.

## 3. Alternatives rejected

**Hide the unenforced rows.** Removing `database.inspect` and the schema
capabilities from the response would have made the table consistent by making it
say less. The rows were not wrong; the enforcement was missing. A capability
table that omits what it does not enforce cannot be read as a boundary at all.

**Document the rows as advisory.** This is the same thing with more words, and
it puts the burden on the reader to know which half of a security report to
believe.

**Keep a policy-free no-workspace path.** The reading tools were reachable with
no session, so the simplest fix was to leave that alone and only enforce where a
session existed. That preserves a mode whose effective policy is the process's
own permissions, which is what ADR 0003 §1 says the agent surface must not be.

**Infer the database class from the URL.** A caller-controlled string deciding
its own verdict is not a policy.

## 4. Consequences

Accepted costs:

- **`ptah mcp` with no flags reads nothing.** No schema source root and no
  database means every read operation refuses with an actionable message. That
  is the fail-closed shape, and it is a real change for anyone who was relying
  on the old behavior.
- **Reading a database now takes two flags**, `--database-url` and
  `--allow-database-inspect`, plus a class for anything but a throwaway.
- **The contract version moved and two names changed.** Pre-GA, and the old
  spelling was the one that could not be enforced.

What did not change: the frozen operation set, the stdio decision, the digest
binding, the gate design, the hard-denied capabilities, and the rule that the
project policy layer may only narrow.

## 5. Trigger for the next record

A second live database target, or any operation that returns row data, needs
approval semantics this record did not decide: what a grant covers when a
process has several targets, and whether a row read is a different capability
from a catalog read.
