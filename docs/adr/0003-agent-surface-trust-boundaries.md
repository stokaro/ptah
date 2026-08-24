# ADR 0003: Agent-surface trust boundaries and threat model

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1484](https://github.com/stokaro/ptah/issues/1484), under [#1483](https://github.com/stokaro/ptah/issues/1483)
- Supersedes: nothing
- Superseded in part: section 3.4 by
  [ADR 0006](0006-one-authorized-agent-runtime.md). The read operations no
  longer read what the caller names with the process's own permissions: schema
  sources resolve inside operator-configured directories, and a live database is
  operator-configured rather than named by the caller. The threat model and the
  rest of the boundary analysis stand.

## 1. Context

[ADR 0002](0002-read-only-agent-mvp-scope-and-transport.md) froze what the agent
surface exposes and what it speaks. It did not say who is trusted, what an
attacker controls, or which of the properties the surface appears to have are
actually there.

[#1484](https://github.com/stokaro/ptah/issues/1484) asks for that document, and
[#1487](https://github.com/stokaro/ptah/issues/1487) — the permission and
constrained-mutation phase — is the child that has to be measured against it. A
permission model written without one is a list of restrictions nobody can say
are the right restrictions.

Everything below was measured against the shipped surface rather than reasoned
from the design. Where a probe found a property, the probe is printed.

### 1.1 The surface as it exists

`ptah mcp` serves four tools over stdio:

| tool | reads |
| --- | --- |
| `ptah_validate_schema` | a declared schema, from paths the caller names |
| `ptah_render_schema` | the same, and returns the DDL it becomes |
| `ptah_schema_lineage` | the same, and returns column-to-column edges |
| `ptah_read_database` | a live database, at a URL the caller names |

Three of the four take a `SchemaSource` — `root_dirs`, `schema_files` — and the
fourth takes a `database_url`. **Every one of those values is chosen by the
model**, not by the person running the client, and that is the whole subject of
this record.

## 2. Trust boundaries

There are four parties, and only two of them are trusted.

**The operator** starts the client and the server, supplies the environment, and
owns the machine. Trusted; everything below assumes the operator's intent is the
thing being protected.

**The Ptah process** runs with the operator's credentials and filesystem
reach. Trusted with them, and unable to hold less: it is an ordinary local
process.

**The model** proposes tool calls, including the paths and URLs they carry.
**Not trusted.** It is influenced by everything in its context, and the tool
results below are part of that context.

**The content** — schema files, Go annotations, database object names and
comments, error text — is **not trusted**. It reaches the model as tool output,
and a schema comment is a place an attacker can write a sentence addressed to
the model.

The boundary that matters is therefore not between Ptah and the network. It is
between **the model's choice of arguments** and **the operator's intent**.

## 3. What was measured

### 3.1 The filesystem reach is the process's, and the model picks the path

```text
schema_files: ["<a path outside the working tree>.sql"]
  -> {"dialect":"postgres","problems":null,"valid":true}

root_dirs: ["/etc"]
  -> {"kind":"source","message":"error parsing packages: open anydesk/cache: permission denied"}
```

Both were served. A path outside the working tree was read and validated, and a
directory the caller has no business scanning was scanned until the operating
system refused a subdirectory.

Two consequences, and they are different:

- **Reach.** The tools read anything the process can read. For a server the
  operator starts on their own machine beside a client that already has file
  access, this is not a privilege escalation — it is the same reach the client
  had. It becomes one the moment the server is reached from anywhere else, which
  is why remote transport is a separate phase
  ([#1492](https://github.com/stokaro/ptah/issues/1492)) and not a flag.
- **Disclosure.** The refusal named `anydesk/cache`. A tool error is an oracle
  for what exists and what is readable, one path at a time, and the caller
  driving it is the untrusted party.

### 3.2 Credentials in a URL are not echoed back

```text
database_url: "postgres://ptah_user:hunter2@…/nope"
  -> "connect: failed to ping database: failed to connect to
      `user=ptah_user database=nope`: … connect: connection refused"
```

The password is absent from the message, and the user and database names are
present. That is the property to keep: an error a model reads must not carry a
secret the operator supplied, and this one does not.

It holds because the driver builds the message, not because Ptah redacts it.
That makes it a property to **test**, not one to rely on — a driver change is
the kind of thing that would take it away quietly, which is exactly the shape
of [#1875](https://github.com/stokaro/ptah/issues/1875)'s driver defect.

### 3.3 Nothing leaves the machine that the caller did not name

The server holds no credentials, stores none, and opens no connection except the
one a tool argument names. `ptah mcp --help` states this, and the tool set is
frozen to ADR 0002's four operations, none of which fetches over the network.

The reachable-host question is therefore the same as the path question: the
model chooses, and the process can reach whatever the operator's network allows.

## 4. Threats

Ordered by what an attacker gets, not by likelihood.

### T1 — Injected content steers a tool argument

A schema file, a column comment, or a database object name contains text
addressed to the model. The model then calls a tool with a path or URL it was
told to use.

**Reachable today.** Section 3.1 shows the argument is honored.

**What it gets:** file contents rendered back into the conversation, or a
connection attempt to a host of the attacker's choosing.

**What limits it:** the operator sees the tool call in the client. That is a
real control and a weak one — it depends on a human reading arguments.

### T2 — Error text as a filesystem oracle

Repeated calls with different paths distinguish "exists and unreadable" from
"does not exist", as section 3.1 measured.

**What it gets:** a map of the filesystem, slowly, without reading a byte.

### T3 — A secret in a tool result

Not reachable through the measured paths: the database read returns schema, not
rows, and the connection error omits the password. It is listed because the
absence is a property that can be lost, and because a future tool that returns
data rather than structure would reintroduce it.

### T4 — A destructive operation behind a read-only name

Not reachable: ADR 0002 excludes `schema inspect`, `schema diff` and
`migrations lint` from the surface precisely because each needs a scratch
database it resets destructively. The exclusion is the control, and it is worth
naming here because it is the control most likely to be undone by someone adding
"one more read-only verb".

### T5 — Reach beyond the operator's machine

Not reachable: the transport is stdio and the process is local. This is what
[#1492](https://github.com/stokaro/ptah/issues/1492) must not change without
returning to this record.

## 5. Decision

1. **The model is untrusted and its arguments are attacker-influenced.** Any
   control that assumes otherwise is not a control. This is the sentence
   [#1487](https://github.com/stokaro/ptah/issues/1487) has to design against.

2. **The read-only surface ships without a path allowlist**, and the reason is
   recorded rather than assumed: the server is local, started by the operator,
   beside a client that already reads files. An allowlist here would constrain
   the operator without constraining the attacker's most valuable move, which is
   T1 rather than T2.

3. **A path or URL policy becomes required before any of these three:** a
   mutation tool, a non-stdio transport, or a tool that returns row data. Each
   changes what T1 is worth, and the first two are named children of the epic.

4. **The credential-redaction property in section 3.2 is a test, not a note.**
   It rests on driver behavior, and a driver bump is how it would be lost.

5. **A new tool restates its threat model or does not land.** Four questions,
   answered in the pull request that adds it: what does it read, what can the
   model steer, what does an error disclose, and does it return structure or
   data.

## 6. Alternatives

**A path allowlist rooted at the working directory.** Rejected for the read-only
surface by decision 2, and pre-approved for the surfaces in decision 3. It costs
the ordinary case — a schema in a sibling repository is a normal thing to
validate — and buys little against T1, where the attacker's move is to have the
model read a file the operator *would* have allowed.

**Refusing tool calls whose arguments were influenced by tool output.**
Attractive and not implementable: the server sees one call at a time and has no
view of the conversation that produced it. It belongs to the client, and this
record says so rather than pretending the server can do it.

**Redacting error text.** Rejected for now. The measured disclosure is path
existence, and a redacted error is one a person cannot act on either. Revisit
with decision 3's transport change, where the reader may not be the operator.

## 7. Consequences

- [#1487](https://github.com/stokaro/ptah/issues/1487) inherits a stated
  adversary rather than inventing one, and inherits three triggers that make a
  policy mandatory.
- [#1492](https://github.com/stokaro/ptah/issues/1492) cannot add a transport
  without returning here, because T5's "not reachable" is an argument about
  stdio.
- Every new tool costs four sentences in its pull request, which is the price of
  not discovering the boundary later.
