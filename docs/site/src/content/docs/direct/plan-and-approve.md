---
title: Plan and approve changes
description: Save a schema change as a plan file, sign it with an SSH key, and refuse an apply whose plan carries no matching approval.
type: how-to
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "How do I save a schema change as a plan file, sign it with an SSH key, and refuse an apply whose plan carries no matching approval?"
goal: "Apply only the reviewed and signed schema plan."
sourceOfTruth:
  - "internal/cli/schema"
  - "migration/schemadiff"
  - "migration/planner"
generated: false
overlaps: []
disposition: keep
sourceMode: source-neutral
owns:
  - cli-ptah-schema-approve
  - cli-ptah-schema-plan
  - cli-ptah-schema-verify-approval
---

`ptah schema apply` computes the SQL and runs it in one command, so the person
who reviews the change is the person who runs it. Four commands split that in
two. `ptah schema plan` writes the SQL to a plan file, `ptah schema approve`
signs that file with an SSH key, `ptah schema verify-approval` checks the
signature against a list of approvers, and `ptah schema apply --require-approval`
refuses a plan file that carries no signature from that list.

The signature covers the plan file byte for byte, so an edited plan stops
verifying. The SQL that runs is the SQL that was signed.

## Before you start

- A `ptah` binary ([Install Ptah](../../start/install/)).
- `ssh-keygen` on your `PATH`. Ptah runs it for both signing and verification
  and never reads a private key itself.
- A database to change and a desired schema as local files. The examples use
  SQLite and two SQL files, so they run without a database server.

## Starting state

The database starts with a `users` table that has two columns. `schema.sql`,
the desired schema, adds a third:

```bash
cat > baseline.sql <<'SQL'
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL
);
SQL

cat > schema.sql <<'SQL'
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL,
    created_at TIMESTAMP
);
SQL

ptah schema apply --db-url "sqlite://$PWD/app.db" --schema-file baseline.sql --auto-approve
```

Expected output includes:

```text
Planned schema changes:
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT NOT NULL
);
Auto-approval enabled; applying schema changes.
Schema apply completed successfully.
```

## Save the plan

`ptah schema plan --save` compares the desired schema with the database and
writes the ordered statements to a file instead of running them:

```bash
ptah schema plan \
  --db-url "sqlite://$PWD/app.db" \
  --schema-file schema.sql \
  --save --name add_created_at
```

Expected output includes:

```text
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
Plan saved to file://add_created_at.plan.json
```

`--save` writes `<name>.plan.json` in the working directory. `--output <path>`
names the file instead and saves it without `--save`, and the directory it
names has to exist already. `--dry-run` prints the same document to stdout and
writes nothing.

The file is what a reviewer reads:

```json
{
  "format_version": 1,
  "name": "add_created_at",
  "dialect": "sqlite",
  "from_fingerprint": "sha256:2259264fba37b68659268236f28164880e7618466b0ba779af09e68ce7f9a390",
  "to_fingerprint": "sha256:33a61bf8700833003ab4c1cfe185a96283ae5f009acc6830b47e303baeb9af58",
  "destructive": false,
  "statements": [
    {
      "sql": "ALTER TABLE \"users\" ADD COLUMN \"created_at\" TIMESTAMP",
      "severity": "safe",
      "reason": "does not remove data or tighten constraints"
    }
  ]
}
```

A statement that a feature owner's operation renders is marked `owned` and
records the owner's verdict when it is higher than what the SQL text says,
because the text of such a statement says little about its effect. A ClickHouse
sorting key that loses an expression on an engine that merges rows by it, such
as `ReplacingMergeTree`, is saved as destructive. A ClickHouse storage change such as a TTL change, and a
skipping-index addition, are saved as a warning. The text of each reads as
safe. When the owner assesses what the statement does to access, the
statement also carries `access` -- `widens`, `narrows`, `unchanged` or
`unknown` -- and `access_reason`. A widening or unknown access effect makes the
statement destructive. Each owner operation is planned on its own, so its
verdict reaches the statements it rendered and no others.

A target served by an [executable provider](../../extend/executable-providers/)
can plan native operations that have no SQL. The file then records such a
statement with a `native` object in place of `sql`: the operation kind, its
subject, and the provider's own encoding of the operation. The plan binds the
provider release, and `ptah schema apply --plan` runs the operation only
through that release of the provider.

A provider can also declare sensitive inputs for a SQL or native operation.
The statement's `inputs` array records each input's `source` (`environment`)
and variable `name`. Review these names with the operation: the plan signature
covers both, and apply refuses a provider that requests different inputs.
Values are read from the applying process's environment immediately before
dispatch. They are absent from plans, previews, and provider process
environments. A missing variable refuses the batch; an explicitly empty
variable supplies an empty value. A dry run validates references without
reading values. Editing or projecting such a plan to Atlas HCL is refused
because it would discard its input approvals.

A JSON plan can also carry `annotations`: ordered notes with `text` and a
zero-based `before` position in the statement list. A position equal to the
statement count places a note after the last operation. Notes alone can be
saved and previewed without creating executable statements. Apply preserves
them in its report and runs only the statements. Notes do not affect schema
fingerprints, but changing them changes the saved document's digest and its
approval. The Atlas `.plan.hcl` format refuses positioned notes because it
cannot preserve this distinction.

An `--edit` that leaves the statements as they were, comments and whitespace
aside, keeps every recorded verdict. After any other edit, a statement whose
text the plan recorded keeps the strongest verdict recorded for that text, so a
copy of a destructive statement is destructive too. When the plan has an
`owned` statement, every `owned` statement and every statement the edit
introduced becomes destructive with an unknown access effect: SQL text cannot
say what they do, and an owner judged each statement beside the others, which
the edit changed. A known widening stays a widening, and the reason each
statement had stays beside the new one. Plan again to get fresh verdicts. The
Atlas `.plan.hcl` format stores only the SQL, so a plan read back from it
carries the text's verdict alone.

Owner transaction requirements are stored in each JSON statement's
`transactions` array. Apply and dev rehearsal reject a mode that contradicts a
recorded requirement before executing the plan. A changed statement sequence
needs a fresh owner assessment when the plan carries these requirements. Atlas
HCL output refuses such a plan because it cannot preserve the requirements.

`from_fingerprint` describes the schemas the database URL covers. A URL that
names no schema covers every schema in the database; one pinned with
`search_path` covers that schema alone. When the desired schema names a schema
outside a pinned URL, the plan also records `schemas_beyond_url`, and applying
the plan reads those schemas as well. A change in any schema the plan writes
then makes the plan stale.

`--schemas`, or `schemas` in `ptah.yaml`, names the schemas to plan for, as it
does for `ptah schema apply`, and takes precedence over the URL. `--schemas`
outranks the file. The plan records the selection as `schemas`, and
`from_fingerprint` then describes those schemas alone. Applying the plan reads
the same schemas, so a change in another schema does not make the plan stale,
and `ptah schema apply --plan` refuses a `--schemas` of its own. Without a
selection, a URL that pins no `search_path` reads every schema, and the plan
drops the tables of each schema the desired schema declares nothing in. The
command then prints the warning `ptah schema apply` prints, once for each such
schema:

```text
warning: the desired schema declares nothing in schema "crm", so its 2 tables are planned for removal; to keep them, pin search_path in the URL or pass --schemas
```

On MySQL and MariaDB, a URL that names no database is the whole server, as it
is for `schema apply`. `from_fingerprint` then describes every user database
the URL's account can see, or the databases `--schemas` names, and `schema
apply --plan` applies the plan through the same URL. Through a URL naming one
of those databases the plan is stale,
and so is a plan for one database applied to the whole server. A `--dev-url`
naming a database beside a whole server is refused, as `schema apply` refuses
it.

`from_fingerprint` describes structure, not rows. When the desired schema
declares reference rows and the plan reads a table that holds them, the plan
also records `managed_rows` -- each table, its key columns and the managed
columns it read -- and `rows_fingerprint`, a digest of the rows those columns
held. Applying the plan reads the same columns again, so a declared row that
changed after the plan was computed makes the plan stale too, and a column the
declaration does not manage does not. The row values themselves are not written
into the plan file. A plan that declares no rows carries neither field, and the
Atlas `.plan.hcl` format, which has no place for them, refuses a plan that does.

`from_fingerprint` also covers the role memberships the plan depends on, and no
others. A membership counts when it connects to one of these roles, directly or
through a chain of groups:

- a role the read names: a holder or grantor of a privilege, the owner of an
  object, or the definer of a routine;
- the role the connection runs as;
- a role a statement in the plan names, such as the role it grants to or
  creates. A table with the same name as a role counts too.

The chain is followed in the two directions privileges travel: what such a role
holds, and who holds it. So a new member of a role the plan grants to makes the
plan stale, and so does a group joining the groups of the role that runs it. A
new member of a group the running role merely belongs to does not, because the
running role holds nothing more.

A credential rotation that creates a user and grants it a group with no tie to
the plan leaves the plan valid. One that grants a group holding a privilege in
the read makes every plan reading that privilege stale, on this database, because
the new user now holds it. On a target that compares memberships, which is YDB,
the plan covers all of them.

When an embedding application selects an executable provider, the native JSON
plan also records `provider`: the provider ID, its release version and the
major version of the provider contract, plus a digest of the server version,
schema scope, capabilities, and identifier rules used during planning. Apply
checks that binding before reading the target.

The binding names a release, not an executable. The same release of the
provider applies the plan after it is installed again or on another platform,
where the executable has another digest. Another provider, another release or
another major contract version refuses the plan, and the message names the
version the plan was computed with and the version selected now. Changed target
facts and a missing binding refuse it too. Each refusal requires a new plan and
approval, even if the schema has not changed. The digest of the installed
executable is still checked each time the provider starts.

Addresses and credentials are excluded. The Atlas `.plan.hcl` format cannot
preserve this binding and refuses such plans. Use the same explicit
[provider configuration](../../extend/executable-providers/) for planning and
application.

When the database already matches the desired schema, the command prints
`Schema is synced, no changes to be made.` and writes no file at all. A script
around this workflow has to treat a missing plan file as a normal outcome.
With `--json` the command says so in a document rather than a sentence:
`outcome` is `no-changes`. The same document carries the plan, its digest and
a typed refusal code when planning refuses, and `ptah schema apply --json`
reports the apply the same way; see
[Read the result in a script](../apply/#read-the-result-in-a-script). The
digest covers the bytes of the file `--output` writes, and the document carries
the plan's content rather than those bytes, so the file is what gets approved
and applied.

### How a column change is rated

A column change is rated by what the comparison knows about the column, because
its SQL cannot say. `ALTER COLUMN c TYPE timestamp(0)` reads the same over a
`timestamp(6)` as over a `date`, and MySQL's `MODIFY COLUMN c bigint` reads the
same whether it gives up a `NOT NULL`, a default or nothing.

| Rating | Change | What happens to a row or a writer |
| --- | --- | --- |
| destructive | The new type cannot hold every value of the old one: a shorter string, a smaller text or blob type, fewer bytes or bits, a smaller integer, decimal or float, a lower temporal precision or range, fewer interval fields, or fewer enum members | The reason reads `column type narrows from X to Y`. `timestamp(6)` under `timestamp(0)` loses `.123456`, and a `double precision` under `real` reads `1.2345679` where `1.23456789012` was |
| destructive | `DROP NOT NULL`, also where a restated column gives it up | A writer can store `NULL` |
| destructive | `DROP IDENTITY` | The server drops the sequence with it, as `DROP SEQUENCE` does, and an insert that leaves the column out fails |
| warning | `DROP DEFAULT`, also where a restated column gives it up | No row changes. An insert that leaves the column out fails on `NOT NULL` or writes `NULL` |
| warning | `SET GENERATED`, and `ADD GENERATED ALWAYS` | No row changes. `ALWAYS` refuses an insert that names the column, and `BY DEFAULT` accepts one whose value can collide with a later one |
| safe | A wider type, a higher precision, or a type that holds every value of the old one | Every value survives. `ptah migrations plan` reports it as a warning, because the column's type still changes. One type in two spellings plans nothing, so nothing is rated |

A change between families asks the same question. A fraction under an integer is lost, a
number with more digits than a float keeps loses the low ones (`bigint` under `double
precision`), and text under a number, a date or an enum may hold what the new type cannot.
Some servers convert without a word and others refuse the value. The rating is the same,
since a plan does not read the rows. A temporal type under text is not rated.

A temporal type that declares no precision holds what its engine says, so
`timestamp(3)` under `timestamp` loses digits on MySQL and gains them on
PostgreSQL:

| Engine | Types | Digits when none are declared |
| --- | --- | --- |
| PostgreSQL, CockroachDB, YugabyteDB | `timestamp`, `timestamptz`, `time`, `timetz`, `interval` | 6 |
| MySQL, MariaDB | `timestamp`, `datetime`, `time` | 0 |
| SQL Server | `datetime2`, `datetimeoffset`, `time` | 7 |
| ClickHouse | `DateTime` holds none, and a bare `DateTime64` is `DateTime64(3)` | 0 or 3 |

Spanner has one timestamp type and SQLite stores the declaration without reading it, so a
precision alone plans nothing there. A change of type on these engines is rated as above.

A type that declares no size holds what its engine says as well. The plan compares
what the two sides hold, so `numeric` declared `numeric(10,2)` is a change and
`decimal(10,0)` declared `decimal` on MySQL is not:

| Engine | `decimal`, `numeric`, `NUMBER` | `varchar`, `nvarchar` | `float` | `real` |
| --- | --- | --- | --- | --- |
| PostgreSQL, CockroachDB, YugabyteDB, Spanner | Any number | No limit | 8 bytes | 4 bytes |
| MySQL, MariaDB | `decimal(10,0)` | The server refuses it | 4 bytes | 8 bytes |
| SQL Server | `decimal(18,0)` | One character, and `(max)` has no limit | 8 bytes | 4 bytes |
| Oracle | Any number, and `NUMBER(*,0)` no fraction | `VARCHAR2(4000)` for `VARCHAR` | `FLOAT(126)` | `BINARY_FLOAT` |

A bare `char`, `nchar`, `binary` or `bit` holds one on every engine. `float(p)` is the
4-byte type up to a `p` of 24 and the 8-byte type above it, except on Oracle, where
it keeps `p`. MySQL and MariaDB size `text` and `blob` in
[four tiers](../../databases/mysql/#a-columns-size). Elsewhere `text`, `bytea`, `CLOB`,
`BLOB`, `ntext`, `image` and `String` have no limit. SQLite stores the declaration without
reading it.

Oracle lowers a size only on a column with no row. `TIMESTAMP(6)` declared
`TIMESTAMP(0)` and `NUMBER` declared `NUMBER(10,2)` plan a `MODIFY` rated
destructive, and while a row exists `ptah schema apply` stops with the server's
`ORA-30082` or `ORA-01440`. No value is lost and the plan does not run: empty the
column, or rebuild it by hand, and plan again. Raising a size runs with the rows in
place. Oracle's `TIMESTAMP(0)` under `DATE` keeps every second and is rated destructive
all the same, since the rating reads the two as a time of day going away.

`ptah schema apply` prints the planned SQL and no rating, so read the plan to see
one. Plan with `ptah schema plan`, review the file, and run it with `ptah schema
apply --plan`.

## List the approvers

Verification reads an OpenSSH `allowed_signers` file, one line per approver.
The examples use a throwaway key so that the whole page runs; in a repository
the public key belongs to the reviewer and the file is committed:

```bash
ssh-keygen -t ed25519 -N "" -C "reviewer@example.com" -f reviewer_key
mkdir -p .ptah
printf 'reviewer@example.com namespaces="ptah-plan" %s\n' \
  "$(cut -d' ' -f1,2 reviewer_key.pub)" > .ptah/allowed_signers
```

Each line is a principal, optional OpenSSH options, and the public key, with
the key material abbreviated here:

```text
reviewer@example.com namespaces="ptah-plan" ssh-ed25519 AAAAC3NzaC1lZDI1NTE5...
```

`namespaces="ptah-plan"` is optional and worth writing. Ptah signs plans in the
`ptah-plan` namespace, and the option restricts that key entry to the same
namespace. A signature the same key made over the same bytes for another
purpose does not verify as a plan approval.

Commit the file. It puts the set of approvers in the same review path as the
code, so changing who may approve is itself a reviewed change.

## Approve the plan

```bash
ptah schema approve --plan add_created_at.plan.json --key reviewer_key
```

Expected output includes:

```text
Approved add_created_at.plan.json
Signature: add_created_at.plan.json.sig
```

The signature is written beside the plan and travels with it. `ptah schema
approve` does not read `allowed_signers`: any key can sign a plan, and only a
key the list names verifies.

## Verify the approval

```bash
ptah schema verify-approval --plan add_created_at.plan.json
```

Expected output includes:

```text
Approved by reviewer@example.com
Plan digest: cb1dcc5b5bd0fed9ff9bf77587bdd45a1b0ff20d8de55c46b846dba86aa46117
```

The digest is the SHA-256 of the plan file, so `shasum -a 256
add_created_at.plan.json` prints the same value. With no `--signer`, the
command tries each principal the list holds and reports the one whose key
verifies. `--signer <principal>` requires the approval to belong to one named
principal and refuses an approval from anybody else.

`--allowed-signers`, or `PTAH_ALLOWED_SIGNERS`, points at a list elsewhere. The
default path `./.ptah/allowed_signers` resolves against the working directory
rather than against the plan file, so a run started from a subdirectory needs
the flag.

In a pipeline, run this command as a step of its own before the apply step. It
exits `0` only when the approval verifies.

## A changed plan is refused

This is what the signature is for. Copy the approved plan and its signature,
change one statement in the copy, and hand the copy to `apply`:

```bash
cp add_created_at.plan.json tampered.plan.json
cp add_created_at.plan.json.sig tampered.plan.json.sig
python3 - <<'PY'
import json

plan = json.load(open('tampered.plan.json'))
plan['statements'][0]['sql'] += '; DROP TABLE "users"'
open('tampered.plan.json', 'w').write(json.dumps(plan, indent=2) + "\n")
PY

ptah schema apply \
  --db-url "sqlite://$PWD/app.db" \
  --plan tampered.plan.json \
  --require-approval --auto-approve
```

Expected output includes:

```text
error: --require-approval: approval does not verify against .ptah/allowed_signers: either the plan changed after it was approved, or it was signed by a key that file does not list
```

The command exits `2` and the database is untouched: the check runs before the
plan is executed. A whitespace-only edit is refused the same way, because the
signature covers the whole document rather than a set of fields chosen as the
ones that matter.

## Apply the approved plan

```bash
ptah schema apply \
  --db-url "sqlite://$PWD/app.db" \
  --plan add_created_at.plan.json \
  --require-approval --auto-approve
```

Expected output includes, on stdout:

```text
Planned schema changes:
ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMP;
Auto-approval enabled; applying schema changes.
Schema apply completed successfully.
```

The receipt naming the approver goes to stderr:

```text
Plan approved by reviewer@example.com
```

`--auto-approve` answers the interactive confirmation prompt. It has no effect
on the signature check, which `--require-approval` turns on.

:::caution
`--plan` on its own executes an unapproved plan and exits `0` without
mentioning that nobody signed it. The gate is `--require-approval`.
:::

## Confirm the result

Planning again reports that nothing is left to change:

```bash
ptah schema plan \
  --db-url "sqlite://$PWD/app.db" \
  --schema-file schema.sql \
  --save --name recheck
```

Expected output:

```text
Schema is synced, no changes to be made.
```

No `recheck.plan.json` is written.

## Failure modes

Every refusal below exits `2`. [Exit codes](../../reference/exit-codes/) covers
the general contract, and `--help` on each verb lists its flags with the
`PTAH_*` environment variable that sets each one.

### Nobody approved the plan

`ptah schema apply --require-approval` names the plan and the command that
signs it:

```text
error: --require-approval: add_created_at.plan.json carries no approval; sign it with `ptah schema approve`
```

`ptah schema verify-approval` reports the missing file:

```text
error: plan has no approval signature: add_created_at.plan.json.sig
```

### The approval does not verify

One message covers three causes:

```text
error: approval does not verify against .ptah/allowed_signers: either the plan changed after it was approved, or it was signed by a key that file does not list
```

The plan changed after it was approved. Or a key the list does not name signed
it — signing succeeds whatever the key is, because `approve` never reads
`allowed_signers`. Or `ssh-keygen` could not run: verification discards what
`ssh-keygen` reported and prints the two-cause message anyway, so a `PATH` that
holds no `ssh-keygen` produces this line byte for byte. Check that
`ssh-keygen -Y verify` runs on the machine before reading the message as a
tampered plan.

### The database moved since the plan was computed

The approval and the fingerprint are separate checks, and the approval runs
first. A plan whose signature verifies against a database that has changed
prints the receipt and then stops:

```text
Plan approved by reviewer@example.com
error: pre-planned migration is stale: the target database schema does not match the plan's source fingerprint (plan sha256:2259264f..., database sha256:b4c38243...); the database changed since the plan was computed, so re-run `schema plan` against the current database and review the fresh plan
```

Re-planning writes a different file, so the fresh plan needs a fresh approval.

The plan records what its URL covered as `scope`: every schema of the database
for a PostgreSQL URL without `search_path`, one schema for a URL that pins one,
and on MySQL and MariaDB one database or the whole server. Applied through a URL
that covers something else, the plan reads a different set of schemas, so the
fingerprint differs even though nothing changed. The refusal then names both
scopes instead:

```text
error: pre-planned migration is stale: the plan was computed for every schema of the database, and the target URL reaches schema "public" (plan sha256:c81456cf..., database sha256:dc76f43f...); apply the plan through a URL that reaches every schema of the database, or re-run `schema plan` through this one and review the fresh plan
```

A plan covers the memberships of the role that computed it, and the file does not
record that role. Applied as another role, the plan verifies only when neither
role holds a membership the rest of the plan does not cover. When the applying
role has memberships, the refusal says so, because the database may not have
changed:

```text
error: pre-planned migration is stale: the target database schema does not match the plan's source fingerprint (plan sha256:b522c397..., database sha256:6b0f19aa...); the database changed since the plan was computed, or the plan was computed as a role other than "migrator" and the fingerprint covers the memberships of the role that runs it, so re-run `schema plan` as "migrator" against the current database and review the fresh plan
```

### The key file is missing or unreadable

Key handling belongs to OpenSSH, and its failures reach you unchanged:

```text
error: ssh-keygen sign: exit status 255: Couldn't load public key nope_key: No such file or directory
```

## What the approval proves

A verified approval proves what was signed, and by whom:

- The plan file holds exactly the bytes that a key listed in `allowed_signers`
  signed, in the `ptah-plan` namespace.
- The principal that key is listed under, which `verify-approval` prints.

`ptah schema apply --require-approval` reads the plan file once. The signature
is checked against those bytes, and the same bytes are decoded, digested and
executed, so a file replaced after the check does not run in place of the plan
that was approved.

It does not prove any of the following, and a process built on it has to cover
them some other way:

- **That the SQL is correct or safe to run.** The `severity` and `destructive`
  fields in the plan file are Ptah's classification of the statements, not a
  reviewer's judgment.
- **That a person read the plan.** A signature is the key's, and whatever holds
  the key can produce one.
- **That more than one person agreed.** One signature verifies or does not.
  There is no threshold, and a rejected plan leaves no record.
- **That the database still matches the plan.** The fingerprint check answers
  that, separately, at apply time.
- **Anything about direct database access.** The refusal happens inside
  `ptah schema apply`, so it constrains this command and not a client that
  connects on its own.

The set of approvers is a file in your repository. Whoever can change
`.ptah/allowed_signers` and the plan in the same change can approve their own
plan, so this control is worth what review of that file is worth.

## Limitations

- Signing and verification are `ssh-keygen -Y sign` and `ssh-keygen -Y verify`
  runs. Key formats, passphrase prompts, revocation lists and validity windows
  behave as OpenSSH defines them. The two halves report an `ssh-keygen` failure
  differently: `ptah schema approve` passes OpenSSH's own wording through, as in
  `error: ssh-keygen sign: exec: "ssh-keygen": executable file not found in
  $PATH:`, while `ptah schema verify-approval` discards it and prints its
  two-cause refusal instead.
- `ptah schema verify-approval` exits `0` when the approval verifies and `2`
  for every refusal, including a plan nobody signed. A pipeline that tells
  "nobody reviewed this" apart from "this plan changed" has to read the message
  text.
- A plan file carries the fingerprint of the database it was computed against.
  Applying it to a database in a different state refuses on that fingerprint,
  whether or not the approval verifies.
- The fingerprint reads memberships as the server's catalog lists them. On
  PostgreSQL that leaves out the reserved `pg_` roles and the bootstrap
  superuser, and the `INHERIT` and `SET` options of a membership, so changing
  those does not make a plan stale.
- A role without superuser that creates another role on PostgreSQL 16 or later
  receives an `ADMIN` membership in it, and on MariaDB any creator holds the role
  it created. Both count as memberships, so a plan run by the creator is stale
  after the role appears, and after that role joins a group.
- On MySQL and MariaDB a read lists every role on the server, and the fingerprint
  covers that list. `CREATE ROLE` anywhere on the server makes a plan stale, and
  `CREATE USER` does not.

## Next steps

- Apply a change without the plan-file split:
  [Apply directly](../apply/).
- Gate a pipeline on divergence instead of on review:
  [Compare and drift](../compare-and-drift/).
- Ship the reviewed change to shared environments as versioned files:
  [Generate migrations](../../versioned/generate/).
