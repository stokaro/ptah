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
  - "cmd/schema"
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

When the database already matches the desired schema, the command prints
`Schema is synced, no changes to be made.` and writes no file at all. A script
around this workflow has to treat a missing plan file as a normal outcome.

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

### The key file is missing or unreadable

Key handling belongs to OpenSSH, and its failures reach you unchanged:

```text
error: ssh-keygen sign: exit status 255: Couldn't load public key nope_key: No such file or directory
```

## What the approval proves

A verified approval proves two things:

- The plan file holds exactly the bytes that a key listed in `allowed_signers`
  signed, in the `ptah-plan` namespace.
- The principal that key is listed under, which `verify-approval` prints.

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

## Next steps

- Apply a change without the plan-file split:
  [Apply directly](../apply/).
- Gate a pipeline on divergence instead of on review:
  [Compare and drift](../compare-and-drift/).
- Ship the reviewed change to shared environments as versioned files:
  [Generate migrations](../../versioned/generate/).
