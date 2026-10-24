---
title: Pull from a Git repository
description: Pull a desired schema or a migration directory from one commit of a Git repository, with its commit and content digest.
type: how-to
audience:
  - "platform-engineer"
readerQuestion: "How do I pull a desired schema or a migration directory straight from a Git repository?"
goal: "Pull a schema file or a migration directory from a pinned commit and check it against its content digest."
sourceOfTruth:
  - "internal/gitsource"
  - "internal/cli/internal/gitpull"
  - "internal/sourcecontent"
generated: false
searchAliases:
  - "Git schema source"
  - "git+https"
  - "git+ssh"
overlaps: []
disposition: keep
---

`ptah schema pull` and `ptah migrations pull` read from a Git repository as
well as from an OCI registry. A `git+https://` or `git+ssh://` reference names
the repository; flags name the commit and the path. Ptah fetches that one
commit shallowly and reads only the selected path from its tree. It checks out
no working tree and runs no hook, filter, submodule or LFS step, so a
repository cannot run anything on the machine that pulls from it.

Ptah Operator delivers schemas and migrations from Git through these commands,
so what a pull reports here is what the operator binds a plan to.

## Pull a schema file

```sh
ptah schema pull git+https://git.example.com/acme/app.git \
  --branch main --path db/schema.hcl --out schema.hcl
```

The output names the commit the branch pointed at and the content digest of
what was read:

```text
Pulled git+https://git.example.com/acme/app.git at commit 3f1c... to /work/schema.hcl
Commit: 3f1c...
Content digest: sha256:dd0b...
```

The file's bytes are written as they are in the repository: no line ending,
encoding or format is changed. A schema file's format comes from its
extension, so give `--out` the same one.

## Pull a migration directory

```sh
ptah migrations pull git+https://git.example.com/acme/app.git \
  --tag v1.4.0 --path db/migrations --out migrations
```

Every file under `db/migrations` is written under `migrations`, at its path
relative to the selected directory. `--path .` selects the repository's root.

## Choose the commit

Exactly one of `--branch` and `--tag`, or `--commit` alone, says which commit
to read:

| Flags | Reads |
| --- | --- |
| `--branch main` | the commit the branch points at now |
| `--tag v1.4.0` | the commit the tag names; an annotated tag is followed to it |
| `--commit <id>` | that commit, given as its full 40-character id |
| `--branch main --commit <id>` | that commit, which the branch named when it was resolved |

A commit the branch or tag still points at is fetched by the ref. Any other
commit is asked for by its id over HTTPS: Git's smart HTTP server gives every
commit a branch or tag reaches, whether or not its advertisement says so. Over
SSH it is asked for by its id only when the server says it allows that
(`uploadpack.allowReachableSHA1InWant`; GitHub and GitLab do). Where the server
does not give the commit by its id, Ptah looks for it along the branch or tag,
up to 4,096 commits back, and refuses `--commit` alone. A commit that is no
longer there, because the branch was force-pushed, is refused and says so.

## Hold a pull to its content

The content digest names the files, not the commit. It is SHA-256 over a
manifest with one line per file, sorted by path:

```text
<SHA-256 of the file, lowercase hex> <size in bytes> <path>
```

and the digest is `sha256:` followed by the SHA-256 of that manifest. A
schema file's path in the manifest is its file name; a migration file's is its
path relative to the selected directory. The same files give the same digest
from any source, which is the digest Ptah Operator binds a plan to.

To pull the same files again and refuse anything else, give the commit and the
digest:

```sh
ptah migrations pull git+https://git.example.com/acme/app.git \
  --branch main --commit 3f1c... --expect-digest sha256:9a4e... \
  --path db/migrations --out migrations
```

## Read the report as JSON

`--json` prints one document instead of the text:

```json
{
  "source": "git",
  "reference": "git+https://git.example.com/acme/app.git",
  "commit": "3f1c...",
  "path": "db/migrations",
  "content_digest": "sha256:9a4e...",
  "files": [
    {"path": "0001_create.up.sql", "size": 25, "sha256": "..."}
  ],
  "written": ["/work/migrations"]
}
```

`files` lists every file read, sorted by path. An OCI pull with `--json`
reports `source`, `reference`, `digest` and `written` instead.

## Authenticate and pin the server

Credentials come from the environment, never from a flag or the URL, so they
reach neither shell history nor the process list:

| Variable | Used for |
| --- | --- |
| `PTAH_GIT_USERNAME` | the HTTPS username; `git` when only a password is set |
| `PTAH_GIT_PASSWORD` | the HTTPS password or access token |
| `PTAH_GIT_SSH_KEY_FILE` | the file holding an unencrypted SSH private key |

Over HTTPS, `--ca-file` names the only roots the server's certificate may
chain to; without it, the system's roots are used. A redirect to another
origin is refused while a credential is set, so the credential is never sent
to a server it was not meant for.

Over SSH, `--known-hosts-file` is required: Ptah never trusts a host key on
first use, and a key the file does not hold is refused.

```sh
PTAH_GIT_SSH_KEY_FILE=/run/secrets/deploy-key \
ptah schema pull git+ssh://git@git.example.com/acme/app.git \
  --branch main --path db/schema.hcl \
  --known-hosts-file /run/secrets/known_hosts --out schema.hcl
```

## What a pull refuses

- a selected path that is a submodule, a symbolic link, or a Git LFS pointer,
  each named in the message: Ptah reads the bytes in the repository and
  fetches nothing beyond it;
- a path with an empty, `.` or `..` element, a backslash or a control
  character, or an element longer than 255 bytes;
- more than 1,000 files, more than 64 MiB of selected content, or more than
  64 MiB of pack data fetched across every attempt;
- an output path that already exists.
