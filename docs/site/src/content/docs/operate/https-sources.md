---
title: Pull from an HTTPS URL
description: Pull a desired schema or a migration directory from an HTTPS URL whose bytes are pinned by their SHA-256, with its content digest.
type: how-to
audience:
  - "platform-engineer"
readerQuestion: "How do I pull a desired schema or a migration directory from a file or an archive served over HTTPS?"
goal: "Pull a schema file or a migration directory from an HTTPS download pinned by its digest, and check what it selects against its content digest."
sourceOfTruth:
  - "internal/httpsource"
  - "internal/sourcearchive"
  - "internal/cli/internal/sourcepull"
  - "internal/sourcecontent"
generated: false
searchAliases:
  - "HTTPS schema source"
  - "tar.gz migrations"
  - "zip migrations"
  - "download digest"
overlaps:
  - /operate/git-sources/
disposition: keep
---

`ptah schema pull` and `ptah migrations pull` read from an `https://` URL as
well as from an OCI registry and a Git repository. The URL names one download:
the schema file itself, or a `tar.gz` or `zip` archive. The download is pinned
by the SHA-256 of its exact bytes, and Ptah reads none of it before that digest
matches, so a server that serves other bytes reaches no archive reader.

Ptah Operator delivers schemas and migrations from HTTPS through these commands,
so what a pull reports here is what the operator binds a plan to.

## Pull a schema file

```sh
ptah schema pull https://artifacts.example.com/inventory/1.4.0/schema \
  --format file --path schema.hcl \
  --expect-download-digest sha256:5f3c... --out schema.hcl
```

With `--format file` the download is the schema file, and `--path` is the one
name it is given: the name in the manifest, whose extension tells the schema's
format. The bytes are written as served, so give `--out` the same extension.

```text
Pulled https://artifacts.example.com/inventory/1.4.0/schema to /work/schema.hcl
Download digest: sha256:5f3c...
Content digest: sha256:dd0b...
```

A schema file can come in an archive too: `--format tar.gz` or `--format zip`,
with `--path` the member to read, such as `db/schema.hcl`.

## Pull a migration directory

```sh
ptah migrations pull https://artifacts.example.com/orders/orders-1.4.0.tar.gz \
  --format tar.gz --path orders-1.4.0/db/migrations \
  --expect-download-digest sha256:9b71... --out migrations
```

A migration directory comes in an archive, `tar.gz` or `zip`. Every file under
`--path` is written under `--out`, at its path relative to the selected
directory; `--path .` selects the archive's root. A leading `./` on an entry's
name is ignored, as `tar -C dir -czf orders.tar.gz .` writes it.

## Pin the download

`--expect-download-digest` is required. It is `sha256:` and the SHA-256 of the
exact bytes the URL serves, which the job that publishes the archive can print:

```sh
shasum -a 256 orders-1.4.0.tar.gz
```

The pull writes the body to a private temporary file, hashing it as it writes,
and compares the digest before it reads the file as an archive. A mismatch
names both digests. The download is deleted once the selection is in memory,
before the output is written, so the temporary directory holds one of the two
at a time.

`--expect-digest` holds the selected files to their content digest as well, as
for [Git](../git-sources/#hold-a-pull-to-its-content). The content digest names
the files, so an archive packed again with the same files keeps it while its
download digest changes.

## Read the report as JSON

`--json` prints one document instead of the text:

```json
{
  "source": "https",
  "reference": "https://artifacts.example.com/orders/orders-1.4.0.tar.gz",
  "format": "tar.gz",
  "download_digest": "sha256:9b71...",
  "download_size": 18234,
  "path": "orders-1.4.0/db/migrations",
  "content_digest": "sha256:9a4e...",
  "files": [
    {"path": "0000000001_create_orders.up.sql", "size": 120, "sha256": "..."}
  ],
  "written": ["/work/migrations"]
}
```

`files` lists every file read, sorted by path, so two pulls of the same content
print the same document.

## Authenticate and pin the server

Credentials come from the environment, never from a flag or the URL:

| Variable | Used for |
| --- | --- |
| `PTAH_HTTPS_USERNAME` and `PTAH_HTTPS_PASSWORD` | Basic authentication |
| `PTAH_HTTPS_TOKEN` | a Bearer token |

Any other combination is refused: a username without a password, a password
without a username, or a token beside either.

`--ca-file` names the only roots the server's certificate may chain to; without
it, the system's roots are used. TLS 1.2 or later is required, and no flag
turns verification off. The transport reads `HTTPS_PROXY` and `NO_PROXY` from
the environment.

A pull follows up to 10 redirects, never to `http://`. While a credential is
set, a redirect to another origin (scheme, host and port) is refused, so the
credential is never sent to a server it was not meant for. Without a
credential, such a redirect is followed, and the digest still pins the bytes.

## What a pull refuses

The URL and the request:

- an `http://` URL, a URL with a user, a query or a fragment, and one with no
  path;
- a status other than `200`, named in the message; the response body is never
  printed;
- a `Content-Encoding` other than identity: the digest names the bytes as
  served, and the pull asks for them unencoded;
- a declared length over 64 MiB, before the body is read, and a body that runs
  past 64 MiB or ends before its declared length.

Every entry of an archive, inside the selection or not:

- an absolute name, a `..` element, a backslash, a drive letter or a NUL,
  whatever `GODEBUG` says about insecure paths;
- more than 10,000 entries, or a `tar.gz` that expands past 1 GiB.

Inside the selection:

- a symbolic link, a hard link, a device, a FIFO or a sparse file, each named
  in the message;
- a duplicate path, or a path that is both a file and a directory;
- a path with an empty, `.` or `..` element, a control character or invalid
  UTF-8;
- an encrypted zip entry, a zip method other than store or deflate, and a CRC
  that does not match; a truncated gzip stream, or a trailer that does not
  match;
- more than 1,000 files or more than 64 MiB of selected content;
- a selected path that is not there, a directory where a schema selects a file,
  and `--format file` for a migration directory.

Entries outside the selection are skipped: a zip entry there is not inflated.

The output path, as for every pull, must not exist.
