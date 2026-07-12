# Online DDL for MySQL/MariaDB (gh-ost / pt-online-schema-change)

`ALTER TABLE` on a large MySQL or MariaDB table can block writes for minutes
to hours. Production teams route such changes through
[gh-ost](https://github.com/github/gh-ost) or
[pt-online-schema-change](https://docs.percona.com/percona-toolkit/pt-online-schema-change.html),
which rebuild the table as a shadow copy with row-level catch-up and swap it
in with a near-instant rename.

`ptah migrate-up` / `ptah migrate-down` can invoke these tools for you
(issue #173). Routing happens two ways.

## Per-migration directive

Annotate a migration file with a ptah directive comment:

```sql
-- +ptah online_ddl_tool=ghost
ALTER TABLE users ADD COLUMN bio TEXT;
```

Every `ALTER TABLE` statement in that migration is routed through the chosen
tool (`ghost` or `pt-osc`); non-ALTER statements run normally on the
migration connection. `online_ddl_tool=none` opts the migration out of
automatic threshold routing (see below).

Directives work without any configuration file.

## Automatic routing via ptah.yaml

Create a `ptah.yaml` next to where you run the CLI (or pass `--config`):

```yaml
online_ddl:
  tool: ghost            # or pt-osc
  threshold_rows: 1000000
  args:
    - --allow-on-master
    - --max-load=Threads_running=25
```

With this config, any `ALTER TABLE` whose target table has an estimated row
count (from `information_schema.TABLES`) at or above `threshold_rows` is
routed through the tool automatically. Smaller tables get a plain `ALTER`.
`args` are appended verbatim to every tool invocation — this is where
deployment-specific switches belong.

## Fallback behavior

The routing degrades safely — the plain `ALTER TABLE` on the migration
connection is always the fallback:

- the tool binary is not on `PATH` → warning + plain ALTER;
- the row-count estimate fails → warning + plain ALTER;
- the dialect is not MySQL/MariaDB → directives are ignored with a warning;
- the tool itself exits non-zero → the migration **fails** (no silent
  fallback once the tool has started: it may have left a shadow table
  behind, and rerunning a plain ALTER on top could double-apply).

`--dry-run` logs the tool invocation that would run without executing it.

## Invocation details

For `ALTER TABLE <tbl> <clause>` with database URL
`mysql://user:pass@host:port/db`, ptah runs:

```text
gh-ost --host=host --port=port --user=user --database=db --table=tbl \
       --alter="<clause>" [--password=pass] [args...] --execute

pt-online-schema-change --alter "<clause>" [args...] --execute \
       h=host,P=port,u=user,D=db,t=tbl,p=pass
```

A schema-qualified table (`ALTER TABLE shop.users ...`) overrides the
database from the URL. The alter clause is passed through verbatim, so
multi-part alters (`ADD COLUMN a INT, ADD INDEX idx (a)`) work.

## Prerequisites you must provide

ptah shells out to the tools; it does not configure your topology. Before
enabling:

- **binlog in ROW format** (`binlog_format=ROW`) — required by gh-ost, which
  reads the binary log to replay ongoing writes onto the shadow table.
- **gh-ost topology flags**: by default gh-ost expects to connect to a
  replica. On a single-node/master-only setup add `--allow-on-master` to
  `online_ddl.args`. For reading from a specific replica, put the replica
  DSN switches (`--assume-master-host`, throttling flags, ...) there too.
- **pt-online-schema-change** uses triggers for catch-up: the table must not
  already have conflicting triggers, and foreign-key handling needs an
  explicit strategy (`--alter-foreign-keys-method`) when children reference
  the table.
- **Privileges**: both tools need more than plain ALTER — gh-ost needs
  REPLICATION SLAVE/CLIENT plus DDL/DML on the schema; pt-osc needs
  CREATE/DROP/TRIGGER.
- **Credentials on the command line** are visible in the process list. For
  shared hosts prefer the tools' own config mechanisms (gh-ost `--conf`,
  pt-osc `/etc/percona-toolkit/`), passing them via `online_ddl.args`, and a
  database URL without an inline password.

## Interaction with migration transactions

The migrator wraps each migration in a transaction, but MySQL DDL commits
implicitly anyway, and the online tools run on their own connections. A
tool-routed migration is therefore **not atomic**: if it fails halfway, fix
the underlying issue (and clean up the tool's shadow/ghost tables if any)
before re-running. Keep online-DDL migrations minimal — ideally one ALTER
per migration, with unrelated statements in separate migrations.
