# ORM loader examples

## What this example demonstrates

The GORM and SQLAlchemy fixtures let an external schema provider turn framework
models into SQL that Ptah reads through `external_schema`. Ptah executes the
configured provider only when the caller opts in with
`--allow-external-schema`.

## Prerequisites

- A built `ptah` binary on `PATH`.
- Go for the GORM provider, or Python 3.12 for the SQLAlchemy provider.
- Network access on the first dependency installation.

Review each `ptah.yaml` and the program it names before running it.

## Run

The GORM path is the shorter first run:

```bash
cd examples/orm-loaders/gorm
go mod download
ptah schema render \
  --config ptah.yaml \
  --allow-external-schema \
  --dialect postgres
```

Use [`sqlalchemy/README.md`](sqlalchemy/README.md) for the pinned Python virtual
environment and its equivalent render command.

## Expected result

Both providers produce PostgreSQL DDL for `users` and `pets`. Stable output
includes a unique email constraint or index, a `pets.user_id` index, and a
foreign key from `pets.user_id` to `users.id` with `ON DELETE CASCADE`.

## Verify

The repository example gate verifies both configurations, executable paths,
provider version pins, lock files, and model files. The child README commands
are the acceptance run because they execute third-party providers and require
their pinned dependencies.

## Cleanup

The GORM loader works in a temporary directory and removes it on exit. Remove
`examples/orm-loaders/sqlalchemy/.venv` after the Python example; no database is
created by either path.

## Learn more

Use [ORM and external schema sources](https://docs.ptah.run/edge/schema/orm-and-external/)
for the execution boundary, opt-in flag, source precedence, and failure modes.
