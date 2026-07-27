# ORM loader examples

These examples feed schemas generated from framework models into Ptah through
the `external_schema` configuration source:

- [`gorm`](gorm/README.md) uses the pinned standalone GORM provider.
- [`sqlalchemy`](sqlalchemy/README.md) uses a pinned Python virtual environment.

Both examples require `--allow-external-schema` because their checked-in
configuration executes a local program. Review each `ptah.yaml` and the program
it references before opting in.
