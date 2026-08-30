# Canonical schema-source fixture

These files describe the same four-table library schema in the common subset
of Ptah's schema frontends:

- `schema.sql`;
- `schema.yaml`;
- `schema.hcl`;
- `schema.dbml`;
- `models/schema.go`;
- the SQL emitted by `external-schema.sh`.

The fixture has a one-to-many `authors` to `books` relationship, a many-to-many
`books` to `tags` relationship through `book_tags`, primary keys on every
table, and unique author-name and join-pair indexes. Keep the representations
equivalent. Run
`scripts/check-source-equivalence.sh` after changing any one of them.

The fixture intentionally omits source-specific features. Examples that teach
features outside the common subset should use a separate, explicitly scoped
fixture rather than weakening this equivalence contract.
