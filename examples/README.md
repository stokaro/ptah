# Ptah examples

Each example has one run, expected-result, verification, cleanup, and canonical
documentation contract in its own README. The executable gate runs local
examples and mechanically verifies provider-backed examples.

<!-- ptah:examples-index:start -->
| Example | What it demonstrates |
| --- | --- |
| [Annotation parser example](annotation_parser/) | This program reads one directory of annotated Go structs, resolves embedded types, and renders the resulting schema for PostgreSQL, MySQL, and MariaDB. It shows table, field, index, enum, and embedded-field annotations without needing a database. |
| [PostgreSQL extension comparison example](extension_ignore/) | This program compares in-memory desired and live schemas under each extension ignore policy. It shows the default plpgsql exception, a replacement ignore list, an additional ignore entry, and the mode that manages every extension. |
| [Embedded migration files example](migrator/) | This package embeds three reversible migration pairs in a Go embed.FS. An application can take the migrations subdirectory and register it with Ptah's migrator without copying SQL files beside the executable. |
| [ORM loader examples](orm-loaders/) | The GORM and SQLAlchemy fixtures let an external schema provider turn framework models into SQL that Ptah reads through external_schema. Ptah executes the configured provider only when the caller opts in with --allow-external-schema. |
| [Reusable Go components example](reusable_components/) | These tests exercise Ptah's supported embedder surface without invoking a CLI: AST construction and rendering, Go and Atlas HCL schema parsing, diff planning, migration integrity and linting, custom lint rules, and capability-aware SQL. |
| [Schema visualization example](viz/) | This fixture renders a connected schema with organization ownership, user and task self-references, task comments, and tag assignment through a join table. The Mermaid, DOT, and SVG files are generated Ptah output rather than hand-authored diagrams. |
<!-- ptah:examples-index:end -->
