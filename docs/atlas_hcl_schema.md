# HCL Schema Input

Ptah can generate SQL from an HCL schema file instead of scanning Go source
annotations. The HCL schema frontend builds the same `goschema.Database`
intermediate representation as Go annotations and YAML schema input, then runs
the normal finalization, dependency ordering, AST conversion, and dialect
renderers.

Ptah's HCL schema format is compatible with the Atlas HCL schema language for
the supported subset. Ptah is not affiliated with or endorsed by Ariga or Atlas.

Use HCL schema input when an existing Atlas-compatible schema file should be
used as a Ptah schema source.

## Generate SQL

```bash
go run ./cmd schema render --schema-file schema.hcl --dialect postgres
```

`--schema-file` accepts `.hcl`, `.yaml`, `.yml`, and `.sql` inputs and is
repeatable. It can be combined with repeatable `--root-dir` values; Ptah merges
all inputs into one desired schema, deduplicates identical named objects, and
rejects conflicting definitions. If `--dialect` is omitted, Ptah renders every
supported dialect.

Relative schema-file inputs are confined to the process working directory after
symbolic-link resolution; pass an absolute pathname for an intentional source
outside it.

## Supported Shape

The parser supports the schema-object subset that maps directly to Ptah's
current schema IR:

- `schema` labels and `comment`, for table namespace references such as
  `schema = schema.main`
- `table` blocks, including Ptah `checks`, `custom`, and nested `platform`
  parity extensions
- `column` blocks with `type`, `null`, `auto_increment`, `unique`,
  `unique_expr`, `default`, `check`, `check_name`, `identity` (including its
  `options`), `comment`, and nested `platform` parity extensions; the Ptah
  `enum` parity extension preserves inline Go-annotation enum values
- `primary_key` blocks with `columns`; PostgreSQL primary keys also support
  `include`
- `index` blocks with `columns`, `on { column = ..., prefix = ... }`,
  `on { expr = "..." }`, `desc`, `unique`, `type`, `where`, and `comment`;
  an `on` block also accepts `nulls_first` or `nulls_last` (not both) for a key
  whose `NULL` ordering differs from its direction's default, which is
  `NULLS LAST` for ascending and `NULLS FIRST` for descending;
  PostgreSQL indexes also support `include`, BRIN `page_per_range`, and
  `nulls_distinct`, ClickHouse data-skipping indexes support `granularity`, and
  the Ptah `ops` parity extension preserves the Go annotation operator class
- Ptah `constraint` blocks when complete annotation metadata cannot fit the
  Atlas-native `check`, `unique`, `primary_key`, or `foreign_key` shape, and
  for `EXCLUDE` constraints
- `unique` blocks with `columns`; PostgreSQL unique constraints also support
  `include` and `nulls_distinct`
- `foreign_key` blocks with one local `columns` entry and one table-qualified
  `ref_columns` entry
- `check` blocks with `expr`
- `default = sql("...")` as a default expression
- `row_security` blocks inside `table` with `enabled = true` and an optional
  `comment`
- PostgreSQL `extension` blocks with `schema`, `if_not_exists`, `version`, and
  `comment`
- PostgreSQL `role` blocks with `login`, `superuser`, `create_db`,
  `create_role`, `inherit`, `replication`, `password`, and `comment`
- PostgreSQL `permission` blocks for table, schema, and sequence targets with
  `to`, `for`, `privileges`, `grantable`, and `comment`
- PostgreSQL `function` blocks with `schema`, `lang`, `arg`, `return`,
  `security`, `volatility`, `as`, and `comment`; the Ptah `params` parity
  extension preserves parameter declarations that cannot be decomposed into
  Atlas-style `arg` blocks without changing their text
- PostgreSQL `view` blocks with `schema`, `as`, `check_option`, and `comment`
- PostgreSQL `materialized` blocks with `schema`, `as`, `refresh_strategy`, and
  `comment`
- PostgreSQL `trigger` blocks with `on`, one of `before`/`after`/`instead_of`,
  `for` or `foreach`, `as`, and `comment`
- PostgreSQL `policy` blocks with `on`, `for`, `to`, `using`, `check`, and
  `comment`
- PostgreSQL `sequence` blocks with `type`, `start`, `increment`, `min_value`,
  `max_value`, `cache`, `cycle`, `owned_by`, `if_not_exists`, and `comment`
- PostgreSQL `domain` blocks with `type`, `null`, `default`, `check`, and
  `comment`
- PostgreSQL `composite` blocks with ordered `field` sub-blocks (each a name
  label and a `type`; quote multi-word types, e.g. `type = "double precision"`)
  and `comment`
- PostgreSQL `range` blocks with `subtype`, `subtype_opclass`, `collation`,
  `canonical`, `subtype_diff`, and `comment`
- Ptah `data` blocks with a table reference, key columns, and a data-file path

Every `schema "pg_catalog" {}` or `schema "information_schema" {}` block is an
explicit schema declaration, even when an extension also refers to it, and is
refused before SQL. To preserve an extension already installed in a
server-owned namespace without requesting `CREATE SCHEMA`, write the placement
as a string, for example `schema = "pg_catalog"`, and omit the schema block.
Ptah's generated HCL uses that spelling. CockroachDB likewise refuses its exact
`crdb_internal` namespace. Quoted lookalikes such as
`schema "PG_CATALOG" {}` or `schema "CRDB_INTERNAL" {}` remain user schemas.

The `extension.schema` attribute accepts an HCL string template or an exact
one-name schema traversal such as `schema.extensions` or
`schema["Extension Store"]`. A string template may evaluate declared variables;
a direct `var.*` traversal, another object namespace such as `table.*`, and an
over-qualified traversal such as `schema.extensions.extra` are refused instead
of being reinterpreted as schema names.

The two-label form `extension "extensions" "citext" {}` uses its first label
as the installation schema. If the block also carries a `schema` attribute,
that value must resolve exactly to the first label. An explicit empty value is
still present, so `schema = ""` conflicts with a nonempty schema label; on the
one-label form it explicitly selects the target's default schema.

Unsupported schema semantics are rejected with an explicit parse error instead
of being silently dropped from the generated Ptah IR.

### Unknown names: strict on Ptah's own commands, tolerant on the Atlas surface

Whether an unmodeled name is a parse error depends on which surface reads the
file, because the two surfaces answer to different contracts.

**Strict — an unmodeled name is a parse error.** This covers `--schema-file`,
every other path through Ptah's own schema loading, the schema artifact reader,
the Go-annotation exporter's render-then-reparse fidelity check, **and the
native `ptah schema apply`, `ptah schema diff`, `ptah schema inspect` and
`ptah schema plan` commands**. Here the file is read by Ptah's own CLI, an
unmodeled name is almost always a typo, and naming it is more useful than
dropping it. The exporter check is the sharper reason: it re-parses HCL that
Ptah itself rendered, so tolerance there would let a renderer that emits a
misspelled attribute pass unnoticed.

**Tolerant — an unmodeled name is dropped.** This covers the Atlas-compatible
command tree only: `ptah-compat schema apply`, `schema inspect`, `schema diff`,
`schema plan`, `schema plan validate` and `migrate diff` reading a `file://`
HCL source. Those files are written for another tool, which accepts constructs
Ptah does not model and drops them without a word; refusing the whole file where
that tool proceeds makes Ptah a non-starter as a replacement.

The split is the one stokaro/ptah#1016 left open, and it is a split in the
command tree, not in the file format: the same `schema.hcl` is refused by
`ptah schema inspect` and accepted by `ptah-compat schema inspect`.

Measured on the community Atlas binary with `schema inspect -u file://...`,
each time comparing the emitted DDL of a schema carrying the construct against
the same schema with it deleted: a top-level `annotation` block, a table-nested
`annotation` block, an unknown block nested in a `column` or an `index` body, an
`invisible = true` column attribute, and an unknown attribute in the `column`,
`table`, `index`, `schema`, `primary_key`, `foreign_key`, `check`, `enum` and
`view` positions all come back exit 0 with byte-identical DDL. Nonsense controls
(`frobnicate_nonsense`, `zzz_nonsense_attr`, `zzz_nonsense_block`) behave the
same as the real names in every one of those positions, which is what makes this
a general "drop names I do not model" policy rather than support for any
particular name. The tolerance therefore covers every position, not a shortlist.

Three limits are deliberate.

Tolerance is name-level, never subtree-level. The body of a dropped construct is
still *evaluated*, so `annotation { gql = "Thing" }` is accepted while each of
these is refused, exactly as the community binary refuses them:

| inside a dropped construct | diagnostic |
| --- | --- |
| `ref = not_a_real_identifier` | unknown variable |
| `ref = variable.v` (the root is `var`, never `variable`) | unknown variable |
| `ref = frobnicate_nonsense("a")` | call to unknown function |
| `ref = 1 + "abc"` | invalid operand |
| `ref = 1 + string` | invalid operand |

Tolerance never repairs structure. A construct whose only nested block was
dropped is still incomplete: `partition { type = "HASH"  zzz_nonsense {} }`
fails with `partition requires columns attribute or by blocks`.

The scope a dropped body is evaluated in is *closed*. It holds three names —
`string`, `int` and `bool`, the only bare identifiers measured to resolve inside
a dropped body on every dialect — plus a measured function table (`format`,
`join`, `jsonencode`, `lower`, `split`, `title`, `trimspace`, `upper`). Nothing
in it is derived from the file being parsed, so a reference to the file's own
blocks or variables does not resolve:

| inside a dropped construct | community binary | Ptah |
| --- | --- | --- |
| `ref = table.t`, `ref = table.t.column.id` | exit 0 | refused, unknown variable `table` |
| `ref = column.id` inside a table body | exit 0 | refused, unknown variable `column` |
| `ref = var.v` with `variable "v"` declared | exit 0 | refused, unknown variable `var` |
| `ref = attr.name` naming a block nested in the dropped one | exit 0 | refused, unknown variable `attr` |
| a call to a function outside the table above | exit 0 | refused, call to unknown function |

Each of those refuses a file the community binary accepts, which is the safe
direction — it costs a user an error message, where the opposite direction would
load a schema the real tool would have rejected. It is the direction this parser
takes on purpose, because the alternative is to model the file's blocks and
variables as reference roots, and every attempt to do that has had to stand in
for something it could not enumerate — an unlabeled block, a variable of unknown
type. Each such stand-in turned into an *accept*-where-the-binary-refuses
divergence: `var.v.nope` on a string variable, `1 + var.v`, `primary_key.nope`,
`inner["typo"]`, `table.t.column` and eight more, all measured. A closed scope
cannot have that failure mode, because there is nothing in it whose members are
guessed.

`partition` is dialect-split on the community binary and cannot be pinned to one
verdict by a dialect-agnostic parser: `type = HASH` unquoted is exit 1 on MySQL
(`There is no variable named "HASH"`) and exit 0 on PostgreSQL, which models
partitions. `type = "HASH"` quoted is exit 0 on both, and on MySQL the whole
`partition` block is dropped from the emitted DDL. What is dialect-independent,
and is pinned, is that `HASH` is not a reference root: `annotation { ref = HASH }`
is exit 1 on both dialects and here.

Dropping an unknown column attribute changes the DDL Ptah emits, so a
misspelled attribute (`nul = false`, `uniqe = true`) becomes an inert no-op on
the tolerant surface rather than a diagnostic. That is the behavior the
Atlas-compatible surface has to reproduce, and it is the reason the strict
default is kept on Ptah's own commands. Callers that want to say something about
what was dropped can pass `atlashcl.Options.RecordIgnored`, the schema-HCL
counterpart of the atlas.hcl parser's `Config.IgnoredConstructs`.

## Ptah Go-annotation parity extensions

Ptah accepts the Atlas-compatible subset above and a small set of explicitly
documented extensions. The extensions preserve Go annotation semantics that
the compatible shape cannot represent; they are Ptah syntax and should not be
assumed to work in the Atlas CLI.

```hcl
schema "app" {}

table "users" {
  schema = schema.app
  checks = ["id > 0"]
  custom = "WITHOUT OIDS"

  platform "mysql" {
    override "engine" {
      value = "InnoDB"
    }
  }

  column "status" {
    type = enum_user_status
    enum = ["active", "disabled"]
  }

  column "score" {
    type       = "DOUBLE PRECISION"
    check      = "score >= 0"
    check_name = "users_score_nonnegative"

    platform "mysql" {
      override "type" {
        value = "BIGINT"
      }
    }
  }

  constraint "users_no_overlap" {
    type      = "EXCLUDE"
    using     = "gist"
    elements  = "id WITH ="
    condition = "id > 0"
    comment   = "No overlapping identifiers"
  }
}

role "app_user" {
  login    = true
  password = "SCRAM-SHA-256$..."
}

function "lookup_user" {
  schema = schema.app
  params = "IN user_id BIGINT, OUT display_value DOUBLE PRECISION"
  return = "DOUBLE PRECISION"
  lang   = "SQL"
  as     = "SELECT user_id::double precision"
}

data {
  table = table.app.users
  keys  = ["id"]
  file  = "users.yaml"
}
```

The `data.file` path is relative to the HCL file. Go annotation export rebases
paths that were relative to a Go source file so the same data file is loaded
after migration. Role passwords remain string literals in the exported HCL;
treat those files as sensitive.

Function, view, materialized-view, and trigger bodies remain raw SQL strings.
Go annotation export emits them as opaque HCL text and reports a warning for
each body because Ptah does not structurally parse dialect-specific SQL
sub-languages. A separate diagnostic reports Unicode normalization that changes
source bytes. These warnings block `--cleanup-go-annotations`, so destructive
cleanup cannot discard the only source while structural completeness remains
unproven. Export without cleanup, review the emitted bodies, remove all Ptah
schema annotations manually in one change, and switch the project to the HCL
source. Do not rerun export after manual removal starts; an export with no
annotations or no exportable HCL object fails without replacing an existing HCL
file.

Cleanup also verifies every recognized standalone annotation against the Go AST
and the parsed schema before publishing HCL. A directive in an unsupported
location, such as a role annotation attached to a constant, or a file-scoped RLS
directive that did not produce an RLS object makes cleanup fail with the source
file and line. Near-prefix comments such as `//ptah:schema:tableau` are not Ptah
directives and remain untouched. Move a recognized directive to one of the
placements listed in the Go annotation reference or remove it explicitly after
review; cleanup never guesses whether ignored text is disposable.

## Minimal Example

```hcl
schema "main" {}

table "users" {
  schema = schema.main

  column "id" {
    type = int
  }

  column "email" {
    type = varchar(255)
    null = false
  }

  column "bio" {
    type = text
  }

  primary_key {
    columns = [column.id]
  }

  index "idx_users_email" {
    unique = true
    columns = [column.email]
  }

  index "idx_users_bio" {
    type = FULLTEXT
    parser = ngram
    columns = [column.bio]
  }
}
```

## PostgreSQL Primary Key Include Example

```hcl
table "users" {
  column "id" {
    type = int
  }

  column "covering" {
    type = int
  }

  primary_key {
    columns = [column.id]
    include = [column.covering]
  }
}
```

## PostgreSQL Index Include Example

```hcl
table "users" {
  column "name" {
    type = text
  }

  column "active" {
    type = bool
  }

  index "idx_users_name" {
    columns = [column.name]
    include = [column.active]
  }
}
```

## PostgreSQL BRIN Storage Parameter Example

```hcl
table "users" {
  column "c" {
    type = int
  }

  index "idx_users_c" {
    type = BRIN
    columns = [column.c]
    page_per_range = 2
  }
}
```

## PostgreSQL NULLS NOT DISTINCT Example

```hcl
table "users" {
  column "c" {
    type = int
  }

  index "users_c_idx" {
    unique = true
    columns = [column.c]
    nulls_distinct = false
  }

  unique "users_c_key" {
    columns = [column.c]
    include = [column.covering]
    nulls_distinct = false
  }

  column "covering" {
    type = int
  }
}
```

## Foreign Key Example

```hcl
table "users" {
  column "id" {
    type = int
  }

  primary_key {
    columns = [column.id]
  }
}

table "posts" {
  column "id" {
    type = int
  }

  column "owner_id" {
    type = int
    null = true
  }
  column "slug" {
    type = text
    as = "lower(name)"
  }
  column "name_key" {
    type = text
    as {
      expr = "lower(name)"
      type = STORED
    }
  }

  foreign_key "owner_id" {
    columns = [column.owner_id]
    ref_columns = [table.users.column.id]
    on_delete = SET_NULL
  }
}
```

## PostgreSQL Identity Columns

Atlas-style `identity` blocks map to PostgreSQL `GENERATED ... AS IDENTITY`
columns:

```hcl
table "users" {
  column "id" {
    type = int
    null = false
    identity {
      generated = BY_DEFAULT
      start = 10
      increment = 5
    }
  }
}
```

`generated` accepts `ALWAYS` or `BY_DEFAULT`. When omitted, Ptah follows
PostgreSQL and Atlas defaults and renders `BY DEFAULT`. The identity block
supports `start`, `increment`, and an `options` string for raw sequence options
(rendered inside `GENERATED ... AS IDENTITY (...)`). Other identity block
attributes are rejected instead of being silently dropped.

## PostgreSQL Schema Objects

Ptah accepts the Atlas-style HCL object blocks that map to its current
`goschema.Database` IR. These blocks are primarily useful with PostgreSQL-family
rendering.

```hcl
schema "public" {}
schema "extensions" {}

extension "pg_trgm" {
  schema        = schema.extensions
  if_not_exists = true
  version       = "1.6"
  comment       = "trigram search"
}

sequence "order_number_seq" {
  schema    = schema.public
  type      = bigint
  start     = 1000
  increment = 1
  cache     = 10
  cycle     = false
}

domain "email" {
  schema = schema.public
  type   = text
  null   = false
  check  = "VALUE ~ '@'"
}

composite "address" {
  schema = schema.public
  field "street" {
    type = text
  }
  field "zip" {
    type = integer
  }
}

range "floatrange" {
  schema       = schema.public
  subtype      = float8
  subtype_diff = float8mi
}

role "app_user" {
  login   = true
  inherit = true
  comment = "application role"
}

table "users" {
  schema = schema.public

  column "id" {
    type = int
  }

  row_security {
    enabled = true
    comment = "tenant isolation"
  }
}

function "get_current_tenant" {
  schema     = schema.public
  lang       = SQL
  return     = text
  security   = INVOKER
  volatility = STABLE
  as         = "SELECT current_setting('app.tenant_id', true)"
}

view "active_users" {
  schema  = schema.public
  as      = "SELECT id FROM users WHERE deleted_at IS NULL"
  comment = "active users"
}

materialized "user_stats" {
  schema           = schema.public
  as               = "SELECT count(*) FROM users"
  refresh_strategy = "concurrently"
}

trigger "users_set_updated_at" {
  on = table.users
  before {
    update = true
  }
  for = ROW
  as  = "NEW.updated_at = now(); RETURN NEW;"
}

policy "users_tenant_policy" {
  on    = table.users
  for   = SELECT
  to    = [role.app_user, PUBLIC]
  using = "get_current_tenant() IS NOT NULL"
}

permission {
  to         = role.app_user
  for        = table.users
  privileges = [SELECT, INSERT]
  grantable  = true
}

permission {
  to         = PUBLIC
  for        = schema.public
  privileges = [USAGE]
}
```

Ptah intentionally supports the subset it can round-trip through its IR. For
example, `row_security.enforced`, materialized-view column blocks, trigger
`execute` blocks, policy `as`, and permission targets other than `table`,
`schema`, or `sequence` are rejected instead of being accepted and dropped.
Function arguments are accepted as Atlas `arg` blocks. Ptah also accepts a raw
`params` string and rejects a function that mixes the two representations.

Some PostgreSQL object blocks documented by Atlas are gated by Atlas plans at
runtime. Ptah can still preserve their HCL shape for schema input/export; this
does not imply that every Atlas CLI command will apply those objects in Atlas
OSS.

## Current Limitations

The HCL schema frontend is intentionally conservative. It does not yet model
Atlas features that Ptah cannot represent without losing semantics, including:

- forced row-level security (`row_security.enforced`)
- grantor metadata
- function options outside Ptah's current IR, such as `leakproof`, `parallel`,
  `return_set`, `return_table`, `config_params`, and argument defaults
- view/materialized-view column metadata
- trigger `execute`, `referencing`, `when`, constraint, and deferrable metadata
- policy `as`
- permission targets other than schema, table, and sequence
- HCL objects outside direct schema definitions, such as realms and other
  dialect-specific object types

A top-level `env` block is refused. It marks the file as an `atlas.hcl` project
file rather than a schema file, and a project file holds no schema objects, so
reading one as a schema would produce an empty desired state and plan to drop
every table the real schema defines. The error names the offending block and its
position. Only a top-level `env` block is treated this way: an `env` attribute is
untouched, and a nested `env` block keeps reporting the surrounding object's own
error. Command-level `atlas.hcl` project config support is documented in
[Atlas Project Config](atlas_project_config.md).

One document declares each object once. A document that declares the same
object twice is refused, naming the kind and the object, because the second
declaration would otherwise be folded into the first and the run would report
success on a document PostgreSQL refuses. Identity is the object's own: `users`
and `Users` are two tables, one table name in two schemas is two tables, and an
index, constraint or foreign key name belongs to its table.

Which kinds refuse is decided by one measured rule: a repeat is refused where
the Atlas community CLI refuses the same document. That is `table`, `column`,
`index`, a named `check` or `constraint`, `foreign_key` (both the single-column
form written onto a column and the multi-column form written as a table
constraint), `enum`, `sequence`, `domain`, `composite`, `range`, `extension`,
`trigger` and `policy`.

Repeats that CLI reads at exit 0 are read at exit 0 here too: `view`,
`materialized` and `role`, whose blocks it drops unread, and `unique`,
`primary_key`, `row_security` and `variable`, which it merges. Setting
`PTAH_HCL_STRICT_REDECLARATIONS=1` refuses a repeated `view`, `materialized`,
`role` or `unique` as well, which is the rule an HCL schema *directory* already
applies across its files.

Four blocks are exempt under every setting: a repeated `schema` block, because a
directory of HCL files is read as one document and its files each open with the
same one; `function`, whose identity in PostgreSQL includes its argument types so
two blocks sharing a name can be two overloads; `permission`, which renders a
GRANT the engine accepts twice; and `data`, which declares no database object.

Setting `PTAH_HCL_MERGE_REDECLARATIONS=1` restores the merge for every kind.

An `enum` block accepts one or two labels. `enum "public" "mood"` names the
schema and then the enum, exactly as `table` does, and is the spelling the Atlas
community CLI's own inspect writes when one enum name exists in more than one
schema. By default two enums sharing a bare name are one object however they are
spelled, because that CLI keys enums by their bare name and answers
`duplicate enum "mood"`. Setting `PTAH_HCL_SCHEMA_SCOPED_ENUMS=1` keys them by
their qualified name, which is what `public.mood` and `other.mood` are, and is
the setting under which Ptah reads its own inspect output for such a database
back.

A top-level `variable` block is accepted but not evaluated. References such as
`var.name` are not substituted, and a variable with no default is not reported as
missing, so a schema file relying on variables renders the reference as literal
text. Schema-file variable evaluation is tracked separately in issue #926.
