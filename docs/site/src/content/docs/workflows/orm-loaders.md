---
title: ORM loaders
description: Feed an ORM's schema into Ptah with the external_schema source.
---

Ptah can take its desired schema from an ORM instead of Go annotations or a
static schema file. The [external-command source](../schema-files/#load-from-an-external-program)
runs a program you choose and reads its standard output as the desired schema —
so any tool that prints a schema as SQL DDL becomes a Ptah source.

This is Ptah's open, local, MIT equivalent of Atlas's `data "external_schema"`
and its ORM provider loaders. Because the contract is just "print the desired
schema as SQL to stdout," the same providers the Atlas ecosystem publishes work
unchanged with Ptah.

## The contract

An ORM loader is any program that prints the **complete desired schema** as SQL
DDL to stdout. Ptah runs it (directly, without a shell), parses the SQL, and uses
the result wherever a desired schema is needed:

```bash
ptah schema render   --schema-cmd "<loader>" --dialect postgres
ptah schema compare  --schema-cmd "<loader>" --db-url "$DATABASE_URL"
ptah schema drift    --schema-cmd "<loader>" --db-url "$DATABASE_URL"
ptah migrations plan --schema-cmd "<loader>" --db-url "$DATABASE_URL"
```

Primary keys, foreign keys, unique constraints, and indexes in the emitted DDL
are all captured, so the loaded schema diffs and migrates faithfully.

## GORM (verified)

[`atlas-provider-gorm`](https://github.com/ariga/atlas-provider-gorm) prints the
SQL DDL for a set of GORM models. Write a tiny loader `main.go`:

```go
package main

import (
	"fmt"
	"io"
	"os"

	"ariga.io/atlas-provider-gorm/gormschema"
	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	Name  string `gorm:"type:varchar(255);not null"`
	Email string `gorm:"uniqueIndex;type:varchar(255)"`
	Pets  []Pet
}

type Pet struct {
	gorm.Model
	Name   string `gorm:"type:varchar(100)"`
	UserID uint
}

func main() {
	stmts, err := gormschema.New("postgres").Load(&User{}, &Pet{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "load gorm schema: %v\n", err)
		os.Exit(1)
	}
	io.WriteString(os.Stdout, stmts)
}
```

Then point Ptah at it:

```bash
ptah schema render --schema-cmd "go run ." --dialect postgres
```

or declare it once in `ptah.yaml` (an explicit argument list, so arguments may
contain spaces):

```yaml
external_schema:
  program: ["go", "run", "."]
```

```bash
ptah migrations plan --config ptah.yaml --db-url "$DATABASE_URL"
```

The generated schema includes the `users` and `pets` tables with their primary
keys, the unique index on `email`, and the `pets → users` foreign key.

## Other ORMs

The Atlas ecosystem publishes schema loaders for many ORMs (GORM, Django,
SQLAlchemy, Sequelize, TypeORM, Hibernate, and more). Each one prints SQL DDL to
stdout, so it plugs into `--schema-cmd` / `external_schema` exactly like the GORM
example above — only the `program` changes. Any tool that can emit your schema as
SQL works too, for example `prisma migrate diff` writing a SQL script, or a
hand-written script that runs your framework's schema dumper.

The Ptah side is always identical — you just supply the loader command:

```yaml
external_schema:
  program: ["<the loader command and its arguments>"]
```

Consult the provider's documentation for the exact loader command (the Atlas
provider packages are named `atlas-provider-<orm>`), then run it once by hand to
confirm it prints SQL DDL before wiring it into Ptah. Only GORM is verified
against Ptah here; treat the others as a starting point and check the emitted DDL
parses cleanly.

## Notes

- **Format**: the external command's output is parsed as SQL (`--schema-format
  sql`, the default). HCL and YAML output formats are not yet supported by the
  external source.
- **No shell**: the program is run with an explicit argument vector, so shell
  features (pipes, globbing, variable expansion) are not available. Wrap a
  complex invocation in a small script and point the loader at that.
- **Timeout and errors**: execution is bounded by a timeout, and if the program
  exits non-zero its stderr is surfaced in the error.
