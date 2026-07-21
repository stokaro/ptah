---
title: YAML schema example
description: A minimal Ptah YAML schema file.
---

```yaml
tables:
  accounts:
    columns:
      id:
        type: int
        primary_key: true
      email:
        type: varchar
        nullable: false
    indexes:
      accounts_email_key:
        columns: [email]
        unique: true
```

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

Use YAML for Ptah-owned schema files. See the full reference in [YAML schema](https://github.com/stokaro/ptah/blob/master/docs/yaml_schema.md).
