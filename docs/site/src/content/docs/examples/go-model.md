---
title: Go model example
description: A tiny annotated Go model and the rendered SQL path.
---

Create a model:

```go
package models

//migrator:schema:table name="accounts"
type Account struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int

	//migrator:schema:field name="email" type="TEXT" unique="true" not_null="true"
	Email string
}
```

Render it:

```bash
ptah schema render --root-dir ./models --dialect postgres
```

Expected shape:

```sql
CREATE TABLE "accounts" (
  "id" SERIAL NOT NULL,
  "email" VARCHAR(255) NOT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT "uni_accounts_email" UNIQUE ("email")
);
```

The exact type rendering depends on the selected dialect and field tags.
