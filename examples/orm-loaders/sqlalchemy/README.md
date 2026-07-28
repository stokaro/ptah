# SQLAlchemy external schema loader

This example uses `atlas-provider-sqlalchemy==0.4.1` and
`SQLAlchemy==2.0.50`, both pinned in `requirements.txt`.
`requirements.lock.txt` pins the complete environment that was verified with
Python 3.12.

From this directory:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install --requirement requirements.lock.txt
ptah schema render \
  --config ptah.yaml \
  --allow-external-schema \
  --dialect postgres
```

`--allow-external-schema` is required because the configuration executes a
local program. Review `ptah.yaml` before opting in.

The rendered schema contains:

- `users` and `pets` tables.
- A primary key on each table.
- A unique constraint on `users.email`.
- A `pets.user_id` foreign key that references `users.id` with
  `ON DELETE CASCADE`.
