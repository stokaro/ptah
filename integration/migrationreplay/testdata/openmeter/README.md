# OpenMeter migration files

`add_rate_card_annotations.up.sql` and `credit_filter_cutover.up.sql` are
copied unchanged from `tools/migrate/migrations/` of
https://github.com/openmeterio/openmeter at commit
f04b561f8dd2dafd1e77a02fb4258fab0edbef8f (Apache-2.0). The original names are
`20260814113138_add_rate_card_annotations.up.sql` and
`20260921133354_credit_filter_cutover.up.sql`.

Each file opens its own transaction with `BEGIN;` and closes it with
`COMMIT;`, because OpenMeter applies its history with golang-migrate, which
sends a file as it is. The tests replay them to show that a history written for
such a runner replays without an enclosing transaction.
