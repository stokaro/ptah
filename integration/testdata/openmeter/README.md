# OpenMeter schema export

Both files derive from the SQL that OpenMeter's Ent schema exporter writes at
commit `f04b561f8dd2dafd1e77a02fb4258fab0edbef8f` of
https://github.com/openmeterio/openmeter, run as
`go run -mod=readonly ./tools/migrate/cmd/entschema`. OpenMeter is licensed
under the Apache License 2.0. `LICENSE` is OpenMeter's license file at that
commit, unchanged.

`ent-export.sql` is that output, unchanged apart from its header and the
checkout path in its `atlas:pos` comments.

`long-names.sql` is reduced from the same output, for the tests of the names
PostgreSQL cuts to 63 bytes. It keeps:

- every foreign key and index whose name is longer than 63 bytes;
- the export's closing `ALTER TABLE "ledger_breakage_records"`, which makes
  five keys deferrable and names them by the 63 bytes the server keeps, and
  the two keys of shorter names among them;
- the tables those keys and indexes belong to or reference, in the export's
  order, each with its primary key and the columns the kept keys and indexes
  use.

The `atlas:pos` comments, the view, and every other table, column, key and
index are left out. What remains keeps the exporter's spelling line for line;
a `CREATE TABLE` or an `ALTER TABLE ... ADD` keeps only the lines it needs.
PostgreSQL 15.19 runs the file and prints one truncation notice for each long
name. `TestOpenMeterLongNamesCutWhatTheExportCuts` holds the reduction to the
export: the reader cuts the same names in both files.
