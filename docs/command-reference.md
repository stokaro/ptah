# Command reference

Every command path of every command tree this repository ships, rendered from
the trees themselves. Regenerate it with `scripts/check-command-reference.sh --write`.

This is the reference half of [`feature-inventory.md`](feature-inventory.md),
which is the hand-written half. A path appears here because a binary answers it;
a path appears there because somebody decided which feature owns it.

## What the strict-mode column says

`PTAH_ATLAS_STRICT_COMPAT=1` does not narrow behavior inside a fixed
`ptah-compat`; it registers a different tree. 53 paths become 25, and the 28
that go do not go the same way.

| Value | Meaning |
| --- | --- |
| `not applicable` | A native path. The selector is a `ptah-compat` contract. |
| `registered` | The strict profile still offers it. 25 paths. |
| `gated` | Registered and hidden, so invoking it produces a named abort rather than `unknown command`. 12 paths. |
| `not registered` | Absent from the strict tree. 16 paths. |

A reference without that column describes one of two shipped surfaces.

## Every path

<!-- BEGIN GENERATED COMMAND REFERENCE -->
| Path | Tree | Kind | Strict mode | Summary |
| --- | --- | --- | --- | --- |
| `ptah` | `ptah` | root | not applicable | Ptah inspects, defines, compares, visualizes, tests, and changes database schemas |
| `ptah assist` | `ptah` | group | not applicable | Work with Ptah through a model you supply |
| `ptah assist context` | `ptah` | verb | not applicable | Print what a question would send to the model provider, and send nothing |
| `ptah assist explain` | `ptah` | verb | not applicable | Ask a model about this project, with Ptah's tools answering |
| `ptah assist provider` | `ptah` | group | not applicable | Inspect and test the model providers this machine can reach |
| `ptah assist provider list` | `ptah` | verb | not applicable | List the configured provider profiles |
| `ptah assist provider test` | `ptah` | verb | not applicable | Check that a provider profile works, by measuring it |
| `ptah assist sessions` | `ptah` | group | not applicable | List, read and remove saved Ptah Assist conversations |
| `ptah assist sessions delete` | `ptah` | verb | not applicable | Remove one saved conversation |
| `ptah assist sessions list` | `ptah` | verb | not applicable | List the saved conversations for this project |
| `ptah assist sessions prune` | `ptah` | verb | not applicable | Remove conversations older than a given age |
| `ptah assist sessions show` | `ptah` | verb | not applicable | Print one saved conversation, including what Ptah did |
| `ptah completion` | `ptah` | group | not applicable | Generate the autocompletion script for the specified shell |
| `ptah completion bash` | `ptah` | verb | not applicable | Generate the autocompletion script for bash |
| `ptah completion fish` | `ptah` | verb | not applicable | Generate the autocompletion script for fish |
| `ptah completion powershell` | `ptah` | verb | not applicable | Generate the autocompletion script for powershell |
| `ptah completion zsh` | `ptah` | verb | not applicable | Generate the autocompletion script for zsh |
| `ptah db` | `ptah` | group | not applicable | Work with live database schemas |
| `ptah db capabilities` | `ptah` | verb | not applicable | Report the capability profile Ptah resolves for a live database |
| `ptah db drop-all` | `ptah` | verb | not applicable | Drop every schema object in a live database (VERY DANGEROUS!) |
| `ptah db read` | `ptah` | verb | not applicable | Read schema from a live database |
| `ptah help` | `ptah` | verb | not applicable | Help about any command |
| `ptah inference` | `ptah` | group | not applicable | Plan, run and cut over embedding-generation migrations |
| `ptah inference backfill` | `ptah` | verb | not applicable | Embed the source into the new generation, resumably |
| `ptah inference catchup` | `ptah` | verb | not applicable | Process the source changes made while the backfill ran |
| `ptah inference cutover` | `ptah` | verb | not applicable | Make the new generation the one queries read |
| `ptah inference evaluate` | `ptah` | verb | not applicable | Measure what the generation actually retrieves, against a corpus you wrote |
| `ptah inference plan` | `ptah` | verb | not applicable | Show what a generation change would do, and where each answer came from |
| `ptah inference prepare` | `ptah` | verb | not applicable | Create the run's own tables, the outbox, and record the snapshot boundary |
| `ptah inference retire` | `ptah` | verb | not applicable | Destroy a generation, which cannot be undone |
| `ptah inference rollback` | `ptah` | verb | not applicable | Put the previous generation back, if it is still a place to go back to |
| `ptah inference status` | `ptah` | verb | not applicable | Show what a run has done and what it is waiting for |
| `ptah inference verify` | `ptah` | verb | not applicable | Run the deterministic checks a cutover rests on |
| `ptah introspect` | `ptah` | verb | not applicable | Generate annotated Go models from a live database |
| `ptah license` | `ptah` | verb | not applicable | Print license and attribution information |
| `ptah mcp` | `ptah` | verb | not applicable | Serve Ptah's operations over the Model Context Protocol |
| `ptah migrations` | `ptah` | group | not applicable | Manage migration plans, files, and revision state |
| `ptah migrations baseline` | `ptah` | verb | not applicable | Record existing migrations as applied |
| `ptah migrations checkpoint` | `ptah` | verb | not applicable | Squash history into a checkpoint |
| `ptah migrations create` | `ptah` | verb | not applicable | Create empty migration files for manual SQL |
| `ptah migrations data` | `ptah` | verb | not applicable | Generate a migration from reference/seed data drift |
| `ptah migrations down` | `ptah` | verb | not applicable | Roll back migrations |
| `ptah migrations edit` | `ptah` | verb | not applicable | Edit a migration and re-hash |
| `ptah migrations generate` | `ptah` | verb | not applicable | Generate migration files from schema differences |
| `ptah migrations hash` | `ptah` | verb | not applicable | Write or update migration directory integrity |
| `ptah migrations import` | `ptah` | verb | not applicable | Import migrations from another tool |
| `ptah migrations lint` | `ptah` | verb | not applicable | Lint migration files |
| `ptah migrations ls` | `ptah` | verb | not applicable | List the migration files in a directory |
| `ptah migrations plan` | `ptah` | verb | not applicable | Plan migration SQL from schema differences |
| `ptah migrations pull` | `ptah` | verb | not applicable | Pull a migration directory from an OCI registry |
| `ptah migrations push` | `ptah` | verb | not applicable | Push a migration directory to an OCI registry |
| `ptah migrations rebase` | `ptah` | verb | not applicable | Move a migration to the end of history |
| `ptah migrations repair` | `ptah` | verb | not applicable | Repair migration revision metadata |
| `ptah migrations rm` | `ptah` | verb | not applicable | Delete a migration and re-hash |
| `ptah migrations set` | `ptah` | verb | not applicable | Set the revision boundary to a version |
| `ptah migrations show` | `ptah` | verb | not applicable | Print the SQL of one or more migrations |
| `ptah migrations status` | `ptah` | verb | not applicable | Show migration status |
| `ptah migrations tag` | `ptah` | verb | not applicable | Record, list, or remove a migration tag |
| `ptah migrations test` | `ptah` | verb | not applicable | Run declarative migration tests |
| `ptah migrations up` | `ptah` | verb | not applicable | Run pending migrations |
| `ptah migrations validate` | `ptah` | verb | not applicable | Validate migration directory integrity |
| `ptah oci` | `ptah` | group | not applicable | Inspect Ptah artifacts in OCI registries |
| `ptah oci capabilities` | `ptah` | verb | not applicable | Report what the registry behind a reference supports |
| `ptah oci copy` | `ptah` | verb | not applicable | Copy an artifact between repositories without rebuilding it |
| `ptah oci fetch` | `ptah` | verb | not applicable | Download the payload of metadata attached to an OCI artifact |
| `ptah oci inspect` | `ptah` | verb | not applicable | Report what an OCI artifact declares, without downloading it |
| `ptah oci login` | `ptah` | verb | not applicable | Store a credential for a registry |
| `ptah oci logout` | `ptah` | verb | not applicable | Remove the credential Ptah stored for a registry |
| `ptah oci referrers` | `ptah` | verb | not applicable | List metadata attached to an OCI artifact |
| `ptah oci reindex` | `ptah` | verb | not applicable | Republish attachments the registry's referrers index does not list |
| `ptah oci resolve` | `ptah` | verb | not applicable | Resolve a mutable tag to the immutable digest it names |
| `ptah oci tag` | `ptah` | verb | not applicable | Move an alias onto an artifact that already exists |
| `ptah oci tags` | `ptah` | verb | not applicable | List the tags a repository carries |
| `ptah oci verify` | `ptah` | verb | not applicable | Check an artifact against a verification policy before it is consumed |
| `ptah project` | `ptah` | group | not applicable | Read a project file and report what Ptah makes of it |
| `ptah project adopt` | `ptah` | verb | not applicable | Adopt this project into native Ptah, or report what that would take |
| `ptah project inspect` | `ptah` | verb | not applicable | Report what Ptah reads from a project file, and what it ignores |
| `ptah schema` | `ptah` | group | not applicable | Work with desired schema definitions |
| `ptah schema annotations` | `ptah` | verb | not applicable | Export Ptah Go annotation metadata |
| `ptah schema apply` | `ptah` | verb | not applicable | Apply a desired schema directly to a database |
| `ptah schema approve` | `ptah` | verb | not applicable | Sign a saved plan, recording that it was reviewed |
| `ptah schema compare` | `ptah` | verb | not applicable | Compare desired schema with a live database |
| `ptah schema diff` | `ptah` | verb | not applicable | Diff two arbitrary schema states |
| `ptah schema drift` | `ptah` | verb | not applicable | Check live database drift against desired schema |
| `ptah schema export` | `ptah` | verb | not applicable | Export one schema source format to another |
| `ptah schema fmt` | `ptah` | verb | not applicable | Format HCL schema files |
| `ptah schema inspect` | `ptah` | verb | not applicable | Inspect a schema as machine-clean HCL, SQL, or JSON |
| `ptah schema lineage` | `ptah` | verb | not applicable | Trace which base columns feed each view column |
| `ptah schema plan` | `ptah` | verb | not applicable | Save a fingerprinted declarative apply plan |
| `ptah schema pull` | `ptah` | verb | not applicable | Pull a desired schema from an OCI registry |
| `ptah schema push` | `ptah` | verb | not applicable | Push a desired schema to an OCI registry |
| `ptah schema render` | `ptah` | verb | not applicable | Render desired schema SQL |
| `ptah schema security` | `ptah` | verb | not applicable | Report security findings over a live schema's roles, grants and policies |
| `ptah schema serve` | `ptah` | verb | not applicable | Serve a live view of the schema and the database it describes |
| `ptah schema stats` | `ptah` | verb | not applicable | Count the objects in a live schema and emit them as OpenMetrics |
| `ptah schema test` | `ptah` | verb | not applicable | Run declarative schema tests |
| `ptah schema validate` | `ptah` | verb | not applicable | Report structural problems in a desired schema without a database |
| `ptah schema verify-approval` | `ptah` | verb | not applicable | Check that a saved plan carries an approval from an allowed signer |
| `ptah seed` | `ptah` | verb | not applicable | Apply environment-scoped SQL seed files |
| `ptah sql` | `ptah` | group | not applicable | Work with standalone SQL files |
| `ptah sql lint` | `ptah` | verb | not applicable | Lint standalone SQL files |
| `ptah version` | `ptah` | verb | not applicable | Print Ptah version information |
| `ptah viz` | `ptah` | verb | not applicable | Render desired schema diagrams |
| `ptah-compat` | `ptah-compat` | root | registered | Atlas-compatible Ptah command tree |
| `ptah-compat completion` | `ptah-compat` | group | registered | Generate the autocompletion script for the specified shell |
| `ptah-compat completion bash` | `ptah-compat` | verb | registered | Generate the autocompletion script for bash |
| `ptah-compat completion fish` | `ptah-compat` | verb | registered | Generate the autocompletion script for fish |
| `ptah-compat completion powershell` | `ptah-compat` | verb | registered | Generate the autocompletion script for powershell |
| `ptah-compat completion zsh` | `ptah-compat` | verb | registered | Generate the autocompletion script for zsh |
| `ptah-compat help` | `ptah-compat` | verb | registered | Help about any command |
| `ptah-compat license` | `ptah-compat` | verb | registered | Print license information |
| `ptah-compat migrate` | `ptah-compat` | group | registered | Atlas migrate commands |
| `ptah-compat migrate apply` | `ptah-compat` | verb | registered | Apply pending migrations |
| `ptah-compat migrate checkpoint` | `ptah-compat` | verb | gated | Squash migration history into a cumulative-schema checkpoint |
| `ptah-compat migrate diff` | `ptah-compat` | verb | registered | Compute migration diff against a desired schema |
| `ptah-compat migrate down` | `ptah-compat` | verb | gated | Roll back migrations |
| `ptah-compat migrate edit` | `ptah-compat` | verb | gated | Edit a migration file and update the directory checksum |
| `ptah-compat migrate hash` | `ptah-compat` | verb | registered | Write or update the migration directory checksum |
| `ptah-compat migrate import` | `ptah-compat` | verb | registered | Import migrations from another tool |
| `ptah-compat migrate lint` | `ptah-compat` | verb | registered | Lint migration files |
| `ptah-compat migrate ls` | `ptah-compat` | verb | not registered | List the migration files in the directory |
| `ptah-compat migrate new` | `ptah-compat` | verb | registered | Create a new migration file |
| `ptah-compat migrate push` | `ptah-compat` | verb | gated | Push migration directory to a remote registry |
| `ptah-compat migrate rebase` | `ptah-compat` | verb | gated | Move a migration to the end of history and update the directory checksum |
| `ptah-compat migrate rm` | `ptah-compat` | verb | gated | Remove a migration file and update the directory checksum |
| `ptah-compat migrate set` | `ptah-compat` | verb | registered | Set migration revision state |
| `ptah-compat migrate show` | `ptah-compat` | verb | not registered | Print the contents of one or more migration files |
| `ptah-compat migrate status` | `ptah-compat` | verb | registered | Show migration status |
| `ptah-compat migrate test` | `ptah-compat` | verb | gated | Run declarative migration tests against a dev database |
| `ptah-compat migrate validate` | `ptah-compat` | verb | registered | Validate migration directory integrity |
| `ptah-compat schema` | `ptah-compat` | group | registered | Atlas schema commands |
| `ptah-compat schema apply` | `ptah-compat` | verb | registered | Apply a desired schema to a database |
| `ptah-compat schema clean` | `ptah-compat` | verb | registered | Clean database schema objects |
| `ptah-compat schema diff` | `ptah-compat` | verb | registered | Diff desired schema against another schema |
| `ptah-compat schema fmt` | `ptah-compat` | verb | registered | Format schema files |
| `ptah-compat schema inspect` | `ptah-compat` | verb | registered | Inspect a database schema |
| `ptah-compat schema plan` | `ptah-compat` | group | gated | Plan a declarative migration for a schema transition |
| `ptah-compat schema plan approve` | `ptah-compat` | verb | not registered | Approve a plan in a remote registry |
| `ptah-compat schema plan lint` | `ptah-compat` | verb | not registered | Run analysis (migration linting) on a plan file |
| `ptah-compat schema plan list` | `ptah-compat` | verb | not registered | List plans in a remote registry |
| `ptah-compat schema plan new` | `ptah-compat` | verb | not registered | Create a new plan file for the schema transition |
| `ptah-compat schema plan pull` | `ptah-compat` | verb | not registered | Pull a plan from a remote registry |
| `ptah-compat schema plan push` | `ptah-compat` | verb | not registered | Push a plan to a remote registry |
| `ptah-compat schema plan rm` | `ptah-compat` | verb | not registered | Remove a plan from a remote registry |
| `ptah-compat schema plan test` | `ptah-compat` | verb | not registered | Run schema plan tests |
| `ptah-compat schema plan validate` | `ptah-compat` | verb | not registered | Validate a plan file against the schema transition |
| `ptah-compat schema push` | `ptah-compat` | verb | gated | Push schema state to a remote registry |
| `ptah-compat schema stats` | `ptah-compat` | group | gated | Schema statistics |
| `ptah-compat schema stats inspect` | `ptah-compat` | verb | not registered | Count the objects in a live schema and emit them as OpenMetrics |
| `ptah-compat schema test` | `ptah-compat` | verb | gated | Run declarative schema tests against a dev database |
| `ptah-compat schema validate` | `ptah-compat` | verb | gated | Report structural problems in a desired schema without a database |
| `ptah-compat script` | `ptah-compat` | group | not registered | Run a declared data operation |
| `ptah-compat script exec` | `ptah-compat` | verb | not registered | Run a declared exec script |
| `ptah-compat script loop` | `ptah-compat` | verb | not registered | Run a declared loop script |
| `ptah-compat script query` | `ptah-compat` | verb | not registered | Run a declared query script |
| `ptah-compat version` | `ptah-compat` | verb | registered | Print Ptah version information |
<!-- END GENERATED COMMAND REFERENCE -->
