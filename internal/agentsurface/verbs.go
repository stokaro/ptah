package agentsurface

// verbs classifies every runnable command in the tree.
//
// The entries are grouped the way the command line groups them, and each reason
// is the command's own account of what it does rather than a paraphrase of its
// name. Where a verb's help states the destructive part outright -- "it is reset
// destructively", "Throwaway database URL" -- the reason quotes it, because that
// is the sentence a reader would otherwise have to go and find.
var verbs = map[string]Verb{
	// Top level.
	"help": {TargetNone, ScratchNone,
		"prints the help text of the verb it names, or of the root when it names none; the verb " +
			"itself is not run and nothing is opened"},
	"introspect": {TargetReads, ScratchNone,
		"reads a live database and writes annotated Go models to disk; the database is only read"},
	"license": {TargetNone, ScratchNone,
		"prints license and attribution text compiled into the binary"},
	"mcp": {TargetNone, ScratchNone,
		"opens no connection of its own; it serves other operations to an MCP client, and each " +
			"of those carries its own classification"},
	"seed": {TargetWrites, ScratchNone,
		"applies environment-scoped SQL seed files to the database it is given"},
	"version": {TargetNone, ScratchNone,
		"prints the version, commit and build date compiled into the binary"},
	"viz": {TargetNone, ScratchNone,
		"renders diagrams from a desired schema; no database is opened"},

	// assist.
	"assist context": {TargetNone, ScratchNone,
		"prints what a question would send to a model provider and sends nothing; it opens " +
			"neither a database nor an endpoint, and writes no file"},
	"assist explain": {TargetNone, ScratchNone,
		"opens no connection of its own; it asks a model a question and lets the model call " +
			"Ptah's own tools, each of which carries its own classification"},
	"assist provider list": {TargetNone, ScratchNone,
		"lists the provider profiles configured locally, and opens neither a database nor an endpoint"},
	"assist provider test": {TargetNone, ScratchNone,
		"measures a provider profile by calling the model endpoint it names; no database is involved"},
	"assist sessions delete": {TargetNone, ScratchNone,
		"removes one saved conversation file from the project; no database is opened, and the " +
			"audit log is a separate file it does not touch"},
	"assist sessions list": {TargetNone, ScratchNone,
		"lists the conversations saved under the project's .ptah directory; neither a database " +
			"nor a model endpoint is opened"},
	"assist sessions prune": {TargetNone, ScratchNone,
		"removes saved conversation files untouched for longer than a given age; no database is " +
			"opened, and the audit log is left alone"},
	"assist sessions show": {TargetNone, ScratchNone,
		"prints one saved conversation from disk, including what Ptah's tools answered during " +
			"it; nothing is opened to do so"},

	// completion. cobra builds this group, not Ptah, so nothing in cmd/ names
	// these four. They are still verbs the shipped binary answers to, and
	// [Walk] reaches them for the reason written there.
	"completion bash": {TargetNone, ScratchNone,
		"writes a bash completion script to stdout, generated from the command tree; it opens " +
			"no database and writes no file"},
	"completion fish": {TargetNone, ScratchNone,
		"writes a fish completion script to stdout, generated from the command tree; it opens " +
			"no database and writes no file"},
	"completion powershell": {TargetNone, ScratchNone,
		"writes a PowerShell completion script to stdout, generated from the command tree; it " +
			"opens no database and writes no file"},
	"completion zsh": {TargetNone, ScratchNone,
		"writes a zsh completion script to stdout, generated from the command tree; it opens " +
			"no database and writes no file"},

	// db.
	"db capabilities": {TargetReads, ScratchNone,
		"reads the server's version and catalogs to report the capability profile Ptah resolves"},
	"db drop-all": {TargetWrites, ScratchNone,
		"drops every schema object in the database it is given"},
	"db read": {TargetReads, ScratchNone,
		"introspects the database and prints what it found"},

	// inference.
	"inference abandon": {TargetWrites, ScratchNone,
		"ends one run and releases its position in shared outbox history; the run-state row " +
			"is written, and this command does not delete the generation or its vectors"},
	"inference backfill": {TargetWrites, ScratchNone,
		"reads the source, sends it to the embedding endpoint the specification names, and " +
			"writes vectors and checkpoints into the target database"},
	"inference catchup": {TargetWrites, ScratchNone,
		"rereads the source rows recorded as changed and writes their vectors, which sends that " +
			"text to the embedding endpoint, and deletes the change records every usable live feeder " +
			"reading that source has processed"},
	"inference cutover": {TargetWrites, ScratchNone,
		"moves the pointer queries read to a different generation, and refuses when the pointer " +
			"is not where the plan it was built from expects"},
	"inference evaluate": {TargetReads, ScratchNone,
		"searches the generation with queries from a corpus, which sends those queries to the " +
			"embedding endpoint; the database is only read"},
	"inference describe": {TargetNone, ScratchNone,
		"reads a specification file and reports what it says; it opens no database, and with " +
			"`--spec` no connection at all, which is what makes it usable where every other " +
			"verb here cannot be; `--release` fetches the release from a registry first"},
	"inference index": {TargetWrites, ScratchNone,
		"builds the generation's vector index concurrently, which writes an index into the " +
			"target database and drops an invalid leftover before rebuilding it"},
	"inference pause": {TargetWrites, ScratchNone,
		"stops a run at the boundary its last checkpoint reached, which writes the run's own " +
			"row: it takes the run for this process, so a worker that was running is refused " +
			"at its next commit"},
	"inference resume": {TargetWrites, ScratchNone,
		"returns a paused run to running, which writes the run's own row and takes the run for " +
			"this process; nothing starts working here"},
	"inference plan": {TargetReads, ScratchNone,
		"resolves a specification against the database and prints what would happen; nothing is " +
			"created and nothing is written"},
	"inference probe": {TargetNone, ScratchNone,
		"sends two fixed strings to the embedding endpoint the specification names and reports " +
			"what came back; it opens no database, and nothing from one is sent"},
	"inference prepare": {TargetWrites, ScratchNone,
		"creates the run's own tables and, under the outbox mode, a companion table and two " +
			"triggers on the source"},
	"inference retire": {TargetWrites, ScratchNone,
		"drops a generation's index and column; it is the one verb here that cannot be undone"},
	"inference rollback": {TargetWrites, ScratchNone,
		"moves the pointer queries read back to its recorded previous generation, when that " +
			"generation is still measurably one you can go back to"},
	"inference status": {TargetReads, ScratchNone,
		"prints a run's phase, progress and watermarks from the run-state tables"},
	"inference verify": {TargetReads, ScratchNone,
		"reads the source and the generation and reports what a cutover would rest on; it writes " +
			"nothing"},

	// migrations.
	"migrations baseline": {TargetWrites, ScratchRewrites,
		"records existing migrations as applied in the target's tracking table, and replays the " +
			"directory into a disposable shadow database to verify it reproduces the schema"},
	"migrations checkpoint": {TargetNone, ScratchRewrites,
		"squashes history into a checkpoint, replaying the directory into an ephemeral shadow " +
			"database; the target is not touched"},
	"migrations create": {TargetNone, ScratchNone,
		"writes an empty up and down migration file pair for someone to fill in by hand"},
	"migrations data": {TargetReads, ScratchNone,
		"reads reference and seed data from the target and writes a migration file for the drift"},
	"migrations down": {TargetWrites, ScratchRewrites,
		"rolls back migrations against the target, after replaying and verifying the rollback " +
			"plan in an ephemeral shadow database"},
	// edit, rebase and rm are one code path with three names. Each rewrites the
	// migration directory through internal/migrateops, which opens nothing, and
	// each reaches a database only through migratemaint.Options.Guard, which
	// reads the applied set and refuses an already-applied version unless
	// --force. So all three read the target and none of them writes it; they
	// carried two different classes until the reference started printing the
	// reason, and a reader met "updates the target's tracking table" for a
	// command that never writes a row.
	"migrations edit": {TargetReads, ScratchNone,
		"rewrites a migration file and re-hashes the directory; the target is read to check " +
			"whether the migration has been applied"},
	"migrations generate": {TargetReads, ScratchRewrites,
		"writes migration files from schema differences; the dev database it replays into \"is " +
			"reset destructively\" and the shadow database verifies the result"},
	"migrations hash": {TargetNone, ScratchNone,
		"writes the directory's integrity file, so a later run can tell a hand-edited migration from an intact one"},
	"migrations import": {TargetNone, ScratchNone,
		"converts another tool's migration directory into Ptah's format on disk"},
	"migrations lint": {TargetNone, ScratchRewrites,
		"lints migration files; the dev database it names is cleaned and replayed into"},
	"migrations ls": {TargetNone, ScratchNone,
		"lists the migration files in a directory, reading nothing but the directory"},
	"migrations plan": {TargetReads, ScratchNone,
		"reads the target and prints the migration SQL the difference implies, writing nothing"},
	"migrations pull": {TargetNone, ScratchNone,
		"downloads a migration directory from an OCI registry and writes it to disk"},
	"migrations push": {TargetNone, ScratchNone,
		"uploads a migration directory from disk to an OCI registry"},
	"migrations rebase": {TargetReads, ScratchNone,
		"re-timestamps a migration to the end of history and re-hashes the directory; the " +
			"target is read to check whether the migration has been applied"},
	"migrations repair": {TargetWrites, ScratchNone,
		"rewrites revision metadata in the target's tracking table"},
	"migrations rm": {TargetReads, ScratchNone,
		"deletes a migration's file pair and re-hashes the directory; the target is read to " +
			"check whether the migration has been applied"},
	"migrations set": {TargetWrites, ScratchNone,
		"sets the revision boundary in the target's tracking table to a named version"},
	"migrations show": {TargetNone, ScratchNone,
		"prints the SQL of one or more migration files, reading nothing but the files"},
	"migrations status": {TargetReads, ScratchNone,
		"reads the target's tracking table and reports which migrations are applied"},
	"migrations tag": {TargetWrites, ScratchNone,
		"records, lists or removes a tag in the target's tracking table; two of the three write"},
	"migrations test": {TargetWrites, ScratchNone,
		"runs declarative test cases against the database named by `--db-url`, whose own help " +
			"calls it a \"Throwaway database URL\": the cases run raw SQL and apply schemas there"},
	"migrations up": {TargetWrites, ScratchNone,
		"runs pending migrations against the target"},
	"migrations validate": {TargetNone, ScratchRewrites,
		"validates the directory against its integrity file; the dev database it names is " +
			"\"used to clean and replay migrations for SQL validation\""},

	// project.
	"project adopt": {TargetReads, ScratchNone,
		"classifies every construct a project file declares as exact, compat-only or " +
			"unsupported; `--check` reports that and writes nothing, the bare verb rewrites " +
			"the compat-only spellings and refuses a project declaring anything " +
			"unsupported, and `--preflight` also reads the revision history in the " +
			"project's database, writing nothing there"},
	"project inspect": {TargetNone, ScratchNone,
		"reads a project file and reports which of its settings Ptah acts on and which it " +
			"read and ignored; it opens no database"},

	// oci.
	"oci capabilities": {TargetNone, ScratchNone,
		"asks the registry behind a reference which features it supports, and prints them"},
	"oci copy": {TargetNone, ScratchNone,
		"copies an artifact between two registry repositories without rebuilding it"},
	"oci fetch": {TargetNone, ScratchNone,
		"downloads the payload of metadata attached to an OCI artifact and writes it to disk"},
	"oci inspect": {TargetNone, ScratchNone,
		"reports what an OCI artifact declares in its manifest, without downloading the payload"},
	"oci login": {TargetNone, ScratchNone,
		"checks a registry credential and stores it; it touches no database and writes only " +
			"Ptah's own credential file"},
	"oci logout": {TargetNone, ScratchNone,
		"removes the credential Ptah stored for a registry, leaving a Docker-placed one alone"},
	"oci referrers": {TargetNone, ScratchNone,
		"asks a registry which metadata artifacts refer to one subject and prints them"},
	"oci reindex": {TargetNone, ScratchNone,
		"republishes attachments a registry's referrers index does not list, so a later query finds them"},
	"oci resolve": {TargetNone, ScratchNone,
		"asks a registry which immutable digest a mutable tag currently names"},
	"oci tag": {TargetNone, ScratchNone,
		"asks a registry to move a tag onto an artifact it already holds; nothing is uploaded"},
	"oci tags": {TargetNone, ScratchNone,
		"asks a registry for the tags one repository carries and prints them"},
	"oci verify": {TargetNone, ScratchNone,
		"checks an artifact against a verification policy before anything consumes it"},

	// schema.
	"schema annotations": {TargetNone, ScratchNone,
		"exports the Go annotation metadata compiled into the binary, as JSON or a JSON Schema"},
	"schema apply": {TargetWrites, ScratchRewrites,
		"applies a desired schema to the target; the dev database is where \"the plan is " +
			"rehearsed on before touching the target\""},
	"schema approve": {TargetNone, ScratchNone,
		"signs a saved plan file with an SSH key and writes the signature beside it"},
	"schema compare": {TargetReads, ScratchProbes,
		"reads the target and reports the difference; on Oracle alone it creates a probe table " +
			"in the dev database and drops it again, to learn how that engine spells a declared " +
			"generated-column expression"},
	"schema diff": {TargetNone, ScratchRewrites,
		"diffs two arbitrary schema states; a non-database source is materialized by replaying " +
			"it into the dev database"},
	"schema drift": {TargetReads, ScratchNone,
		"reads the target and reports how it differs from the desired schema"},
	"schema export": {TargetNone, ScratchNone,
		"converts one desired-schema source format into another on disk; no database is opened"},
	"schema fmt": {TargetNone, ScratchNone,
		"rewrites HCL schema files in the repository into canonical form; no database is opened"},
	"schema inspect": {TargetReads, ScratchRewrites,
		"reads a schema source and prints it; the dev database it names \"is reset destructively\""},
	"schema lineage": {TargetReads, ScratchNone,
		"traces which columns each view and routine reads and writes; `--db-url` reads the target"},
	"schema plan": {TargetReads, ScratchRewrites,
		"saves a fingerprinted apply plan; the dev database is where the plan is rehearsed"},
	"schema pull": {TargetNone, ScratchNone,
		"downloads a desired-schema document from an OCI registry and writes it to disk"},
	"schema push": {TargetNone, ScratchNone,
		"uploads a desired-schema document from disk to an OCI registry"},
	"schema render": {TargetNone, ScratchNone,
		"renders the desired schema as SQL with no connection at all; the dialect comes from a flag"},
	"schema security": {TargetReads, ScratchNone,
		"reads the target's roles, grants and policies and reports security findings"},
	"schema serve": {TargetReads, ScratchNone,
		"serves a live read-only view of the schema over HTTP; it opens a listener, which is " +
			"an exposure of its own even though the database is only read"},
	"schema stats": {TargetReads, ScratchNone,
		"counts the objects in the target and emits them as OpenMetrics"},
	"schema test": {TargetWrites, ScratchNone,
		"runs declarative test cases against the database named by `--db-url`, whose own help " +
			"calls it a \"Throwaway database URL\": measured on PostgreSQL 17.11, a case with an " +
			"apply_schema step created a table there and an exec step inserted into it"},
	"schema validate": {TargetNone, ScratchNone,
		"reports structural problems in a desired schema without a database"},
	"schema verify-approval": {TargetNone, ScratchNone,
		"checks a saved plan's signature against an allowed-signers file"},

	// sql.
	"sql lint": {TargetNone, ScratchNone,
		"lints standalone SQL files on disk and reports findings; no database is opened"},
}
