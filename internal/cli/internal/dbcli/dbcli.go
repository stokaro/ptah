// Package dbcli holds small helpers shared by the CLI subcommands that connect
// to a database. Centralising the connect-timeout flag and context
// construction keeps behavior consistent across commands.
//
// For the close-with-warning idiom used after a successful Connect, prefer
// [ptah.run/dbschema.CloseAndWarn] — it lives next to the
// DatabaseConnection type so non-CLI consumers (for example the migration
// generator) can also use it.
package dbcli

import (
	"context"
	"encoding/csv"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/envbinding"
)

const (
	// ConnectTimeoutFlagName is the shared initial database connection timeout flag name.
	ConnectTimeoutFlagName = "connect-timeout"
	// SchemasFlagName is the shared database schema allow-list flag name.
	SchemasFlagName = "schemas"
	// MigrationsSchemaFlagName is the CLI flag name for the schema_migrations schema.
	MigrationsSchemaFlagName = "migrations-schema"
	// MigrationsTableFlagName is the CLI flag name for the schema_migrations table name.
	MigrationsTableFlagName = "migrations-table"
	// RevisionTableFormatFlagName is the CLI flag name for the migration revision table layout.
	RevisionTableFormatFlagName = "revision-format"
	// MigrationsEngineFlagName is the CLI flag name for the revision table's
	// storage engine.
	MigrationsEngineFlagName = "migrations-engine"
)

// DefaultConnectTimeout is the default value for [ConnectTimeoutFlagName]. It
// matches the value suggested by issue #139.
const DefaultConnectTimeout = 10 * time.Second

// RegisterConnectTimeoutFlag registers a string-valued flag that accepts a
// [time.Duration] literal (for example "5s" or "2m"). The flag is intentionally
// a string so a value of "0" disables the timeout, while still supporting the
// usual duration suffixes.
func RegisterConnectTimeoutFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, ConnectTimeoutFlagName, DefaultConnectTimeout.String(), "Maximum time to wait when establishing the initial database connection (for example 5s or 1m). Use 0 to disable the timeout.")
}

// ConnectionDefaultSchemas is what leaving out the flag [RegisterSchemasFlag]
// registers does, in the words the refusal of an empty variable gives. A verb
// that forwards to one of those verbs without --schemas reads the same.
const ConnectionDefaultSchemas = "read the connection default schema, and on YDB every directory"

// SchemasQuoting is the sentence each schema-list flag's help ends with: the
// grammar [ParseSchemas] reads.
const SchemasQuoting = `Double-quote a name to keep the spaces around it or a comma in it, as in '" a",b'.`

// SchemasURLLimit describes the preconnection selection check on a MySQL-family
// URL that already names the one database the command can read.
const SchemasURLLimit = "A MySQL or MariaDB URL naming a database accepts only that database's exact URL spelling. "

// RegisterSchemasFlag registers a comma-separated schema allow-list flag for a
// verb whose empty value leaves the read at the schema the connection landed
// in.
func RegisterSchemasFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, SchemasFlagName, "",
		"Comma-separated database schemas to introspect: PostgreSQL-family and SQL Server schemas, "+
			"or YDB directories relative to the database root. Empty uses the connection default schema, and on YDB every directory. "+
			SchemasURLLimit+SchemasQuoting,
	)
	RefuseEmptySchemasEnv(flags, SchemasFlagName, ConnectionDefaultSchemas)
}

// RefuseEmptySchemasEnv makes the variable of the schema-selecting flag name
// refuse a value that names no schema: the empty value, and one whose
// comma-separated fields are all blank; see [cmdflags.RefuseEmptyEnv]. A
// pipeline variable nobody filled reaches the flag as such a value, and read as
// no selection it would select the schemas omitted describes rather than the
// ones the operator meant, on a verb that may plan a DROP for every schema it
// reads.
func RefuseEmptySchemasEnv(flags *pflag.FlagSet, name, omitted string) {
	if err := cmdflags.RefuseEmptyEnv(flags, name, envbinding.EmptyRule{Omitted: omitted, List: true}); err != nil {
		panic(err) // the caller registers the flag first and names what leaving it out does
	}
}

// What leaving a selecting flag out does, in the words the refusal of its
// empty variable prints after "to"; see [RefuseEmptyListEnv].
const (
	// OmittedExclude is what leaving --exclude out does.
	OmittedExclude = "exclude nothing"
	// OmittedInclude is what leaving --include out does.
	OmittedInclude = "include every object"
	// OmittedProtectedTable is what leaving --protected-table out does.
	OmittedProtectedTable = "protect no table"
)

// RefuseEmptyListEnv makes the variable of the list flag name refuse a value
// that names nothing: the empty value, and one whose comma-separated fields are
// all blank. omitted is what leaving the flag out does; see
// [cmdflags.RefuseEmptyEnv]. It is for a verb that writes, where the variable
// read as unset would select more than any value does: an empty PTAH_EXCLUDE
// plans the DROP a value keeps out. A pipeline variable nobody filled reaches
// the flag as such a value. A read-only verb keeps reading it as unset.
func RefuseEmptyListEnv(flags *pflag.FlagSet, name, omitted string) {
	refuseEmptyEnv(flags, name, envbinding.EmptyRule{Omitted: omitted, List: true})
}

// RefuseEmptyValueEnv is [RefuseEmptyListEnv] for a flag that takes one value:
// its variable refuses the empty value and a value of only white space. An empty
// PTAH_TO_VERSION read as unset applies every pending migration.
func RefuseEmptyValueEnv(flags *pflag.FlagSet, name, omitted string) {
	refuseEmptyEnv(flags, name, envbinding.EmptyRule{Omitted: omitted})
}

// RefuseEmptyStopEnv is [RefuseEmptyValueEnv] for a flag that names where a
// run stops, beside the flags unless names, which name the stop too. A value
// other than its default typed for one of them decides the stop, so beside it
// the variable's empty value cannot widen the run and is read as unset; see
// [envbinding.EmptyRule.Unless]. Without one typed, a stop another variable
// sets would decide in its place: an empty PTAH_LIMIT beside PTAH_TARGET=0
// rolls the whole history back, so the empty value is refused. A typed
// --limit 0 or an empty --to-tag names no stop and is the same as none typed.
func RefuseEmptyStopEnv(flags *pflag.FlagSet, name, omitted string, unless ...string) {
	refuseEmptyEnv(flags, name, envbinding.EmptyRule{Omitted: omitted, Unless: unless})
}

func refuseEmptyEnv(flags *pflag.FlagSet, name string, rule envbinding.EmptyRule) {
	if err := cmdflags.RefuseEmptyEnv(flags, name, rule); err != nil {
		panic(err) // the caller registers the flag first and names what leaving it out does
	}
}

// RegisterURLScopedSchemasFlag registers the same flag for the verbs whose
// empty value lets the database URL decide, through
// [ptah.run/internal/schemascope.ReadNames]: a PostgreSQL-family URL that
// pins a schema with `search_path` is read at that schema, and one that pins
// none is read across every schema of the connected database. A MySQL-family
// URL that names no database is a whole server where the verb accepts one, and
// the flag then names its databases (stokaro/ptah#3789).
//
// The two spellings exist because the two groups of verbs really do differ, and
// one help string covering both would be false for one of them. `schema
// inspect`, `schema diff`, `schema apply`, `schema compare`, `schema drift`,
// `migrations plan` and `migrations generate` compare against a description
// that may name any schema of the database, so a read narrower than the URL
// turns silence into intent (stokaro/ptah#1264, stokaro/ptah#1276,
// stokaro/ptah#5030); `introspect`, `db read` and the rest describe a database
// without comparing it, and keep the narrower default they document.
func RegisterURLScopedSchemasFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, SchemasFlagName, "", "Comma-separated database schemas to introspect: PostgreSQL-family and SQL Server schemas, "+
		"the databases of a MySQL or MariaDB URL that names none, or YDB directories relative to the database root. "+
		"Empty lets the URL decide: a URL pinning search_path is read at that schema, one pinning none across every schema, "+
		"a MySQL or MariaDB URL naming no database across every user database, a SQL Server URL at the schema its schema parameter "+
		"names or else at the connection's default schema, and a YDB URL across every directory. "+
		SchemasURLLimit+SchemasQuoting)
	RefuseEmptySchemasEnv(flags, SchemasFlagName, urlDecidesSchemas)
}

// urlDecidesSchemas is what leaving a URL-scoped --schemas out does, as the
// refusal of its empty variable says it.
const urlDecidesSchemas = "let the URL decide which schemas are read"

// RegisterURLScopedDatabaseSchemasFlag is [RegisterURLScopedSchemasFlag] for
// a verb that reads one database and refuses a MySQL or MariaDB URL that
// names none: `migrations plan`, whose versioned workflow -- `migrations
// generate`, `up` and `status` -- runs against one database. Its help leaves
// the whole server out, because the verb refuses the URL the clause describes.
func RegisterURLScopedDatabaseSchemasFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, SchemasFlagName, "", "Comma-separated database schemas to introspect: PostgreSQL-family and SQL Server schemas, "+
		"or YDB directories relative to the database root. "+
		"Empty lets the URL decide: a URL pinning search_path is read at that schema, one pinning none across every schema, "+
		"a SQL Server URL at the schema its schema parameter names or else at the connection's default schema, "+
		"and a YDB URL across every directory. "+
		SchemasURLLimit+SchemasQuoting)
	RefuseEmptySchemasEnv(flags, SchemasFlagName, urlDecidesSchemas)
}

// What leaving a flag that names the migration history out does, in the words
// the refusal of its empty variable prints after "to"; see
// [RefuseEmptyHistoryEnv].
const (
	// OmittedMigrationsSchema is what leaving --migrations-schema out does.
	OmittedMigrationsSchema = "use the project file's schema or the default schema"
	// OmittedMigrationsTable is what leaving --migrations-table out does.
	OmittedMigrationsTable = "use the project file's table or the revision format default"
	// OmittedRevisionFormat is what leaving --revision-format out does.
	OmittedRevisionFormat = "use the project file's format or ptah"
	// OmittedRevisionsSchema is what leaving the --revisions-schema of
	// `ptah-compat migrate` out does.
	OmittedRevisionsSchema = "use the project file's schema or the Atlas default"
)

// RefuseEmptyHistoryEnv makes the variable of the flag name, which says where
// the history of applied migrations is, refuse the empty value and a value of
// only white space; omitted is what leaving the flag out does. Read as unset, such a
// value names the default history instead of the one the operator meant. Over a
// history recorded under another table, an empty PTAH_MIGRATIONS_TABLE finds
// nothing applied: `migrations up` runs every migration again, and
// `migrations generate` writes a DROP of the history tables. Every verb that
// reads the history refuses it, a read-only one included, since a status read
// from the default history reports every migration pending.
func RefuseEmptyHistoryEnv(flags *pflag.FlagSet, name, omitted string) {
	refuseEmptyEnv(flags, name, envbinding.EmptyRule{Omitted: omitted})
}

// RegisterMigrationsSchemaFlag registers the migration tracking table schema
// flag. Its variable refuses an empty value; see [RefuseEmptyHistoryEnv].
func RegisterMigrationsSchemaFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, MigrationsSchemaFlagName, "",
		"Schema for the migration tracking table. Empty uses the connection default schema, and on SQL Server "+
			"the URL's schema parameter or else dbo, except that "+
			"--revision-format atlas on a PostgreSQL-family URL with no search_path uses atlas_schema_revisions, as Atlas does.",
	)
	RefuseEmptyHistoryEnv(flags, MigrationsSchemaFlagName, OmittedMigrationsSchema)
}

// RegisterMigrationsTableFlag registers the migration tracking table name
// flag. Its variable refuses an empty value; see [RefuseEmptyHistoryEnv].
func RegisterMigrationsTableFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, MigrationsTableFlagName, "", "Table name for the migration tracking table. Empty uses the revision format default.")
	RefuseEmptyHistoryEnv(flags, MigrationsTableFlagName, OmittedMigrationsTable)
}

// ActorFlagName is the flag that names who a run records itself as.
const ActorFlagName = "actor"

// RegisterActorFlag registers the actor a run records in log, the name of the
// log the command writes, such as "migration log".
//
// It is unverified by construction: a name typed on a command line is what
// somebody wrote, and the log stores where the name came from beside it so a
// later reader is not left treating it as an identity Ptah checked. Empty
// records the user the process runs as, under a different provenance.
func RegisterActorFlag(flags *pflag.FlagSet, target *string, log string) {
	flags.StringVar(target, ActorFlagName, "",
		"Name recorded as the actor of this run in the "+log+" (unverified; "+
			"empty records the user the process runs as)")
}

// RegisterMigrationsEngineFlag registers the revision table's storage engine.
//
// It exists for ClickHouse, where a table has no engine unless one is named and
// the server's own `default_table_engine` decides whether the statement is even
// legal -- its default value is `None`, which answers
// `Table engine is not specified in CREATE query`. Naming a default is half the
// fix; the other half is this flag, because a replicated deployment needs
// `ReplicatedMergeTree` with its keeper path and replica name or the migration
// history exists on one node only and every replica reports itself consistent
// (stokaro/ptah#2234).
//
// Empty means the dialect's own default, which is what every other engine has
// always used. The variable, PTAH_MIGRATIONS_ENGINE, refuses an empty value
// all the same: read as unset, it creates the revision table with the default
// engine, and on a replicated cluster that table exists on one node only.
func RegisterMigrationsEngineFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, MigrationsEngineFlagName, "",
		"Storage engine for Ptah's migration tracking table (ClickHouse; for example \"ReplicatedMergeTree('/clickhouse/tables/{shard}/schema_migrations', '{replica}')\"). Empty uses the dialect default.")
	RefuseEmptyValueEnv(flags, MigrationsEngineFlagName, OmittedMigrationsEngine)
}

// OmittedMigrationsEngine is what leaving --migrations-engine out does, in the
// words the refusal of an empty PTAH_MIGRATIONS_ENGINE prints.
const OmittedMigrationsEngine = "use the dialect's default engine"

// RegisterRevisionTableFormatFlag registers the migration revision table
// layout flag. The layout names the table a run reads, so its variable refuses
// an empty value; see [RefuseEmptyHistoryEnv].
func RegisterRevisionTableFormatFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, RevisionTableFormatFlagName, "ptah", "Migration revision table format: ptah or atlas.")
	RefuseEmptyHistoryEnv(flags, RevisionTableFormatFlagName, OmittedRevisionFormat)
}

// SchemaField is one field of a schema list as [ReadSchemaFields] reads it.
type SchemaField struct {
	// Text is the field as encoding/csv reads it: a quoted field without its
	// quotes, with `""` read as one quote.
	Text string
	// Quoted reports a field written between double quotes.
	Quoted bool
}

// ReadSchemaFields reads raw as one line of comma-separated fields in the
// grammar of encoding/csv and says which fields were quoted, so a reader can
// keep a quoted field's bytes and trim a bare one. With trimLeadingSpace the
// white space before a field is skipped, so `a, " a"` reads a quoted second
// field; without it, the reader refuses that spelling as `bare " in
// non-quoted-field`, as the compatibility surface's flag must. A value
// encoding/csv cannot read is refused with its error.
func ReadSchemaFields(raw string, trimLeadingSpace bool) ([]SchemaField, error) {
	reader := csv.NewReader(strings.NewReader(raw))
	reader.TrimLeadingSpace = trimLeadingSpace
	texts, err := reader.Read()
	if err != nil {
		return nil, err
	}
	fields := make([]SchemaField, 0, len(texts))
	for i, text := range texts {
		// FieldPos names the byte a field begins at, the opening quote of a
		// quoted one, after any white space the reader skipped. An empty last
		// field begins past the end.
		_, column := reader.FieldPos(i)
		fields = append(fields, SchemaField{Text: text, Quoted: column <= len(raw) && raw[column-1] == '"'})
	}
	return fields, nil
}

// ParseSchemas parses a schema allow-list: one line of comma-separated fields
// in the grammar of encoding/csv, read by [ReadSchemaFields] with the white
// space before each field skipped. A bare field is trimmed of white space and
// left out when blank, so `a, b` names `a` and `b`. A double-quoted field keeps
// its bytes, and `""` inside it is one quote, so `a, " a"` names the schemas `a`
// and ` a`, and `"Sales"` names Sales. A quoted field that names no schema,
// such as `""` or `" "`, is refused: the quotes say a name was meant, and an
// empty selection reads the connection default or the whole database. A bare
// quote, text after a closing quote and a line break are refused too. A
// repeated name is left out, and the order is the order of first appearance.
func ParseSchemas(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	if strings.ContainsAny(raw, "\r\n") {
		return nil, fmt.Errorf("invalid schema list %q: a schema list is one line", raw)
	}
	fields, err := ReadSchemaFields(raw, true)
	if err != nil {
		return nil, fmt.Errorf("invalid schema list %q: %w", raw, err)
	}
	schemas := make([]string, 0, len(fields))
	for _, field := range fields {
		name := field.Text
		if field.Quoted {
			if strings.TrimSpace(name) == "" {
				return nil, fmt.Errorf("invalid schema list %q: the quoted field %q names no schema", raw, name)
			}
		} else if name = strings.TrimSpace(name); name == "" {
			continue
		}
		if !slices.Contains(schemas, name) {
			schemas = append(schemas, name)
		}
	}
	return schemas, nil
}

// JoinSchemas formats a schema list as the value [ParseSchemas] reads back as
// the same list. A name that holds a comma, a quote or a line break, or that
// begins or ends with white space, is written as a quoted field.
func JoinSchemas(schemas []string) string {
	fields := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		if strings.ContainsAny(schema, ",\"\r\n") || schema != strings.TrimSpace(schema) {
			schema = `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
		}
		fields = append(fields, schema)
	}
	return strings.Join(fields, ",")
}

// ParseConnectTimeout parses the raw string value returned by the
// [ConnectTimeoutFlagName] flag. A zero duration is accepted and signals that
// callers should not wrap the parent context with a deadline.
func ParseConnectTimeout(raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid --%s value %q: %w", ConnectTimeoutFlagName, raw, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid --%s value %q: must not be negative", ConnectTimeoutFlagName, raw)
	}
	return d, nil
}

// ConnectContext derives a context for the initial database connection from
// the supplied parent. When timeout is zero or negative, the parent is
// returned unchanged together with a no-op CancelFunc so callers can `defer
// cancel()` unconditionally; canceling the returned function in that case
// does not affect the parent context.
func ConnectContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}
