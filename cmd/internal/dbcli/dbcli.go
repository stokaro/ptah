// Package dbcli holds small helpers shared by the CLI subcommands that connect
// to a database. Centralising the connect-timeout flag and context
// construction keeps behavior consistent across commands.
//
// For the close-with-warning idiom used after a successful Connect, prefer
// [github.com/stokaro/ptah/dbschema.CloseAndWarn] — it lives next to the
// DatabaseConnection type so non-CLI consumers (for example the migration
// generator) can also use it.
package dbcli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"
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

// RegisterSchemasFlag registers a comma-separated schema allow-list flag.
func RegisterSchemasFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, SchemasFlagName, "", "Comma-separated database schemas to introspect (PostgreSQL-family only). Empty uses the connection default schema.")
}

// RegisterMigrationsSchemaFlag registers the migration tracking table schema flag.
func RegisterMigrationsSchemaFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, MigrationsSchemaFlagName, "", "Schema for Ptah's migration tracking table. Empty uses the connection default schema.")
}

// RegisterMigrationsTableFlag registers the migration tracking table name flag.
func RegisterMigrationsTableFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, MigrationsTableFlagName, "", "Table name for the migration tracking table. Empty uses the revision format default.")
}

// RegisterRevisionTableFormatFlag registers the migration revision table layout flag.
func RegisterRevisionTableFormatFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, RevisionTableFormatFlagName, "ptah", "Migration revision table format: ptah or atlas.")
}

// ParseSchemas parses a comma-separated schema allow-list.
func ParseSchemas(raw string) []string {
	parts := strings.Split(raw, ",")
	schemas := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		schema := strings.TrimSpace(part)
		if schema == "" {
			continue
		}
		if _, ok := seen[schema]; ok {
			continue
		}
		seen[schema] = struct{}{}
		schemas = append(schemas, schema)
	}
	return schemas
}

// JoinSchemas formats a project-config schema list as the CLI flag value.
func JoinSchemas(schemas []string) string {
	return strings.Join(schemas, ",")
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
