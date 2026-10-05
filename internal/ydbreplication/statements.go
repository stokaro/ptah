package ydbreplication

import (
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// Path writes name, a replication's or a transfer's canonical reference, as
// one quoted YDB path: `<directory>/<name>`, or the name alone at the
// database root.
func Path(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return sqlident.Quote(platform.YDB, name)
	}
	return sqlident.Qualified(platform.YDB, ref.Schema, ref.Name)
}

// CreateReplicationStatement writes what creates the replication name as
// spec declares it: its items in the order declared, its connection, its
// credential and the consistency settings it names. A setting spec leaves out
// is left to YDB, which gives it the value [CompareReplication] reads the
// declaration with.
func CreateReplicationStatement(name string, spec ast.AsyncReplicationSpec) string {
	items := make([]string, len(spec.Items))
	for i, item := range spec.Items {
		items[i] = quotePath(item.Source) + " AS " + quotePath(item.Target)
	}
	settings := append(connectionStringSettings(spec.Connection), credentialSettings(spec.Connection)...)
	if spec.ConsistencyLevel != "" {
		settings = append(settings, "CONSISTENCY_LEVEL = "+quoteString(strings.ToUpper(spec.ConsistencyLevel)))
	}
	if spec.CommitInterval != "" {
		millis, _ := CommitIntervalMillis(spec.CommitInterval)
		settings = append(settings, "COMMIT_INTERVAL = "+intervalLiteral(FormatMillis(millis)))
	}
	return "CREATE ASYNC REPLICATION " + Path(name) + " FOR " + strings.Join(items, ", ") +
		" WITH (" + strings.Join(settings, ", ") + ");"
}

// AlterReplicationStatement writes what moves the connection and the
// credential of the paused replication name from previous to desired, and ""
// when they are the same.
//
// It names the connection string when that differs, and the whole credential
// whenever any part of it differs: the user with the password secret, or the
// token secret. Measured on 25.1.4.7 and 26.2.1.14, a SET naming one of them
// keeps the others, so naming the whole group is not needed for the change to
// take, and it keeps the statement saying what the replication holds after
// it.
func AlterReplicationStatement(name string, desired, previous ast.AsyncReplicationSpec) string {
	settings := alteredConnection(desired.Connection, previous.Connection)
	if len(settings) == 0 {
		return ""
	}
	return "ALTER ASYNC REPLICATION " + Path(name) + " SET (" + strings.Join(settings, ", ") + ");"
}

// DropReplicationStatement writes what drops the replication node names.
// With [ast.DropAsyncReplicationNode.Cascade] it drops the replica tables
// too; without it they stay, writable only where the replication was failed
// over first: measured on 25.1.4.7 and 26.2.1.14, the replica of a running
// replication dropped without CASCADE refuses every write and every ALTER for
// good (`path is an async replica table`).
func DropReplicationStatement(node *ast.DropAsyncReplicationNode) string {
	statement := "DROP ASYNC REPLICATION " + Path(node.Name)
	if node.Cascade {
		statement += " CASCADE"
	}
	return statement + ";"
}

// CreateTransferStatement writes what creates the transfer name as spec
// declares it, the lambda as written.
func CreateTransferStatement(name string, spec ast.TransferSpec) string {
	var b strings.Builder
	b.WriteString("CREATE TRANSFER " + Path(name) + " FROM " + quotePath(spec.Source) + " TO " +
		quotePath(spec.Target) + " USING " + strings.TrimSpace(spec.Lambda))
	settings := append(connectionStringSettings(spec.Connection), credentialSettings(spec.Connection)...)
	if spec.Consumer != "" {
		settings = append(settings, "CONSUMER = "+quoteString(spec.Consumer))
	}
	if spec.BatchSizeBytes != 0 {
		settings = append(settings, "BATCH_SIZE_BYTES = "+strconv.FormatUint(spec.BatchSizeBytes, 10))
	}
	if spec.FlushInterval != "" {
		seconds, _ := FlushIntervalSeconds(spec.FlushInterval)
		settings = append(settings, "FLUSH_INTERVAL = "+intervalLiteral(FormatMillis(seconds*1000)))
	}
	if len(settings) > 0 {
		b.WriteString(" WITH (" + strings.Join(settings, ", ") + ")")
	}
	return b.String() + ";"
}

// AlterTransferStatement writes what moves the transfer name from previous to
// desired in place, and "" when nothing it can change differs: the lambda,
// both batch settings whenever either differs, and the connection and the
// credential as [AlterReplicationStatement] writes them. Measured on 26.2.1.14,
// one ALTER TRANSFER takes `SET USING ..., SET (...)`, and a SET naming one
// batch setting keeps the other.
func AlterTransferStatement(name string, desired, previous ast.TransferSpec) string {
	changes := CompareTransfer(desired, previous)
	var actions []string
	if changes.Lambda {
		actions = append(actions, "SET USING "+strings.TrimSpace(desired.Lambda))
	}
	settings := alteredConnection(desired.Connection, previous.Connection)
	if changes.Batch {
		settings = append(settings,
			"BATCH_SIZE_BYTES = "+strconv.FormatUint(batchSize(desired), 10),
			"FLUSH_INTERVAL = "+intervalLiteral(FormatMillis(flushSeconds(desired)*1000)))
	}
	if len(settings) > 0 {
		actions = append(actions, "SET ("+strings.Join(settings, ", ")+")")
	}
	if len(actions) == 0 {
		return ""
	}
	return "ALTER TRANSFER " + Path(name) + " " + strings.Join(actions, ", ") + ";"
}

// DropTransferStatement writes what drops the transfer name. YDB drops the
// consumer it created for the transfer and keeps one the transfer was given,
// measured on 26.2.1.14; the topic and the table stay.
func DropTransferStatement(name string) string {
	return "DROP TRANSFER " + Path(name) + ";"
}

// alteredConnection writes the SET settings that move a connection from
// previous to desired.
func alteredConnection(desired, previous ast.ReplicationConnectionSpec) []string {
	var settings []string
	if CanonicalConnectionString(desired.ConnectionString) != CanonicalConnectionString(previous.ConnectionString) {
		settings = append(settings, connectionStringSettings(desired)...)
	}
	if !credentialsEqual(desired, previous) {
		settings = append(settings, credentialSettings(desired)...)
	}
	return settings
}

// connectionStringSettings writes a connection's string as a setting, or none
// where the connection names none.
func connectionStringSettings(connection ast.ReplicationConnectionSpec) []string {
	if connection.ConnectionString == "" {
		return nil
	}
	return []string{"CONNECTION_STRING = " + quoteString(connection.ConnectionString)}
}

// credentialSettings writes a connection's credential as its settings: the
// token secret, or the user with the password secret, each by name or by path.
func credentialSettings(connection ast.ReplicationConnectionSpec) []string {
	var settings []string
	switch {
	case connection.TokenSecretName != "":
		settings = append(settings, "TOKEN_SECRET_NAME = "+quoteString(connection.TokenSecretName))
	case connection.TokenSecretPath != "":
		settings = append(settings, "TOKEN_SECRET_PATH = "+quoteString(connection.TokenSecretPath))
	case connection.PasswordSecretName != "":
		settings = append(settings, "USER = "+quoteString(connection.User),
			"PASSWORD_SECRET_NAME = "+quoteString(connection.PasswordSecretName))
	case connection.PasswordSecretPath != "":
		settings = append(settings, "USER = "+quoteString(connection.User),
			"PASSWORD_SECRET_PATH = "+quoteString(connection.PasswordSecretPath))
	}
	return settings
}

// quotePath quotes a path as one YQL identifier, a leading slash included.
func quotePath(value string) string {
	return sqlident.Quote(platform.YDB, value)
}

// quoteString writes a YQL string literal.
func quoteString(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), "'", `\'`) + "'"
}

// intervalLiteral writes an ISO 8601 duration as YDB's Interval literal.
func intervalLiteral(text string) string {
	return "Interval(" + quoteString(text) + ")"
}
