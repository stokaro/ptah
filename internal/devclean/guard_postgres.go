package devclean

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/lexer"
)

func validatePostgresReplayStatement(dialect string, tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}

	first := normalizedIdentifier(tokens[0])
	if first == "DO" {
		return unsafeReplayStatement(dialect, "DO sublanguage")
	}
	if first == "CALL" {
		return unsafeReplayStatement(dialect, "CALL sublanguage")
	}
	if definesPostgresRoutine(tokens) {
		return unsafeReplayStatement(dialect, first+" routine definition")
	}
	if namespace := protectedPostgresMutationNamespace(tokens); namespace != "" {
		return unsafeReplayStatement(
			dialect,
			fmt.Sprintf("protected namespace %q mutation", namespace),
		)
	}
	if first == "IMPORT" && containsTokenSequence(tokens, "FOREIGN", "SCHEMA") {
		return unsafeReplayStatement(dialect, "IMPORT FOREIGN SCHEMA")
	}
	if operation := postgresGlobalDCLOrMetadata(tokens); operation != "" {
		return unsafeReplayStatement(dialect, operation)
	}
	if operation := postgresRoleOrAuthorizationChange(tokens); operation != "" {
		return unsafeReplayStatement(dialect, operation)
	}
	if operation := postgresDialectGlobalOperation(dialect, tokens); operation != "" {
		return unsafeReplayStatement(dialect, operation)
	}
	if operation := postgresSessionOrControlOperation(tokens); operation != "" {
		return unsafeReplayStatement(dialect, operation)
	}
	if selectsIntoPostgresTemporaryObject(tokens) {
		return unsafeReplayStatement(dialect, "TEMP object")
	}
	if operation := dangerousPostgresFunctionCall(tokens); operation != "" {
		return unsafeReplayStatement(dialect, operation)
	}

	return validatePostgresReplayStatementBase(dialect, tokens)
}

func protectedPostgresMutationNamespace(tokens []lexer.Token) string {
	if namespace := protectedPostgresSelectIntoNamespace(tokens); namespace != "" {
		return namespace
	}
	for _, target := range postgresMutationTargets(tokens) {
		if namespace := protectedNamespaceFromMutationTarget(tokens, target); namespace != "" {
			return namespace
		}
	}
	if namespace := protectedPostgresMutationListNamespace(tokens); namespace != "" {
		return namespace
	}
	if namespace := protectedPostgresSpecialMutationNamespace(tokens); namespace != "" {
		return namespace
	}

	switch normalizedIdentifier(tokens[0]) {
	case "ALTER":
		if tokenSequenceAt(tokens, 1, "DEFAULT", "PRIVILEGES") {
			return protectedPostgresSchemaClause(tokens)
		}
	case "COMMENT", "SECURITY":
		return protectedPostgresObjectTarget(tokens)
	case "GRANT", "REVOKE":
		if namespace := protectedPostgresSchemaClause(tokens); namespace != "" {
			return namespace
		}
		return protectedPostgresObjectTarget(tokens)
	}
	return ""
}

func protectedPostgresSelectIntoNamespace(tokens []lexer.Token) string {
	tokens = leadingCTEExecutableTokens(tokens)
	if normalizedIdentifier(tokens[0]) != "SELECT" {
		return ""
	}
	for index := 1; index < len(tokens); index++ {
		if normalizedIdentifier(tokens[index]) != "INTO" {
			continue
		}
		targetIndex := index + 1
		for targetIndex < len(tokens) {
			switch normalizedIdentifier(tokens[targetIndex]) {
			case "GLOBAL", "LOCAL", "TEMP", "TEMPORARY", "UNLOGGED", "TABLE":
				targetIndex++
			default:
				return protectedPostgresQualifiedNamespaceAt(tokens, targetIndex)
			}
		}
		return ""
	}
	return ""
}

func postgresMutationTargets(tokens []lexer.Token) [][]string {
	targets := mutationTargets(tokens)
	for index := 1; index < len(tokens); index++ {
		switch normalizedIdentifier(tokens[index]) {
		case "DELETE", "INSERT", "MERGE", "TRUNCATE", "UPDATE":
			targets = append(targets, mutationTargets(tokens[index:])...)
		}
	}
	return targets
}

func protectedNamespaceFromMutationTarget(tokens []lexer.Token, target []string) string {
	if len(target) >= 2 && isProtectedPostgresNamespace(target[0]) {
		return target[0]
	}
	if len(target) == 0 {
		return ""
	}

	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex != mutationTargetNotFound &&
		normalizedIdentifier(tokens[kindIndex]) == "SCHEMA" &&
		isProtectedPostgresNamespace(target[0]) {
		return target[0]
	}
	return ""
}

func protectedPostgresSchemaClause(tokens []lexer.Token) string {
	for index := 0; index+1 < len(tokens); index++ {
		if normalizedIdentifier(tokens[index]) != "SCHEMA" {
			continue
		}
		if index > 0 {
			previous := normalizedIdentifier(tokens[index-1])
			if previous != "IN" && previous != "ON" {
				continue
			}
		}
		if namespace := protectedPostgresObjectListNamespace(
			tokens,
			index+1,
			postgresPrivilegeRecipientKeyword(tokens),
			protectedPostgresNamespaceObjectAt,
		); namespace != "" {
			return namespace
		}
	}
	return ""
}

func protectedPostgresMutationListNamespace(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "DROP":
		const kindIndex = 1
		if kindIndex >= len(tokens) {
			return ""
		}
		kind := normalizedIdentifier(tokens[kindIndex])
		if !postgresDropSupportsObjectList(tokens, kindIndex, kind) {
			return ""
		}
		targetAt := protectedPostgresQualifiedNamespaceAt
		if kind == "SCHEMA" {
			targetAt = protectedPostgresNamespaceObjectAt
		}
		return protectedPostgresObjectListNamespace(
			tokens,
			objectNameIndex(tokens, kindIndex),
			"",
			targetAt,
		)
	case "TRUNCATE":
		index := 1
		if tokenSequenceAt(tokens, index, "TABLE") {
			index++
		}
		if tokenSequenceAt(tokens, index, "ONLY") {
			index++
		}
		return protectedPostgresObjectListNamespace(
			tokens,
			index,
			"",
			protectedPostgresQualifiedNamespaceAt,
		)
	default:
		return ""
	}
}

func postgresDropSupportsObjectList(tokens []lexer.Token, kindIndex int, kind string) bool {
	switch kind {
	case "AGGREGATE", "COLLATION", "CONVERSION", "DOMAIN", "FUNCTION", "INDEX",
		"OPERATOR", "PROCEDURE", "ROUTINE", "SCHEMA", "SEQUENCE", "STATISTICS",
		"TABLE", "TYPE", "VIEW":
		return true
	case "FOREIGN":
		return tokenSequenceAt(tokens, kindIndex, "FOREIGN", "TABLE")
	case "MATERIALIZED":
		return tokenSequenceAt(tokens, kindIndex, "MATERIALIZED", "VIEW")
	default:
		return false
	}
}

func protectedPostgresSpecialMutationNamespace(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "ALTER", "CREATE", "DROP":
		return protectedPostgresSpecialDDLNamespace(tokens)
	case "IMPORT":
		if containsTokenSequence(tokens, "FOREIGN", "SCHEMA") {
			return protectedPostgresNamespaceAfterKeyword(
				tokens,
				"INTO",
				1,
				protectedPostgresNamespaceObjectAt,
			)
		}
	case "REFRESH":
		if tokenSequenceAt(tokens, 1, "MATERIALIZED", "VIEW") {
			index := 3
			if tokenSequenceAt(tokens, index, "CONCURRENTLY") {
				index++
			}
			return protectedPostgresQualifiedNamespaceAt(tokens, index)
		}
	case "REINDEX":
		return protectedPostgresReindexNamespace(tokens)
	}
	return ""
}

func protectedPostgresSpecialDDLNamespace(tokens []lexer.Token) string {
	kindIndex := postgresSpecialDDLKindIndex(tokens)
	if kindIndex >= len(tokens) {
		return ""
	}
	kind := normalizedIdentifier(tokens[kindIndex])
	switch kind {
	case "AGGREGATE", "COLLATION", "CONVERSION", "OPERATOR", "STATISTICS":
		return protectedPostgresNamedSpecialDDLNamespace(tokens, kindIndex, kind)
	case "EXTENSION":
		return protectedPostgresNamespaceAfterKeyword(
			tokens,
			"SCHEMA",
			kindIndex+1,
			protectedPostgresNamespaceObjectAt,
		)
	case "POLICY":
		return protectedPostgresNamespaceAfterKeyword(
			tokens,
			"ON",
			kindIndex+1,
			protectedPostgresQualifiedNamespaceAt,
		)
	case "RULE":
		keyword := "ON"
		if normalizedIdentifier(tokens[0]) == "CREATE" {
			keyword = "TO"
		}
		return protectedPostgresNamespaceAfterKeyword(
			tokens,
			keyword,
			kindIndex+1,
			protectedPostgresQualifiedNamespaceAt,
		)
	default:
		return ""
	}
}

func postgresSpecialDDLKindIndex(tokens []lexer.Token) int {
	index := 1
	if normalizedIdentifier(tokens[0]) == "CREATE" &&
		tokenSequenceAt(tokens, index, "OR", "REPLACE") {
		index += 2
	}
	return index
}

func protectedPostgresNamedSpecialDDLNamespace(
	tokens []lexer.Token,
	kindIndex int,
	kind string,
) string {
	nameIndex := kindIndex + 1
	if kind == "OPERATOR" &&
		(tokenSequenceAt(tokens, nameIndex, "CLASS") ||
			tokenSequenceAt(tokens, nameIndex, "FAMILY")) {
		nameIndex++
	}
	nameIndex = skipNameModifiers(tokens, nameIndex)
	if namespace := protectedPostgresObjectListNamespace(
		tokens,
		nameIndex,
		"",
		protectedPostgresQualifiedNamespaceAt,
	); namespace != "" {
		return namespace
	}
	if kind == "STATISTICS" {
		return protectedPostgresNamespaceAfterKeyword(
			tokens,
			"FROM",
			nameIndex,
			protectedPostgresQualifiedNamespaceAt,
		)
	}
	return ""
}

func protectedPostgresReindexNamespace(tokens []lexer.Token) string {
	for index := 1; index < len(tokens); index++ {
		switch normalizedIdentifier(tokens[index]) {
		case "DATABASE", "SYSTEM":
			return ""
		case "INDEX", "TABLE":
			targetIndex := skipNameModifiers(tokens, index+1)
			return protectedPostgresQualifiedNamespaceAt(tokens, targetIndex)
		case "SCHEMA":
			targetIndex := skipNameModifiers(tokens, index+1)
			return protectedPostgresNamespaceObjectAt(tokens, targetIndex)
		}
	}
	return ""
}

func protectedPostgresNamespaceAfterKeyword(
	tokens []lexer.Token,
	keyword string,
	start int,
	targetAt func([]lexer.Token, int) string,
) string {
	index := findPostgresKeyword(tokens, keyword, start)
	if index == mutationTargetNotFound {
		return ""
	}
	return targetAt(tokens, index+1)
}

func protectedPostgresObjectTarget(tokens []lexer.Token) string {
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	if onIndex == mutationTargetNotFound || onIndex+1 >= len(tokens) {
		return ""
	}

	targetIndex := onIndex + 1
	classWidth := postgresObjectClassWidth(tokens, targetIndex)
	targetIndex += classWidth
	targetAt := protectedPostgresQualifiedNamespaceAt
	if classWidth == 1 && normalizedIdentifier(tokens[onIndex+1]) == "SCHEMA" {
		targetAt = protectedPostgresNamespaceObjectAt
	}
	if namespace := protectedPostgresObjectListNamespace(
		tokens,
		targetIndex,
		postgresPrivilegeRecipientKeyword(tokens),
		targetAt,
	); namespace != "" {
		return namespace
	}

	kind := normalizedIdentifier(tokens[onIndex+1])
	if kind != "CONSTRAINT" && kind != "POLICY" && kind != "RULE" && kind != "TRIGGER" {
		return ""
	}
	relationOnIndex := findPostgresKeyword(tokens, "ON", targetIndex+1)
	if relationOnIndex == mutationTargetNotFound {
		return ""
	}
	return protectedPostgresQualifiedNamespaceAt(tokens, relationOnIndex+1)
}

func protectedPostgresObjectListNamespace(
	tokens []lexer.Token,
	start int,
	stopKeyword string,
	targetAt func([]lexer.Token, int) string,
) string {
	expectTarget := true
	depth := 0
	for index := start; index < len(tokens); index++ {
		if depth == 0 && stopKeyword != "" && postgresKeywordAt(tokens[index], stopKeyword) {
			return ""
		}
		if tokens[index].MatchOperatorValue("(") {
			depth++
			continue
		}
		if tokens[index].MatchOperatorValue(")") {
			depth = max(depth-1, 0)
			continue
		}
		if depth != 0 {
			continue
		}
		if tokens[index].MatchOperatorValue(",") {
			expectTarget = true
			continue
		}
		if !expectTarget {
			continue
		}
		if namespace := targetAt(tokens, index); namespace != "" {
			return namespace
		}
		parts, next := qualifiedIdentifierAt(tokens, index)
		if len(parts) == 0 {
			continue
		}
		expectTarget = false
		index = next - 1
	}
	return ""
}

func protectedPostgresNamespaceObjectAt(tokens []lexer.Token, index int) string {
	if index < 0 || index >= len(tokens) {
		return ""
	}
	name := identifierValue(tokens[index])
	if !isProtectedPostgresNamespace(name) {
		return ""
	}
	return name
}

func protectedPostgresQualifiedNamespaceAt(tokens []lexer.Token, index int) string {
	name := protectedPostgresNamespaceObjectAt(tokens, index)
	if name == "" {
		return ""
	}
	if index+1 < len(tokens) && tokens[index+1].MatchOperatorValue(".") {
		return name
	}
	return ""
}

func isProtectedPostgresNamespace(namespace string) bool {
	namespace = strings.ToLower(strings.TrimSpace(namespace))
	return namespace == "information_schema" ||
		namespace == "crdb_internal" ||
		strings.HasPrefix(namespace, "pg_")
}

func postgresGlobalDCLOrMetadata(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "ALTER":
		return postgresGlobalAlterOperation(tokens)
	case "COMMENT", "SECURITY":
		return postgresGlobalMetadataOperation(tokens)
	case "CREATE":
		if tokenSequenceAt(tokens, 1, "GROUP") {
			return "CREATE GROUP"
		}
	case "DROP":
		if tokenSequenceAt(tokens, 1, "GROUP") {
			return "DROP GROUP"
		}
		if tokenSequenceAt(tokens, 1, "OWNED") {
			return "DROP OWNED"
		}
	case "GRANT", "REVOKE":
		return postgresGlobalPrivilegeOperation(tokens)
	case "REASSIGN":
		if tokenSequenceAt(tokens, 1, "OWNED") {
			return "REASSIGN OWNED"
		}
	}
	return ""
}

func postgresGlobalAlterOperation(tokens []lexer.Token) string {
	switch {
	case tokenSequenceAt(tokens, 1, "DEFAULT", "PRIVILEGES") &&
		!containsTokenSequence(tokens, "IN", "SCHEMA"):
		return "ALTER DEFAULT PRIVILEGES without IN SCHEMA"
	case tokenSequenceAt(tokens, 1, "GROUP"):
		return "ALTER GROUP"
	case tokenSequenceAt(tokens, 1, "LARGE", "OBJECT"):
		return "ALTER LARGE OBJECT"
	case tokenSequenceAt(tokens, 1, "EXTENSION"):
		return "ALTER EXTENSION"
	case tokenSequenceAt(tokens, 1, "SCHEMA"):
		return "ALTER SCHEMA metadata"
	default:
		return ""
	}
}

func postgresGlobalPrivilegeOperation(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	recipientKeyword := postgresPrivilegeRecipientKeyword(tokens)
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	recipientIndex := findPostgresKeyword(tokens, recipientKeyword, 1)
	if onIndex == mutationTargetNotFound ||
		(recipientIndex != mutationTargetNotFound && onIndex > recipientIndex) {
		return first + " role membership"
	}
	if onIndex+1 >= len(tokens) {
		return first + " global privilege"
	}

	classIndex := onIndex + 1
	if normalizedIdentifier(tokens[classIndex]) == "ALL" {
		classIndex++
	}
	if classIndex >= len(tokens) {
		return first + " global privilege"
	}
	switch normalizedIdentifier(tokens[classIndex]) {
	case "DATABASE", "LANGUAGE", "PARAMETER", "ROLE", "SCHEMA", "TABLEGROUP", "TABLESPACE":
		return first + " database or global privilege"
	case "FOREIGN":
		return first + " foreign server privilege"
	case "LARGE":
		if tokenSequenceAt(tokens, classIndex, "LARGE", "OBJECT") ||
			tokenSequenceAt(tokens, classIndex, "LARGE", "OBJECTS") {
			return first + " large object privilege"
		}
	case "SERVER":
		return first + " server privilege"
	}
	return ""
}

func postgresGlobalMetadataOperation(tokens []lexer.Token) string {
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	if onIndex == mutationTargetNotFound || onIndex+1 >= len(tokens) {
		return ""
	}
	switch normalizedIdentifier(tokens[onIndex+1]) {
	case "ACCESS", "CAST", "DATABASE", "EVENT", "EXTENSION", "LANGUAGE", "PUBLICATION",
		"ROLE", "SCHEMA", "SERVER", "SUBSCRIPTION", "TABLEGROUP", "TABLESPACE", "TRANSFORM":
		return normalizedIdentifier(tokens[0]) + " ON global metadata"
	case "FOREIGN":
		return normalizedIdentifier(tokens[0]) + " ON foreign server metadata"
	case "LARGE":
		if tokenSequenceAt(tokens, onIndex+1, "LARGE", "OBJECT") {
			return normalizedIdentifier(tokens[0]) + " ON large object metadata"
		}
	}
	return ""
}

func postgresRoleOrAuthorizationChange(tokens []lexer.Token) string {
	if normalizedIdentifier(tokens[0]) != "SET" {
		return ""
	}

	if tokenSequenceAt(tokens, 1, "ROLE") ||
		tokenSequenceAt(tokens, 1, "LOCAL", "ROLE") ||
		tokenSequenceAt(tokens, 1, "SESSION", "ROLE") {
		return "SET ROLE"
	}
	if tokenSequenceAt(tokens, 1, "SESSION", "AUTHORIZATION") ||
		tokenSequenceAt(tokens, 1, "LOCAL", "SESSION", "AUTHORIZATION") ||
		tokenSequenceAt(tokens, 1, "SESSION", "SESSION", "AUTHORIZATION") {
		return "SET SESSION AUTHORIZATION"
	}
	return ""
}

func postgresDialectGlobalOperation(dialect string, tokens []lexer.Token) string {
	switch platform.NormalizeDialect(dialect) {
	case platform.CockroachDB:
		return cockroachGlobalOperation(tokens)
	case platform.YugabyteDB:
		return yugabyteGlobalOperation(tokens)
	default:
		return ""
	}
}

func cockroachGlobalOperation(tokens []lexer.Token) string {
	if operation := cockroachClusterSettingOperation(tokens); operation != "" {
		return operation
	}
	if operation := cockroachGlobalDDLOperation(tokens); operation != "" {
		return operation
	}
	return cockroachJobOrIOOperation(tokens)
}

func cockroachClusterSettingOperation(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	switch {
	case first == "SET" && tokenSequenceAt(tokens, 1, "CLUSTER", "SETTING"):
		return "SET CLUSTER SETTING"
	case first == "RESET" && tokenSequenceAt(tokens, 1, "CLUSTER", "SETTING"):
		return "RESET CLUSTER SETTING"
	default:
		return ""
	}
}

func cockroachGlobalDDLOperation(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	switch {
	case (first == "CREATE" || first == "ALTER" || first == "DROP") &&
		tokenSequenceAt(tokens, 1, "CHANGEFEED"):
		return first + " CHANGEFEED"
	case (first == "CREATE" || first == "ALTER" || first == "DROP") &&
		tokenSequenceAt(tokens, 1, "EXTERNAL", "CONNECTION"):
		return first + " EXTERNAL CONNECTION"
	case (first == "CREATE" || first == "ALTER" || first == "DROP") &&
		tokenSequenceAt(tokens, 1, "TENANT"):
		return first + " TENANT"
	case (first == "CREATE" || first == "ALTER" || first == "DROP") &&
		tokenSequenceAt(tokens, 1, "SCHEDULE"):
		return first + " SCHEDULE"
	case first == "ALTER" && tokenSequenceAt(tokens, 1, "RANGE"):
		return "ALTER RANGE"
	default:
		return ""
	}
}

func cockroachJobOrIOOperation(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "BACKUP", "EXPORT", "IMPORT", "RESTORE":
		return first
	case "CANCEL", "PAUSE", "RESUME":
		if tokenSequenceAt(tokens, 1, "JOB") ||
			tokenSequenceAt(tokens, 1, "JOBS") ||
			tokenSequenceAt(tokens, 1, "SCHEDULE") ||
			tokenSequenceAt(tokens, 1, "SCHEDULES") {
			return first + " cluster job"
		}
	default:
		return ""
	}
	return ""
}

func yugabyteGlobalOperation(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	if (first == "CREATE" || first == "ALTER" || first == "DROP") &&
		tokenSequenceAt(tokens, 1, "TABLEGROUP") {
		return first + " TABLEGROUP"
	}
	return ""
}

func postgresSessionOrControlOperation(tokens []lexer.Token) string {
	first := normalizedIdentifier(tokens[0])
	if first == "REINDEX" &&
		(containsIdentifier(tokens, "DATABASE") || containsIdentifier(tokens, "SYSTEM")) {
		return "REINDEX database or system"
	}
	if first == "SET" && setsPostgresSearchPath(tokens) {
		return "SET search_path"
	}
	switch first {
	case "ABORT", "BEGIN", "CHECKPOINT", "COMMIT", "DEALLOCATE", "DECLARE",
		"DISCARD", "END", "EXECUTE", "FETCH", "LISTEN", "LOAD", "LOCK", "MOVE",
		"NOTIFY", "PREPARE", "RELEASE", "RESET", "ROLLBACK", "SAVEPOINT", "SET",
		"START", "UNLISTEN":
		return first + " session or transaction state"
	default:
		return ""
	}
}

func setsPostgresSearchPath(tokens []lexer.Token) bool {
	index := 1
	if tokenSequenceAt(tokens, index, "LOCAL") || tokenSequenceAt(tokens, index, "SESSION") {
		index++
	}
	return tokenSequenceAt(tokens, index, "SEARCH_PATH")
}

func selectsIntoPostgresTemporaryObject(tokens []lexer.Token) bool {
	for index := 0; index+1 < len(tokens); index++ {
		if normalizedIdentifier(tokens[index]) != "INTO" {
			continue
		}
		targetIndex := index + 1
		if tokenSequenceAt(tokens, targetIndex, "GLOBAL") ||
			tokenSequenceAt(tokens, targetIndex, "LOCAL") {
			targetIndex++
		}
		if tokenSequenceAt(tokens, targetIndex, "TEMP") ||
			tokenSequenceAt(tokens, targetIndex, "TEMPORARY") {
			return true
		}
	}
	return false
}

func dangerousPostgresFunctionCall(tokens []lexer.Token) string {
	if definesPostgresRoutine(tokens) || normalizedIdentifier(tokens[0]) == "CALL" {
		return ""
	}
	for index := 0; index+1 < len(tokens); index++ {
		if !tokens[index+1].MatchOperatorValue("(") {
			continue
		}
		switch normalizedIdentifier(tokens[index]) {
		case "DBLINK", "DBLINK_CANCEL_QUERY", "DBLINK_CLOSE", "DBLINK_CONNECT",
			"DBLINK_CONNECT_U", "DBLINK_DISCONNECT", "DBLINK_EXEC", "DBLINK_FETCH",
			"DBLINK_GET_RESULT", "DBLINK_OPEN", "DBLINK_SEND_QUERY":
			return "external dblink operation"
		case "LO_CLOSE", "LO_CREATE", "LO_EXPORT", "LO_FROM_BYTEA", "LO_IMPORT",
			"LO_LSEEK", "LO_LSEEK64", "LO_OPEN", "LO_PUT", "LO_TRUNCATE",
			"LO_TRUNCATE64", "LO_UNLINK", "LOWRITE":
			return "large object operation"
		case "PG_ADVISORY_LOCK", "PG_ADVISORY_LOCK_SHARED", "PG_TRY_ADVISORY_LOCK",
			"PG_TRY_ADVISORY_LOCK_SHARED":
			return "session advisory lock"
		case "PG_CANCEL_BACKEND", "PG_CREATE_RESTORE_POINT", "PG_NOTIFY", "PG_PROMOTE",
			"PG_RELOAD_CONF", "PG_ROTATE_LOGFILE", "PG_SWITCH_WAL", "PG_TERMINATE_BACKEND",
			"PG_WAL_REPLAY_PAUSE", "PG_WAL_REPLAY_RESUME", "SET_CONFIG":
			return "cluster control function"
		case "PG_COPY_LOGICAL_REPLICATION_SLOT", "PG_COPY_PHYSICAL_REPLICATION_SLOT",
			"PG_CREATE_LOGICAL_REPLICATION_SLOT", "PG_CREATE_PHYSICAL_REPLICATION_SLOT",
			"PG_DROP_REPLICATION_SLOT", "PG_REPLICATION_ORIGIN_ADVANCE",
			"PG_REPLICATION_ORIGIN_CREATE", "PG_REPLICATION_ORIGIN_DROP",
			"PG_REPLICATION_ORIGIN_SESSION_RESET", "PG_REPLICATION_ORIGIN_SESSION_SETUP",
			"PG_REPLICATION_ORIGIN_XACT_RESET", "PG_REPLICATION_ORIGIN_XACT_SETUP":
			return "replication state operation"
		}
	}
	return ""
}

func definesPostgresRoutine(tokens []lexer.Token) bool {
	if len(tokens) == 0 {
		return false
	}
	first := normalizedIdentifier(tokens[0])
	if first != "CREATE" && first != "ALTER" {
		return false
	}
	index := 1
	if first == "CREATE" && tokenSequenceAt(tokens, index, "OR", "REPLACE") {
		index += 2
	}
	return tokenSequenceAt(tokens, index, "FUNCTION") ||
		tokenSequenceAt(tokens, index, "PROCEDURE") ||
		first == "ALTER" && tokenSequenceAt(tokens, index, "ROUTINE")
}

func postgresObjectClassWidth(tokens []lexer.Token, index int) int {
	switch normalizedIdentifier(tokens[index]) {
	case "ACCESS", "EVENT", "LARGE", "MATERIALIZED":
		return 2
	case "FOREIGN":
		if tokenSequenceAt(tokens, index, "FOREIGN", "DATA", "WRAPPER") {
			return 3
		}
		return 2
	case "OPERATOR":
		if tokenSequenceAt(tokens, index, "OPERATOR", "CLASS") ||
			tokenSequenceAt(tokens, index, "OPERATOR", "FAMILY") {
			return 2
		}
		return 1
	case "TEXT":
		if tokenSequenceAt(tokens, index, "TEXT", "SEARCH") {
			return min(3, len(tokens)-index)
		}
		return 1
	case "AGGREGATE", "CAST", "COLLATION", "COLUMN", "CONSTRAINT", "CONVERSION",
		"DATABASE", "DOMAIN", "EXTENSION", "FUNCTION", "INDEX", "LANGUAGE",
		"PARAMETER", "POLICY", "PROCEDURE", "PUBLICATION", "ROLE", "ROUTINE",
		"RULE", "SCHEMA", "SEQUENCE", "SERVER", "STATISTICS", "SUBSCRIPTION",
		"TABLE", "TABLEGROUP", "TABLESPACE", "TRANSFORM", "TRIGGER", "TYPE", "VIEW":
		return 1
	default:
		return 0
	}
}

func postgresPrivilegeRecipientKeyword(tokens []lexer.Token) string {
	if len(tokens) > 0 && normalizedIdentifier(tokens[0]) == "REVOKE" {
		return "FROM"
	}
	return "TO"
}

func findPostgresKeyword(tokens []lexer.Token, value string, start int) int {
	for index := max(start, 0); index < len(tokens); index++ {
		if postgresKeywordAt(tokens[index], value) {
			return index
		}
	}
	return mutationTargetNotFound
}

func postgresKeywordAt(token lexer.Token, value string) bool {
	return token.Type == lexer.TokenIdentifier && strings.EqualFold(token.Value, value)
}
