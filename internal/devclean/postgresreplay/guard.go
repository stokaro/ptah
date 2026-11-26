package postgresreplay

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/internal/pgsystemschema"
)

func validatePostgresReplayStatement(dialect string, names sessionNames, tokens []lexer.Token, scope replay.Scope) error {
	if len(tokens) == 0 {
		return nil
	}
	policy := protection{
		dialect:       dialect,
		sessionTables: scope.Session && dialect == platform.Postgres,
		trustedSQL:    scope.TrustedSQL,
		publications:  scope.Publications,
		catalog:       names.catalog,
		creation:      names.creation,
		unqualified:   new(string),
	}

	first := replay.NormalizedIdentifier(tokens[0])
	if first == "DO" {
		return replay.Refuse(dialect, "DO sublanguage")
	}
	if first == "CALL" {
		return replay.Refuse(dialect, "CALL sublanguage")
	}
	if definesPostgresRoutine(tokens) {
		return replay.Refuse(dialect, first+" routine definition")
	}
	if namespace := policy.protectedPostgresMutationNamespace(tokens); namespace != "" {
		return refuseProtectedNamespace(dialect, fmt.Sprintf("protected namespace %q mutation", namespace), *policy.unqualified)
	}
	// It reads a remote server's catalog into foreign tables of the dev
	// database, which the cleanup drops; only the read reaches past the server.
	if first == "IMPORT" && replay.ContainsTokenSequence(tokens, "FOREIGN", "SCHEMA") && !scope.TrustedSQL {
		return replay.Refuse(dialect, "IMPORT FOREIGN SCHEMA")
	}
	if operation := postgresGlobalDCLOrMetadata(tokens); operation != "" &&
		!postgresSchemaStateReturned(tokens, scope) && !policy.commentOnDroppedObject(tokens, scope.Survivors) {
		return replay.Refuse(dialect, operation)
	}
	if operation := postgresRoleOrAuthorizationChange(tokens); operation != "" {
		return replay.Refuse(dialect, operation)
	}
	if operation := postgresDialectGlobalOperation(dialect, tokens); operation != "" {
		return replay.Refuse(dialect, operation)
	}
	if first == "LOCK" {
		if operation := policy.transactionScopedLockRefusal(tokens); operation != "" {
			return refuseProtectedNamespace(dialect, operation, *policy.unqualified)
		}
	} else if operation := postgresSessionOrControlOperation(tokens); operation != "" {
		return replay.Refuse(dialect, operation)
	}
	if selectsIntoPostgresTemporaryObject(tokens) {
		return replay.Refuse(dialect, "TEMP object")
	}
	if operation := dangerousPostgresFunctionCall(tokens, scope); operation != "" {
		return replay.Refuse(dialect, operation)
	}

	return policy.validateBase(tokens)
}

// protection reads the namespaces a replayed statement must not change on one
// member of the PostgreSQL family: the ones its server owns.
type protection struct {
	dialect       string
	sessionTables bool
	// trustedSQL is [replay.Scope.TrustedSQL]: it lifts an external COPY, whose
	// only reason to be refused is that it reaches past the server.
	trustedSQL bool
	// publications is [replay.Scope.Publications]; see confinedPublicationDDL.
	publications replay.PublicationScope
	// catalog is what an unqualified name reaches in the namespaces the server
	// owns, as the dev session's catalog names it; see
	// [protection.unqualifiedNamespace].
	catalog catalogNames
	// creation is the schema the dev session creates an object in when a
	// CREATE writes its name without one, or ""; see
	// [protection.creationNamespace].
	creation string
	// unqualified receives the first name written without a schema that
	// reached a namespace the server owns, so the refusal can say to qualify
	// it. Every reader returns its first protected namespace at once, so the
	// name is the one that refusal names. Nil records none.
	unqualified *string
}

func (p protection) protectedPostgresMutationNamespace(tokens []lexer.Token) string {
	if namespace := p.protectedPostgresSelectIntoNamespace(tokens); namespace != "" {
		return namespace
	}
	class := mutationTargetClass(tokens)
	for _, target := range replay.MutationTargets(tokens) {
		if namespace := p.protectedNamespaceFromMutationTarget(tokens, target, class); namespace != "" {
			return namespace
		}
	}
	if namespace := p.createdTargetNamespace(tokens); namespace != "" {
		return namespace
	}
	for index := 1; index < len(tokens); index++ {
		switch replay.NormalizedIdentifier(tokens[index]) {
		case "DELETE", "INSERT", "MERGE", "TRUNCATE", "UPDATE":
			for _, target := range replay.MutationTargets(tokens[index:]) {
				if namespace := p.protectedNamespaceFromMutationTarget(tokens, target, classRelation); namespace != "" {
					return namespace
				}
			}
		}
	}
	if namespace := p.protectedPostgresMutationListNamespace(tokens); namespace != "" {
		return namespace
	}
	if namespace := p.protectedPostgresSpecialMutationNamespace(tokens); namespace != "" {
		return namespace
	}

	switch replay.NormalizedIdentifier(tokens[0]) {
	case "ALTER":
		if replay.TokenSequenceAt(tokens, 1, "DEFAULT", "PRIVILEGES") {
			return p.protectedPostgresSchemaClause(tokens)
		}
	case "COPY":
		return p.copyTargetNamespace(tokens)
	case "COMMENT", "SECURITY":
		return p.protectedPostgresObjectTarget(tokens)
	case "GRANT", "REVOKE":
		if namespace := p.protectedPostgresSchemaClause(tokens); namespace != "" {
			return namespace
		}
		return p.protectedPostgresObjectTarget(tokens)
	}
	return ""
}

func (p protection) protectedPostgresSelectIntoNamespace(tokens []lexer.Token) string {
	tokens = replay.LeadingCTEExecutableTokens(tokens)
	if replay.NormalizedIdentifier(tokens[0]) != "SELECT" {
		return ""
	}
	for index := 1; index < len(tokens); index++ {
		if replay.NormalizedIdentifier(tokens[index]) != "INTO" {
			continue
		}
		targetIndex := index + 1
		for targetIndex < len(tokens) {
			switch replay.NormalizedIdentifier(tokens[targetIndex]) {
			case "GLOBAL", "LOCAL", "TEMP", "TEMPORARY", "UNLOGGED", "TABLE":
				targetIndex++
			default:
				return p.createdNameAt(tokens, targetIndex)
			}
		}
		return ""
	}
	return ""
}

// mutationTargetClass returns the class of the existing objects
// [replay.MutationTargets] reads off a statement: the table a DML statement
// writes, the object of the kind an ALTER or DROP names, and the table a
// CREATE INDEX indexes. It is "" for a statement whose targets are no object
// of a namespace, a trigger's name among them, and for every other CREATE:
// its target is the object it creates, which no lookup finds, and
// [protection.createdTargetNamespace] reads it.
func mutationTargetClass(tokens []lexer.Token) catalogClass {
	switch replay.NormalizedIdentifier(tokens[0]) {
	case "DELETE", "INSERT", "MERGE", "TRUNCATE", "UPDATE":
		return classRelation
	case "ALTER", "DROP":
		kindIndex := replay.StatementObjectKindIndex(tokens)
		if kindIndex == replay.MutationTargetNotFound {
			return ""
		}
		if class, ok := objectKindClass(tokens, kindIndex); ok {
			return class
		}
	case "CREATE":
		if createsIndex(tokens) {
			return classRelation
		}
	}
	return ""
}

// createsIndex reports CREATE INDEX and CREATE UNIQUE INDEX. Their mutation
// target is the table after ON, which exists: the index is created in that
// table's schema, whatever its own name.
func createsIndex(tokens []lexer.Token) bool {
	kindIndex := replay.StatementObjectKindIndex(tokens)
	return kindIndex != replay.MutationTargetNotFound &&
		(replay.TokenSequenceAt(tokens, kindIndex, "INDEX") ||
			replay.TokenSequenceAt(tokens, kindIndex, "UNIQUE", "INDEX"))
}

// createdTargetNamespace returns the namespace the server owns that a CREATE
// creates its object in when the object's name is written without a schema,
// as [protection.createdNameAt] answers, or "". A name written with a schema
// is read by [protection.protectedNamespaceFromMutationTarget]. The kinds read
// here are the ones [replay.MutationTargets] finds the created name of; the
// others are read by [protection.protectedPostgresNamedSpecialDDLNamespace].
func (p protection) createdTargetNamespace(tokens []lexer.Token) string {
	if replay.NormalizedIdentifier(tokens[0]) != "CREATE" || createsIndex(tokens) {
		return ""
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return ""
	}
	if _, ok := objectKindClass(tokens, kindIndex); !ok {
		return ""
	}
	for _, target := range replay.MutationTargets(tokens) {
		if len(target) != 1 {
			continue
		}
		if namespace := p.creationNamespace(target[0]); namespace != "" {
			return namespace
		}
	}
	return ""
}

// protectedNamespaceFromMutationTarget returns the namespace the server owns
// that target is in: the one its first part names, or for a name of class
// written without a schema, the one the server finds it in. A class of ""
// reads no name without a schema.
func (p protection) protectedNamespaceFromMutationTarget(
	tokens []lexer.Token,
	target []string,
	class catalogClass,
) string {
	if len(target) >= 2 && p.isProtectedPostgresNamespace(target[0]) {
		if !p.sessionTables || target[0] != "pg_temp" {
			return target[0]
		}
	}
	if len(target) == 0 {
		return ""
	}
	if class != "" && len(target) == 1 {
		if namespace := p.unqualifiedNamespace(class, target[0]); namespace != "" {
			return namespace
		}
	}

	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex != replay.MutationTargetNotFound &&
		replay.NormalizedIdentifier(tokens[kindIndex]) == "SCHEMA" &&
		p.isProtectedPostgresNamespace(target[0]) {
		return target[0]
	}
	return ""
}

func (p protection) protectedPostgresSchemaClause(tokens []lexer.Token) string {
	for index := 0; index+1 < len(tokens); index++ {
		if replay.NormalizedIdentifier(tokens[index]) != "SCHEMA" {
			continue
		}
		if index > 0 {
			previous := replay.NormalizedIdentifier(tokens[index-1])
			if previous != "IN" && previous != "ON" {
				continue
			}
		}
		if namespace := p.protectedPostgresObjectListNamespace(
			tokens,
			index+1,
			postgresPrivilegeRecipientKeyword(tokens),
			p.protectedPostgresNamespaceObjectAt,
		); namespace != "" {
			return namespace
		}
	}
	return ""
}

func (p protection) protectedPostgresMutationListNamespace(tokens []lexer.Token) string {
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "DROP":
		const kindIndex = 1
		if kindIndex >= len(tokens) {
			return ""
		}
		kind := replay.NormalizedIdentifier(tokens[kindIndex])
		if !postgresDropSupportsObjectList(tokens, kindIndex, kind) {
			return ""
		}
		targetAt := p.protectedPostgresQualifiedNamespaceAt
		if class, ok := objectKindClass(tokens, kindIndex); ok {
			targetAt = p.protectedNameAt(class)
		}
		if kind == "SCHEMA" {
			targetAt = p.protectedPostgresNamespaceObjectAt
		}
		return p.protectedPostgresObjectListNamespace(
			tokens,
			replay.ObjectNameIndex(tokens, kindIndex),
			"",
			targetAt,
		)
	case "TRUNCATE":
		index := 1
		if replay.TokenSequenceAt(tokens, index, "TABLE") {
			index++
		}
		if replay.TokenSequenceAt(tokens, index, "ONLY") {
			index++
		}
		return p.protectedPostgresObjectListNamespace(
			tokens,
			index,
			"",
			p.protectedNameAt(classRelation),
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
		return replay.TokenSequenceAt(tokens, kindIndex, "FOREIGN", "TABLE")
	case "MATERIALIZED":
		return replay.TokenSequenceAt(tokens, kindIndex, "MATERIALIZED", "VIEW")
	default:
		return false
	}
}

func (p protection) protectedPostgresSpecialMutationNamespace(tokens []lexer.Token) string {
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "ALTER", "CREATE", "DROP":
		return p.protectedPostgresSpecialDDLNamespace(tokens)
	case "IMPORT":
		if replay.ContainsTokenSequence(tokens, "FOREIGN", "SCHEMA") {
			return p.protectedPostgresNamespaceAfterKeyword(
				tokens,
				"INTO",
				1,
				p.protectedPostgresNamespaceObjectAt,
			)
		}
	case "REFRESH":
		if replay.TokenSequenceAt(tokens, 1, "MATERIALIZED", "VIEW") {
			index := 3
			if replay.TokenSequenceAt(tokens, index, "CONCURRENTLY") {
				index++
			}
			return p.protectedNameAt(classRelation)(tokens, index)
		}
	case "REINDEX":
		return p.protectedPostgresReindexNamespace(tokens)
	}
	return ""
}

func (p protection) protectedPostgresSpecialDDLNamespace(tokens []lexer.Token) string {
	kindIndex := postgresSpecialDDLKindIndex(tokens)
	if kindIndex >= len(tokens) {
		return ""
	}
	kind := replay.NormalizedIdentifier(tokens[kindIndex])
	switch kind {
	case "AGGREGATE", "COLLATION", "CONVERSION", "OPERATOR", "STATISTICS":
		return p.protectedPostgresNamedSpecialDDLNamespace(tokens, kindIndex, kind)
	case "INDEX":
		// CREATE INDEX names the table after ON, which the mutation targets
		// read; ALTER INDEX names the index itself.
		if replay.NormalizedIdentifier(tokens[0]) != "ALTER" {
			return ""
		}
		return p.protectedNameAt(classRelation)(tokens, replay.ObjectNameIndex(tokens, kindIndex))
	case "EXTENSION":
		return p.protectedPostgresNamespaceAfterKeyword(
			tokens,
			"SCHEMA",
			kindIndex+1,
			p.protectedPostgresNamespaceObjectAt,
		)
	case "POLICY", "TRIGGER":
		return p.protectedPostgresNamespaceAfterKeyword(
			tokens,
			"ON",
			kindIndex+1,
			p.protectedNameAt(classRelation),
		)
	case "CONSTRAINT":
		if !replay.TokenSequenceAt(tokens, kindIndex, "CONSTRAINT", "TRIGGER") {
			return ""
		}
		return p.protectedPostgresNamespaceAfterKeyword(
			tokens,
			"ON",
			kindIndex+2,
			p.protectedNameAt(classRelation),
		)
	case "RULE":
		keyword := "ON"
		if replay.NormalizedIdentifier(tokens[0]) == "CREATE" {
			keyword = "TO"
		}
		return p.protectedPostgresNamespaceAfterKeyword(
			tokens,
			keyword,
			kindIndex+1,
			p.protectedNameAt(classRelation),
		)
	default:
		return ""
	}
}

func postgresSpecialDDLKindIndex(tokens []lexer.Token) int {
	index := 1
	if replay.NormalizedIdentifier(tokens[0]) == "CREATE" &&
		replay.TokenSequenceAt(tokens, index, "OR", "REPLACE") {
		index += 2
	}
	return index
}

func (p protection) protectedPostgresNamedSpecialDDLNamespace(
	tokens []lexer.Token,
	kindIndex int,
	kind string,
) string {
	nameIndex := kindIndex + 1
	if kind == "OPERATOR" &&
		(replay.TokenSequenceAt(tokens, nameIndex, "CLASS") ||
			replay.TokenSequenceAt(tokens, nameIndex, "FAMILY")) {
		nameIndex++
	}
	nameIndex = replay.SkipNameModifiers(tokens, nameIndex)
	class, _ := objectKindClass(tokens, kindIndex)
	nameAt := p.protectedNameAt(class)
	if replay.NormalizedIdentifier(tokens[0]) == "CREATE" {
		nameAt = p.createdNameAt
	}
	if namespace := p.protectedPostgresObjectListNamespace(
		tokens,
		nameIndex,
		"",
		nameAt,
	); namespace != "" {
		return namespace
	}
	if kind == "STATISTICS" {
		return p.protectedPostgresNamespaceAfterKeyword(
			tokens,
			"FROM",
			nameIndex,
			p.protectedNameAt(classRelation),
		)
	}
	return ""
}

func (p protection) protectedPostgresReindexNamespace(tokens []lexer.Token) string {
	for index := 1; index < len(tokens); index++ {
		switch replay.NormalizedIdentifier(tokens[index]) {
		case "DATABASE", "SYSTEM":
			return ""
		case "INDEX", "TABLE":
			targetIndex := replay.SkipNameModifiers(tokens, index+1)
			return p.protectedNameAt(classRelation)(tokens, targetIndex)
		case "SCHEMA":
			targetIndex := replay.SkipNameModifiers(tokens, index+1)
			return p.protectedPostgresNamespaceObjectAt(tokens, targetIndex)
		}
	}
	return ""
}

func (p protection) protectedPostgresNamespaceAfterKeyword(
	tokens []lexer.Token,
	keyword string,
	start int,
	targetAt func([]lexer.Token, int) string,
) string {
	index := findPostgresKeyword(tokens, keyword, start)
	if index == replay.MutationTargetNotFound {
		return ""
	}
	return targetAt(tokens, index+1)
}

func (p protection) protectedPostgresObjectTarget(tokens []lexer.Token) string {
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	if onIndex == replay.MutationTargetNotFound || onIndex+1 >= len(tokens) {
		return ""
	}

	targetIndex := onIndex + 1
	classWidth := postgresObjectClassWidth(tokens, targetIndex)
	targetIndex += classWidth
	targetAt := p.protectedPostgresQualifiedNamespaceAt
	switch class, ok := objectKindClass(tokens, onIndex+1); {
	case classWidth == 0:
		targetAt = p.protectedNameAt(classRelation)
	case classWidth == 1 && replay.NormalizedIdentifier(tokens[onIndex+1]) == "SCHEMA":
		targetAt = p.protectedPostgresNamespaceObjectAt
	case classWidth == 1 && replay.NormalizedIdentifier(tokens[onIndex+1]) == "COLUMN":
		targetAt = p.protectedColumnAt
	case ok:
		targetAt = p.protectedNameAt(class)
	}
	if namespace := p.protectedPostgresObjectListNamespace(
		tokens,
		targetIndex,
		postgresPrivilegeRecipientKeyword(tokens),
		targetAt,
	); namespace != "" {
		return namespace
	}

	kind := replay.NormalizedIdentifier(tokens[onIndex+1])
	if kind != "CONSTRAINT" && kind != "POLICY" && kind != "RULE" && kind != "TRIGGER" {
		return ""
	}
	relationOnIndex := findPostgresKeyword(tokens, "ON", targetIndex+1)
	if relationOnIndex == replay.MutationTargetNotFound {
		return ""
	}
	return p.protectedNameAt(classRelation)(tokens, relationOnIndex+1)
}

// protectedColumnAt reads the column a COMMENT or a SECURITY LABEL names:
// schema.table.column, whose first part is a namespace, or table.column,
// whose first part is a table written without a schema.
func (p protection) protectedColumnAt(tokens []lexer.Token, index int) string {
	parts, _ := replay.QualifiedIdentifierAt(tokens, index)
	switch len(parts) {
	case 0, 1:
		return ""
	case 2:
		return p.unqualifiedNamespace(classRelation, parts[0])
	}
	return p.protectedPostgresQualifiedNamespaceAt(tokens, index)
}

func (p protection) protectedPostgresObjectListNamespace(
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
		parts, next := replay.QualifiedIdentifierAt(tokens, index)
		if len(parts) == 0 {
			continue
		}
		expectTarget = false
		index = next - 1
	}
	return ""
}

func (p protection) protectedPostgresNamespaceObjectAt(tokens []lexer.Token, index int) string {
	if index < 0 || index >= len(tokens) {
		return ""
	}
	name := replay.IdentifierValue(tokens[index])
	if !p.isProtectedPostgresNamespace(name) {
		return ""
	}
	return name
}

func (p protection) protectedPostgresQualifiedNamespaceAt(tokens []lexer.Token, index int) string {
	name := p.protectedPostgresNamespaceObjectAt(tokens, index)
	if name == "" {
		return ""
	}
	if index+1 < len(tokens) && tokens[index+1].MatchOperatorValue(".") {
		if p.sessionTables && replay.IdentifierValue(tokens[index]) == "pg_temp" {
			return ""
		}
		return name
	}
	return ""
}

// isProtectedPostgresNamespace reports whether namespace is one the server of
// the dialect owns ([pgsystemschema.IsPostgresFamilySystemSchema]). The name
// is folded to lower case first, so a quoted lookalike in another case is
// refused as well: a replay refuses where it cannot tell.
func (p protection) isProtectedPostgresNamespace(namespace string) bool {
	return pgsystemschema.IsPostgresFamilySystemSchema(p.dialect, strings.ToLower(strings.TrimSpace(namespace)))
}

func postgresGlobalDCLOrMetadata(tokens []lexer.Token) string {
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "ALTER":
		return postgresGlobalAlterOperation(tokens)
	case "COMMENT", "SECURITY":
		return postgresGlobalMetadataOperation(tokens)
	case "CREATE":
		if replay.TokenSequenceAt(tokens, 1, "GROUP") {
			return "CREATE GROUP"
		}
	case "DROP":
		if replay.TokenSequenceAt(tokens, 1, "GROUP") {
			return "DROP GROUP"
		}
		if replay.TokenSequenceAt(tokens, 1, "OWNED") {
			return "DROP OWNED"
		}
	case "GRANT", "REVOKE":
		return postgresGlobalPrivilegeOperation(tokens)
	case "REASSIGN":
		if replay.TokenSequenceAt(tokens, 1, "OWNED") {
			return "REASSIGN OWNED"
		}
	}
	return ""
}

// postgresGlobalAlterOperation names an ALTER whose effect outlives the realm
// cleanup, or "" for any other.
//
// ALTER DEFAULT PRIVILEGES without IN SCHEMA is not one of them: it sets a
// global default, which pg_default_acl records per database, and the realm
// cleanup returns every global default of a named role to the built-in one
// (stokaro/ptah#3772). CockroachDB's FOR ALL ROLES without IN SCHEMA is: no
// read can say what it holds apart from the built-in default, so the cleanup
// cannot reset it.
func postgresGlobalAlterOperation(tokens []lexer.Token) string {
	switch {
	case isGlobalAllRolesDefaultPrivileges(tokens):
		return "ALTER DEFAULT PRIVILEGES FOR ALL ROLES without IN SCHEMA"
	case replay.TokenSequenceAt(tokens, 1, "GROUP"):
		return "ALTER GROUP"
	case replay.TokenSequenceAt(tokens, 1, "LARGE", "OBJECT"):
		return "ALTER LARGE OBJECT"
	case replay.TokenSequenceAt(tokens, 1, "EXTENSION"):
		return "ALTER EXTENSION"
	case replay.TokenSequenceAt(tokens, 1, "SCHEMA"):
		return "ALTER SCHEMA metadata"
	default:
		return ""
	}
}

// isGlobalAllRolesDefaultPrivileges reports ALTER DEFAULT PRIVILEGES FOR ALL
// ROLES without IN SCHEMA.
func isGlobalAllRolesDefaultPrivileges(tokens []lexer.Token) bool {
	return replay.TokenSequenceAt(tokens, 1, "DEFAULT", "PRIVILEGES", "FOR", "ALL", "ROLES") &&
		!replay.ContainsTokenSequence(tokens, "IN", "SCHEMA")
}

func postgresGlobalPrivilegeOperation(tokens []lexer.Token) string {
	first := replay.NormalizedIdentifier(tokens[0])
	recipientKeyword := postgresPrivilegeRecipientKeyword(tokens)
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	recipientIndex := findPostgresKeyword(tokens, recipientKeyword, 1)
	if onIndex == replay.MutationTargetNotFound ||
		(recipientIndex != replay.MutationTargetNotFound && onIndex > recipientIndex) {
		return first + " role membership"
	}
	if onIndex+1 >= len(tokens) {
		return first + " global privilege"
	}

	classIndex := onIndex + 1
	if replay.NormalizedIdentifier(tokens[classIndex]) == "ALL" {
		classIndex++
	}
	if classIndex >= len(tokens) {
		return first + " global privilege"
	}
	switch replay.NormalizedIdentifier(tokens[classIndex]) {
	case "DATABASE", "LANGUAGE", "PARAMETER", "ROLE", "SCHEMA", "TABLEGROUP", "TABLESPACE":
		return first + " database or global privilege"
	case "FOREIGN":
		return first + " foreign server privilege"
	case "LARGE":
		if replay.TokenSequenceAt(tokens, classIndex, "LARGE", "OBJECT") ||
			replay.TokenSequenceAt(tokens, classIndex, "LARGE", "OBJECTS") {
			return first + " large object privilege"
		}
	case "SERVER":
		return first + " server privilege"
	}
	return ""
}

// postgresSchemaStateReturned reports a statement whose effect the scope's reset
// undoes: a schema's comment and its privileges in a plan rehearsal, and its
// comment alone in a migration replay.
func postgresSchemaStateReturned(tokens []lexer.Token, scope replay.Scope) bool {
	if !postgresSchemaStateStatement(tokens) {
		return false
	}
	return scope.SchemaStateReturned ||
		(scope.SchemaCommentReturned && replay.NormalizedIdentifier(tokens[0]) == "COMMENT")
}

// postgresSchemaStateStatement reports a comment on a schema, or a privilege
// granted or revoked on one: what a reset that returns a schema's state undoes;
// see [replay.Scope.SchemaStateReturned]. A security label is not one, because
// no reset reads it.
func postgresSchemaStateStatement(tokens []lexer.Token) bool {
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	if onIndex == replay.MutationTargetNotFound || replay.NormalizedIdentifier(replay.TokenAt(tokens, onIndex+1)) != "SCHEMA" {
		return false
	}
	switch replay.NormalizedIdentifier(tokens[0]) {
	case "COMMENT", "GRANT", "REVOKE":
		return true
	}
	return false
}

// commentOnDroppedObject reports a comment on an extension or a schema that
// every cleanup of the run drops: COMMENT ON EXTENSION or COMMENT ON SCHEMA,
// a single name and IS, on a run whose survivors were recorded, naming an
// extension or a schema they do not name. The run created it, and the comment
// is dropped with it; see [replay.CleanupSurvivors]. pg_dump writes one for
// every extension it dumps, and Ptah renders one for an extension or a schema
// that declares a comment.
//
// The name is compared as the server stores it ([storedName]). A Unicode
// escape reaches it decoded, as the quoted identifier it spells
// ([decodedUnicodeIdentifiers]), and a schema the server owns is refused
// whatever the claim found: the server creates pg_temp and
// pg_toast_temp schemas as sessions need them and keeps them afterwards, so
// one the run's own session created is missing from the claim and outlives
// the run.
func (p protection) commentOnDroppedObject(tokens []lexer.Token, survivors replay.CleanupSurvivors) bool {
	if !survivors.Recorded || len(tokens) != 6 ||
		!replay.TokenSequenceAt(tokens, 0, "COMMENT", "ON") || !replay.TokenSequenceAt(tokens, 4, "IS") {
		return false
	}
	name := tokens[3]
	if name.Type != lexer.TokenIdentifier && !quotedIdentifier(name) {
		return false
	}
	stored := storedName(name)
	switch replay.NormalizedIdentifier(tokens[2]) {
	case "EXTENSION":
		return !slices.Contains(survivors.Extensions, stored)
	case "SCHEMA":
		return !p.isProtectedPostgresNamespace(stored) && !slices.Contains(survivors.Schemas, stored)
	}
	return false
}

func postgresGlobalMetadataOperation(tokens []lexer.Token) string {
	onIndex := findPostgresKeyword(tokens, "ON", 1)
	if onIndex == replay.MutationTargetNotFound || onIndex+1 >= len(tokens) {
		return ""
	}
	switch replay.NormalizedIdentifier(tokens[onIndex+1]) {
	case "ACCESS", "CAST", "DATABASE", "EVENT", "EXTENSION", "LANGUAGE", "PUBLICATION",
		"ROLE", "SCHEMA", "SERVER", "SUBSCRIPTION", "TABLEGROUP", "TABLESPACE", "TRANSFORM":
		return replay.NormalizedIdentifier(tokens[0]) + " ON global metadata"
	case "FOREIGN":
		return replay.NormalizedIdentifier(tokens[0]) + " ON foreign server metadata"
	case "LARGE":
		if replay.TokenSequenceAt(tokens, onIndex+1, "LARGE", "OBJECT") {
			return replay.NormalizedIdentifier(tokens[0]) + " ON large object metadata"
		}
	}
	return ""
}

func postgresRoleOrAuthorizationChange(tokens []lexer.Token) string {
	if replay.NormalizedIdentifier(tokens[0]) != "SET" {
		return ""
	}

	if replay.TokenSequenceAt(tokens, 1, "ROLE") ||
		replay.TokenSequenceAt(tokens, 1, "LOCAL", "ROLE") ||
		replay.TokenSequenceAt(tokens, 1, "SESSION", "ROLE") {
		return "SET ROLE"
	}
	if replay.TokenSequenceAt(tokens, 1, "SESSION", "AUTHORIZATION") ||
		replay.TokenSequenceAt(tokens, 1, "LOCAL", "SESSION", "AUTHORIZATION") ||
		replay.TokenSequenceAt(tokens, 1, "SESSION", "SESSION", "AUTHORIZATION") {
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
	first := replay.NormalizedIdentifier(tokens[0])
	switch {
	case first == "SET" && replay.TokenSequenceAt(tokens, 1, "CLUSTER", "SETTING"):
		return "SET CLUSTER SETTING"
	case first == "RESET" && replay.TokenSequenceAt(tokens, 1, "CLUSTER", "SETTING"):
		return "RESET CLUSTER SETTING"
	default:
		return ""
	}
}

func cockroachGlobalDDLOperation(tokens []lexer.Token) string {
	first := replay.NormalizedIdentifier(tokens[0])
	switch {
	case (first == "CREATE" || first == "ALTER" || first == "DROP") &&
		replay.TokenSequenceAt(tokens, 1, "CHANGEFEED"):
		return first + " CHANGEFEED"
	case (first == "CREATE" || first == "ALTER" || first == "DROP") &&
		replay.TokenSequenceAt(tokens, 1, "EXTERNAL", "CONNECTION"):
		return first + " EXTERNAL CONNECTION"
	case (first == "CREATE" || first == "ALTER" || first == "DROP") &&
		replay.TokenSequenceAt(tokens, 1, "TENANT"):
		return first + " TENANT"
	case (first == "CREATE" || first == "ALTER" || first == "DROP") &&
		replay.TokenSequenceAt(tokens, 1, "SCHEDULE"):
		return first + " SCHEDULE"
	case first == "ALTER" && replay.TokenSequenceAt(tokens, 1, "RANGE"):
		return "ALTER RANGE"
	default:
		return ""
	}
}

func cockroachJobOrIOOperation(tokens []lexer.Token) string {
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "BACKUP", "EXPORT", "IMPORT", "RESTORE":
		return first
	case "CANCEL", "PAUSE", "RESUME":
		if replay.TokenSequenceAt(tokens, 1, "JOB") ||
			replay.TokenSequenceAt(tokens, 1, "JOBS") ||
			replay.TokenSequenceAt(tokens, 1, "SCHEDULE") ||
			replay.TokenSequenceAt(tokens, 1, "SCHEDULES") {
			return first + " cluster job"
		}
	default:
		return ""
	}
	return ""
}

func yugabyteGlobalOperation(tokens []lexer.Token) string {
	first := replay.NormalizedIdentifier(tokens[0])
	if (first == "CREATE" || first == "ALTER" || first == "DROP") &&
		replay.TokenSequenceAt(tokens, 1, "TABLEGROUP") {
		return first + " TABLEGROUP"
	}
	return ""
}

func postgresSessionOrControlOperation(tokens []lexer.Token) string {
	first := replay.NormalizedIdentifier(tokens[0])
	if first == "REINDEX" &&
		(replay.ContainsIdentifier(tokens, "DATABASE") || replay.ContainsIdentifier(tokens, "SYSTEM")) {
		return "REINDEX database or system"
	}
	if first == "SET" || first == "RESET" {
		return postgresSettingOperation(first, tokens)
	}
	switch first {
	case "ABORT", "BEGIN", "CHECKPOINT", "COMMIT", "DEALLOCATE", "DECLARE",
		"DISCARD", "END", "EXECUTE", "FETCH", "LISTEN", "LOAD", "LOCK", "MOVE",
		"NOTIFY", "PREPARE", "RELEASE", "ROLLBACK", "SAVEPOINT", "START", "UNLISTEN":
		return first + " session or transaction state"
	default:
		return ""
	}
}

// postgresCallArguments splits the arguments of the call whose opening
// parenthesis is at open into their tokens, at the commas outside any nested
// parentheses. It returns nil when the call is not closed.
func postgresCallArguments(tokens []lexer.Token, open int) [][]lexer.Token {
	if open >= len(tokens) || !tokens[open].MatchOperatorValue("(") {
		return nil
	}
	var arguments [][]lexer.Token
	var current []lexer.Token
	depth := 0
	for index := open + 1; index < len(tokens); index++ {
		token := tokens[index]
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")") && depth == 0:
			return append(arguments, current)
		case token.MatchOperatorValue(")"):
			depth--
		case token.MatchOperatorValue(",") && depth == 0:
			arguments = append(arguments, current)
			current = nil
			continue
		}
		current = append(current, token)
	}
	return nil
}

func selectsIntoPostgresTemporaryObject(tokens []lexer.Token) bool {
	for index := 0; index+1 < len(tokens); index++ {
		if replay.NormalizedIdentifier(tokens[index]) != "INTO" {
			continue
		}
		targetIndex := index + 1
		if replay.TokenSequenceAt(tokens, targetIndex, "GLOBAL") ||
			replay.TokenSequenceAt(tokens, targetIndex, "LOCAL") {
			targetIndex++
		}
		if replay.TokenSequenceAt(tokens, targetIndex, "TEMP") ||
			replay.TokenSequenceAt(tokens, targetIndex, "TEMPORARY") {
			return true
		}
	}
	return false
}

// dangerousPostgresFunctionCall names the first call the replay refuses. Under
// [replay.Scope.TrustedSQL], a dblink call is not one: it reaches another server and changes
// nothing in this one. The control, replication, large object and advisory
// lock calls are refused either way, because they act on the server the run
// uses, on state the realm cleanup cannot remove, or on the replay's session.
func dangerousPostgresFunctionCall(tokens []lexer.Token, scope replay.Scope) string {
	if definesPostgresRoutine(tokens) || replay.NormalizedIdentifier(tokens[0]) == "CALL" {
		return ""
	}
	// The control and replication functions are the ones the PostgreSQL
	// descriptor states, which the plan and check-assertion scans of
	// internal/sqlreach refuse too.
	described, _ := platform.Lookup(platform.Postgres)
	for index := 0; index+1 < len(tokens); index++ {
		if !tokens[index+1].MatchOperatorValue("(") {
			continue
		}
		name := replay.NormalizedIdentifier(tokens[index])
		// The control and replication names are shared with the
		// check-assertion validator, which refuses the same set in a predicate
		// it is about to send to a live database. The rest below are this
		// guard's own: they answer what a dev-clean run may execute, which is
		// a wider question.
		if name == "SET_CONFIG" {
			if setting, ok := postgresSetConfig(tokens, index+1); ok && setting.accepted() {
				continue
			}
		}
		if slices.Contains(described.Reach.ControlFunctions.Names, name) {
			return "cluster control function"
		}
		if slices.Contains(described.Reach.ReplicationFunctions.Names, name) {
			return "replication state operation"
		}
		switch name {
		case "DBLINK", "DBLINK_CANCEL_QUERY", "DBLINK_CLOSE", "DBLINK_CONNECT",
			"DBLINK_CONNECT_U", "DBLINK_DISCONNECT", "DBLINK_EXEC", "DBLINK_FETCH",
			"DBLINK_GET_RESULT", "DBLINK_OPEN", "DBLINK_SEND_QUERY":
			if !scope.TrustedSQL {
				return "external dblink operation"
			}
		case "LO_CLOSE", "LO_CREATE", "LO_EXPORT", "LO_FROM_BYTEA", "LO_IMPORT",
			"LO_LSEEK", "LO_LSEEK64", "LO_OPEN", "LO_PUT", "LO_TRUNCATE",
			"LO_TRUNCATE64", "LO_UNLINK", "LOWRITE":
			return "large object operation"
		case "PG_ADVISORY_LOCK", "PG_ADVISORY_LOCK_SHARED", "PG_TRY_ADVISORY_LOCK",
			"PG_TRY_ADVISORY_LOCK_SHARED":
			return "session advisory lock"
		}
	}
	return ""
}

func definesPostgresRoutine(tokens []lexer.Token) bool {
	if len(tokens) == 0 {
		return false
	}
	first := replay.NormalizedIdentifier(tokens[0])
	if first != "CREATE" && first != "ALTER" {
		return false
	}
	index := 1
	if first == "CREATE" && replay.TokenSequenceAt(tokens, index, "OR", "REPLACE") {
		index += 2
	}
	return replay.TokenSequenceAt(tokens, index, "FUNCTION") ||
		replay.TokenSequenceAt(tokens, index, "PROCEDURE") ||
		first == "ALTER" && replay.TokenSequenceAt(tokens, index, "ROUTINE")
}

func postgresObjectClassWidth(tokens []lexer.Token, index int) int {
	switch replay.NormalizedIdentifier(tokens[index]) {
	case "ACCESS", "EVENT", "LARGE", "MATERIALIZED":
		return 2
	case "FOREIGN":
		if replay.TokenSequenceAt(tokens, index, "FOREIGN", "DATA", "WRAPPER") {
			return 3
		}
		return 2
	case "OPERATOR":
		if replay.TokenSequenceAt(tokens, index, "OPERATOR", "CLASS") ||
			replay.TokenSequenceAt(tokens, index, "OPERATOR", "FAMILY") {
			return 2
		}
		return 1
	case "TEXT":
		if replay.TokenSequenceAt(tokens, index, "TEXT", "SEARCH") {
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
	if len(tokens) > 0 && replay.NormalizedIdentifier(tokens[0]) == "REVOKE" {
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
	return replay.MutationTargetNotFound
}

func postgresKeywordAt(token lexer.Token, value string) bool {
	return token.Type == lexer.TokenIdentifier && strings.EqualFold(token.Value, value)
}
