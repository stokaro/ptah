package clickhousereplay

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// validateClickHouseReplayStatement refuses a statement whose effects cannot be
// confined to the dev database scope names.
//
// Under [replay.Scope.TrustedSQL] it lifts the refusals whose only
// reason is that the effect reaches past the dev database: a SQL function,
// which ClickHouse keeps for the whole server; a database, user or role
// created or dropped, a row policy and a named collection; GRANT and REVOKE; ON
// CLUSTER, which runs the statement on other nodes too; a write to another
// database or through a table function; and a table engine, a dictionary
// source, BACKUP, FREEZE, UNFREEZE and INTO OUTFILE, which move data across the
// server's edge.
//
// It keeps every refusal that protects the run: USE, SET ROLE and SET DEFAULT
// ROLE, which change the replay's session; SYSTEM and a global SET, which act
// on the server, but for the STOP MERGES and START MERGES of one table in the
// dev database ([confinedClickHouseMergeControl]); ALTER of a database, user or role, a quota and a settings
// profile, which can change the database or the limits and settings the run's
// own user runs with; a workload and a resource, which schedule the server;
// ATTACH, DETACH, UNDROP and RESTORE, which move tables in and out of what the
// realm cleanup sees; a TEMP object; and PARALLEL WITH, whose statements run at
// once and are read here as one.
func validateClickHouseReplayStatement(tokens []lexer.Token, scope replay.Scope) error {
	if len(tokens) == 0 {
		return nil
	}
	if containsClickHouseKeywordSequence(tokens, "PARALLEL", "WITH") {
		return replay.Refuse(platform.ClickHouse, "PARALLEL WITH")
	}
	if err := rejectClickHouseGlobalMutation(tokens, scope); err != nil {
		return err
	}
	if source := unsafeClickHouseDictionarySource(tokens); source != "" && !scope.TrustedSQL {
		return replay.Refuse(platform.ClickHouse, source+" dictionary source")
	}
	if engine := unconfinedClickHouseEngine(tokens); engine != "" && !scope.TrustedSQL {
		return replay.Refuse(platform.ClickHouse, engine+" table engine")
	}
	if err := validateClickHouseReplayStatementBase(tokens, scope); err != nil {
		return err
	}
	if scope.TrustedSQL {
		return nil
	}
	if err := validateClickHouseInsertTarget(scope.Database, tokens); err != nil {
		return err
	}
	return validateClickHouseSecondaryTargets(scope.Database, tokens)
}

func unconfinedClickHouseEngine(tokens []lexer.Token) string {
	if replay.NormalizedIdentifier(tokens[0]) != "CREATE" || replay.UsesTemporaryObject(tokens) {
		return ""
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound ||
		!isClickHouseTableLikeKind(replay.NormalizedIdentifier(tokens[kindIndex])) {
		return ""
	}
	engine, assigned := replay.TopLevelValueAfterKeyword(
		tokens,
		"ENGINE",
		replay.ObjectNameIndex(tokens, kindIndex)+1,
	)
	if !assigned || engine == "" {
		if replay.NormalizedIdentifier(tokens[kindIndex]) == "TABLE" ||
			replay.NormalizedIdentifier(tokens[kindIndex]) == "MATERIALIZED" &&
				!replay.ContainsIdentifier(tokens, "TO") {
			return "implicit/default"
		}
		return ""
	}
	if isConfinedClickHouseEngine(engine) {
		return ""
	}
	return engine
}

func unsafeClickHouseDictionarySource(tokens []lexer.Token) string {
	if !replay.IsDDLAction(tokens) {
		return ""
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound ||
		replay.NormalizedIdentifier(tokens[kindIndex]) != "DICTIONARY" {
		return ""
	}
	source := clickHouseDictionarySource(tokens, replay.ObjectNameIndex(tokens, kindIndex)+1)
	if source == "" || source == "NULL" {
		return ""
	}
	return source
}

func clickHouseDictionarySource(tokens []lexer.Token, start int) string {
	depth := 0
	for index := max(start, 0); index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && replay.NormalizedIdentifier(tokens[index]) == "SOURCE":
			index++
			if index < len(tokens) && tokens[index].MatchOperatorValue("(") {
				index++
			}
			if index < len(tokens) {
				return replay.NormalizedIdentifier(tokens[index])
			}
			return ""
		}
	}
	return ""
}

func isClickHouseTableLikeKind(kind string) bool {
	switch kind {
	case "LIVE", "MATERIALIZED", "TABLE", "WINDOW":
		return true
	default:
		return false
	}
}

// Only engines whose data is owned by the disposable local table are allowed.
// Unknown engines fail closed because their storage and replication scope is
// not structurally provable from the statement.
func isConfinedClickHouseEngine(engine string) bool {
	switch engine {
	case "AGGREGATINGMERGETREE", "COLLAPSINGMERGETREE", "EMBEDDEDROCKSDB",
		"GENERATERANDOM", "GRAPHITEMERGETREE", "JOIN", "LOG", "MEMORY", "MERGE",
		"MERGETREE", "NULL", "REPLACINGMERGETREE", "SET", "STRIPELOG",
		"SUMMINGMERGETREE", "TINYLOG",
		"VERSIONEDCOLLAPSINGMERGETREE":
		return true
	default:
		return false
	}
}

// rejectClickHouseGlobalMutation refuses a statement by its first words. Under
// [replay.Scope.TrustedSQL], GRANT, REVOKE, BACKUP, a table snapshot and a database-wide
// TRUNCATE are not refused; see [validateClickHouseReplayStatement].
func rejectClickHouseGlobalMutation(tokens []lexer.Token, scope replay.Scope) error {
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "GRANT", "REVOKE":
		if !scope.TrustedSQL {
			return replay.Refuse(platform.ClickHouse, first+" RBAC change")
		}
	case "BACKUP":
		if !scope.TrustedSQL {
			return replay.Refuse(platform.ClickHouse, first)
		}
	case "ATTACH", "DETACH", "UNDROP", "RESTORE":
		return replay.Refuse(platform.ClickHouse, first)
	case "ALTER":
		if clickHouseHasPersistentSnapshotAction(tokens) && !scope.TrustedSQL {
			return replay.Refuse(platform.ClickHouse, "persistent table snapshot")
		}
	case "SET":
		if clickHouseKeywordSequenceAt(tokens, 1, "ROLE") ||
			clickHouseKeywordSequenceAt(tokens, 1, "DEFAULT", "ROLE") {
			return replay.Refuse(platform.ClickHouse, "role change")
		}
	case "TRUNCATE":
		if !scope.TrustedSQL && (clickHouseKeywordSequenceAt(tokens, 1, "DATABASE") ||
			clickHouseKeywordSequenceAt(tokens, 1, "ALL") ||
			clickHouseKeywordSequenceAt(tokens, 1, "TABLES")) {
			return replay.Refuse(platform.ClickHouse, "database-wide TRUNCATE")
		}
	}
	return nil
}

func clickHouseHasPersistentSnapshotAction(tokens []lexer.Token) bool {
	if !clickHouseKeywordSequenceAt(tokens, 0, "ALTER", "TABLE") {
		return false
	}
	_, index := replay.QualifiedIdentifierAt(tokens, 2)
	if clickHouseKeywordSequenceAt(tokens, index, "ON", "CLUSTER") {
		_, index = replay.QualifiedIdentifierAt(tokens, index+2)
	}
	expectAction := true
	depth := 0
	for ; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && tokens[index].MatchOperatorValue(","):
			expectAction = true
		case depth == 0 && expectAction:
			if clickHouseKeywordSequenceAt(tokens, index, "FREEZE") ||
				clickHouseKeywordSequenceAt(tokens, index, "UNFREEZE") {
				return true
			}
			expectAction = false
		}
	}
	return false
}

func validateClickHouseInsertTarget(database string, tokens []lexer.Token) error {
	insertIndex := clickHouseInsertIndex(tokens)
	if insertIndex == replay.MutationTargetNotFound {
		return nil
	}
	intoIndex := findClickHouseKeyword(tokens, "INTO", insertIndex+1)
	if intoIndex == replay.MutationTargetNotFound {
		return nil
	}

	targetIndex := intoIndex + 1
	if clickHouseKeywordSequenceAt(tokens, targetIndex, "TABLE") {
		targetIndex++
	}
	if clickHouseKeywordSequenceAt(tokens, targetIndex, "FUNCTION") {
		return replay.Refuse(platform.ClickHouse, "table-function write")
	}
	target, _ := replay.QualifiedIdentifierAt(tokens, targetIndex)
	return rejectClickHouseCrossDatabaseTarget(database, target)
}

func clickHouseInsertIndex(tokens []lexer.Token) int {
	if clickHouseKeywordSequenceAt(tokens, 0, "INSERT") {
		return 0
	}
	if !clickHouseKeywordSequenceAt(tokens, 0, "WITH") {
		return replay.MutationTargetNotFound
	}
	return findTopLevelClickHouseKeyword(tokens, "INSERT", 1)
}

func validateClickHouseSecondaryTargets(database string, tokens []lexer.Token) error {
	switch replay.NormalizedIdentifier(tokens[0]) {
	case "RENAME":
		return validateClickHouseRenameTargets(database, tokens)
	case "EXCHANGE":
		return validateClickHouseExchangeTargets(database, tokens)
	case "ALTER":
		return validateClickHouseMovePartitionTarget(database, tokens)
	case "CREATE":
		return validateClickHouseViewTarget(database, tokens)
	default:
		return nil
	}
}

func validateClickHouseRenameTargets(database string, tokens []lexer.Token) error {
	index := 1
	if clickHouseKeywordSequenceAt(tokens, index, "TABLE") ||
		clickHouseKeywordSequenceAt(tokens, index, "DICTIONARY") {
		index++
	}
	for index < len(tokens) {
		source, next := replay.QualifiedIdentifierAt(tokens, index)
		if err := rejectClickHouseCrossDatabaseTarget(database, source); err != nil {
			return err
		}
		if !clickHouseKeywordSequenceAt(tokens, next, "TO") {
			return nil
		}
		destination, next := replay.QualifiedIdentifierAt(tokens, next+1)
		if err := rejectClickHouseCrossDatabaseTarget(database, destination); err != nil {
			return err
		}
		index = next
		if index >= len(tokens) || !tokens[index].MatchOperatorValue(",") {
			return nil
		}
		index++
	}
	return nil
}

func validateClickHouseExchangeTargets(database string, tokens []lexer.Token) error {
	index := 1
	if clickHouseKeywordSequenceAt(tokens, index, "TABLES") ||
		clickHouseKeywordSequenceAt(tokens, index, "DICTIONARIES") {
		index++
	}
	for index < len(tokens) {
		left, next := replay.QualifiedIdentifierAt(tokens, index)
		if err := rejectClickHouseCrossDatabaseTarget(database, left); err != nil {
			return err
		}
		if !clickHouseKeywordSequenceAt(tokens, next, "AND") {
			return nil
		}
		right, next := replay.QualifiedIdentifierAt(tokens, next+1)
		if err := rejectClickHouseCrossDatabaseTarget(database, right); err != nil {
			return err
		}
		index = next
		if index >= len(tokens) || !tokens[index].MatchOperatorValue(",") {
			return nil
		}
		index++
	}
	return nil
}

func validateClickHouseMovePartitionTarget(database string, tokens []lexer.Token) error {
	for index := 1; index+2 < len(tokens); index++ {
		if !clickHouseKeywordSequenceAt(tokens, index, "TO", "TABLE") {
			continue
		}
		target, _ := replay.QualifiedIdentifierAt(tokens, index+2)
		if err := rejectClickHouseCrossDatabaseTarget(database, target); err != nil {
			return err
		}
	}
	return nil
}

func validateClickHouseViewTarget(database string, tokens []lexer.Token) error {
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound ||
		!isClickHouseTargetViewKind(replay.NormalizedIdentifier(tokens[kindIndex])) {
		return nil
	}

	nameIndex := replay.ObjectNameIndex(tokens, kindIndex)
	_, searchStart := replay.QualifiedIdentifierAt(tokens, nameIndex)
	asIndex := findClickHouseKeyword(tokens, "AS", searchStart)
	searchEnd := len(tokens)
	if asIndex != replay.MutationTargetNotFound {
		searchEnd = asIndex
	}
	for index := searchStart; index < searchEnd; index++ {
		if !clickHouseKeywordSequenceAt(tokens, index, "TO") {
			continue
		}
		target, _ := replay.QualifiedIdentifierAt(tokens, index+1)
		return rejectClickHouseCrossDatabaseTarget(database, target)
	}
	return nil
}

func isClickHouseTargetViewKind(kind string) bool {
	switch kind {
	case "LIVE", "MATERIALIZED", "WINDOW":
		return true
	default:
		return false
	}
}

func rejectClickHouseCrossDatabaseTarget(database string, target []string) error {
	if len(target) < 2 {
		return nil
	}
	if strings.TrimSpace(database) == "" {
		return replay.Refuse(platform.ClickHouse, "qualified target without a configured database")
	}
	if target[0] == database {
		return nil
	}
	return replay.Refuse(
		platform.ClickHouse,
		fmt.Sprintf("cross-database target %q", strings.Join(target, ".")),
	)
}

func containsClickHouseKeywordSequence(tokens []lexer.Token, values ...string) bool {
	for index := range tokens {
		if clickHouseKeywordSequenceAt(tokens, index, values...) {
			return true
		}
	}
	return false
}

func clickHouseKeywordSequenceAt(tokens []lexer.Token, index int, values ...string) bool {
	if index < 0 || index+len(values) > len(tokens) {
		return false
	}
	for offset, value := range values {
		token := tokens[index+offset]
		if token.Type != lexer.TokenIdentifier || replay.NormalizedIdentifier(token) != value {
			return false
		}
	}
	return true
}

func findClickHouseKeyword(tokens []lexer.Token, value string, start int) int {
	for index := max(start, 0); index < len(tokens); index++ {
		if clickHouseKeywordSequenceAt(tokens, index, value) {
			return index
		}
	}
	return replay.MutationTargetNotFound
}

func findTopLevelClickHouseKeyword(tokens []lexer.Token, value string, start int) int {
	depth := 0
	for index := max(start, 0); index < len(tokens); index++ {
		if tokens[index].MatchOperatorValue("(") {
			depth++
			continue
		}
		if tokens[index].MatchOperatorValue(")") {
			depth = max(depth-1, 0)
			continue
		}
		if depth == 0 && clickHouseKeywordSequenceAt(tokens, index, value) {
			return index
		}
	}
	return replay.MutationTargetNotFound
}
