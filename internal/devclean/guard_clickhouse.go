package devclean

import (
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/lexer"
)

func validateClickHouseReplayStatement(database string, tokens []lexer.Token) error {
	if len(tokens) == 0 {
		return nil
	}
	if containsClickHouseKeywordSequence(tokens, "PARALLEL", "WITH") {
		return unsafeReplayStatement(platform.ClickHouse, "PARALLEL WITH")
	}
	if err := rejectClickHouseGlobalMutation(tokens); err != nil {
		return err
	}
	if source := unsafeClickHouseDictionarySource(tokens); source != "" {
		return unsafeReplayStatement(platform.ClickHouse, source+" dictionary source")
	}
	if engine := unconfinedClickHouseEngine(tokens); engine != "" {
		return unsafeReplayStatement(platform.ClickHouse, engine+" table engine")
	}
	if err := validateClickHouseReplayStatementBase(database, tokens); err != nil {
		return err
	}
	if err := validateClickHouseInsertTarget(database, tokens); err != nil {
		return err
	}
	return validateClickHouseSecondaryTargets(database, tokens)
}

func unconfinedClickHouseEngine(tokens []lexer.Token) string {
	if normalizedIdentifier(tokens[0]) != "CREATE" || usesTemporaryObject(tokens) {
		return ""
	}
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound ||
		!isClickHouseTableLikeKind(normalizedIdentifier(tokens[kindIndex])) {
		return ""
	}
	engine, assigned := topLevelValueAfterKeyword(
		tokens,
		"ENGINE",
		objectNameIndex(tokens, kindIndex)+1,
	)
	if !assigned || engine == "" {
		if normalizedIdentifier(tokens[kindIndex]) == "TABLE" ||
			normalizedIdentifier(tokens[kindIndex]) == "MATERIALIZED" &&
				!containsIdentifier(tokens, "TO") {
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
	if !isDDLAction(tokens) {
		return ""
	}
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound ||
		normalizedIdentifier(tokens[kindIndex]) != "DICTIONARY" {
		return ""
	}
	source := clickHouseDictionarySource(tokens, objectNameIndex(tokens, kindIndex)+1)
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
		case depth == 0 && normalizedIdentifier(tokens[index]) == "SOURCE":
			index++
			if index < len(tokens) && tokens[index].MatchOperatorValue("(") {
				index++
			}
			if index < len(tokens) {
				return normalizedIdentifier(tokens[index])
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

func rejectClickHouseGlobalMutation(tokens []lexer.Token) error {
	first := normalizedIdentifier(tokens[0])
	switch first {
	case "GRANT", "REVOKE":
		return unsafeReplayStatement(platform.ClickHouse, first+" RBAC change")
	case "ATTACH", "DETACH", "UNDROP", "BACKUP", "RESTORE":
		return unsafeReplayStatement(platform.ClickHouse, first)
	case "ALTER":
		if clickHouseHasPersistentSnapshotAction(tokens) {
			return unsafeReplayStatement(platform.ClickHouse, "persistent table snapshot")
		}
	case "SET":
		if clickHouseKeywordSequenceAt(tokens, 1, "ROLE") ||
			clickHouseKeywordSequenceAt(tokens, 1, "DEFAULT", "ROLE") {
			return unsafeReplayStatement(platform.ClickHouse, "role change")
		}
	case "TRUNCATE":
		if clickHouseKeywordSequenceAt(tokens, 1, "DATABASE") ||
			clickHouseKeywordSequenceAt(tokens, 1, "ALL") ||
			clickHouseKeywordSequenceAt(tokens, 1, "TABLES") {
			return unsafeReplayStatement(platform.ClickHouse, "database-wide TRUNCATE")
		}
	}
	return nil
}

func clickHouseHasPersistentSnapshotAction(tokens []lexer.Token) bool {
	if !clickHouseKeywordSequenceAt(tokens, 0, "ALTER", "TABLE") {
		return false
	}
	_, index := qualifiedIdentifierAt(tokens, 2)
	if clickHouseKeywordSequenceAt(tokens, index, "ON", "CLUSTER") {
		_, index = qualifiedIdentifierAt(tokens, index+2)
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
	if insertIndex == mutationTargetNotFound {
		return nil
	}
	intoIndex := findClickHouseKeyword(tokens, "INTO", insertIndex+1)
	if intoIndex == mutationTargetNotFound {
		return nil
	}

	targetIndex := intoIndex + 1
	if clickHouseKeywordSequenceAt(tokens, targetIndex, "TABLE") {
		targetIndex++
	}
	if clickHouseKeywordSequenceAt(tokens, targetIndex, "FUNCTION") {
		return unsafeReplayStatement(platform.ClickHouse, "table-function write")
	}
	target, _ := qualifiedIdentifierAt(tokens, targetIndex)
	return rejectClickHouseCrossDatabaseTarget(database, target)
}

func clickHouseInsertIndex(tokens []lexer.Token) int {
	if clickHouseKeywordSequenceAt(tokens, 0, "INSERT") {
		return 0
	}
	if !clickHouseKeywordSequenceAt(tokens, 0, "WITH") {
		return mutationTargetNotFound
	}
	return findTopLevelClickHouseKeyword(tokens, "INSERT", 1)
}

func validateClickHouseSecondaryTargets(database string, tokens []lexer.Token) error {
	switch normalizedIdentifier(tokens[0]) {
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
		source, next := qualifiedIdentifierAt(tokens, index)
		if err := rejectClickHouseCrossDatabaseTarget(database, source); err != nil {
			return err
		}
		if !clickHouseKeywordSequenceAt(tokens, next, "TO") {
			return nil
		}
		destination, next := qualifiedIdentifierAt(tokens, next+1)
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
		left, next := qualifiedIdentifierAt(tokens, index)
		if err := rejectClickHouseCrossDatabaseTarget(database, left); err != nil {
			return err
		}
		if !clickHouseKeywordSequenceAt(tokens, next, "AND") {
			return nil
		}
		right, next := qualifiedIdentifierAt(tokens, next+1)
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
		target, _ := qualifiedIdentifierAt(tokens, index+2)
		if err := rejectClickHouseCrossDatabaseTarget(database, target); err != nil {
			return err
		}
	}
	return nil
}

func validateClickHouseViewTarget(database string, tokens []lexer.Token) error {
	kindIndex := statementObjectKindIndex(tokens)
	if kindIndex == mutationTargetNotFound ||
		!isClickHouseTargetViewKind(normalizedIdentifier(tokens[kindIndex])) {
		return nil
	}

	nameIndex := objectNameIndex(tokens, kindIndex)
	_, searchStart := qualifiedIdentifierAt(tokens, nameIndex)
	asIndex := findClickHouseKeyword(tokens, "AS", searchStart)
	searchEnd := len(tokens)
	if asIndex != mutationTargetNotFound {
		searchEnd = asIndex
	}
	for index := searchStart; index < searchEnd; index++ {
		if !clickHouseKeywordSequenceAt(tokens, index, "TO") {
			continue
		}
		target, _ := qualifiedIdentifierAt(tokens, index+1)
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
		return unsafeReplayStatement(platform.ClickHouse, "qualified target without a configured database")
	}
	if target[0] == database {
		return nil
	}
	return unsafeReplayStatement(
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
		if token.Type != lexer.TokenIdentifier || normalizedIdentifier(token) != value {
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
	return mutationTargetNotFound
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
	return mutationTargetNotFound
}
