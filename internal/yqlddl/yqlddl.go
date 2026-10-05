// Package yqlddl reads what one YQL schema statement does: the table, view or
// topic it names, the columns, key, indexes and TTL column a CREATE TABLE
// declares, the actions an ALTER TABLE takes, the settings and consumers a
// CREATE TOPIC declares and the actions an ALTER TOPIC takes, the restart an
// ALTER SEQUENCE makes, the tables a query reads, and the table a data
// statement writes.
//
// It is the reading both linters share, so `ptah migrations lint` and
// `ptah sql lint` cannot disagree about what a YQL statement does. It reads
// tokens from internal/lexer in its YQL mode and recognizes the forms the
// lint rules ask about; it is not a parser. A statement it does not recognize
// is [Other], and a part of one it does not recognize is left out, so a rule
// that reads the result finds less rather than something that is not there.
// internal/parser, which models a YQL CREATE TABLE fully, does not read YQL
// yet (stokaro/ptah#4015).
//
// Names are read the way YDB resolves them: unquoted, case-sensitive, and a
// table name is a path relative to the database root. A name written as a
// named expression (`CREATE TABLE $t ...`) is not known before the query runs
// and reads as empty.
package yqlddl

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Kind is what a statement is.
type Kind int

const (
	// Other is a statement this package does not read.
	Other Kind = iota
	// CreateTable is CREATE TABLE.
	CreateTable
	// AlterTable is ALTER TABLE.
	AlterTable
	// DropTable is DROP TABLE.
	DropTable
	// CreateView is CREATE VIEW.
	CreateView
	// DropView is DROP VIEW.
	DropView
	// AlterSequence is ALTER SEQUENCE, the one statement that changes the
	// sequence behind a Serial column. Its name is the sequence's path as
	// written.
	AlterSequence
	// CreateTopic is CREATE TOPIC.
	CreateTopic
	// AlterTopic is ALTER TOPIC.
	AlterTopic
	// DropTopic is DROP TOPIC.
	DropTopic
)

// Statement is what one YQL statement does.
type Statement struct {
	// Kind is what the statement is.
	Kind Kind
	// Name is the table or view the statement names. It is empty for
	// [Other] and for a name written as a named expression.
	Name string
	// IfExists records IF EXISTS on a DROP and IF NOT EXISTS on a CREATE.
	IfExists bool

	// Columns are the columns a CREATE TABLE declares, in order.
	Columns []Column
	// PrimaryKey reports that a CREATE TABLE declares its PRIMARY KEY clause.
	PrimaryKey bool
	// Indexes are the indexes a CREATE TABLE declares inline, in order.
	Indexes []Index
	// TTLColumn is the column a CREATE TABLE's TTL setting reads, and empty
	// when it sets none.
	TTLColumn string
	// Settings are the settings the WITH clause of a CREATE TABLE or a
	// CREATE TOPIC sets, in order.
	Settings []Setting

	// Actions are the actions an ALTER TABLE or an ALTER TOPIC takes, in
	// order.
	Actions []Action

	// Consumers are the consumers a CREATE TOPIC declares, in order.
	Consumers []Consumer

	// Reads are the tables the query of a CREATE VIEW reads; see
	// [TablesRead].
	Reads []string

	// Restart reports RESTART in an ALTER SEQUENCE, and RestartWith the value
	// it names, which is empty for a RESTART that names none and restarts at
	// the start.
	Restart     bool
	RestartWith string
}

// Consumer is one consumer a CREATE TOPIC declares or an ALTER TOPIC adds.
type Consumer struct {
	// Name is the consumer's name.
	Name string
	// Settings are what its WITH clause sets.
	Settings []Setting
}

// Column is one column a statement declares or adds.
type Column struct {
	// Name is the column's name.
	Name string
	// Type is the first word of the column's type as written, such as Utf8,
	// Decimal or SmallSerial, and empty when the type is not a word.
	Type string
	// NotNull reports a NOT NULL in the declaration.
	NotNull bool
	// Default reports a DEFAULT in the declaration.
	Default bool
}

// Index is one index a statement declares or adds.
type Index struct {
	// Name is the index's name.
	Name string
	// Unique reports UNIQUE among the words before ON.
	Unique bool
	// Method is the kind USING names, in lower case, such as
	// vector_kmeans_tree, and empty for an index that names none.
	Method string
	// VectorType is the vector_type a vector index's WITH (...) names, in
	// lower case, and empty where it names none.
	VectorType string
	// Columns are the key columns, from ON (...).
	Columns []string
	// Cover are the covered columns, from COVER (...).
	Cover []string
}

// vectorMethod is the kind USING names for a vector index.
const vectorMethod = "vector_kmeans_tree"

// Uses reports whether the index keys or covers column.
func (i Index) Uses(column string) bool {
	return slices.Contains(i.Columns, column) || slices.Contains(i.Cover, column)
}

// ActionKind is what one action of an ALTER TABLE does.
type ActionKind int

const (
	// OtherAction is an action this package does not read.
	OtherAction ActionKind = iota
	// AddColumn is ADD [COLUMN].
	AddColumn
	// DropColumn is DROP [COLUMN].
	DropColumn
	// AddIndex is ADD INDEX.
	AddIndex
	// DropIndex is DROP INDEX.
	DropIndex
	// RenameIndex is RENAME INDEX ... TO.
	RenameIndex
	// RenameTable is RENAME TO.
	RenameTable
	// SetSettings is SET (...), or SET with one setting and no parentheses.
	SetSettings
	// ResetSettings is RESET (...).
	ResetSettings
	// AddChangefeed is ADD CHANGEFEED.
	AddChangefeed
	// DropChangefeed is DROP CHANGEFEED.
	DropChangefeed
	// AddConsumer is an ALTER TOPIC's ADD CONSUMER.
	AddConsumer
	// DropConsumer is an ALTER TOPIC's DROP CONSUMER.
	DropConsumer
	// SetConsumerSettings is an ALTER TOPIC's ALTER CONSUMER ... SET (...).
	SetConsumerSettings
	// ResetConsumerSettings is an ALTER TOPIC's ALTER CONSUMER ... RESET (...).
	ResetConsumerSettings
)

// Action is one action of an ALTER TABLE.
type Action struct {
	// Kind is what the action does.
	Kind ActionKind
	// Column is the column ADD COLUMN adds; for DROP COLUMN only its Name is
	// set.
	Column Column
	// Index is the index ADD INDEX adds; for DROP INDEX and RENAME INDEX
	// only its Name is set.
	Index Index
	// NewName is the new name RENAME INDEX and RENAME TO give.
	NewName string
	// Changefeed is the changefeed ADD CHANGEFEED adds or DROP CHANGEFEED
	// drops.
	Changefeed string
	// Settings are what SET sets or RESET resets, of the table, the topic or
	// the consumer.
	Settings []Setting
	// Consumer is the consumer an ALTER TOPIC action adds, drops or changes.
	Consumer string
}

// Setting is one setting a SET, a RESET or a WITH clause names: of a table,
// a topic or a consumer.
type Setting struct {
	// Name is the setting's name in upper case, such as
	// AUTO_PARTITIONING_BY_SIZE or TTL.
	Name string
	// Value is the first word or number of the value in upper case, such as
	// ENABLED or 2, and empty for RESET and for a value that is neither.
	Value string
	// Text is the content of a value written as one string literal, such as
	// raw,gzip for supported_codecs = 'raw,gzip', and empty for any other.
	Text string
	// Column is the column a TTL setting reads, and empty for any other.
	Column string
	// Items is the number of items in a value written as a parenthesized
	// list, such as the keys PARTITION_AT_KEYS splits a table at, and zero
	// for any other value.
	Items int
}

// Requirement is a capability a statement needs from the YDB line it runs on.
type Requirement struct {
	// Capability is the key the target has to hold.
	Capability capability.Capability
	// Action is the position in [Statement.Actions] of the action that needs
	// it, or, where Inline holds, the position in [Statement.Indexes] of the
	// index a CREATE TABLE declares that needs it.
	Action int
	// Inline reports a requirement of an index a CREATE TABLE declares.
	Inline bool
}

// Requirements returns the capabilities a CREATE TABLE or an ALTER TABLE
// needs that a YDB line may lack, in index and action order. Both linters
// judge a statement against its target through this one list, so they cannot
// disagree about what a line refuses. Measured on 26.2.1.14 and 25.1.4.7:
//
//   - ADD INDEX ... UNIQUE needs [capability.UniqueIndexOnExistingTable]: a
//     table the statement alters exists, and YDB keeps adding a unique index
//     to one behind a feature flag (`Adding a unique index to an existing
//     table is disabled` on 26.2.1.14, `Unknown index type:
//     syncGlobalUnique` on 25.1.4.7);
//   - ADD COLUMN with a DEFAULT needs [capability.AddColumnWithDefault]
//     (`Column addition with default value is not supported now` on
//     25.1.4.7);
//   - a vector index, inline or added, needs [capability.VectorIndexes]
//     (`Vector index support is disabled` on 25.1.4.7 with its flag off),
//     and one over bit vectors needs [capability.VectorBitType] too (`bit
//     vector type is not supported` on 25.1.4.7, `Unsupported vector_type:
//     VECTOR_TYPE_BIT` on 25.4.1.15).
//
// Any other statement needs nothing listed here.
func (s Statement) Requirements() []Requirement {
	var requirements []Requirement
	if s.Kind == CreateTable {
		for i, index := range s.Indexes {
			for _, key := range index.vectorRequirements() {
				requirements = append(requirements, Requirement{Capability: key, Action: i, Inline: true})
			}
		}
		return requirements
	}
	if s.Kind != AlterTable {
		return nil
	}
	for i, action := range s.Actions {
		switch {
		case action.Kind == AddIndex && action.Index.Unique:
			requirements = append(requirements, Requirement{Capability: capability.UniqueIndexOnExistingTable, Action: i})
		case action.Kind == AddColumn && action.Column.Default:
			requirements = append(requirements, Requirement{Capability: capability.AddColumnWithDefault, Action: i})
		case action.Kind == AddIndex:
			for _, key := range action.Index.vectorRequirements() {
				requirements = append(requirements, Requirement{Capability: key, Action: i})
			}
		}
	}
	return requirements
}

// Vector reports a vector index.
func (i Index) Vector() bool {
	return i.Method == vectorMethod
}

// vectorRequirements are the capabilities a vector index needs, and none for
// any other index.
func (i Index) vectorRequirements() []capability.Capability {
	switch {
	case !i.Vector():
		return nil
	case i.VectorType == "bit":
		return []capability.Capability{capability.VectorIndexes, capability.VectorBitType}
	default:
		return []capability.Capability{capability.VectorIndexes}
	}
}

// Read reads one YQL statement. Comments and a terminating semicolon are
// allowed around it.
func Read(statement string) Statement {
	tokens := significant(statement)
	switch {
	case startsWith(tokens, "CREATE", "TABLE"):
		return readCreateTable(tokens[2:])
	case startsWith(tokens, "ALTER", "TABLE"):
		return readAlterTable(tokens[2:])
	case startsWith(tokens, "DROP", "TABLE"):
		return readDrop(DropTable, tokens[2:])
	case startsWith(tokens, "CREATE", "VIEW"):
		return readCreateView(tokens[2:])
	case startsWith(tokens, "DROP", "VIEW"):
		return readDrop(DropView, tokens[2:])
	case startsWith(tokens, "ALTER", "SEQUENCE"):
		return readAlterSequence(tokens[2:])
	case startsWith(tokens, "CREATE", "TOPIC"):
		return readCreateTopic(tokens[2:])
	case startsWith(tokens, "ALTER", "TOPIC"):
		return readAlterTopic(tokens[2:])
	case startsWith(tokens, "DROP", "TOPIC"):
		return readDrop(DropTopic, tokens[2:])
	default:
		return Statement{}
	}
}

// WrittenTable returns the table a data statement writes rows into: the name
// after INSERT, UPSERT or REPLACE ... INTO, after UPDATE, and after DELETE
// FROM, each also behind BATCH. It reports false for any other statement and
// for a name written as a named expression, which is not known before the
// query runs.
func WrittenTable(statement string) (string, bool) {
	tokens, _ := skipWords(significant(statement), "BATCH")
	switch {
	case startsWith(tokens, "INSERT"), startsWith(tokens, "UPSERT"), startsWith(tokens, "REPLACE"):
		for i := range tokens {
			if tokens[i].MatchIdentifierValue("INTO") {
				tokens = tokens[i+1:]
				break
			}
		}
	case startsWith(tokens, "UPDATE"):
		tokens = tokens[1:]
	case startsWith(tokens, "DELETE", "FROM"):
		tokens = tokens[2:]
	default:
		return "", false
	}
	name, _ := readName(tokens)
	return name, name != ""
}

// TablesRead returns the tables the statement tokens start reads, in order: the
// name after each FROM and each JOIN before the statement's semicolon, unless
// it is a named expression, a call such as AS_TABLE(...), or a parenthesized
// source, whose own FROM is read where it comes.
func TablesRead(tokens []lexer.Token) []string {
	var tables []string
	for i, token := range tokens {
		if token.Type == lexer.TokenSemicolon {
			break
		}
		if !token.MatchIdentifierValue("FROM") && !token.MatchIdentifierValue("JOIN") {
			continue
		}
		if i+1 >= len(tokens) || tokens[i+1].Type != lexer.TokenIdentifier || isNamedExpression(tokens[i+1]) {
			continue
		}
		if i+2 < len(tokens) && tokens[i+2].MatchOperatorValue("(") {
			continue
		}
		if name, ok := lexer.YQLIdentifierValue(tokens[i+1].Value); ok {
			tables = append(tables, name)
		}
	}
	return tables
}

// significant returns the tokens of statement that are not whitespace or
// comments, read by the YQL lexer. A translation setting such as
// --!syntax_v1 is left out with the comments.
func significant(statement string) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.YDB))
	var tokens []lexer.Token
	for {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment, lexer.TokenUnknown:
			continue
		default:
			tokens = append(tokens, token)
		}
	}
}

func readCreateTable(tokens []lexer.Token) Statement {
	stmt := Statement{Kind: CreateTable}
	tokens, stmt.IfExists = skipWords(tokens, "IF", "NOT", "EXISTS")
	stmt.Name, tokens = readName(tokens)
	items, rest := parenthesized(tokens)
	for _, item := range splitTopLevel(items) {
		switch {
		case startsWith(item, "PRIMARY", "KEY"):
			stmt.PrimaryKey = true
		case startsWith(item, "INDEX"):
			stmt.Indexes = append(stmt.Indexes, readIndex(item[1:]))
		case startsWith(item, "FAMILY"), startsWith(item, "CHANGEFEED"):
		default:
			if column, ok := readColumn(item); ok {
				stmt.Columns = append(stmt.Columns, column)
			}
		}
	}
	for i := range rest {
		if rest[i].MatchIdentifierValue("WITH") && i+1 < len(rest) && rest[i+1].MatchOperatorValue("(") {
			settings, _ := parenthesized(rest[i+1:])
			stmt.Settings = readSettings(settings)
			for _, setting := range stmt.Settings {
				if setting.Name == "TTL" {
					stmt.TTLColumn = setting.Column
				}
			}
			break
		}
	}
	return stmt
}

func readAlterTable(tokens []lexer.Token) Statement {
	stmt := Statement{Kind: AlterTable}
	stmt.Name, tokens = readName(tokens)
	for _, action := range splitTopLevel(tokens) {
		stmt.Actions = append(stmt.Actions, readAction(action))
	}
	return stmt
}

// readAction reads one action of an ALTER TABLE.
func readAction(tokens []lexer.Token) Action {
	switch {
	case startsWith(tokens, "ADD", "INDEX"):
		return Action{Kind: AddIndex, Index: readIndex(tokens[2:])}
	case startsWith(tokens, "DROP", "INDEX"):
		name, _ := readName(tokens[2:])
		return Action{Kind: DropIndex, Index: Index{Name: name}}
	case startsWith(tokens, "RENAME", "INDEX"):
		name, rest := readName(tokens[2:])
		newName := ""
		if startsWith(rest, "TO") {
			newName, _ = readName(rest[1:])
		}
		return Action{Kind: RenameIndex, Index: Index{Name: name}, NewName: newName}
	case startsWith(tokens, "RENAME", "TO"):
		newName, _ := readName(tokens[2:])
		return Action{Kind: RenameTable, NewName: newName}
	case startsWith(tokens, "ADD", "CHANGEFEED"):
		name, _ := readName(tokens[2:])
		return Action{Kind: AddChangefeed, Changefeed: name}
	case startsWith(tokens, "DROP", "CHANGEFEED"):
		name, _ := readName(tokens[2:])
		return Action{Kind: DropChangefeed, Changefeed: name}
	case startsWith(tokens, "ADD", "FAMILY"), startsWith(tokens, "DROP", "FAMILY"):
		return Action{}
	case startsWith(tokens, "ADD"):
		rest, _ := skipWords(tokens[1:], "COLUMN")
		if column, ok := readColumn(rest); ok {
			return Action{Kind: AddColumn, Column: column}
		}
		return Action{}
	case startsWith(tokens, "DROP"):
		rest, _ := skipWords(tokens[1:], "COLUMN")
		name, _ := readName(rest)
		return Action{Kind: DropColumn, Column: Column{Name: name}}
	case startsWith(tokens, "SET"):
		return Action{Kind: SetSettings, Settings: readSetAction(tokens[1:])}
	case startsWith(tokens, "RESET"):
		return Action{Kind: ResetSettings, Settings: readResetNames(tokens[1:])}
	default:
		return Action{}
	}
}

// readSetAction reads what SET sets: a parenthesized list, or one setting
// written as a name and a value, `SET AUTO_PARTITIONING_BY_LOAD ENABLED`.
func readSetAction(tokens []lexer.Token) []Setting {
	if len(tokens) > 0 && tokens[0].MatchOperatorValue("(") {
		settings, _ := parenthesized(tokens)
		return readSettings(settings)
	}
	if len(tokens) == 0 || tokens[0].Type != lexer.TokenIdentifier {
		return nil
	}
	return []Setting{readSetting(tokens[0], tokens[1:])}
}

// readSettings reads a settings list, `NAME = value, ...`. A TTL with tiers
// carries its own commas, `TTL = Interval("P1D") TO EXTERNAL DATA SOURCE s,
// Interval("P2D") DELETE ON ts`, so an item that does not start NAME = belongs
// to the setting before it.
func readSettings(tokens []lexer.Token) []Setting {
	var settings []Setting
	var current []lexer.Token
	flush := func() {
		if len(current) > 1 && current[1].MatchOperatorValue("=") {
			settings = append(settings, readSetting(current[0], current[2:]))
		}
		current = nil
	}
	for _, item := range splitTopLevel(tokens) {
		if len(item) > 1 && item[0].Type == lexer.TokenIdentifier && item[1].MatchOperatorValue("=") {
			flush()
		}
		current = append(current, item...)
	}
	flush()
	return settings
}

// readSetting reads one setting from its name and the tokens of its value.
func readSetting(name lexer.Token, value []lexer.Token) Setting {
	setting := Setting{Name: strings.ToUpper(name.Value)}
	if len(value) > 0 && value[0].Type == lexer.TokenIdentifier {
		setting.Value = strings.ToUpper(value[0].Value)
	}
	if list, _ := parenthesized(value); len(list) > 0 {
		setting.Items = len(splitTopLevel(list))
	}
	if len(value) == 1 && value[0].Type == lexer.TokenString {
		setting.Text = stringContent(value[0].Value)
	}
	if setting.Name == "TTL" {
		for i := range value {
			if value[i].MatchIdentifierValue("ON") && i+1 < len(value) {
				setting.Column, _ = readName(value[i+1:])
				break
			}
		}
	}
	return setting
}

// readIndex reads an index from its name on: name [GLOBAL | LOCAL] [UNIQUE]
// [SYNC | ASYNC] [USING kind] ON (columns) [COVER (columns)] [WITH (...)].
func readIndex(tokens []lexer.Token) Index {
	var index Index
	index.Name, tokens = readName(tokens)
	for i := range tokens {
		switch {
		case tokens[i].MatchIdentifierValue("UNIQUE"):
			index.Unique = true
		case tokens[i].MatchIdentifierValue("USING") && i+1 < len(tokens):
			index.Method = strings.ToLower(tokens[i+1].Value)
		case tokens[i].MatchIdentifierValue("ON"):
			columns, rest := parenthesized(tokens[i+1:])
			index.Columns = readNames(columns)
			if startsWith(rest, "COVER") {
				var cover []lexer.Token
				cover, rest = parenthesized(rest[1:])
				index.Cover = readNames(cover)
			}
			if startsWith(rest, "WITH") {
				settings, _ := parenthesized(rest[1:])
				index.VectorType = indexSetting(settings, "vector_type")
			}
			return index
		}
	}
	return index
}

// indexSetting is the value an index's WITH (...) list gives the setting
// name, in lower case, written as a word or a string, and empty where the
// list names it otherwise or not at all.
func indexSetting(tokens []lexer.Token, name string) string {
	for _, item := range splitTopLevel(tokens) {
		if len(item) != 3 || !item[0].MatchIdentifierValue(name) || !item[1].MatchOperatorValue("=") {
			continue
		}
		switch item[2].Type {
		case lexer.TokenIdentifier:
			return strings.ToLower(item[2].Value)
		case lexer.TokenString:
			return strings.ToLower(strings.Trim(item[2].Value, `"'`))
		}
	}
	return ""
}

// readColumn reads a column declaration: name type [NOT NULL] [DEFAULT ...].
func readColumn(tokens []lexer.Token) (Column, bool) {
	name, rest := readName(tokens)
	if name == "" || len(rest) == 0 {
		return Column{}, false
	}
	column := Column{Name: name}
	if rest[0].Type == lexer.TokenIdentifier {
		column.Type = rest[0].Value
	}
	for i := range rest {
		switch {
		case rest[i].MatchIdentifierValue("NOT") && i+1 < len(rest) && rest[i+1].MatchIdentifierValue("NULL"):
			column.NotNull = true
		case rest[i].MatchIdentifierValue("DEFAULT"):
			column.Default = true
		}
	}
	return column, true
}

// readAlterSequence reads ALTER SEQUENCE [IF EXISTS] path and the RESTART
// among its actions: START [WITH] n, INCREMENT [BY] n and RESTART [[WITH] n],
// the only ones YDB takes.
func readAlterSequence(tokens []lexer.Token) Statement {
	stmt := Statement{Kind: AlterSequence}
	tokens, stmt.IfExists = skipWords(tokens, "IF", "EXISTS")
	stmt.Name, tokens = readName(tokens)
	for i := range tokens {
		if !tokens[i].MatchIdentifierValue("RESTART") {
			continue
		}
		stmt.Restart = true
		rest, _ := skipWords(tokens[i+1:], "WITH")
		if len(rest) > 0 && isDigits(rest[0].Value) {
			stmt.RestartWith = rest[0].Value
		}
	}
	return stmt
}

// readCreateTopic reads CREATE TOPIC [IF NOT EXISTS] path [(CONSUMER name
// [WITH (...)], ...)] [WITH (...)].
func readCreateTopic(tokens []lexer.Token) Statement {
	stmt := Statement{Kind: CreateTopic}
	tokens, stmt.IfExists = skipWords(tokens, "IF", "NOT", "EXISTS")
	stmt.Name, tokens = readName(tokens)
	consumers, rest := parenthesized(tokens)
	for _, item := range splitTopLevel(consumers) {
		if startsWith(item, "CONSUMER") {
			stmt.Consumers = append(stmt.Consumers, readConsumer(item[1:]))
		}
	}
	if startsWith(rest, "WITH") {
		settings, _ := parenthesized(rest[1:])
		stmt.Settings = readSettings(settings)
	}
	return stmt
}

// readConsumer reads a consumer from its name on: name [WITH (...)].
func readConsumer(tokens []lexer.Token) Consumer {
	var consumer Consumer
	consumer.Name, tokens = readName(tokens)
	if startsWith(tokens, "WITH") {
		settings, _ := parenthesized(tokens[1:])
		consumer.Settings = readSettings(settings)
	}
	return consumer
}

// readAlterTopic reads ALTER TOPIC [IF EXISTS] path and its actions: SET (...),
// RESET (...), ADD CONSUMER name [WITH (...)], DROP CONSUMER name and ALTER
// CONSUMER name SET (...) or RESET (...).
func readAlterTopic(tokens []lexer.Token) Statement {
	stmt := Statement{Kind: AlterTopic}
	tokens, stmt.IfExists = skipWords(tokens, "IF", "EXISTS")
	stmt.Name, tokens = readName(tokens)
	for _, action := range splitTopLevel(tokens) {
		stmt.Actions = append(stmt.Actions, readTopicAction(action))
	}
	return stmt
}

// readTopicAction reads one action of an ALTER TOPIC.
func readTopicAction(tokens []lexer.Token) Action {
	switch {
	case startsWith(tokens, "ADD", "CONSUMER"):
		consumer := readConsumer(tokens[2:])
		return Action{Kind: AddConsumer, Consumer: consumer.Name, Settings: consumer.Settings}
	case startsWith(tokens, "DROP", "CONSUMER"):
		name, _ := readName(tokens[2:])
		return Action{Kind: DropConsumer, Consumer: name}
	case startsWith(tokens, "ALTER", "CONSUMER"):
		name, rest := readName(tokens[2:])
		switch {
		case startsWith(rest, "SET"):
			settings, _ := parenthesized(rest[1:])
			return Action{Kind: SetConsumerSettings, Consumer: name, Settings: readSettings(settings)}
		case startsWith(rest, "RESET"):
			return Action{Kind: ResetConsumerSettings, Consumer: name, Settings: readResetNames(rest[1:])}
		default:
			return Action{Consumer: name}
		}
	case startsWith(tokens, "SET"):
		settings, _ := parenthesized(tokens[1:])
		return Action{Kind: SetSettings, Settings: readSettings(settings)}
	case startsWith(tokens, "RESET"):
		return Action{Kind: ResetSettings, Settings: readResetNames(tokens[1:])}
	default:
		return Action{}
	}
}

// readResetNames reads the names a RESET lists.
func readResetNames(tokens []lexer.Token) []Setting {
	names, _ := parenthesized(tokens)
	var settings []Setting
	for _, item := range splitTopLevel(names) {
		if len(item) > 0 {
			settings = append(settings, Setting{Name: strings.ToUpper(item[0].Value)})
		}
	}
	return settings
}

// stringContent is the text inside a string literal token: between its
// quotes, with a literal suffix such as the u of 'raw'u left out and a
// backslash escape read as the character it escapes.
func stringContent(literal string) string {
	if literal == "" || (literal[0] != '\'' && literal[0] != '"') {
		return ""
	}
	end := strings.LastIndexByte(literal, literal[0])
	if end <= 0 {
		return ""
	}
	var b strings.Builder
	inner := literal[1:end]
	for i := 0; i < len(inner); i++ {
		if inner[i] == '\\' && i+1 < len(inner) {
			i++
		}
		b.WriteByte(inner[i])
	}
	return b.String()
}

func readCreateView(tokens []lexer.Token) Statement {
	stmt := Statement{Kind: CreateView}
	tokens, stmt.IfExists = skipWords(tokens, "IF", "NOT", "EXISTS")
	stmt.Name, tokens = readName(tokens)
	if startsWith(tokens, "WITH") {
		_, tokens = parenthesized(tokens[1:])
	}
	if startsWith(tokens, "AS") {
		stmt.Reads = TablesRead(tokens[1:])
	}
	return stmt
}

func readDrop(kind Kind, tokens []lexer.Token) Statement {
	stmt := Statement{Kind: kind}
	tokens, stmt.IfExists = skipWords(tokens, "IF", "EXISTS")
	stmt.Name, _ = readName(tokens)
	return stmt
}

// readName reads the name at the head of tokens and returns what follows it.
// A named expression, or anything that is not a name, reads as empty.
func readName(tokens []lexer.Token) (string, []lexer.Token) {
	if len(tokens) == 0 || tokens[0].Type != lexer.TokenIdentifier {
		return "", tokens
	}
	if isNamedExpression(tokens[0]) {
		return "", tokens[1:]
	}
	name, ok := lexer.YQLIdentifierValue(tokens[0].Value)
	if !ok {
		return "", tokens[1:]
	}
	return name, tokens[1:]
}

// readNames reads a comma-separated list of names.
func readNames(tokens []lexer.Token) []string {
	var names []string
	for _, item := range splitTopLevel(tokens) {
		if name, _ := readName(item); name != "" {
			names = append(names, name)
		}
	}
	return names
}

// parenthesized returns the tokens between the parenthesis that opens tokens
// and the one that closes it, and what follows. Tokens that do not open with
// a parenthesis return nothing inside and themselves after.
func parenthesized(tokens []lexer.Token) (inside, rest []lexer.Token) {
	if len(tokens) == 0 || !tokens[0].MatchOperatorValue("(") {
		return nil, tokens
	}
	depth := 0
	for i, token := range tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return tokens[1:i], tokens[i+1:]
			}
		}
	}
	return tokens[1:], nil
}

// splitTopLevel splits tokens at the commas outside any parentheses, and stops
// at a semicolon.
func splitTopLevel(tokens []lexer.Token) [][]lexer.Token {
	var items [][]lexer.Token
	depth := 0
	start := 0
	for i, token := range tokens {
		switch {
		case token.Type == lexer.TokenSemicolon && depth == 0:
			return appendItem(items, tokens[start:i])
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case token.MatchOperatorValue(",") && depth == 0:
			items = appendItem(items, tokens[start:i])
			start = i + 1
		}
	}
	return appendItem(items, tokens[start:])
}

func appendItem(items [][]lexer.Token, item []lexer.Token) [][]lexer.Token {
	if len(item) == 0 {
		return items
	}
	return append(items, item)
}

// startsWith reports whether tokens open with the words given.
func startsWith(tokens []lexer.Token, words ...string) bool {
	if len(tokens) < len(words) {
		return false
	}
	for i, word := range words {
		if !tokens[i].MatchIdentifierValue(word) {
			return false
		}
	}
	return true
}

// skipWords drops the words given from the head of tokens when all of them
// are there, and reports whether they were.
func skipWords(tokens []lexer.Token, words ...string) ([]lexer.Token, bool) {
	if !startsWith(tokens, words...) {
		return tokens, false
	}
	return tokens[len(words):], true
}

// isDigits reports whether value is a whole number written in decimal digits,
// which the YQL lexer reads as an identifier.
func isDigits(value string) bool {
	return value != "" && strings.Trim(value, "0123456789") == ""
}

// isNamedExpression reports whether token is a `$name`.
func isNamedExpression(token lexer.Token) bool {
	return token.Type == lexer.TokenIdentifier && strings.HasPrefix(token.Value, "$")
}
