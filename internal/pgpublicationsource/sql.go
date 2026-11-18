package pgpublicationsource

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgpublication"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/objectowner"
)

// The names of the owner's SQL patterns, which each claim carries back.
const (
	sqlCreate = "create-publication"
	sqlAlter  = "alter-publication"
	sqlDrop   = "drop-publication"
)

// SQL is the publication owner's contribution to the SQL frontend: the CREATE
// PUBLICATION, ALTER PUBLICATION and DROP PUBLICATION statements of a document
// that names PostgreSQL, or names no target. A document of another target
// reads them through the shared grammar, which refuses them.
//
// A CREATE declares a publication. An ALTER that adds, drops or sets members,
// sets options or names the owner amends the publication a CREATE before it
// declared, in the same document or an earlier one of the load, as pg_dump
// writes a publication: the CREATE with its options, then one ALTER ... ADD per
// member and an ALTER ... OWNER TO. The reading is syntax only: an ALTER of a
// publication nothing declared still reads, as the amendment it states, and
// the reader of the load refuses it. A DROP is a change a migration states and
// declares nothing, so a schema document's reader refuses it.
//
// A row filter, a column list and TABLES IN SCHEMA CURRENT_SCHEMA are refused
// by name: Ptah does not write the first two, and the schema the third names
// depends on the session that runs it.
//
// Its claim is [DocumentCoverage]'s.
func SQL() sqlext.Extension {
	return sqlext.Extension{
		Owner:   pgpublication.Owner,
		Targets: Targets(),
		Kinds:   []schemaext.Kind{pgpublication.PublicationKind},
		Patterns: []sqlext.Pattern{
			{Name: sqlCreate, Site: sqlext.Statement, Words: []string{"CREATE", "PUBLICATION"}},
			{Name: sqlAlter, Site: sqlext.Statement, Words: []string{"ALTER", "PUBLICATION"}},
			{Name: sqlDrop, Site: sqlext.Statement, Words: []string{"DROP", "PUBLICATION"}},
		},
		Parser: sqlParser{},
		Coverage: func(sqlext.CoverageRequest) (schemaext.Coverage, error) {
			return DocumentCoverage()
		},
	}
}

// sqlParser reads the statements the owner's patterns claim.
type sqlParser struct{}

// ParseSQL reads every claim of one document, in order, so an ALTER finds the
// publication a CREATE before it declared.
func (sqlParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	known := make(map[string]schemaext.Object, len(request.Declared))
	for _, object := range request.Declared {
		known[object.Ref.Name.Normalized] = object
	}
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		tokens := newStatementTokens(request.Target, claim)
		var value sqlext.Value
		var err error
		switch claim.Pattern {
		case sqlCreate:
			value, err = tokens.create()
		case sqlAlter:
			value, err = tokens.alter(known)
		case sqlDrop:
			value, err = tokens.drop()
		default:
			err = fmt.Errorf("%w: PostgreSQL publication SQL pattern %q", schemaext.ErrInvalidValue, claim.Pattern)
		}
		if err != nil {
			return sqlext.Result{}, err
		}
		for _, object := range slices.Concat(value.Objects, value.Replaces) {
			known[object.Ref.Name.Normalized] = object
		}
		values = append(values, value)
	}
	return sqlext.Result{Values: values}, nil
}

// statementTokens is a claimed statement as the frontend's lexer reads it for
// the document's target, positioned against the document.
type statementTokens struct {
	lexing lexer.Options
	offset int
	end    int
	tokens []lexer.Token
	pos    int
}

func newStatementTokens(target string, claim sqlext.Claim) *statementTokens {
	options := dialectlexer.Options(platform.Postgres)
	if platform.NormalizeDialect(target) == "" {
		options = lexer.Options{BackslashEscapes: true}
	}
	scan := lexer.NewLexerWithOptions(claim.Text, options)
	tokens := &statementTokens{lexing: options, offset: claim.Offset, end: len(claim.Text)}
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment:
		default:
			tokens.tokens = append(tokens.tokens, token)
		}
	}
	return tokens
}

// current is the token at the cursor, an EOF token at the end of the
// statement.
func (t *statementTokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: t.end, End: t.end}
	}
	return t.tokens[t.pos]
}

// errorf is a refusal positioned at the current token.
func (t *statementTokens) errorf(format string, args ...any) error {
	return ptaherr.At(t.offset+t.current().Start, fmt.Errorf(format, args...))
}

func (t *statementTokens) keyword(word string) bool {
	token := t.current()
	return token.MatchIdentifierValue(word)
}

func (t *statementTokens) operator(value string) bool {
	token := t.current()
	return token.Type == lexer.TokenOperator && token.Value == value
}

// expect consumes word, or refuses the statement as after says.
func (t *statementTokens) expect(word, after string) error {
	if !t.keyword(word) {
		return t.errorf("expected %s after %s", word, after)
	}
	t.pos++
	return nil
}

// name reads a name: a plain identifier, folded to lower case as PostgreSQL
// folds one, or a double-quoted one, taken as written.
func (t *statementTokens) name(what string) (string, error) {
	token := t.current()
	switch {
	case token.Type == lexer.TokenIdentifier:
		t.pos++
		return identifier.ComparisonASCIIInsensitive.IdentityKey(token.Value), nil
	case token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`):
		t.pos++
		return strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(token.Value, `"`), `"`), `""`, `"`), nil
	default:
		return "", t.errorf("expected %s: expected identifier, got %s", what, token.Type)
	}
}

// table reads a table name, qualified by its schema or not.
func (t *statementTokens) table() (pgpublication.Member, error) {
	first, err := t.name("table name")
	if err != nil {
		return pgpublication.Member{}, err
	}
	if !t.operator(".") {
		return pgpublication.Member{Table: first}, nil
	}
	t.pos++
	second, err := t.name("table name")
	if err != nil {
		return pgpublication.Member{}, err
	}
	return pgpublication.Member{Schema: first, Table: second}, nil
}

// finished refuses anything after a statement's last clause.
func (t *statementTokens) finished(statement string) error {
	if token := t.current(); token.Type != lexer.TokenEOF {
		return t.errorf("unexpected %q after %s", token.Value, statement)
	}
	return nil
}

// membership is the tables and schemas one publication object list names.
type membership struct {
	tables  []pgpublication.Member
	schemas []string
}

// objects reads a publication object list: `TABLE [ONLY] name [*] [, ...]`
// and `TABLES IN SCHEMA name [, ...]`, in any order. A comma followed by
// neither TABLE nor TABLES continues the kind before it.
func (t *statementTokens) objects(publication string) (membership, error) {
	var result membership
	kind := ""
	for {
		switch {
		case t.keyword("TABLES"):
			t.pos++
			if err := t.expect("IN", "TABLES"); err != nil {
				return membership{}, err
			}
			if err := t.expect("SCHEMA", "TABLES IN"); err != nil {
				return membership{}, err
			}
			kind = "schema"
		case t.keyword("TABLE"):
			t.pos++
			kind = "table"
		case kind == "":
			return membership{}, t.errorf("expected TABLE or TABLES IN SCHEMA in publication %s", publication)
		}
		if kind == "schema" {
			schema, err := t.schema(publication)
			if err != nil {
				return membership{}, err
			}
			result.schemas = append(result.schemas, schema)
		} else {
			member, err := t.member(publication)
			if err != nil {
				return membership{}, err
			}
			result.tables = append(result.tables, member)
		}
		if !t.operator(",") {
			return result, nil
		}
		t.pos++
	}
}

// schema reads the schema of TABLES IN SCHEMA. CURRENT_SCHEMA is refused: the
// schema it names is whichever the session that runs the statement has first
// on its search path.
func (t *statementTokens) schema(publication string) (string, error) {
	if t.keyword("CURRENT_SCHEMA") {
		return "", t.errorf("publication %s names TABLES IN SCHEMA CURRENT_SCHEMA, which names whichever schema the session that runs "+
			"the statement has first on its search path; name the schema", publication)
	}
	return t.name("schema name")
}

// member reads one table of TABLE: `[ONLY] name [*]`. A column list and a row
// filter are refused by name.
func (t *statementTokens) member(publication string) (pgpublication.Member, error) {
	if t.keyword("ONLY") {
		t.pos++
	}
	member, err := t.table()
	if err != nil {
		return pgpublication.Member{}, err
	}
	if t.operator("*") {
		t.pos++
	}
	if t.operator("(") {
		return pgpublication.Member{}, t.errorf("publication %s publishes a column list of table %s; Ptah does not write a publication's "+
			"column lists, and leaving one out would publish every column; publish the whole table", publication, member.Qualified())
	}
	if t.keyword("WHERE") {
		return pgpublication.Member{}, t.errorf("publication %s publishes table %s with a row filter; Ptah does not write a publication's "+
			"row filters, and leaving one out would publish every row; publish the whole table", publication, member.Qualified())
	}
	return member, nil
}

// settings are the options one WITH or SET list names, each at most once.
type settings struct {
	publish          []string
	publishSet       bool
	viaPartitionRoot *bool
	generatedColumns string
}

// options reads `(option = value [, ...])`, with the cursor on "(".
func (t *statementTokens) options(publication string) (settings, error) {
	var result settings
	if !t.operator("(") {
		return settings{}, t.errorf("expected ( after WITH in publication %s", publication)
	}
	t.pos++
	seen := make(map[string]bool)
	for {
		token := t.current()
		if token.Type != lexer.TokenIdentifier {
			return settings{}, t.errorf("expected a publication option name, got %s", token.Type)
		}
		option := strings.ToLower(token.Value)
		if seen[option] {
			return settings{}, t.errorf("publication %s names option %s twice", publication, option)
		}
		seen[option] = true
		t.pos++
		if !t.operator("=") {
			return settings{}, t.errorf("expected = after publication option %s", option)
		}
		t.pos++
		value, err := t.optionValue(option)
		if err != nil {
			return settings{}, err
		}
		if err := result.set(publication, option, value); err != nil {
			return settings{}, ptaherr.At(t.offset+token.Start, err)
		}
		switch {
		case t.operator(","):
			t.pos++
		case t.operator(")"):
			t.pos++
			return result, nil
		default:
			return settings{}, t.errorf("expected , or ) in the options of publication %s", publication)
		}
	}
}

// optionValue reads an option's value: a string constant, or a word such as
// true or stored.
func (t *statementTokens) optionValue(option string) (string, error) {
	token := t.current()
	switch {
	case token.Type == lexer.TokenString && !strings.HasPrefix(token.Value, `"`):
		text, ok := lexer.StringValue(token.Value, t.lexing)
		if !ok {
			return "", t.errorf("the value of publication option %s is not a string the server reads", option)
		}
		t.pos++
		return text, nil
	case token.Type == lexer.TokenIdentifier:
		t.pos++
		return token.Value, nil
	default:
		return "", t.errorf("expected the value of publication option %s", option)
	}
}

// set records one option. publish is a comma-separated list of operations,
// and an empty one publishes none.
func (s *settings) set(publication, option, value string) error {
	switch option {
	case "publish":
		s.publishSet = true
		s.publish = make([]string, 0)
		for operation := range strings.SplitSeq(value, ",") {
			if operation = strings.ToLower(strings.TrimSpace(operation)); operation != "" {
				s.publish = append(s.publish, operation)
			}
		}
	case "publish_via_partition_root":
		enabled, ok := boolean(value)
		if !ok {
			return fmt.Errorf("publication %s: publish_via_partition_root requires a Boolean value, got %q", publication, value)
		}
		s.viaPartitionRoot = &enabled
	case "publish_generated_columns":
		s.generatedColumns = strings.ToLower(value)
	default:
		return fmt.Errorf("publication %s: unsupported publication option %q; the options are publish, publish_via_partition_root and "+
			"publish_generated_columns", publication, option)
	}
	return nil
}

// apply writes the options s names onto declared.
func (s settings) apply(declared *pgpublication.DesiredPublication) {
	if s.publishSet {
		declared.Publish = s.publish
	}
	if s.viaPartitionRoot != nil {
		declared.ViaPartitionRoot = *s.viaPartitionRoot
	}
	if s.generatedColumns != "" {
		declared.GeneratedColumns = s.generatedColumns
	}
}

// boolean reads a Boolean option value as PostgreSQL does.
func boolean(value string) (parsed, valid bool) {
	switch strings.ToLower(value) {
	case "true", "on", "yes", "1":
		return true, true
	case "false", "off", "no", "0":
		return false, true
	default:
		return false, false
	}
}

// membership reads the optional `FOR ALL TABLES` or `FOR objects` of a CREATE
// PUBLICATION as the declaration it starts.
func (t *statementTokens) membership(name string) (pgpublication.DesiredPublication, error) {
	var declared pgpublication.DesiredPublication
	if !t.keyword("FOR") {
		return declared, nil
	}
	t.pos++
	if t.keyword("ALL") {
		t.pos++
		if err := t.expect("TABLES", "FOR ALL"); err != nil {
			return pgpublication.DesiredPublication{}, err
		}
		declared.AllTables = true
		return declared, nil
	}
	members, err := t.objects(name)
	if err != nil {
		return pgpublication.DesiredPublication{}, err
	}
	declared.Tables, declared.Schemas = members.tables, members.schemas
	return declared, nil
}

// create reads `CREATE PUBLICATION name [FOR ALL TABLES | FOR objects] [WITH
// (options)]`.
func (t *statementTokens) create() (sqlext.Value, error) {
	t.pos = 2
	name, err := t.name("publication name")
	if err != nil {
		return sqlext.Value{}, err
	}
	declared, err := t.membership(name)
	if err != nil {
		return sqlext.Value{}, err
	}
	if t.keyword("WITH") {
		t.pos++
		options, err := t.options(name)
		if err != nil {
			return sqlext.Value{}, err
		}
		options.apply(&declared)
	}
	if err := t.finished("CREATE PUBLICATION " + name); err != nil {
		return sqlext.Value{}, err
	}
	object, err := Declare(name, declared)
	if err != nil {
		return sqlext.Value{}, err
	}
	operation := &pgpublication.Operation{Name: name, Action: pgpublication.Create, AllTables: declared.AllTables,
		Tables: declared.Tables, Schemas: declared.Schemas, Publish: declared.Publish, ViaPartitionRoot: declared.ViaPartitionRoot,
		GeneratedColumns: declared.GeneratedColumns}
	return sqlext.Value{Statement: operation, Objects: []schemaext.Object{object}}, nil
}

// alter reads `ALTER PUBLICATION name ADD|DROP|SET objects`, `SET
// (options)` and `OWNER TO role`. Each amends the declaration known holds
// for the publication; without one it states the amendment alone, which the
// reader of the load refuses.
func (t *statementTokens) alter(known map[string]schemaext.Object) (sqlext.Value, error) {
	t.pos = 2
	name, err := t.name("publication name")
	if err != nil {
		return sqlext.Value{}, err
	}
	amended := pgpublication.DesiredPublication{}
	held, declared := known[pgpublication.Ref(name).Name.Normalized]
	if declared {
		if value, ok := held.Value.(*pgpublication.DesiredPublication); ok {
			amended = *value.Copy()
		}
	}
	statement := "ALTER PUBLICATION " + name
	operation := &pgpublication.Operation{Name: name}
	switch {
	case t.keyword("ADD"), t.keyword("DROP"), t.keyword("SET") && !t.next("("):
		action := strings.ToUpper(t.current().Value)
		t.pos++
		members, err := t.objects(name)
		if err != nil {
			return sqlext.Value{}, err
		}
		if err := (amendment{name: name, action: action, members: members, declared: declared}).apply(&amended); err != nil {
			return sqlext.Value{}, t.errorf("%s", err.Error())
		}
		operation.Action = map[string]pgpublication.Action{"ADD": pgpublication.AddMembers, "DROP": pgpublication.DropMembers,
			"SET": pgpublication.SetMembers}[action]
		operation.Tables, operation.Schemas = members.tables, members.schemas
	case t.keyword("SET"):
		t.pos++
		options, err := t.options(name)
		if err != nil {
			return sqlext.Value{}, err
		}
		options.apply(&amended)
		operation.Action = pgpublication.SetOptions
		operation.Publish, operation.GeneratedColumns = amended.EffectivePublish(), options.generatedColumns
		operation.ViaPartitionRoot = amended.ViaPartitionRoot
	case t.keyword("OWNER"):
		t.pos++
		if err := t.expect("TO", "OWNER"); err != nil {
			return sqlext.Value{}, err
		}
		role, err := t.role(name)
		if err != nil {
			return sqlext.Value{}, err
		}
		amended.Owner = role
		operation.Action, operation.Owner = pgpublication.SetOwner, role
	default:
		return sqlext.Value{}, t.errorf("unsupported %s action %q: the publication owner reads ADD, DROP, SET and OWNER TO", statement, t.current().Value)
	}
	if err := t.finished(statement); err != nil {
		return sqlext.Value{}, err
	}
	object, err := Declare(name, amended)
	if err != nil {
		return sqlext.Value{}, err
	}
	if declared {
		object.Targets = held.Targets
	}
	return sqlext.Value{Statement: operation, Replaces: []schemaext.Object{object}}, nil
}

// next reports whether the token after the current one is the operator value.
func (t *statementTokens) next(value string) bool {
	if t.pos+1 >= len(t.tokens) {
		return false
	}
	token := t.tokens[t.pos+1]
	return token.Type == lexer.TokenOperator && token.Value == value
}

// role reads the role of OWNER TO, refusing one no declaration can name.
func (t *statementTokens) role(publication string) (string, error) {
	token := t.current()
	role, err := t.name("role name")
	if err != nil {
		return "", err
	}
	written := role
	if token.Type == lexer.TokenIdentifier {
		written = token.Value
	}
	if err := objectowner.ValidateRole(written); err != nil {
		return "", ptaherr.At(t.offset+token.Start, fmt.Errorf("publication %s: %w", publication, err))
	}
	return role, nil
}

// amendment is one ALTER PUBLICATION ... ADD, DROP or SET: the publication
// it names, the action, its members, and whether a CREATE declared the
// publication before it.
type amendment struct {
	name, action string
	members      membership
	declared     bool
}

// apply applies ADD, DROP or SET to the declared membership. Where the
// publication was declared, a table or schema added twice, or dropped while
// not a member, and a membership change of a FOR ALL TABLES publication are
// refused, as the server refuses them.
func (a amendment) apply(amended *pgpublication.DesiredPublication) error {
	name, action, members, declared := a.name, a.action, a.members, a.declared
	if declared && amended.AllTables {
		return fmt.Errorf("publication %s is FOR ALL TABLES, and ALTER PUBLICATION %s cannot change its members", name, action)
	}
	switch action {
	case "SET":
		amended.Tables, amended.Schemas = members.tables, members.schemas
	case "ADD":
		amended.Tables = append(amended.Tables, members.tables...)
		amended.Schemas = append(amended.Schemas, members.schemas...)
	default:
		for _, member := range members.tables {
			index := slices.IndexFunc(amended.Tables, func(held pgpublication.Member) bool {
				return held.Schema == member.Schema && held.Table == member.Table
			})
			if index < 0 && declared {
				return fmt.Errorf("publication %s does not publish table %s", name, member.Qualified())
			}
			if index >= 0 {
				amended.Tables = slices.Delete(amended.Tables, index, index+1)
			}
		}
		for _, schema := range members.schemas {
			index := slices.Index(amended.Schemas, schema)
			if index < 0 && declared {
				return fmt.Errorf("publication %s does not publish schema %s", name, schema)
			}
			if index >= 0 {
				amended.Schemas = slices.Delete(amended.Schemas, index, index+1)
			}
		}
	}
	return nil
}

// drop reads `DROP PUBLICATION [IF EXISTS] name [CASCADE | RESTRICT]`.
// PostgreSQL also takes several names in one statement; the operation drops
// one, so that form is refused rather than read as part of itself.
func (t *statementTokens) drop() (sqlext.Value, error) {
	t.pos = 2
	operation := &pgpublication.Operation{Action: pgpublication.Drop}
	if t.keyword("IF") {
		t.pos++
		if err := t.expect("EXISTS", "IF"); err != nil {
			return sqlext.Value{}, err
		}
		operation.IfExists = true
	}
	name, err := t.name("publication name")
	if err != nil {
		return sqlext.Value{}, err
	}
	operation.Name = name
	if t.operator(",") {
		return sqlext.Value{}, t.errorf("DROP PUBLICATION %s, ...: the publication owner reads one publication per DROP PUBLICATION; "+
			"write a statement for each", name)
	}
	if t.keyword("CASCADE") || t.keyword("RESTRICT") {
		t.pos++
	}
	if err := t.finished("DROP PUBLICATION " + name); err != nil {
		return sqlext.Value{}, err
	}
	if err := operation.Validate(); err != nil {
		return sqlext.Value{}, err
	}
	return sqlext.Value{Statement: operation}, nil
}
