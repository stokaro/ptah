package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/parser"
)

// bootstrapMode separates syntax validation from applying selected declarations.
// Every branch is parsed; only the selected branch contributes role state.
type bootstrapMode uint8

const (
	bootstrapValidate bootstrapMode = iota
	bootstrapApply
)

// roleBootstrap interprets a bounded declaration language against the roles
// earlier statements in this desired document declared. It never executes SQL
// or reads a deployment server. Unknown control flow is refused, including in
// an unselected branch, because a partial interpretation is not a schema.
type roleBootstrap struct {
	sql     string
	tokens  []lexer.Token
	pos     int
	roles   map[string]bool
	created []schemamodel.Role
}

func appendRoleBootstrap(
	database *schemamodel.Database, document *Document, block *ast.PostgresDoBlockNode, dialect string,
) error {
	if platform.NormalizeDialect(dialect) != platform.Postgres && dialect != "" {
		return fmt.Errorf("%w: procedural role declarations require PostgreSQL", ErrUnmodeledStatement)
	}
	if block.Language != "" && !strings.EqualFold(block.Language, "plpgsql") {
		return fmt.Errorf("%w: desired-state DO blocks require the supported PL/pgSQL role-bootstrap form", ErrUnmodeledStatement)
	}
	p := roleBootstrap{sql: block.Body.SQL, roles: make(map[string]bool)}
	for _, role := range database.Roles {
		p.roles[role.Name] = true
	}
	if document.base != nil {
		for _, role := range document.base.Roles {
			p.roles[role.Name] = true
		}
	}
	if err := p.tokenize(); err != nil {
		return err
	}
	if err := p.block(bootstrapApply, 0); err != nil {
		return err
	}
	p.take(";")
	if p.pos != len(p.tokens) {
		return p.unsupported("unexpected text after END")
	}
	database.Roles = append(database.Roles, p.created...)
	return nil
}

func (p *roleBootstrap) unsupported(reason string) error {
	position := len(p.sql)
	if p.pos < len(p.tokens) {
		position = p.tokens[p.pos].Start
	}
	return fmt.Errorf("%w: desired-state DO block at body byte %d: %s; use supported role-bootstrap declarations or an explicit migration",
		ErrUnmodeledStatement, position, reason)
}

// take compares token spelling, never fragments inside a string or quoted
// identifier. Keywords in this grammar are unquoted PostgreSQL keywords.
func (p *roleBootstrap) take(value string) bool {
	if p.pos >= len(p.tokens) {
		return false
	}
	token := p.tokens[p.pos]
	if token.Type == lexer.TokenString || !strings.EqualFold(token.Value, value) {
		return false
	}
	p.pos++
	return true
}

func (p *roleBootstrap) expect(value string) error {
	if !p.take(value) {
		return p.unsupported("expected " + value)
	}
	return nil
}

func (p *roleBootstrap) block(mode bootstrapMode, depth int) error {
	if err := p.expect("BEGIN"); err != nil {
		return err
	}
	if err := p.statements(mode, depth); err != nil {
		return err
	}
	return p.expect("END")
}

func (p *roleBootstrap) statements(mode bootstrapMode, depth int) error {
	if depth > 64 {
		return p.unsupported("role-bootstrap nesting exceeds 64 levels")
	}
	for p.pos < len(p.tokens) {
		switch strings.ToUpper(p.tokens[p.pos].Value) {
		case "END", "ELSE":
			return nil
		case "IF":
			if err := p.conditional(mode, depth+1); err != nil {
				return err
			}
		case "BEGIN":
			if err := p.block(mode, depth+1); err != nil {
				return err
			}
		case "CREATE":
			role, err := p.createRole()
			if err != nil {
				return err
			}
			if mode == bootstrapApply {
				if p.roles[role.Name] {
					return p.unsupported("CREATE ROLE would repeat a role already declared in this desired schema")
				}
				p.roles[role.Name] = true
				p.created = append(p.created, *role)
			}
		case "NULL":
			p.pos++
		default:
			return p.unsupported("only CREATE ROLE, NULL, BEGIN, and supported IF conditions are allowed")
		}
		if err := p.expect(";"); err != nil {
			return err
		}
	}
	return p.unsupported("unterminated block")
}

func (p *roleBootstrap) conditional(mode bootstrapMode, depth int) error {
	p.pos++ // IF
	condition, err := p.condition()
	if err != nil {
		return err
	}
	if err := p.expect("THEN"); err != nil {
		return err
	}
	thenMode, elseMode := bootstrapValidate, bootstrapValidate
	if condition {
		thenMode = mode
	} else {
		elseMode = mode
	}
	if err := p.statements(thenMode, depth); err != nil {
		return err
	}
	if p.take("ELSE") {
		if err := p.statements(elseMode, depth); err != nil {
			return err
		}
	}
	if err := p.expect("END"); err != nil {
		return err
	}
	return p.expect("IF")
}

func (p *roleBootstrap) condition() (bool, error) {
	if p.take("TRUE") {
		return true, nil
	}
	if p.take("FALSE") {
		return false, nil
	}
	negate := p.take("NOT")
	for _, token := range []string{"EXISTS", "(", "SELECT", "1", "FROM"} {
		if err := p.expect(token); err != nil {
			return false, err
		}
	}
	if p.take("pg_catalog") {
		if err := p.expect("."); err != nil {
			return false, err
		}
	}
	for _, token := range []string{"pg_roles", "WHERE", "rolname", "="} {
		if err := p.expect(token); err != nil {
			return false, err
		}
	}
	name, err := p.roleLiteral()
	if err != nil {
		return false, err
	}
	if err := p.expect(")"); err != nil {
		return false, err
	}
	return p.roles[name] != negate, nil
}

func (p *roleBootstrap) roleLiteral() (string, error) {
	if p.pos >= len(p.tokens) {
		return "", p.unsupported("expected a role-name string literal")
	}
	token := p.tokens[p.pos]
	// Escape strings, casts, and expressions have different
	// semantics; none may be read as an ordinary PostgreSQL string literal.
	if token.Type != lexer.TokenString || len(token.Value) < 2 || token.Value[0] != '\'' || token.Value[len(token.Value)-1] != '\'' {
		return "", p.unsupported("expected an ordinary role-name string literal")
	}
	name, valid := lexer.StringValue(token.Value, dialectlexer.Options(platform.Postgres))
	if !valid {
		return "", p.unsupported("invalid role-name string literal")
	}
	if strings.HasPrefix(name, "pg_") || name == "postgres" {
		return "", p.unsupported("role conditions must name application roles, not server infrastructure")
	}
	p.pos++
	return name, nil
}

func (p *roleBootstrap) createRole() (*schemamodel.Role, error) {
	start := p.pos
	for _, token := range []string{"CREATE", "ROLE"} {
		if err := p.expect(token); err != nil {
			return nil, err
		}
	}
	for p.pos < len(p.tokens) && p.tokens[p.pos].Type != lexer.TokenSemicolon {
		p.pos++
	}
	if p.pos == len(p.tokens) {
		return nil, p.unsupported("unterminated CREATE ROLE")
	}
	if p.pos <= start+2 {
		return nil, p.unsupported("expected an application role name")
	}
	name := p.tokens[start+2]
	if strings.HasPrefix(name.Value, "`") || (name.Type == lexer.TokenString && !strings.HasPrefix(name.Value, `"`)) {
		return nil, p.unsupported("role identifiers must be unquoted or double-quoted")
	}
	if err := p.roleOptions(start + 3); err != nil {
		return nil, err
	}
	sql := p.sql[p.tokens[start].Start:p.tokens[p.pos].Start]
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.Postgres)).Parse()
	if err != nil || len(statements.Statements) != 1 {
		// A role declaration can contain a password. Keep parser fragments out
		// of this error, including for malformed declarations.
		return nil, p.unsupported("invalid CREATE ROLE declaration")
	}
	node, ok := statements.Statements[0].(*ast.CreateRoleNode)
	if !ok {
		return nil, p.unsupported("expected CREATE ROLE")
	}
	role := toRole(node, platform.Postgres)
	if role.Name == "" || len(role.Name) > 63 || strings.HasPrefix(role.Name, "pg_") || role.Name == "postgres" {
		return nil, p.unsupported("CREATE ROLE must name an application role of 1 to 63 bytes")
	}
	return &role, nil
}

// roleOptions refuses option repetitions that the general DDL parser reduces
// to the last value but PostgreSQL rejects. Interpretation must not give an
// invalid executable declaration a successful desired-state meaning.
func (p *roleBootstrap) roleOptions(start int) error {
	seen := make(map[string]bool)
	for i := start; i < p.pos; i++ {
		token := p.tokens[i]
		option := strings.ToUpper(token.Value)
		if option == "WITH" && i == start {
			continue
		}
		option = strings.TrimPrefix(option, "NO")
		switch option {
		case "LOGIN", "SUPERUSER", "CREATEDB", "CREATEROLE", "INHERIT", "REPLICATION":
		case "PASSWORD":
			i++ // The shared parser validates the literal.
		default:
			return p.unsupported("unsupported CREATE ROLE option")
		}
		if token.Type != lexer.TokenIdentifier || seen[option] {
			return p.unsupported("invalid or repeated CREATE ROLE option")
		}
		seen[option] = true
	}
	return nil
}

func (p *roleBootstrap) tokenize() error {
	if strings.ContainsRune(p.sql, 0) {
		return p.unsupported("NUL is not allowed in a procedural body")
	}
	options := dialectlexer.Options(platform.Postgres)
	options.DisableHashComments = true // PostgreSQL does not treat # as a comment.
	l := lexer.NewLexerWithOptions(p.sql, options)
	for token := l.NextToken(); token.Type != lexer.TokenEOF; token = l.NextToken() {
		if token.Type == lexer.TokenComment && strings.HasPrefix(token.Value, "/*") {
			// The shared lexer ends a block comment at its first closing marker.
			// Refuse nested comments rather than interpret their remaining text.
			if !strings.HasSuffix(token.Value, "*/") || strings.Contains(token.Value[2:], "/*") {
				return p.unsupported("unterminated or nested block comment")
			}
		}
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			p.tokens = append(p.tokens, token)
		}
	}
	return nil
}
