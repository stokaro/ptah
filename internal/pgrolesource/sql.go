package pgrolesource

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgrole"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
)

// SQL registers ALTER ROLE ... SET and RESET, and ALTER USER, its synonym. A
// SQL document states a role's settings as the statements that give them, so
// RESET is the explicit reset and SET name TO DEFAULT is the same statement.
//
// Three forms are refused by name rather than read as something else: IN
// DATABASE stores a default for one database, ALL stores one for every role,
// and FROM CURRENT stores the value of whatever session runs the statement.
// RESET ALL is refused too, because it takes away settings the document does
// not name.
func SQL() sqlext.Extension {
	var patterns []sqlext.Pattern
	for _, head := range []string{"ROLE", "USER"} {
		for _, after := range []string{"SET", "RESET", "IN"} {
			patterns = append(patterns, sqlext.Pattern{Name: strings.ToLower("alter-" + head + "-" + after), Site: sqlext.Statement,
				Words: []string{"ALTER", head}, AfterName: []string{after}})
		}
	}
	return sqlext.Extension{Owner: pgrole.Owner, Targets: []string{platform.Postgres}, Patterns: patterns,
		Facets: []schemaext.Kind{pgrole.SettingsKind}, Parser: sqlParser{},
		Coverage: func(sqlext.CoverageRequest) (schemaext.Coverage, error) { return Coverage() },
	}
}

type sqlParser struct{}

// roleGroup is the statements of one document that give one role its
// settings. Each statement still requires its role at its own position; the
// last carries the folded settings, as the server leaves them after running
// the statements in order.
type roleGroup struct {
	written  string
	settings map[string]pgrole.Setting
	claims   []int
}

func (sqlParser) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	result := sqlext.Result{Values: make([]sqlext.Value, len(request.Claims))}
	groups := make(map[string]*roleGroup)
	var order []*roleGroup
	for i, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		statement, err := readStatement(claim)
		if err != nil {
			return sqlext.Result{}, err
		}
		key := sqlident.UnquoteDoubleQuoted(statement.role)
		if !strings.HasPrefix(statement.role, `"`) {
			key = strings.ToLower(key)
		}
		group, found := groups[key]
		if !found {
			group = &roleGroup{written: statement.role, settings: make(map[string]pgrole.Setting)}
			groups[key] = group
			order = append(order, group)
		}
		group.settings[pgrole.Key(statement.setting.Name)] = statement.setting
		group.claims = append(group.claims, i)
		change := pgrole.SettingChange{Name: statement.setting.Name}
		if !statement.setting.Reset {
			change.After = new(statement.setting.Value)
		}
		result.Values[i] = sqlext.Value{
			Statement:      &pgrole.AlterSettings{Role: key, Change: pgrole.SettingsChange{Settings: []pgrole.SettingChange{change}}},
			RoleReferences: []string{statement.role},
		}
	}
	for _, group := range order {
		settings := make([]pgrole.Setting, 0, len(group.settings))
		for _, setting := range group.settings {
			settings = append(settings, setting)
		}
		facets, err := pgrole.WithSettings(schemaext.Facets{}, settings)
		if err != nil {
			return sqlext.Result{}, settingError(request.Claims[group.claims[0]], err)
		}
		last := group.claims[len(group.claims)-1]
		result.Values[last].RoleFacets = &sqlext.RoleFacets{Name: group.written, Facets: facets}
		for _, index := range group.claims[:len(group.claims)-1] {
			result.Values[index].DescribedBy = last + 1
		}
	}
	return result, ctx.Err()
}

// roleStatement is one ALTER ROLE statement: the role as written, and the
// setting it gives or takes away.
type roleStatement struct {
	role    string
	setting pgrole.Setting
}

// settingTokens reads one claim, keeping each token's place in the claim.
type settingTokens struct {
	claim  sqlext.Claim
	tokens []lexer.Token
	pos    int
}

func newSettingTokens(claim sqlext.Claim) *settingTokens {
	t := &settingTokens{claim: claim}
	scan := lexer.NewLexerWithOptions(claim.Text, dialectlexer.Options(platform.Postgres))
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment && token.Type != lexer.TokenSemicolon {
			t.tokens = append(t.tokens, token)
		}
	}
	return t
}

func (t *settingTokens) current() lexer.Token {
	if t.pos >= len(t.tokens) {
		return lexer.Token{Type: lexer.TokenEOF, Start: len(t.claim.Text), End: len(t.claim.Text)}
	}
	return t.tokens[t.pos]
}

func (t *settingTokens) keyword(word string) bool {
	token := t.current()
	return token.MatchIdentifierValue(word)
}

func (t *settingTokens) operator(value string) bool {
	token := t.current()
	return token.MatchOperatorValue(value)
}

func (t *settingTokens) refuse(message string) error {
	return ptaherr.At(t.claim.Offset+t.current().Start, errors.New(message))
}

// readStatement reads `ALTER ROLE role SET name = value`, `... SET name TO
// value`, `... SET name = DEFAULT` and `... RESET name`, refusing every other
// form a pattern claims.
func readStatement(claim sqlext.Claim) (roleStatement, error) {
	t := newSettingTokens(claim)
	t.pos = 1
	head := strings.ToUpper(t.current().Value)
	t.pos++
	role := t.current()
	switch {
	case role.MatchIdentifierValue("ALL"):
		return roleStatement{}, t.refuse(fmt.Sprintf("ALTER %s ALL stores a default for every role, which Ptah does not model; "+
			"give each role its settings with ALTER ROLE name SET", head))
	case role.Type == lexer.TokenIdentifier && isRoleKeyword(role.Value):
		return roleStatement{}, t.refuse(fmt.Sprintf("ALTER %s %s names the role of the session that runs it; name the role",
			head, strings.ToUpper(role.Value)))
	case role.Type != lexer.TokenIdentifier && !(role.Type == lexer.TokenString && strings.HasPrefix(role.Value, `"`)):
		return roleStatement{}, t.refuse(fmt.Sprintf("ALTER %s expects a role name", head))
	}
	t.pos++
	if t.keyword("IN") {
		return roleStatement{}, t.refuse(fmt.Sprintf("ALTER %s ... IN DATABASE stores a default for one database, which Ptah does not model; "+
			"Ptah manages the settings a role holds in every database", head))
	}
	statement := roleStatement{role: role.Value}
	switch {
	case t.keyword("RESET"):
		t.pos++
		if t.keyword("ALL") && t.pos == len(t.tokens)-1 {
			return roleStatement{}, t.refuse("RESET ALL takes away settings the document does not name; reset each setting by name")
		}
		name, err := t.settingName()
		if err != nil {
			return roleStatement{}, err
		}
		if t.pos != len(t.tokens) {
			return roleStatement{}, t.refuse("expected the end of the statement after the setting name")
		}
		statement.setting = pgrole.Setting{Name: name, Reset: true}
		return statement, nil
	case t.keyword("SET"):
		t.pos++
	default:
		return roleStatement{}, t.refuse(fmt.Sprintf("ALTER %s expects SET or RESET after the role", head))
	}
	name, err := t.settingName()
	if err != nil {
		return roleStatement{}, err
	}
	switch {
	case t.keyword("FROM"):
		return roleStatement{}, t.refuse(fmt.Sprintf("SET %s FROM CURRENT stores the value of the session that runs the statement, "+
			"which no declaration can compare; write the value", name))
	case t.keyword("TO"), t.operator("="):
		t.pos++
	default:
		return roleStatement{}, t.refuse(fmt.Sprintf("expected = or TO after setting %s", name))
	}
	if t.pos >= len(t.tokens) {
		return roleStatement{}, t.refuse(fmt.Sprintf("setting %s has no value", name))
	}
	start := t.current().Start
	value, form, err := pgrole.ParseValue(claim.Text[start:t.tokens[len(t.tokens)-1].End])
	if err != nil {
		return roleStatement{}, ptaherr.At(claim.Offset+start, err)
	}
	statement.setting = pgrole.Setting{Name: name, Value: value, Reset: form == pgrole.ValueDefault}
	if statement.setting.Reset {
		statement.setting.Value = ""
	}
	return statement, nil
}

// settingName reads a setting's name, dotted for a custom one, up to the word
// or operator that follows it.
func (t *settingTokens) settingName() (string, error) {
	start := t.current().Start
	end := start
	for t.pos < len(t.tokens) {
		token := t.current()
		if token.MatchIdentifierValue("TO") || token.MatchIdentifierValue("FROM") || token.MatchOperatorValue("=") {
			break
		}
		end = token.End
		t.pos++
	}
	if end == start {
		return "", t.refuse("expected a setting name")
	}
	name, err := pgrole.ParseName(t.claim.Text[start:end])
	if err != nil {
		return "", ptaherr.At(t.claim.Offset+start, err)
	}
	return name, nil
}

// isRoleKeyword reports the words ALTER ROLE reads as the session's role
// rather than as a name.
func isRoleKeyword(word string) bool {
	switch strings.ToUpper(word) {
	case "CURRENT_USER", "CURRENT_ROLE", "SESSION_USER":
		return true
	}
	return false
}

func settingError(claim sqlext.Claim, err error) error {
	return ptaherr.At(claim.Offset, err)
}
