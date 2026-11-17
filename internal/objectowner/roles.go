package objectowner

import (
	"slices"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// RoleStatement is what one statement of a plan does to the roles an
// ownership statement after it depends on: the role it creates, or the
// memberships it grants. A statement that does neither is the zero value.
//
// The statements are read from their tokens rather than through the schema
// parser, which refuses spellings a desired schema cannot declare and the
// server accepts: CREATE USER, CREATE GROUP, a CREATE ROLE option Ptah does not
// model, and GRANT of a role.
type RoleStatement struct {
	// Text is the statement, from its first token to its last, without the
	// semicolon that ends it.
	Text string
	// Creates is the role CREATE ROLE, CREATE USER or CREATE GROUP creates, as
	// the server stores its name, or empty.
	Creates string
	// Grants is the memberships a GRANT of roles grants, one for each role and
	// member it names, in order.
	Grants []Membership
}

// Membership is one membership a GRANT of a role grants.
type Membership struct {
	// Role is the role granted, and Member the role that receives it, as the
	// server stores their names. Member is empty where the statement names
	// the role of the session that runs it.
	Role, Member string
	// Set is the SET option the statement states, nil where it states none.
	Set *bool
}

// ReadRoleStatements reads each statement of text, in order, as the server of
// dialect reads it, for what it does to roles. Only role creation and GRANT of
// a role are read; any other statement, and one these do not recognize,
// carries only its Text, so a reader can only learn of a role or a membership
// a statement makes.
func ReadRoleStatements(dialect, text string) []RoleStatement {
	lex := lexer.NewLexerWithOptions(text, dialectlexer.Options(dialect))
	var statements []RoleStatement
	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenSemicolon, lexer.TokenEOF:
			if len(tokens) > 0 {
				statement := readRoleStatement(dialect, tokens)
				statement.Text = text[tokens[0].Start:tokens[len(tokens)-1].End]
				statements = append(statements, statement)
			}
			if token.Type == lexer.TokenEOF {
				return statements
			}
			tokens = nil
			continue
		}
		tokens = append(tokens, token)
	}
}

func readRoleStatement(dialect string, tokens []lexer.Token) RoleStatement {
	switch {
	case len(tokens) > 2 && tokens[0].MatchIdentifierValue("CREATE") &&
		(tokens[1].MatchIdentifierValue("ROLE") || tokens[1].MatchIdentifierValue("USER") || tokens[1].MatchIdentifierValue("GROUP")):
		rest := tokens[2:]
		if len(rest) > 3 && rest[0].MatchIdentifierValue("IF") && rest[1].MatchIdentifierValue("NOT") && rest[2].MatchIdentifierValue("EXISTS") {
			rest = rest[3:]
		}
		if name, ok := roleName(dialect, rest[0]); ok {
			return RoleStatement{Creates: name}
		}
	case len(tokens) > 0 && tokens[0].MatchIdentifierValue("GRANT"):
		return RoleStatement{Grants: readMembershipGrant(dialect, tokens[1:])}
	}
	return RoleStatement{}
}

// readMembershipGrant reads `role [, ...] TO member [, ...] [WITH option value
// [, ...]] [GRANTED BY role]`, the GRANT that follows its keyword. A GRANT of
// privileges names ON before TO, which no role list holds, and reads as none.
func readMembershipGrant(dialect string, tokens []lexer.Token) []Membership {
	roles, rest := roleNames(tokens, func(token lexer.Token) (string, bool) { return roleName(dialect, token) })
	if len(roles) == 0 || len(rest) == 0 || !rest[0].MatchIdentifierValue("TO") {
		return nil
	}
	members, rest := roleNames(rest[1:], func(token lexer.Token) (string, bool) {
		if isSessionRole(token) {
			return "", true
		}
		return roleName(dialect, token)
	})
	if len(members) == 0 {
		return nil
	}
	set := grantSetOption(rest)
	var grants []Membership
	for _, role := range roles {
		for _, member := range members {
			grants = append(grants, Membership{Role: role, Member: member, Set: set})
		}
	}
	return grants
}

// roleNames reads a comma-separated list of the names read reads, and returns
// it with the tokens after it. A member list also takes the words that name
// the role of the session, which its read reads as an empty name. A token read
// does not take ends the list with nothing read.
func roleNames(tokens []lexer.Token, read func(lexer.Token) (string, bool)) (names []string, rest []lexer.Token) {
	for len(tokens) > 0 {
		name, ok := read(tokens[0])
		if !ok {
			return nil, nil
		}
		names = append(names, name)
		tokens = tokens[1:]
		if len(tokens) == 0 || !tokens[0].MatchOperatorValue(",") {
			return names, tokens
		}
		tokens = tokens[1:]
	}
	return nil, nil
}

// grantSetOption is the SET option a WITH clause states, the last one where
// it states several, as the server keeps the last: OPTION and TRUE grant it,
// FALSE withholds it.
func grantSetOption(tokens []lexer.Token) *bool {
	if len(tokens) == 0 || !tokens[0].MatchIdentifierValue("WITH") {
		return nil
	}
	var set *bool
	for i := 1; i+1 < len(tokens) && !tokens[i].MatchIdentifierValue("GRANTED"); i++ {
		if tokens[i].MatchIdentifierValue("SET") {
			set = new(!tokens[i+1].MatchIdentifierValue("FALSE"))
		}
	}
	return set
}

// roleName is the stored name of a role a token names: an unquoted word or a
// double-quoted identifier.
func roleName(dialect string, token lexer.Token) (string, bool) {
	quoted := token.Type == lexer.TokenString && len(token.Value) >= 2 && token.Value[0] == '"'
	if token.Type != lexer.TokenIdentifier && !quoted {
		return "", false
	}
	return StoredRole(dialect, token.Value), true
}

// isSessionRole reports whether token is a word that names the role of the
// session that runs a statement.
func isSessionRole(token lexer.Token) bool {
	return slices.ContainsFunc(sessionRoles, token.MatchIdentifierValue)
}

// Gains reports whether the memberships grants give the connection the
// privilege an owner change requires on role, once they have run, where the
// server answered that the connection does not hold it now. A grant counts
// where its member is the connection or a role the connection holds the
// privilege on, the server's answer or an earlier grant's: through a chain of
// memberships, every one of which the server follows, so a grant whose SET
// option is FALSE does not count where the server checks SET. A grant that
// states no SET option keeps the one an earlier grant of the same membership
// stated, and is TRUE otherwise, as for a new membership.
//
// created is the roles statements before the grants create: the connection
// holds none of them until a grant gives it one. heldBy returns the roles a
// role the server holds holds the privilege on, through the memberships the
// server holds, and is asked once for each role a grant gives the connection.
func (t Transfers) Gains(dialect, role string, created []string, grants []Membership, heldBy func(string) ([]string, error)) (bool, error) {
	if t.Privilege == "" {
		return false, nil
	}
	same := func(a string) func(string) bool { return func(b string) bool { return SameRole(dialect, a, b) } }
	var gained []string
	holds := func(member string) bool {
		if member == "" || SameRole(dialect, member, t.Connected) || slices.ContainsFunc(gained, same(member)) {
			return true
		}
		return !slices.ContainsFunc(created, same(member)) && !t.Refuses(dialect, member)
	}
	counted := t.countedGrants(dialect, grants)
	for changed := true; changed; {
		changed = false
		for _, grant := range counted {
			if slices.ContainsFunc(gained, same(grant.Role)) || !holds(grant.Member) {
				continue
			}
			gained, changed = append(gained, grant.Role), true
			if slices.ContainsFunc(created, same(grant.Role)) {
				continue
			}
			held, err := heldBy(grant.Role)
			if err != nil {
				return false, err
			}
			gained = append(gained, held...)
		}
	}
	return slices.ContainsFunc(gained, same(role)), nil
}

// countedGrants is grants without the ones the server does not follow when
// it checks the privilege: on a server that checks SET, a grant whose SET
// option is FALSE, stated or kept from an earlier grant of the membership.
func (t Transfers) countedGrants(dialect string, grants []Membership) []Membership {
	if t.Privilege != "SET" {
		return grants
	}
	var counted []Membership
	for i, grant := range grants {
		set := true
		for _, earlier := range slices.Backward(grants[:i+1]) {
			if earlier.Set != nil && SameRole(dialect, earlier.Role, grant.Role) && SameRole(dialect, earlier.Member, grant.Member) {
				set = *earlier.Set
				break
			}
		}
		if set {
			counted = append(counted, grant)
		}
	}
	return counted
}
