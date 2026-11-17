package objectowner

import (
	"slices"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// RoleStatement is what one statement of a plan does to the roles an
// ownership statement after it depends on: the role it creates, or the
// memberships it grants or takes away. A statement that does none of these
// carries only its text.
//
// The statements are read from their tokens rather than through the schema
// parser, which refuses spellings a desired schema cannot declare and the
// server accepts: CREATE USER, CREATE GROUP, a CREATE ROLE option Ptah does not
// model, GRANT and REVOKE of a role, and DROP USER.
type RoleStatement struct {
	// Text is the statement, from its first token to its last, without the
	// semicolon that ends it.
	Text string
	// Creates is the role CREATE ROLE, CREATE USER or CREATE GROUP creates, as
	// the server stores its name, or empty.
	Creates string
	// Grants is the memberships a GRANT or a REVOKE of roles grants or takes
	// away, one for each role and member it names, and the roles a DROP ROLE,
	// USER or GROUP drops, in order.
	Grants []Membership
}

// Membership is one membership a GRANT of a role grants, or one a REVOKE or a
// DROP takes away.
type Membership struct {
	// Role is the role granted, and Member the role that receives it, as the
	// server stores their names. Member is empty where the statement names
	// the role of the session that runs it, and for a DROP.
	Role, Member string
	// Set is the SET option the statement states, nil where it states none.
	Set *bool
	// Revoked reports that the statement takes Role away rather than grants
	// it: a REVOKE of the role, whatever option it names, or a DROP of it.
	Revoked bool
}

// ReadRoleStatements reads each statement of text, in order, as the server of
// dialect reads it, for what it does to roles. Only role creation, GRANT and
// REVOKE of a role and DROP of a role are read; any other statement, and one
// these do not recognize, carries only its Text.
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
	case len(tokens) > 2 && tokens[0].MatchIdentifierValue("CREATE") && isRoleWord(tokens[1]):
		return readCreateRole(dialect, tokens[2:])
	case len(tokens) > 2 && tokens[0].MatchIdentifierValue("DROP") && isRoleWord(tokens[1]):
		return readDropRole(dialect, tokens[2:])
	case len(tokens) > 0 && tokens[0].MatchIdentifierValue("GRANT"):
		return RoleStatement{Grants: readMembershipGrant(dialect, tokens[1:], "TO")}
	case len(tokens) > 0 && tokens[0].MatchIdentifierValue("REVOKE"):
		return readRevokeRole(dialect, tokens[1:])
	}
	return RoleStatement{}
}

// isRoleWord reports whether token is the word CREATE and DROP name a role
// with: ROLE, USER or GROUP.
func isRoleWord(token lexer.Token) bool {
	return token.MatchIdentifierValue("ROLE") || token.MatchIdentifierValue("USER") || token.MatchIdentifierValue("GROUP")
}

// readCreateRole reads `[IF NOT EXISTS] name ...`, what follows CREATE ROLE.
func readCreateRole(dialect string, tokens []lexer.Token) RoleStatement {
	if len(tokens) > 3 && tokens[0].MatchIdentifierValue("IF") && tokens[1].MatchIdentifierValue("NOT") && tokens[2].MatchIdentifierValue("EXISTS") {
		tokens = tokens[3:]
	}
	if name, ok := roleName(dialect, tokens[0]); ok {
		return RoleStatement{Creates: name}
	}
	return RoleStatement{}
}

// readDropRole reads `[IF EXISTS] name [, ...]`, what follows DROP ROLE.
func readDropRole(dialect string, tokens []lexer.Token) RoleStatement {
	if len(tokens) > 2 && tokens[0].MatchIdentifierValue("IF") && tokens[1].MatchIdentifierValue("EXISTS") {
		tokens = tokens[2:]
	}
	dropped, _ := roleNames(tokens, func(token lexer.Token) (string, bool) { return roleName(dialect, token) })
	var drops []Membership
	for _, role := range dropped {
		drops = append(drops, Membership{Role: role, Revoked: true})
	}
	return RoleStatement{Grants: drops}
}

// readRevokeRole reads `[option OPTION FOR] role [, ...] FROM member [, ...]
// ...`, what follows REVOKE.
func readRevokeRole(dialect string, tokens []lexer.Token) RoleStatement {
	if len(tokens) > 3 && tokens[1].MatchIdentifierValue("OPTION") && tokens[2].MatchIdentifierValue("FOR") {
		tokens = tokens[3:]
	}
	revokes := readMembershipGrant(dialect, tokens, "FROM")
	for i := range revokes {
		revokes[i].Set, revokes[i].Revoked = nil, true
	}
	return RoleStatement{Grants: revokes}
}

// readMembershipGrant reads `role [, ...] TO member [, ...] [WITH option value
// [, ...]] [GRANTED BY role]`, the GRANT that follows its keyword, or the
// REVOKE whose roles and members FROM separates. A GRANT or REVOKE of
// privileges names ON before TO or FROM, which no role list holds, and reads
// as none.
func readMembershipGrant(dialect string, tokens []lexer.Token, to string) []Membership {
	roles, rest := roleNames(tokens, func(token lexer.Token) (string, bool) { return roleName(dialect, token) })
	if len(roles) == 0 || len(rest) == 0 || !rest[0].MatchIdentifierValue(to) {
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
// stated, and is TRUE otherwise, as for a new membership. The grantor is not
// read: a GRANT names one only where it says GRANTED BY.
//
// A REVOKE or a DROP of a role is read the conservative way, since the grantor
// decides which grant a REVOKE removes: every grant of the role before it
// stops counting, whichever member and option it names; the role no longer
// counts as one the connection holds because the server answered so; and
// once a plan revokes or drops any role, no chain through the memberships
// the server holds counts, since the plan may have broken one.
//
// created is the roles statements before the grants create: the connection
// holds none of them until a grant gives it one. heldBy returns the roles a
// role the server holds holds the privilege on, through the memberships the
// server holds, itself included, and nothing for a role the server does not
// hold, whose grant then gains nothing. It is asked once for each role a grant
// gives the connection that no statement before creates, and for a member
// the server answered the connection holds, to learn that the server holds
// it at all.
func (t Transfers) Gains(dialect, role string, created []string, grants []Membership, heldBy func(string) ([]string, error)) (bool, error) {
	if t.Privilege == "" {
		return false, nil
	}
	reach := &grantReach{transfers: t, dialect: dialect, created: created, grants: grants, heldBy: heldBy,
		chains: !slices.ContainsFunc(grants, func(m Membership) bool { return m.Revoked }), answers: make(map[string][]string)}
	counted := t.countedGrants(dialect, grants)
	for changed := true; changed; {
		changed = false
		for _, grant := range counted {
			taken, err := reach.take(grant)
			if err != nil {
				return false, err
			}
			changed = changed || taken
		}
	}
	return reach.has(reach.gained, role), nil
}

// grantReach is the roles [Transfers.Gains] has found the connection gains,
// with what it asked the server on the way.
type grantReach struct {
	transfers Transfers
	dialect   string
	created   []string
	grants    []Membership
	heldBy    func(string) ([]string, error)
	// chains reports that no grant is revoked, so the memberships the server
	// holds still count.
	chains bool
	// answers is heldBy's answer for each role asked about.
	answers map[string][]string
	gained  []string
}

// has reports whether roles holds role.
func (r *grantReach) has(roles []string, role string) bool {
	return slices.ContainsFunc(roles, func(name string) bool { return SameRole(r.dialect, name, role) })
}

// revoked reports whether a REVOKE or a DROP of the plan names role.
func (r *grantReach) revoked(role string) bool {
	return slices.ContainsFunc(r.grants, func(m Membership) bool { return m.Revoked && SameRole(r.dialect, m.Role, role) })
}

// ask is heldBy's answer about role, asked once.
func (r *grantReach) ask(role string) ([]string, error) {
	if held, ok := r.answers[role]; ok {
		return held, nil
	}
	held, err := r.heldBy(role)
	if err != nil {
		return nil, err
	}
	r.answers[role] = held
	return held, nil
}

// holds reports whether the connection holds the privilege on member: the
// session's role, a role a grant gave it, or a role the server answered it
// holds, which the server holds and the plan neither creates nor revokes.
func (r *grantReach) holds(member string) (bool, error) {
	switch {
	case member == "" || SameRole(r.dialect, member, r.transfers.Connected) || r.has(r.gained, member):
		return true, nil
	case r.has(r.created, member) || r.revoked(member) || r.transfers.Refuses(r.dialect, member):
		return false, nil
	}
	held, err := r.ask(member)
	return r.has(held, member), err
}

// take adds what grant gives the connection, where its member is one the
// connection holds, and reports whether it added a role.
func (r *grantReach) take(grant Membership) (bool, error) {
	if r.has(r.gained, grant.Role) {
		return false, nil
	}
	member, err := r.holds(grant.Member)
	if err != nil || !member {
		return false, err
	}
	if r.has(r.created, grant.Role) {
		r.gained = append(r.gained, grant.Role)
		return true, nil
	}
	held, err := r.ask(grant.Role)
	if err != nil {
		return false, err
	}
	taken := false
	for _, name := range held {
		if (r.chains || SameRole(r.dialect, name, grant.Role)) && !r.has(r.gained, name) {
			r.gained, taken = append(r.gained, name), true
		}
	}
	return taken, nil
}

// countedGrants is the grants of grants the server still follows when it
// checks the privilege: none that a later REVOKE or DROP of its role may have
// taken away, and on a server that checks SET, none whose SET option is FALSE,
// stated or kept from an earlier grant of the membership.
func (t Transfers) countedGrants(dialect string, grants []Membership) []Membership {
	var counted []Membership
	for i, grant := range grants {
		taken := slices.ContainsFunc(grants[i:], func(m Membership) bool { return m.Revoked && SameRole(dialect, m.Role, grant.Role) })
		if taken {
			continue
		}
		set := true
		for _, earlier := range slices.Backward(grants[:i+1]) {
			if earlier.Set != nil && SameRole(dialect, earlier.Role, grant.Role) && SameRole(dialect, earlier.Member, grant.Member) {
				set = *earlier.Set
				break
			}
		}
		if set || t.Privilege != "SET" {
			counted = append(counted, grant)
		}
	}
	return counted
}
