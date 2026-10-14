package chsource

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chpolicysource"
	"ptah.run/internal/lexer"
)

const (
	sqlCreateRowPolicy = "create-row-policy"
	sqlAlterRowPolicy  = "alter-row-policy"
	sqlDropRowPolicy   = "drop-row-policy"
)

func readRowPolicy(claim sqlext.Claim, known map[objectidentity.Key]schemaext.Object) (sqlext.Value, error) {
	command, err := newRowPolicyTokens(claim).command()
	if err != nil {
		return sqlext.Value{}, err
	}
	value := sqlext.Value{Statement: command}
	if command.Action == "DROP" {
		value.UnmodeledReason = "DROP ROW POLICY is a migration operation; a desired schema omits the policy declaration"
		return value, nil
	}
	ref := command.Subject()
	held, found := known[ref.Key()]
	desired := command.Values.Copy()

	if command.Action == "ALTER" && !found {
		value.UnmodeledReason = fmt.Sprintf("ALTER ROW POLICY %q on %q requires an earlier CREATE ROW POLICY in this schema", command.Name, command.Table)
		return value, nil
	}
	if command.Action == "CREATE" && found {
		value.UnmodeledReason = fmt.Sprintf("CREATE ROW POLICY %q on %q is declared twice", command.Name, command.Table)
		return value, nil
	}
	if command.Action == "ALTER" {
		desired, err = amendRowPolicy(held, command)
		if err != nil {
			return sqlext.Value{}, err
		}
	}

	object, err := chschema.DesiredRowPolicyObject(ref, *desired)
	if err != nil {
		return sqlext.Value{}, err
	}
	if command.Action == "ALTER" {
		object.Targets = held.Targets
		value.Replaces = []schemaext.Object{object}
	} else {
		value.Objects = []schemaext.Object{object}
	}
	known[ref.Key()] = object
	return value, nil
}

func (t *rowPolicyTokens) command() (*chast.RowPolicyCommand, error) {
	command := &chast.RowPolicyCommand{Action: strings.ToUpper(t.current().Value)}
	if err := t.words(command.Action, "ROW", "POLICY"); err != nil {
		return nil, err
	}
	if t.keyword("IF") || t.keyword("OR") {
		return nil, t.fail("conditional and replacement policy declarations are not supported")
	}
	var err error
	if command.Name, err = t.name(); err != nil {
		return nil, err
	}
	if err = t.words("ON"); err != nil {
		return nil, err
	}
	if t.keyword("CLUSTER") {
		return nil, t.fail("ON CLUSTER is not supported")
	}
	if command.Database, command.Table, err = t.table(); err != nil {
		return nil, err
	}
	if command.Action != "DROP" {
		command.Values = &chschema.DesiredRowPolicy{}
		if err = t.clauses(command); err != nil {
			return nil, err
		}
	}
	if t.current().Type != lexer.TokenEOF {
		return nil, t.fail("unsupported row policy clause")
	}
	if err = command.Validate(); err != nil {
		return nil, err
	}
	return command, nil
}

func amendRowPolicy(held schemaext.Object, command *chast.RowPolicyCommand) (*chschema.DesiredRowPolicy, error) {
	before, ok := held.Value.(*chschema.DesiredRowPolicy)
	if !ok || before == nil {
		return nil, fmt.Errorf("%w: ALTER ROW POLICY requires a declaration", schemaext.ErrInvalidValue)
	}
	desired := before.Copy()
	if command.FilterSet {
		desired.Filter = command.Values.Filter
		desired.NormalizedFilter = nil
	}
	if command.RolesSet {
		desired.Roles = command.Values.Roles.Clone()
	}
	if command.Values.Composition != "" {
		desired.Composition = command.Values.Composition
	}
	return desired, nil
}

func (t *rowPolicyTokens) clauses(command *chast.RowPolicyCommand) error {
	seen := make(map[string]bool)
	for t.current().Type != lexer.TokenEOF {
		clause := strings.ToUpper(t.current().Value)
		if t.current().Type != lexer.TokenIdentifier || seen[clause] {
			return t.fail("unsupported or repeated row policy clause")
		}
		seen[clause] = true
		if err := t.clause(command, clause, seen); err != nil {
			return err
		}
	}
	return nil
}

func (t *rowPolicyTokens) clause(command *chast.RowPolicyCommand, clause string, seen map[string]bool) error {
	t.pos++
	switch clause {
	case "FOR":
		if !t.keyword("SELECT") && !t.keyword("ALL") {
			return t.fail("a row policy filters SELECT only")
		}
		t.pos++
		if !t.keyword("USING") || seen["USING"] {
			return t.fail("FOR SELECT requires USING")
		}
	case "USING":
		return t.using(command)
	case "AS":
		if !t.keyword("PERMISSIVE") && !t.keyword("RESTRICTIVE") {
			return t.fail("AS requires PERMISSIVE or RESTRICTIVE")
		}
		command.Values.Composition = chschema.Composition(strings.ToLower(t.current().Value))
		t.pos++
	case "TO":
		return t.roles(command)
	default:
		return t.fail("unsupported row policy clause " + clause)
	}
	return nil
}

func (t *rowPolicyTokens) nextClause() bool {
	return slices.ContainsFunc([]string{"AS", "TO", "WITH", "ON", "FOR", "USING", "RENAME"}, t.keyword)
}

func (t *rowPolicyTokens) using(command *chast.RowPolicyCommand) error {
	command.FilterSet = true
	start := t.pos
	text, err := t.clauseText(t.nextClause)
	if err != nil {
		return err
	}
	if !strings.EqualFold(text, "NONE") {
		command.Values.Filter = new(t.filterText(start))
	}
	return nil
}

func (t *rowPolicyTokens) roles(command *chast.RowPolicyCommand) error {
	command.RolesSet = true
	start := t.pos
	if _, err := t.clauseText(t.nextClause); err != nil {
		return err
	}
	var parts []string
	for _, token := range t.tokens[start:t.pos] {
		if strings.Contains(token.Value, `\`) {
			return t.fail("backslash escapes in role names are not supported")
		}
		parts = append(parts, token.Value)
	}
	roles, err := chpolicysource.ParseRoles(strings.Join(parts, " "))
	command.Values.Roles = roles
	return err
}
