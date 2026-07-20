package atlashcl

import (
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"github.com/stokaro/ptah/core/goschema"
)

func (p *parser) parseExtension(block *hclsyntax.Block) error {
	name, err := p.objectName(block, "extension")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedExtensionAttrs(block); err != nil {
		return err
	}
	p.db.Extensions = append(p.db.Extensions, goschema.Extension{
		Name:    name,
		Version: p.optionalString(block.Body.Attributes["version"]),
		Comment: p.optionalString(block.Body.Attributes["comment"]),
	})
	return nil
}

func (p *parser) parseFunction(block *hclsyntax.Block) error {
	name, err := p.objectName(block, "function")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedFunctionAttrs(block); err != nil {
		return err
	}
	args, err := p.parseFunctionArgs(block)
	if err != nil {
		return err
	}
	body := p.optionalString(block.Body.Attributes["as"])
	if body == "" {
		return p.blockError(block, "function %q requires as", name)
	}
	function := goschema.Function{
		Name:       qualifyObjectName(p.optionalRefName(block.Body.Attributes["schema"]), name),
		Parameters: strings.Join(args, ", "),
		Returns:    p.optionalRawExpr(block.Body.Attributes["return"]),
		Language:   p.optionalString(block.Body.Attributes["lang"]),
		Security:   p.optionalString(block.Body.Attributes["security"]),
		Volatility: p.optionalString(block.Body.Attributes["volatility"]),
		Body:       body,
		Comment:    p.optionalString(block.Body.Attributes["comment"]),
	}
	function.Canonicalize()
	p.db.Functions = append(p.db.Functions, function)
	return nil
}

func (p *parser) parseFunctionArgs(block *hclsyntax.Block) ([]string, error) {
	args := make([]string, 0, len(block.Body.Blocks))
	for _, nested := range block.Body.Blocks {
		if nested.Type != "arg" {
			return nil, p.blockError(nested, "unsupported function block %q", nested.Type)
		}
		if err := p.rejectUnsupportedFunctionArgAttrs(nested); err != nil {
			return nil, err
		}
		typeAttr := nested.Body.Attributes["type"]
		if typeAttr == nil {
			return nil, p.blockError(nested, "function arg requires type")
		}
		if len(nested.Labels) != 1 {
			return nil, p.blockError(nested, "function arg requires exactly one name label")
		}
		args = append(args, nested.Labels[0]+" "+p.rawExpr(typeAttr))
	}
	return args, nil
}

func (p *parser) parseView(block *hclsyntax.Block) error {
	name, err := p.objectName(block, "view")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedViewAttrs(block); err != nil {
		return err
	}
	body := p.optionalString(block.Body.Attributes["as"])
	if body == "" {
		return p.blockError(block, "view %q requires as", name)
	}
	p.db.Views = append(p.db.Views, goschema.View{
		Name:      qualifyObjectName(p.optionalRefName(block.Body.Attributes["schema"]), name),
		Body:      body,
		WithCheck: block.Body.Attributes["check_option"] != nil,
		Comment:   p.optionalString(block.Body.Attributes["comment"]),
	})
	return nil
}

func (p *parser) parseMaterializedView(block *hclsyntax.Block) error {
	name, err := p.objectName(block, "materialized")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedMaterializedAttrs(block); err != nil {
		return err
	}
	body := p.optionalString(block.Body.Attributes["as"])
	if body == "" {
		return p.blockError(block, "materialized %q requires as", name)
	}
	p.db.MaterializedViews = append(p.db.MaterializedViews, goschema.MaterializedView{
		Name:    qualifyObjectName(p.optionalRefName(block.Body.Attributes["schema"]), name),
		Body:    body,
		Comment: p.optionalString(block.Body.Attributes["comment"]),
	})
	return nil
}

func (p *parser) parseTrigger(block *hclsyntax.Block) error {
	name, err := p.objectName(block, "trigger")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedTriggerAttrs(block); err != nil {
		return err
	}
	eventSpec, err := p.parseTriggerEvent(block)
	if err != nil {
		return err
	}
	table := objectRefName(p.optionalRawExpr(block.Body.Attributes["on"]), "table")
	if table == "" {
		return p.blockError(block, "trigger %q requires on", name)
	}
	body := p.optionalString(block.Body.Attributes["as"])
	if body == "" {
		return p.blockError(block, "trigger %q requires as", name)
	}
	p.db.Triggers = append(p.db.Triggers, goschema.Trigger{
		Name:    name,
		Table:   table,
		Timing:  eventSpec.timing,
		Event:   eventSpec.event,
		ForEach: firstNonEmpty(p.optionalString(block.Body.Attributes["for"]), p.optionalString(block.Body.Attributes["foreach"])),
		Body:    body,
		Comment: p.optionalString(block.Body.Attributes["comment"]),
	})
	return nil
}

type triggerEventSpec struct {
	timing string
	event  string
}

func (p *parser) parseTriggerEvent(block *hclsyntax.Block) (triggerEventSpec, error) {
	var timing string
	var event string
	for _, nested := range block.Body.Blocks {
		currentTiming := triggerTimingFromBlock(nested.Type)
		if currentTiming == "" {
			return triggerEventSpec{}, p.blockError(nested, "unsupported trigger block %q", nested.Type)
		}
		if timing != "" {
			return triggerEventSpec{}, p.blockError(nested, "trigger contains multiple timing blocks")
		}
		if err := p.rejectUnsupportedTriggerEventAttrs(nested); err != nil {
			return triggerEventSpec{}, err
		}
		currentEvent := triggerEventFromAttrs(nested)
		if currentEvent == "" {
			return triggerEventSpec{}, p.blockError(nested, "trigger timing block requires an event")
		}
		timing = currentTiming
		event = currentEvent
	}
	if timing == "" || event == "" {
		return triggerEventSpec{}, p.blockError(block, "trigger requires one timing block")
	}
	return triggerEventSpec{timing: timing, event: event}, nil
}

func (p *parser) parsePolicy(block *hclsyntax.Block) error {
	name, err := p.objectName(block, "policy")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedPolicyAttrs(block); err != nil {
		return err
	}
	roles, err := p.parseRoleTargets(block, "to")
	if err != nil {
		return err
	}
	table := objectRefName(p.optionalRawExpr(block.Body.Attributes["on"]), "table")
	if table == "" {
		return p.blockError(block, "policy %q requires on", name)
	}
	p.db.RLSPolicies = append(p.db.RLSPolicies, goschema.RLSPolicy{
		Name:                name,
		Table:               table,
		PolicyFor:           p.optionalString(block.Body.Attributes["for"]),
		ToRoles:             strings.Join(roles, ","),
		UsingExpression:     p.optionalString(block.Body.Attributes["using"]),
		WithCheckExpression: p.optionalString(block.Body.Attributes["check"]),
		Comment:             p.optionalString(block.Body.Attributes["comment"]),
	})
	return nil
}

func (p *parser) parseRowSecurity(table *goschema.Table, block *hclsyntax.Block) (goschema.RLSEnabledTable, error) {
	if len(block.Labels) != 0 {
		return goschema.RLSEnabledTable{}, p.blockError(block, "row_security block does not accept labels")
	}
	if err := p.rejectUnsupportedRowSecurityAttrs(block); err != nil {
		return goschema.RLSEnabledTable{}, err
	}
	enabled, err := p.boolAttr(block, "enabled", "row_security", false)
	if err != nil {
		return goschema.RLSEnabledTable{}, err
	}
	if !enabled {
		return goschema.RLSEnabledTable{}, p.blockError(block, "row_security requires enabled = true")
	}
	return goschema.RLSEnabledTable{
		StructName: table.StructName,
		Table:      table.QualifiedName(),
	}, nil
}

func (p *parser) parseRole(block *hclsyntax.Block) error {
	name, err := p.objectName(block, "role")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedRoleAttrs(block); err != nil {
		return err
	}
	attrs, err := p.parseRoleBoolAttrs(block)
	if err != nil {
		return err
	}
	p.db.Roles = append(p.db.Roles, goschema.Role{
		Name:        name,
		Login:       attrs.login,
		Superuser:   attrs.superuser,
		CreateDB:    attrs.createDB,
		CreateRole:  attrs.createRole,
		Inherit:     attrs.inherit,
		Replication: attrs.replication,
		Comment:     p.optionalString(block.Body.Attributes["comment"]),
	})
	return nil
}

type roleBoolAttrs struct {
	login       bool
	superuser   bool
	createDB    bool
	createRole  bool
	inherit     bool
	replication bool
}

func (p *parser) parseRoleBoolAttrs(block *hclsyntax.Block) (roleBoolAttrs, error) {
	login, err := p.boolAttr(block, "login", "role", false)
	if err != nil {
		return roleBoolAttrs{}, err
	}
	superuser, err := p.boolAttr(block, "superuser", "role", false)
	if err != nil {
		return roleBoolAttrs{}, err
	}
	createDB, err := p.boolAttr(block, "create_db", "role", false)
	if err != nil {
		return roleBoolAttrs{}, err
	}
	createRole, err := p.boolAttr(block, "create_role", "role", false)
	if err != nil {
		return roleBoolAttrs{}, err
	}
	inherit, err := p.boolAttr(block, "inherit", "role", true)
	if err != nil {
		return roleBoolAttrs{}, err
	}
	replication, err := p.boolAttr(block, "replication", "role", false)
	if err != nil {
		return roleBoolAttrs{}, err
	}
	return roleBoolAttrs{
		login:       login,
		superuser:   superuser,
		createDB:    createDB,
		createRole:  createRole,
		inherit:     inherit,
		replication: replication,
	}, nil
}

func (p *parser) parsePermission(block *hclsyntax.Block) error {
	if len(block.Labels) != 0 {
		return p.blockError(block, "permission block does not accept labels")
	}
	if err := p.rejectUnsupportedPermissionAttrs(block); err != nil {
		return err
	}
	privileges, err := p.rawListAttr(block, "privileges")
	if err != nil {
		return err
	}
	if len(privileges) == 0 {
		return p.blockError(block, "permission requires privileges")
	}
	target := p.optionalRawExpr(block.Body.Attributes["for"])
	grant := goschema.Grant{
		Role:       roleTargetName(p.optionalRawExpr(block.Body.Attributes["to"])),
		Privileges: privileges,
		Comment:    p.optionalString(block.Body.Attributes["comment"]),
	}
	grantable, err := p.boolAttr(block, "grantable", "permission", false)
	if err != nil {
		return err
	}
	grant.WithOption = grantable
	if table := objectRefName(target, "table"); table != "" {
		grant.OnTable = table
	} else if schema := objectRefName(target, "schema"); schema != "" {
		grant.OnSchema = schema
	} else {
		return p.blockError(block, "permission requires table or schema target")
	}
	if grant.Role == "" {
		return p.blockError(block, "permission requires to")
	}
	p.db.Grants = append(p.db.Grants, grant)
	return nil
}

func (p *parser) objectName(block *hclsyntax.Block, blockType string) (string, error) {
	switch len(block.Labels) {
	case 1:
		return block.Labels[0], nil
	case 2:
		return block.Labels[0] + "." + block.Labels[1], nil
	default:
		return "", p.blockError(block, "%s block requires one name label", blockType)
	}
}

func (p *parser) parseRoleTargets(block *hclsyntax.Block, attrName string) ([]string, error) {
	attr := block.Body.Attributes[attrName]
	if attr == nil {
		return nil, nil
	}
	exprs := []hclsyntax.Expression{attr.Expr}
	if tuple, ok := attr.Expr.(*hclsyntax.TupleConsExpr); ok {
		exprs = tuple.Exprs
	}
	values := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		values = append(values, roleTargetName(p.rawExprNode(expr)))
	}
	return values, nil
}

func (p *parser) rawListAttr(block *hclsyntax.Block, attrName string) ([]string, error) {
	attr := block.Body.Attributes[attrName]
	if attr == nil {
		return nil, nil
	}
	exprs := []hclsyntax.Expression{attr.Expr}
	if tuple, ok := attr.Expr.(*hclsyntax.TupleConsExpr); ok {
		exprs = tuple.Exprs
	}
	values := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		values = append(values, rawIdentifierOrString(p.rawExprNode(expr)))
	}
	return values, nil
}

func (p *parser) rejectUnsupportedExtensionAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported extension block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"version": true,
		"comment": true,
	}, "extension")
}

func (p *parser) rejectUnsupportedFunctionAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":     true,
		"lang":       true,
		"return":     true,
		"security":   true,
		"volatility": true,
		"as":         true,
		"comment":    true,
	}, "function")
}

func (p *parser) rejectUnsupportedFunctionArgAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported function arg block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"type": true,
	}, "function arg")
}

func (p *parser) rejectUnsupportedViewAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported view block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":       true,
		"as":           true,
		"check_option": true,
		"comment":      true,
	}, "view")
}

func (p *parser) rejectUnsupportedMaterializedAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported materialized block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":  true,
		"as":      true,
		"comment": true,
	}, "materialized")
}

func (p *parser) rejectUnsupportedTriggerAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"on":      true,
		"for":     true,
		"foreach": true,
		"as":      true,
		"comment": true,
	}, "trigger")
}

func (p *parser) rejectUnsupportedTriggerEventAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported trigger event block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"insert":   true,
		"update":   true,
		"delete":   true,
		"truncate": true,
	}, "trigger event")
}

func (p *parser) rejectUnsupportedPolicyAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported policy block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"on":      true,
		"for":     true,
		"to":      true,
		"using":   true,
		"check":   true,
		"comment": true,
	}, "policy")
}

func (p *parser) rejectUnsupportedRowSecurityAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported row_security block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"enabled": true,
	}, "row_security")
}

func (p *parser) rejectUnsupportedRoleAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported role block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"login":       true,
		"superuser":   true,
		"create_db":   true,
		"create_role": true,
		"inherit":     true,
		"replication": true,
		"comment":     true,
	}, "role")
}

func (p *parser) rejectUnsupportedPermissionAttrs(block *hclsyntax.Block) error {
	if len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "unsupported permission block %q", block.Body.Blocks[0].Type)
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"to":         true,
		"for":        true,
		"privileges": true,
		"grantable":  true,
		"comment":    true,
	}, "permission")
}

func triggerTimingFromBlock(value string) string {
	switch value {
	case "before":
		return "BEFORE"
	case "after":
		return "AFTER"
	case "instead_of":
		return "INSTEAD OF"
	default:
		return ""
	}
}

func triggerEventFromAttrs(block *hclsyntax.Block) string {
	for _, event := range []string{"insert", "update", "delete", "truncate"} {
		if attr := block.Body.Attributes[event]; attr != nil && attrBool(attr) {
			return strings.ToUpper(event)
		}
	}
	return ""
}

func attrBool(attr *hclsyntax.Attribute) bool {
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.Type() != cty.Bool {
		return false
	}
	return value.True()
}

func (p *parser) boolAttr(block *hclsyntax.Block, name, label string, fallback bool) (bool, error) {
	attr := block.Body.Attributes[name]
	if attr == nil {
		return fallback, nil
	}
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.Type() != cty.Bool {
		return false, p.blockError(block, "%s attribute %q must be a bool", label, name)
	}
	return value.True(), nil
}

func qualifyObjectName(schema, name string) string {
	if schema == "" || strings.Contains(name, ".") {
		return name
	}
	return schema + "." + name
}

func objectRefName(raw, kind string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	raw = rawIdentifierOrString(raw)
	prefix := kind + "."
	if name, ok := strings.CutPrefix(raw, prefix); ok {
		return name
	}
	if name, ok := bracketObjectRefName(raw, kind); ok {
		return name
	}
	if strings.Contains(raw, ".") || strings.HasPrefix(raw, kind+"[") {
		return ""
	}
	return raw
}

func roleTargetName(raw string) string {
	raw = rawIdentifierOrString(raw)
	if name, ok := strings.CutPrefix(raw, "role."); ok {
		return name
	}
	if name, ok := bracketObjectRefName(raw, "role"); ok {
		return name
	}
	return raw
}

func rawIdentifierOrString(raw string) string {
	raw = strings.TrimSpace(raw)
	if unquoted, err := strconv.Unquote(raw); err == nil {
		return unquoted
	}
	return raw
}

func firstNonEmpty(first, second string) string {
	if first != "" {
		return first
	}
	return second
}

func bracketObjectRefName(raw, kind string) (string, bool) {
	prefix := kind + "["
	if !strings.HasPrefix(raw, prefix) || !strings.HasSuffix(raw, "]") {
		return "", false
	}
	name := strings.TrimSpace(raw[len(prefix) : len(raw)-1])
	unquoted, err := strconv.Unquote(name)
	if err != nil {
		return "", false
	}
	return unquoted, true
}
