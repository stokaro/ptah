package atlashcl

import (
	"math/big"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/tableref"
)

func (p *parser) parseExtension(block *hclsyntax.Block) error {
	name, err := p.objectName(block, "extension")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedExtensionAttrs(block); err != nil {
		return err
	}
	ifNotExists, err := p.boolAttr(block, "if_not_exists", "extension", false)
	if err != nil {
		return err
	}
	p.db.Extensions = append(p.db.Extensions, goschema.Extension{
		Name:        name,
		IfNotExists: ifNotExists,
		Version:     p.optionalString(block.Body.Attributes["version"]),
		Comment:     p.optionalString(block.Body.Attributes["comment"]),
	})
	return nil
}

func (p *parser) parseFunction(block *hclsyntax.Block) error {
	schema, name, err := p.objectSchemaAndName(block, "function")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedFunctionAttrs(block); err != nil {
		return err
	}
	parameters, err := p.stringAttr(block, "params", "function")
	if err != nil {
		return err
	}
	if block.Body.Attributes["params"] != nil && len(block.Body.Blocks) > 0 {
		return p.blockError(block.Body.Blocks[0], "function cannot mix params attribute with arg blocks")
	}
	if block.Body.Attributes["params"] == nil {
		args, err := p.parseFunctionArgs(block)
		if err != nil {
			return err
		}
		parameters = strings.Join(args, ", ")
	}
	body := p.optionalString(block.Body.Attributes["as"])
	if body == "" {
		return p.blockError(block, "function %q requires as", name)
	}
	function := goschema.Function{
		Name:       tableref.Canonical(schema, name),
		Parameters: parameters,
		Returns:    p.optionalString(block.Body.Attributes["return"]),
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
			if err := p.rejectUnsupportedBlock(nested, "function"); err != nil {
				return nil, err
			}
			continue
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
		// optionalRawExpr, not rawExpr: an argument type is written as a bare
		// keyword (`type = bigint`) so it has no string value to evaluate, but
		// it can also be written with Atlas's sql() escape hatch, and the raw
		// source of that call is not a type -- issue #1106.
		args = append(args, nested.Labels[0]+" "+p.optionalRawExpr(typeAttr))
	}
	return args, nil
}

func (p *parser) parseView(block *hclsyntax.Block) error {
	schema, name, err := p.objectSchemaAndName(block, "view")
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
		Name:      tableref.Canonical(schema, name),
		Body:      body,
		WithCheck: block.Body.Attributes["check_option"] != nil,
		Comment:   p.optionalString(block.Body.Attributes["comment"]),
	})
	return nil
}

func (p *parser) parseMaterializedView(block *hclsyntax.Block) error {
	schema, name, err := p.objectSchemaAndName(block, "materialized")
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
	view := goschema.MaterializedView{
		Name:            tableref.Canonical(schema, name),
		Body:            body,
		RefreshStrategy: p.optionalString(block.Body.Attributes["refresh_strategy"]),
		Comment:         p.optionalString(block.Body.Attributes["comment"]),
	}
	// Canonicalize lowercases refresh_strategy and defaults it to "manual",
	// mirroring the Go-annotation path (parseMatViewComment). The Go path applies
	// no further validation, so neither does this one: any strategy string is
	// accepted, keeping both frontends' MaterializedView identical for the same
	// schema.
	view.Canonicalize()
	p.db.MaterializedViews = append(p.db.MaterializedViews, view)
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
			if err := p.rejectUnsupportedBlock(nested, "trigger"); err != nil {
				return triggerEventSpec{}, err
			}
			continue
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
		Comment:    p.optionalString(block.Body.Attributes["comment"]),
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
	password, err := p.stringAttr(block, "password", "role")
	if err != nil {
		return err
	}
	p.db.Roles = append(p.db.Roles, goschema.Role{
		Name:        name,
		Login:       attrs.login,
		Password:    password,
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
	} else if sequence := objectRefName(target, "sequence"); sequence != "" {
		grant.OnSequence = sequence
	} else {
		return p.blockError(block, "permission requires table, schema, or sequence target")
	}
	if grant.Role == "" {
		return p.blockError(block, "permission requires to")
	}
	p.db.Grants = append(p.db.Grants, grant)
	return nil
}

func (p *parser) parseManagedData(block *hclsyntax.Block) error {
	if len(block.Labels) != 0 {
		return p.labeledDataBlockError(block)
	}
	if err := p.rejectNestedBlocks(block, "data"); err != nil {
		return err
	}
	if err := p.rejectUnsupportedAttrs(block, map[string]bool{
		"table": true,
		"keys":  true,
		"file":  true,
	}, "data"); err != nil {
		return err
	}
	return p.parseManagedDataBody(block)
}

// labeledDataBlockError explains a labeled data block. In a schema file, data
// declares managed seed rows and takes no labels; the labeled data sources
// users paste here by accident are atlas.hcl project-config constructs, so the
// known Atlas labels get an error that points at the right file.
func (p *parser) labeledDataBlockError(block *hclsyntax.Block) error {
	switch label := block.Labels[0]; label {
	case "external_schema":
		return p.blockError(block,
			"data %q is an atlas.hcl project-config data source, not a schema declaration; declare it in atlas.hcl, where Ptah supports it",
			label)
	case "composite_schema", "remote_dir", "hcl_schema":
		return p.blockError(block,
			"data %q is an atlas.hcl project-config data source, not a schema declaration",
			label)
	default:
		return p.blockError(block, "data block does not accept labels")
	}
}

func (p *parser) parseManagedDataBody(block *hclsyntax.Block) error {
	tableName, ok := traversalObjectRefName(p.optionalRawExpr(block.Body.Attributes["table"]), "table")
	if !ok {
		return p.blockError(block, "data requires table reference")
	}
	tableRef, ok := tableref.Parse(tableName)
	if !ok {
		return p.blockError(block, "data table reference is invalid")
	}
	keys, err := p.stringListAttr(block, "keys")
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return p.blockError(block, "data requires keys")
	}
	file, err := p.stringAttr(block, "file", "data")
	if err != nil {
		return err
	}
	if file == "" {
		return p.blockError(block, "data requires file")
	}
	p.db.ManagedData = append(p.db.ManagedData, goschema.ManagedData{
		Table:     tableRef.Name,
		Schema:    tableRef.Schema,
		Keys:      keys,
		File:      file,
		SourceDir: p.sourceDir,
	})
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
	if err := p.rejectNestedBlocks(block, "extension"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"if_not_exists": true,
		"version":       true,
		"comment":       true,
	}, "extension")
}

func (p *parser) rejectUnsupportedFunctionAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":     true,
		"params":     true,
		"lang":       true,
		"return":     true,
		"security":   true,
		"volatility": true,
		"as":         true,
		"comment":    true,
	}, "function")
}

func (p *parser) rejectUnsupportedFunctionArgAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "function arg"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"type": true,
	}, "function arg")
}

func (p *parser) rejectUnsupportedViewAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "view"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":       true,
		"as":           true,
		"check_option": true,
		"comment":      true,
	}, "view")
}

func (p *parser) rejectUnsupportedMaterializedAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "materialized"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":           true,
		"as":               true,
		"refresh_strategy": true,
		"comment":          true,
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
	if err := p.rejectNestedBlocks(block, "trigger event"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"insert":   true,
		"update":   true,
		"delete":   true,
		"truncate": true,
	}, "trigger event")
}

func (p *parser) rejectUnsupportedPolicyAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "policy"); err != nil {
		return err
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
	if err := p.rejectNestedBlocks(block, "row_security"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"enabled": true,
		"comment": true,
	}, "row_security")
}

func (p *parser) rejectUnsupportedRoleAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "role"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"login":       true,
		"password":    true,
		"superuser":   true,
		"create_db":   true,
		"create_role": true,
		"inherit":     true,
		"replication": true,
		"comment":     true,
	}, "role")
}

func (p *parser) rejectUnsupportedPermissionAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "permission"); err != nil {
		return err
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

func objectRefName(raw, kind string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if unquoted, err := strconv.Unquote(raw); err == nil {
		return unquoted
	}
	if name, ok := traversalObjectRefName(raw, kind); ok {
		return name
	}
	if strings.Contains(raw, ".") || strings.HasPrefix(raw, kind+"[") {
		return ""
	}
	return raw
}

func traversalObjectRefName(raw, kind string) (string, bool) {
	expr, diags := hclsyntax.ParseExpression([]byte(raw), "object-reference.hcl", hcl.InitialPos)
	if diags.HasErrors() {
		return "", false
	}
	traversal, diags := hcl.AbsTraversalForExpr(expr)
	if diags.HasErrors() || len(traversal) < 2 || len(traversal) > 3 {
		return "", false
	}
	root, ok := traversal[0].(hcl.TraverseRoot)
	if !ok || root.Name != kind {
		return "", false
	}
	parts := make([]string, 0, len(traversal)-1)
	for _, step := range traversal[1:] {
		part, ok := traversalPart(step)
		if !ok {
			return "", false
		}
		parts = append(parts, part)
	}
	if len(parts) == 1 {
		return tableref.Canonical("", parts[0]), true
	}
	return tableref.Canonical(parts[0], parts[1]), true
}

func traversalPart(step hcl.Traverser) (string, bool) {
	switch step := step.(type) {
	case hcl.TraverseAttr:
		return step.Name, true
	case hcl.TraverseIndex:
		if !step.Key.IsKnown() || step.Key.IsNull() || step.Key.Type() != cty.String {
			return "", false
		}
		return step.Key.AsString(), true
	default:
		return "", false
	}
}

func roleTargetName(raw string) string {
	if name, ok := traversalObjectRefName(raw, "role"); ok {
		if ref, parsed := tableref.Parse(name); parsed && !ref.Qualified {
			return ref.Name
		}
	}
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
	expr, diags := hclsyntax.ParseExpression([]byte(raw), "literal.hcl", hcl.InitialPos)
	if !diags.HasErrors() {
		value, valueDiags := expr.Value(nil)
		if !valueDiags.HasErrors() && value.IsKnown() && !value.IsNull() && value.Type() == cty.String {
			return value.AsString()
		}
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

// parseSequence parses a top-level sequence block into a goschema.Sequence.
//
// The name and optional schema come from the block labels (one label = name,
// two labels = schema and name) or from a schema attribute. The integer bounds
// (start, increment, min_value, max_value, cache) are optional integer
// attributes; cycle and if_not_exists are optional booleans.
func (p *parser) parseSequence(block *hclsyntax.Block) error {
	schema, name, err := p.objectSchemaAndName(block, "sequence")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedSequenceAttrs(block); err != nil {
		return err
	}
	start, err := p.optionalInt64(block, "start", "sequence")
	if err != nil {
		return err
	}
	increment, err := p.optionalInt64(block, "increment", "sequence")
	if err != nil {
		return err
	}
	minValue, err := p.optionalInt64(block, "min_value", "sequence")
	if err != nil {
		return err
	}
	maxValue, err := p.optionalInt64(block, "max_value", "sequence")
	if err != nil {
		return err
	}
	cache, err := p.optionalInt64(block, "cache", "sequence")
	if err != nil {
		return err
	}
	cycle, err := p.boolAttr(block, "cycle", "sequence", false)
	if err != nil {
		return err
	}
	ifNotExists, err := p.boolAttr(block, "if_not_exists", "sequence", false)
	if err != nil {
		return err
	}
	seq := goschema.Sequence{
		Name:        name,
		Schema:      schema,
		AsType:      p.optionalString(block.Body.Attributes["type"]),
		Start:       start,
		Increment:   increment,
		MinValue:    minValue,
		MaxValue:    maxValue,
		Cache:       cache,
		Cycle:       cycle,
		OwnedBy:     p.optionalString(block.Body.Attributes["owned_by"]),
		IfNotExists: ifNotExists,
		Comment:     p.optionalString(block.Body.Attributes["comment"]),
	}
	// Canonicalize before validating so recognized integer-type aliases (int8,
	// int4, ...) map to their canonical spelling, matching the Go-annotation
	// path and keeping the AS clause from churning against introspection.
	seq.Canonicalize()
	if !goschema.IsValidSequenceType(seq.AsType) {
		return p.blockError(block, "sequence %q type %q is invalid; expected smallint, integer, or bigint", name, seq.AsType)
	}
	p.db.Sequences = append(p.db.Sequences, seq)
	return nil
}

func (p *parser) rejectUnsupportedSequenceAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "sequence"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":        true,
		"type":          true,
		"start":         true,
		"increment":     true,
		"min_value":     true,
		"max_value":     true,
		"cache":         true,
		"cycle":         true,
		"owned_by":      true,
		"if_not_exists": true,
		"comment":       true,
	}, "sequence")
}

// parseDomain parses a top-level domain block into a goschema.Domain.
//
// The base type is required. Nullability follows the column convention: null
// defaults to true, so NOT NULL is expressed as null = false. A default may be
// a literal or an sql("...") expression, and check carries the domain's CHECK
// expression (which references VALUE).
func (p *parser) parseDomain(block *hclsyntax.Block) error {
	schema, name, err := p.objectSchemaAndName(block, "domain")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedDomainAttrs(block); err != nil {
		return err
	}
	baseType := p.optionalString(block.Body.Attributes["type"])
	if baseType == "" {
		return p.blockError(block, "domain %q requires type", name)
	}
	nullable, err := p.boolAttr(block, "null", "domain", true)
	if err != nil {
		return err
	}
	domain := goschema.Domain{
		Name:     name,
		Schema:   schema,
		BaseType: baseType,
		NotNull:  !nullable,
		Check:    p.optionalSQLExpression(block.Body.Attributes["check"]),
		Comment:  p.optionalString(block.Body.Attributes["comment"]),
	}
	if defAttr := block.Body.Attributes["default"]; defAttr != nil {
		if value, ok := p.sqlExpression(defAttr); ok {
			domain.DefaultExpr = value
		} else {
			domain.Default = p.exprString(defAttr)
		}
	}
	domain.Canonicalize()
	p.db.Domains = append(p.db.Domains, domain)
	return nil
}

func (p *parser) rejectUnsupportedDomainAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "domain"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":  true,
		"type":    true,
		"null":    true,
		"default": true,
		"check":   true,
		"comment": true,
	}, "domain")
}

// parseComposite parses a top-level composite block into a
// goschema.CompositeType. The ordered fields come from nested field blocks,
// each carrying a name label and a type attribute.
func (p *parser) parseComposite(block *hclsyntax.Block) error {
	schema, name, err := p.objectSchemaAndName(block, "composite")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedCompositeAttrs(block); err != nil {
		return err
	}
	fields, err := p.parseCompositeFields(block)
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		return p.blockError(block, "composite %q requires at least one field", name)
	}
	composite := goschema.CompositeType{
		Name:    name,
		Schema:  schema,
		Fields:  fields,
		Comment: p.optionalString(block.Body.Attributes["comment"]),
	}
	composite.Canonicalize()
	p.db.CompositeTypes = append(p.db.CompositeTypes, composite)
	return nil
}

func (p *parser) parseCompositeFields(block *hclsyntax.Block) ([]goschema.CompositeTypeField, error) {
	var fields []goschema.CompositeTypeField
	for _, nested := range block.Body.Blocks {
		if nested.Type != "field" {
			if err := p.rejectUnsupportedBlock(nested, "composite"); err != nil {
				return nil, err
			}
			continue
		}
		if len(nested.Labels) != 1 {
			return nil, p.blockError(nested, "composite field requires exactly one name label")
		}
		if err := p.rejectNestedBlocks(nested, "composite field"); err != nil {
			return nil, err
		}
		if err := p.rejectUnsupportedAttrs(nested, map[string]bool{"type": true}, "composite field"); err != nil {
			return nil, err
		}
		typeAttr := nested.Body.Attributes["type"]
		if typeAttr == nil {
			return nil, p.blockError(nested, "composite field %q requires type", nested.Labels[0])
		}
		// exprString (not rawExpr) so a quoted string literal is unquoted:
		// multi-word types like "double precision" have no bare HCL spelling,
		// so they must be written as a string, while bare references (text) and
		// function-call shapes (numeric(10,2)) fall back to the raw source.
		fields = append(fields, goschema.CompositeTypeField{
			Name: nested.Labels[0],
			Type: p.exprString(typeAttr),
		})
	}
	return fields, nil
}

func (p *parser) rejectUnsupportedCompositeAttrs(block *hclsyntax.Block) error {
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":  true,
		"comment": true,
	}, "composite")
}

// parseRange parses a top-level range block into a goschema.Range. The subtype
// is required; the remaining attributes are optional PostgreSQL range options.
func (p *parser) parseRange(block *hclsyntax.Block) error {
	schema, name, err := p.objectSchemaAndName(block, "range")
	if err != nil {
		return err
	}
	if err := p.rejectUnsupportedRangeAttrs(block); err != nil {
		return err
	}
	subtype := p.optionalString(block.Body.Attributes["subtype"])
	if subtype == "" {
		return p.blockError(block, "range %q requires subtype", name)
	}
	rng := goschema.Range{
		Name:           name,
		Schema:         schema,
		Subtype:        subtype,
		SubtypeOpClass: p.optionalString(block.Body.Attributes["subtype_opclass"]),
		Collation:      p.optionalString(block.Body.Attributes["collation"]),
		Canonical:      p.optionalString(block.Body.Attributes["canonical"]),
		SubtypeDiff:    p.optionalString(block.Body.Attributes["subtype_diff"]),
		Comment:        p.optionalString(block.Body.Attributes["comment"]),
	}
	rng.Canonicalize()
	p.db.Ranges = append(p.db.Ranges, rng)
	return nil
}

func (p *parser) rejectUnsupportedRangeAttrs(block *hclsyntax.Block) error {
	if err := p.rejectNestedBlocks(block, "range"); err != nil {
		return err
	}
	return p.rejectUnsupportedAttrs(block, map[string]bool{
		"schema":          true,
		"subtype":         true,
		"subtype_opclass": true,
		"collation":       true,
		"canonical":       true,
		"subtype_diff":    true,
		"comment":         true,
	}, "range")
}

// objectSchemaAndName resolves an object's bare name and optional schema from
// the block labels and an optional schema attribute. One label is the name; two
// labels are schema and name. A schema attribute that disagrees with a
// two-label schema is a conflict. Unlike qualifyObjectName (which folds the
// schema into the name), this keeps them separate for objects whose IR carries
// a dedicated Schema field.
func (p *parser) objectSchemaAndName(block *hclsyntax.Block, blockType string) (schema, name string, err error) {
	schemaAttr := p.optionalRefName(block.Body.Attributes["schema"])
	switch len(block.Labels) {
	case 1:
		return schemaAttr, block.Labels[0], nil
	case 2:
		if schemaAttr != "" && schemaAttr != block.Labels[0] {
			return "", "", p.blockError(
				block,
				"%s %q schema label conflicts with schema attribute %q",
				blockType,
				block.Labels[1],
				schemaAttr,
			)
		}
		return block.Labels[0], block.Labels[1], nil
	default:
		return "", "", p.blockError(block, "%s block requires one or two name labels", blockType)
	}
}

// optionalInt64 reads an optional integer attribute, returning nil when absent.
// A non-integer value (including a fractional number) is an error.
func (p *parser) optionalInt64(block *hclsyntax.Block, name, label string) (*int64, error) {
	attr := block.Body.Attributes[name]
	if attr == nil {
		return nil, nil
	}
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() || value.Type() != cty.Number {
		return nil, p.blockError(block, "%s attribute %q must be an integer", label, name)
	}
	// Int64 reports big.Exact only when the value is a whole number that fits in
	// an int64; a fractional or out-of-range value would otherwise be silently
	// truncated or clamped, diverging from the Go-annotation path's ParseInt.
	f := value.AsBigFloat()
	result, accuracy := f.Int64()
	if !f.IsInt() || accuracy != big.Exact {
		return nil, p.blockError(block, "%s attribute %q must be an integer within the int64 range", label, name)
	}
	return &result, nil
}
