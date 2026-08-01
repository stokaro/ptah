package atlashclrender

import (
	"cmp"
	"slices"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/tableref"
)

func (r *renderer) renderExtensions() {
	extensions := append([]goschema.Extension(nil), r.db.Extensions...)
	slices.SortFunc(extensions, func(a, b goschema.Extension) int {
		return cmp.Compare(a.Name, b.Name)
	})
	for _, extension := range extensions {
		r.linef(`extension %s {`, quote(extension.Name))
		if extension.IfNotExists {
			r.trueAttr(1, "if_not_exists")
		}
		r.stringAttr(1, "version", extension.Version)
		r.stringAttr(1, "comment", extension.Comment)
		r.line("}")
		r.line("")
	}
}

func (r *renderer) renderSequences() {
	sequences := append([]goschema.Sequence(nil), r.db.Sequences...)
	slices.SortFunc(sequences, func(a, b goschema.Sequence) int {
		return cmp.Compare(a.QualifiedName(), b.QualifiedName())
	})
	for _, sequence := range sequences {
		sequence.Canonicalize()
		r.linef(`sequence %s {`, quote(sequence.Name))
		if sequence.Schema != "" {
			r.rawAttr(1, "schema", schemaRef(sequence.Schema))
		}
		if sequence.AsType != "" {
			r.rawAttr(1, "type", typeExpr(sequence.AsType))
		}
		r.int64PtrAttr(1, "start", sequence.Start)
		r.int64PtrAttr(1, "increment", sequence.Increment)
		r.int64PtrAttr(1, "min_value", sequence.MinValue)
		r.int64PtrAttr(1, "max_value", sequence.MaxValue)
		r.int64PtrAttr(1, "cache", sequence.Cache)
		if sequence.Cycle {
			r.trueAttr(1, "cycle")
		}
		r.stringAttr(1, "owned_by", sequence.OwnedBy)
		if sequence.IfNotExists {
			r.trueAttr(1, "if_not_exists")
		}
		r.stringAttr(1, "comment", sequence.Comment)
		r.line("}")
		r.line("")
	}
}

func (r *renderer) renderUserTypes() {
	domains := append([]goschema.Domain(nil), r.db.Domains...)
	slices.SortFunc(domains, func(a, b goschema.Domain) int {
		return cmp.Compare(a.QualifiedName(), b.QualifiedName())
	})
	for _, domain := range domains {
		r.renderDomain(domain)
	}

	composites := append([]goschema.CompositeType(nil), r.db.CompositeTypes...)
	slices.SortFunc(composites, func(a, b goschema.CompositeType) int {
		return cmp.Compare(a.QualifiedName(), b.QualifiedName())
	})
	for _, composite := range composites {
		r.renderComposite(composite)
	}

	ranges := append([]goschema.Range(nil), r.db.Ranges...)
	slices.SortFunc(ranges, func(a, b goschema.Range) int {
		return cmp.Compare(a.QualifiedName(), b.QualifiedName())
	})
	for _, rangeType := range ranges {
		r.renderRange(rangeType)
	}
}

func (r *renderer) renderDomain(domain goschema.Domain) {
	domain.Canonicalize()
	r.linef(`domain %s {`, quote(domain.Name))
	if domain.Schema != "" {
		r.rawAttr(1, "schema", schemaRef(domain.Schema))
	}
	r.rawAttr(1, "type", typeExpr(domain.BaseType))
	if domain.NotNull {
		r.rawAttr(1, "null", "false")
	}
	if domain.DefaultExpr != "" {
		r.rawAttr(1, "default", sqlCall(domain.DefaultExpr))
	} else {
		r.stringAttr(1, "default", domain.Default)
	}
	if domain.Check != "" {
		r.rawAttr(1, "check", sqlCall(domain.Check))
	}
	r.stringAttr(1, "comment", domain.Comment)
	r.line("}")
	r.line("")
}

func (r *renderer) renderComposite(composite goschema.CompositeType) {
	composite.Canonicalize()
	r.linef(`composite %s {`, quote(composite.Name))
	if composite.Schema != "" {
		r.rawAttr(1, "schema", schemaRef(composite.Schema))
	}
	r.stringAttr(1, "comment", composite.Comment)
	for _, field := range composite.Fields {
		r.linef(`  field %s {`, quote(field.Name))
		r.rawAttr(2, "type", typeExpr(field.Type))
		r.line("  }")
	}
	r.line("}")
	r.line("")
}

func (r *renderer) renderRange(rangeType goschema.Range) {
	rangeType.Canonicalize()
	r.linef(`range %s {`, quote(rangeType.Name))
	if rangeType.Schema != "" {
		r.rawAttr(1, "schema", schemaRef(rangeType.Schema))
	}
	r.rawAttr(1, "subtype", typeExpr(rangeType.Subtype))
	r.stringAttr(1, "subtype_opclass", rangeType.SubtypeOpClass)
	r.stringAttr(1, "collation", rangeType.Collation)
	r.stringAttr(1, "canonical", rangeType.Canonical)
	r.stringAttr(1, "subtype_diff", rangeType.SubtypeDiff)
	r.stringAttr(1, "comment", rangeType.Comment)
	r.line("}")
	r.line("")
}

func (r *renderer) renderManagedData() {
	managedData := append([]goschema.ManagedData(nil), r.db.ManagedData...)
	slices.SortFunc(managedData, func(a, b goschema.ManagedData) int {
		return cmp.Or(
			cmp.Compare(managedDataTable(a), managedDataTable(b)),
			cmp.Compare(strings.Join(a.Keys, ","), strings.Join(b.Keys, ",")),
			cmp.Compare(a.File, b.File),
		)
	})
	for _, data := range managedData {
		r.line("data {")
		r.rawAttr(1, "table", objectRef("table", managedDataTable(data)))
		r.rawAttr(1, "keys", stringList(data.Keys))
		r.stringAttr(1, "file", data.File)
		r.line("}")
		r.line("")
	}
}

func managedDataTable(data goschema.ManagedData) string {
	if data.Schema == "" {
		return data.Table
	}
	if ref, ok := tableref.Parse(data.Table); ok && ref.Qualified {
		return data.Table
	}
	return tableref.Canonical(data.Schema, data.Table)
}

func (r *renderer) renderRoles() {
	roles := append([]goschema.Role(nil), r.db.Roles...)
	slices.SortFunc(roles, func(a, b goschema.Role) int {
		return cmp.Compare(a.Name, b.Name)
	})
	for _, role := range roles {
		r.linef(`role %s {`, quote(role.Name))
		if role.Login {
			r.trueAttr(1, "login")
		}
		if role.Superuser {
			r.trueAttr(1, "superuser")
		}
		if role.CreateDB {
			r.trueAttr(1, "create_db")
		}
		if role.CreateRole {
			r.trueAttr(1, "create_role")
		}
		if role.Inherit {
			r.trueAttr(1, "inherit")
		} else {
			r.rawAttr(1, "inherit", "false")
		}
		if role.Replication {
			r.trueAttr(1, "replication")
		}
		r.stringAttr(1, "password", role.Password)
		r.stringAttr(1, "comment", role.Comment)
		r.line("}")
		r.line("")
	}
}

func (r *renderer) renderRowSecurity(rlsEnabled *goschema.RLSEnabledTable) {
	if rlsEnabled == nil {
		return
	}
	r.line("  row_security {")
	r.rawAttr(2, "enabled", "true")
	r.stringAttr(2, "comment", rlsEnabled.Comment)
	r.line("  }")
}

func (r *renderer) renderFunctions() {
	functions := append([]goschema.Function(nil), r.db.Functions...)
	slices.SortFunc(functions, func(a, b goschema.Function) int {
		return cmp.Compare(a.Name, b.Name)
	})
	for _, function := range functions {
		r.renderFunction(function)
	}
}

func (r *renderer) renderFunction(function goschema.Function) {
	function.Canonicalize()
	if function.Body == "" {
		r.warn("functions."+function.Name, "function body is required for HCL schema export")
		return
	}
	name := objectNameFromQualified(function.Name)
	r.linef(`function %s {`, quote(name))
	if schema := schemaNameFromQualified(function.Name); schema != "" {
		r.rawAttr(1, "schema", schemaRef(schema))
	}
	r.rawAttr(1, "lang", atlasLanguage(function.Language))
	if function.Returns != "" {
		r.rawAttr(1, "return", typeExpr(function.Returns))
	}
	r.renderFunctionArgs(function)
	r.rawAttr(1, "security", rawIdentifier(function.Security))
	r.rawAttr(1, "volatility", rawIdentifier(function.Volatility))
	r.stringAttr(1, "as", function.Body)
	r.stringAttr(1, "comment", function.Comment)
	r.line("}")
	r.line("")
}

func (r *renderer) renderFunctionArgs(function goschema.Function) {
	args, ok := splitFunctionArgs(function.Parameters)
	if !ok {
		r.stringAttr(1, "params", function.Parameters)
		return
	}
	for _, arg := range args {
		r.linef(`  arg %s {`, quote(arg.name))
		r.rawAttr(2, "type", typeExpr(arg.typ))
		r.line("  }")
	}
}

func (r *renderer) renderViews() {
	views := append([]goschema.View(nil), r.db.Views...)
	slices.SortFunc(views, func(a, b goschema.View) int {
		return cmp.Compare(a.Name, b.Name)
	})
	for _, view := range views {
		if view.Body == "" {
			r.warn("views."+view.Name, "view body is required for HCL schema export")
			continue
		}
		name := objectNameFromQualified(view.Name)
		r.linef(`view %s {`, quote(name))
		if schema := schemaNameFromQualified(view.Name); schema != "" {
			r.rawAttr(1, "schema", schemaRef(schema))
		}
		r.stringAttr(1, "as", view.Body)
		if view.WithCheck {
			r.rawAttr(1, "check_option", "LOCAL")
		}
		r.stringAttr(1, "comment", view.Comment)
		r.line("}")
		r.line("")
	}
}

func (r *renderer) renderMaterializedViews() {
	views := append([]goschema.MaterializedView(nil), r.db.MaterializedViews...)
	slices.SortFunc(views, func(a, b goschema.MaterializedView) int {
		return cmp.Compare(a.Name, b.Name)
	})
	for _, view := range views {
		r.renderMaterializedView(view)
	}
}

func (r *renderer) renderMaterializedView(view goschema.MaterializedView) {
	view.Canonicalize()
	if view.Body == "" {
		r.warn("materialized_views."+view.Name, "materialized view body is required for HCL schema export")
		return
	}
	name := objectNameFromQualified(view.Name)
	r.linef(`materialized %s {`, quote(name))
	if schema := schemaNameFromQualified(view.Name); schema != "" {
		r.rawAttr(1, "schema", schemaRef(schema))
	}
	r.stringAttr(1, "as", view.Body)
	// Emit refresh_strategy only when it differs from the canonical default so
	// output stays minimal; the Ptah HCL parser defaults an absent attribute back
	// to "manual", keeping this render/parse pair symmetric.
	if view.RefreshStrategy != "" && view.RefreshStrategy != "manual" {
		r.stringAttr(1, "refresh_strategy", view.RefreshStrategy)
	}
	r.stringAttr(1, "comment", view.Comment)
	r.line("}")
	r.line("")
}

func (r *renderer) renderTriggers() {
	triggers := append([]goschema.Trigger(nil), r.db.Triggers...)
	slices.SortFunc(triggers, func(a, b goschema.Trigger) int {
		return cmp.Or(cmp.Compare(a.Table, b.Table), cmp.Compare(a.Name, b.Name))
	})
	for _, trigger := range triggers {
		r.renderTrigger(trigger)
	}
}

func (r *renderer) renderTrigger(trigger goschema.Trigger) {
	trigger.Canonicalize()
	path := TriggerDiagnosticPath(trigger.Table, trigger.Name)
	if trigger.Table == "" || trigger.Body == "" {
		r.warn(path, "trigger requires table and body for HCL schema export")
		return
	}
	timing, ok := triggerTimingBlock(trigger.Timing)
	if !ok {
		r.warn(path, "trigger timing cannot be represented in HCL schema output")
		return
	}
	event, ok := triggerEventAttr(trigger.Event)
	if !ok {
		r.warn(path, "trigger event cannot be represented in HCL schema output")
		return
	}
	r.linef(`trigger %s {`, quote(trigger.Name))
	r.rawAttr(1, "on", objectRef("table", trigger.Table))
	r.linef("  %s {", timing)
	r.rawAttr(2, event, "true")
	r.line("  }")
	r.rawAttr(1, "for", rawIdentifier(firstNonEmpty(trigger.ForEach, "ROW")))
	r.stringAttr(1, "as", trigger.Body)
	r.stringAttr(1, "comment", trigger.Comment)
	r.line("}")
	r.line("")
}

func (r *renderer) renderRLSPolicies() {
	policies := append([]goschema.RLSPolicy(nil), r.db.RLSPolicies...)
	slices.SortFunc(policies, func(a, b goschema.RLSPolicy) int {
		return cmp.Or(cmp.Compare(a.Table, b.Table), cmp.Compare(a.Name, b.Name))
	})
	for _, policy := range policies {
		if policy.Table == "" {
			r.warn("rls_policies."+policy.Name, "RLS policy requires a target table for HCL schema export")
			continue
		}
		r.linef(`policy %s {`, quote(policy.Name))
		r.rawAttr(1, "on", objectRef("table", policy.Table))
		r.rawAttr(1, "for", rawIdentifier(firstNonEmpty(strings.ToUpper(policy.PolicyFor), "ALL")))
		if policy.ToRoles != "" {
			r.rawAttr(1, "to", roleTargets(policy.ToRoles))
		}
		r.stringAttr(1, "using", policy.UsingExpression)
		r.stringAttr(1, "check", policy.WithCheckExpression)
		r.stringAttr(1, "comment", policy.Comment)
		r.line("}")
		r.line("")
	}
}

func (r *renderer) renderGrants() {
	grants := append([]goschema.Grant(nil), r.db.Grants...)
	slices.SortFunc(grants, func(a, b goschema.Grant) int {
		return cmp.Or(
			cmp.Compare(a.Role, b.Role),
			cmp.Compare(grantTarget(a), grantTarget(b)),
			cmp.Compare(strings.Join(a.Privileges, ","), strings.Join(b.Privileges, ",")),
		)
	})
	for _, grant := range grants {
		grant.Canonicalize()
		target := grantTarget(grant)
		if grant.Role == "" || target == "" || len(grant.Privileges) == 0 {
			r.warn("grants."+grant.Role, "grant requires role, table, schema, or sequence target, and at least one privilege")
			continue
		}
		r.line("permission {")
		r.rawAttr(1, "to", roleTarget(grant.Role))
		r.rawAttr(1, "for", target)
		r.rawAttr(1, "privileges", rawList(grant.Privileges))
		if grant.WithOption {
			r.trueAttr(1, "grantable")
		}
		r.stringAttr(1, "comment", grant.Comment)
		if grant.GrantedBy != "" {
			r.warn("grants."+grant.Role, "grantor metadata cannot be represented in HCL schema permission blocks")
		}
		r.line("}")
		r.line("")
	}
}

func groupRLSEnabledByTable(
	values []goschema.RLSEnabledTable,
	tables []goschema.Table,
) (map[string]*goschema.RLSEnabledTable, []goschema.RLSEnabledTable) {
	result := make(map[string]*goschema.RLSEnabledTable)
	var orphan []goschema.RLSEnabledTable
	for i := range values {
		rlsEnabled := &values[i]
		table := resolveTable(tables, rlsEnabled.StructName, rlsEnabled.Table)
		if table == nil {
			orphan = append(orphan, *rlsEnabled)
			continue
		}
		result[table.QualifiedName()] = rlsEnabled
	}
	return result, orphan
}

type functionArg struct {
	name string
	typ  string
}

func splitFunctionArgs(value string) ([]functionArg, bool) {
	original := value
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, true
	}
	if value != original {
		return nil, false
	}
	parts, ok := splitTopLevelComma(value)
	if !ok {
		return nil, false
	}
	args := make([]functionArg, 0, len(parts))
	rendered := make([]string, 0, len(parts))
	for _, part := range parts {
		arg, ok := parseFunctionArg(part)
		if !ok || typeExpr(arg.typ) != arg.typ {
			return nil, false
		}
		args = append(args, arg)
		rendered = append(rendered, arg.name+" "+arg.typ)
	}
	if strings.Join(rendered, ", ") != value {
		return nil, false
	}
	return args, true
}

func parseFunctionArg(value string) (functionArg, bool) {
	fields := strings.Fields(strings.TrimSpace(value))
	if len(fields) < 2 {
		return functionArg{}, false
	}
	if strings.ContainsAny(value, "=") {
		return functionArg{}, false
	}
	if containsFold(fields, "DEFAULT") {
		return functionArg{}, false
	}
	mode := strings.ToUpper(fields[0])
	if mode != "" && mode != "IN" && slices.Contains([]string{"OUT", "INOUT", "VARIADIC"}, mode) {
		return functionArg{}, false
	}
	if mode == "IN" {
		fields = fields[1:]
	}
	if len(fields) < 2 {
		return functionArg{}, false
	}
	return functionArg{name: strings.Trim(fields[0], `"`), typ: strings.Join(fields[1:], " ")}, true
}

func splitTopLevelComma(value string) ([]string, bool) {
	var parts []string
	start := 0
	depth := 0
	var quote rune
	for pos, r := range value {
		if quote != 0 {
			if r == quote {
				quote = 0
			}
			continue
		}
		switch r {
		case '\'', '"':
			quote = r
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, false
			}
		case ',':
			if depth == 0 {
				parts = append(parts, value[start:pos])
				start = pos + len(string(r))
			}
		}
	}
	if quote != 0 || depth != 0 {
		return nil, false
	}
	parts = append(parts, value[start:])
	return parts, true
}

func triggerTimingBlock(timing string) (string, bool) {
	switch strings.ToUpper(timing) {
	case "AFTER":
		return "after", true
	case "INSTEAD OF":
		return "instead_of", true
	case "BEFORE":
		return "before", true
	}
	return "", false
}

func triggerEventAttr(event string) (string, bool) {
	switch strings.ToUpper(event) {
	case "INSERT", "UPDATE", "DELETE", "TRUNCATE":
		return strings.ToLower(event), true
	}
	return "", false
}

func atlasLanguage(language string) string {
	switch strings.ToLower(language) {
	case "sql":
		return "SQL"
	case "plpgsql":
		return "PLpgSQL"
	default:
		return quote(language)
	}
}

func roleTargets(value string) string {
	roles, ok := splitTopLevelComma(value)
	if !ok {
		return stringList([]string{value})
	}
	targets := make([]string, 0, len(roles))
	for _, role := range roles {
		targets = append(targets, roleTarget(role))
	}
	return "[" + strings.Join(targets, ", ") + "]"
}

func roleTarget(value string) string {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "PUBLIC") {
		return "PUBLIC"
	}
	if value == "" {
		return quote(value)
	}
	return "role" + objectRefPart(value)
}

func grantTarget(grant goschema.Grant) string {
	if grant.OnSchema != "" {
		return schemaRef(grant.OnSchema)
	}
	if grant.OnTable != "" {
		return objectRef("table", grant.OnTable)
	}
	if grant.OnSequence != "" {
		return objectRef("sequence", grant.OnSequence)
	}
	return ""
}

func objectRef(kind, name string) string {
	if name == "" {
		return quote("")
	}
	ref, ok := tableref.Parse(name)
	if !ok {
		return kind + objectRefPart(name)
	}
	parts := []string{ref.Name}
	if ref.Qualified {
		parts = []string{ref.Schema, ref.Name}
	}
	refParts := make([]string, 0, len(parts))
	for _, part := range parts {
		refParts = append(refParts, objectRefPart(part))
	}
	return kind + strings.Join(refParts, "")
}

func rawList(values []string) string {
	items := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		items = append(items, rawIdentifier(value))
	}
	return "[" + strings.Join(items, ", ") + "]"
}

func rawIdentifier(value string) string {
	if isHCLIdentifier(value) {
		return value
	}
	return quote(value)
}

func objectNameFromQualified(value string) string {
	ref, ok := tableref.Parse(value)
	if !ok {
		return value
	}
	return ref.Name
}

func schemaNameFromQualified(value string) string {
	ref, ok := tableref.Parse(value)
	if !ok || !ref.Qualified {
		return ""
	}
	return ref.Schema
}

func (r *renderer) trueAttr(indent int, name string) {
	r.rawAttr(indent, name, "true")
}

func (r *renderer) int64PtrAttr(indent int, name string, value *int64) {
	if value == nil {
		return
	}
	r.rawAttr(indent, name, strconv.FormatInt(*value, 10))
}

func objectRefPart(value string) string {
	if isHCLIdentifier(value) {
		return "." + value
	}
	return "[" + quote(value) + "]"
}

func isHCLIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' {
			continue
		}
		if i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}
