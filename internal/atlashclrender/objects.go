package atlashclrender

import (
	"cmp"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/tableref"
)

func (r *renderer) renderExtensions() {
	extensions := append([]goschema.Extension(nil), r.db.Extensions...)
	slices.SortFunc(extensions, func(a, b goschema.Extension) int {
		return cmp.Compare(a.Name, b.Name)
	})
	for _, extension := range extensions {
		// The extension's own name AND everything it supplies: a document that
		// depends on `isn` says `isbn`, never `isn`. Provides is empty for
		// sources with no catalog behind them, and the check then degenerates to
		// the label, which is the most that can be known about such a source.
		keepAlive := append([]string{extension.Name}, extension.Provides...)
		if r.omitRefusedBlock("extensions."+extension.Name, blockExtension, keepAlive...) {
			continue
		}
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
		if r.omitRefusedBlock("sequences."+sequence.QualifiedName(), blockSequence, sequence.Name) {
			continue
		}
		sequence.Canonicalize()
		r.linef(`sequence %s {`, quote(sequence.Name))
		if sequence.Schema != "" {
			r.rawAttr(1, "schema", schemaRef(sequence.Schema))
		}
		// A quoted string, not the bare word `bigint`. Bare, it is an HCL
		// variable reference with nothing behind it and the pinned Atlas
		// community binary v1.3.0 refuses the whole file with `There is no
		// variable named "bigint"`. Quoted, the file evaluates and that binary
		// gets as far as its own feature gap -- `postgres: sequences are not
		// supported by this version` -- which is the signal that nothing in this
		// block is unreadable any more (stokaro/ptah#1251).
		//
		// The gap means that binary never writes a sequence block, so it cannot
		// say which readable spelling it would prefer; `sql("bigint")` reaches
		// the same message. The quoted string is chosen because it is what Ptah's
		// own parser reads back to the same AsType, and because the value is
		// always one of smallint/integer/bigint, never SQL needing an escape.
		r.stringAttr(1, "type", sequence.AsType)
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
	r.rawAttr(1, "type", userTypeExpr(domain.BaseType))
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
		r.rawAttr(2, "type", userTypeExpr(field.Type))
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
	r.rawAttr(1, "subtype", userTypeExpr(rangeType.Subtype))
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
		r.rawAttr(1, "table", r.tableRef(managedDataTable(data)))
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
	// Every one of these four is a quoted string rather than a bare word.
	// Measured on the pinned Atlas community binary v1.3.0, one attribute varied
	// at a time against a function block it accepts:
	//
	//	lang = PLpgSQL         exit 1  There is no variable named "PLpgSQL"
	//	lang = SQL             exit 1  There is no variable named "SQL"
	//	lang = "PLpgSQL"       exit 0
	//	return = trigger       exit 1  There is no variable named "trigger"
	//	return = bigint        exit 1  There is no variable named "bigint"
	//	return = "trigger"     exit 0
	//	security = INVOKER     exit 1  There is no variable named "INVOKER"
	//	security = "INVOKER"   exit 0
	//	volatility = VOLATILE  exit 1  There is no variable named "VOLATILE"
	//	volatility = "VOLATILE" exit 0
	//
	// `return` needed its own rows because it looks like a type position and is
	// not: a bare `bigint` evaluates as a column's `type` and fails here, so
	// typeExpr was the wrong renderer for it whatever the return type is.
	//
	// That binary accepts the block and emits nothing for it -- its output for a
	// file with the function is byte-identical to the same file without it, and
	// `lang = "BANANA"` is accepted too -- so it validates none of these values
	// and the only thing to match is that the file evaluates at all.
	r.rawAttr(1, "lang", atlasLanguage(function.Language))
	r.stringAttr(1, "return", function.Returns)
	r.renderFunctionArgs(function)
	r.stringAttr(1, "security", function.Security)
	r.stringAttr(1, "volatility", function.Volatility)
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
	r.rawAttr(1, "on", r.tableRef(trigger.Table))
	r.linef("  %s {", timing)
	r.rawAttr(2, event, "true")
	r.line("  }")
	// Quoted, for the same reason as everywhere else in this file. Measured on
	// the pinned Atlas community binary v1.3.0 against a trigger block it
	// accepts, one operand varied:
	//
	//	for = ROW          exit 1  There is no variable named "ROW"
	//	for = "ROW"        exit 0
	//	for = "STATEMENT"  exit 0
	//	(attribute absent) exit 0
	//
	// The block is accepted and dropped, not modeled: that binary's output for a
	// file containing it is byte-identical to the same file without it, and
	// `for = "BANANA"` is accepted just as readily. So there is no spelling of
	// its own to match, only the requirement that the file evaluate.
	r.stringAttr(1, "for", firstNonEmpty(trigger.ForEach, "ROW"))
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
		if r.omitRefusedBlock("rls_policies."+policy.Name, blockPolicy, policy.Name) {
			continue
		}
		r.linef(`policy %s {`, quote(policy.Name))
		r.rawAttr(1, "on", r.tableRef(policy.Table))
		// Quoted. Measured on the pinned Atlas community binary v1.3.0, one
		// operand varied against an otherwise identical policy block:
		//
		//	for = ALL      exit 1  There is no variable named "ALL"
		//	for = "ALL"    exit 1  postgres: policies are not supported by this version
		//	for = "SELECT" exit 1  postgres: policies are not supported by this version
		//
		// Like the sequence, the block never gets past that binary's own feature
		// gap, so the reachable target is the message changing from a parse
		// failure over Ptah's rendering to a statement about what that binary
		// models. The quoted string is what Ptah's parser reads back to the same
		// PolicyFor.
		r.stringAttr(1, "for", firstNonEmpty(strings.ToUpper(policy.PolicyFor), "ALL"))
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
			cmp.Compare(r.grantTarget(a), r.grantTarget(b)),
			cmp.Compare(strings.Join(a.Privileges, ","), strings.Join(b.Privileges, ",")),
		)
	})
	for _, grant := range grants {
		grant.Canonicalize()
		target := r.grantTarget(grant)
		if grant.Role == "" || target == "" || len(grant.Privileges) == 0 {
			r.warn("grants."+grant.Role, "grant requires role, table, schema, or sequence target, and at least one privilege")
			continue
		}
		r.line("permission {")
		r.rawAttr(1, "to", roleTarget(grant.Role))
		r.rawAttr(1, "for", target)
		r.rawAttr(1, "privileges", privilegeList(grant.Privileges))
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

// atlasLanguage renders a function's `lang` attribute: the canonical spelling
// of the language name, quoted. See renderFunction for the measurement that
// says the bare form is refused for every value, canonical or not.
func atlasLanguage(language string) string {
	switch strings.ToLower(language) {
	case "sql":
		return quote("SQL")
	case "plpgsql":
		return quote("PLpgSQL")
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

// roleTarget renders one grantee.
//
// PUBLIC is written as a quoted string rather than the bare word it is in SQL.
// Bare, it is an HCL variable reference with nothing to resolve it to, and the
// pinned Atlas community binary v1.3.0 refuses the whole file with
// `There is no variable named "PUBLIC"` -- it drops a block whose name it does
// not model, but only after the body evaluates (stokaro/ptah#1234).
//
// A named role stays a `role.<name>` traversal, which is measured to evaluate
// on that binary when the file also declares the matching `role` block, so
// quoting it would lose a reference for nothing. Ptah's own parser reads either
// spelling through rawIdentifierOrString, so the round trip is unaffected.
func roleTarget(value string) string {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "PUBLIC") {
		return quote("PUBLIC")
	}
	if value == "" {
		return quote(value)
	}
	return "role" + objectRefPart(value)
}

func (r *renderer) grantTarget(grant goschema.Grant) string {
	if grant.OnSchema != "" {
		return schemaRef(grant.OnSchema)
	}
	if grant.OnTable != "" {
		return r.tableRef(grant.OnTable)
	}
	if grant.OnSequence != "" {
		return objectRef("sequence", grant.OnSequence)
	}
	return ""
}

// tableRef renders a reference to a table block.
//
// A reference in HCL names a BLOCK, and a block is named by its labels. Ptah
// writes a table block with one label, so `table.<schema>.<name>` does not read
// as "the table <name> in schema <schema>" -- it reads as "the <schema>
// attribute of the table object", which is exactly what the pinned Atlas
// community binary v1.3.0 says. Measured on PostgreSQL 17, realm-scope dev URL,
// with `users` in `other` and `posts` in `public`, one operand varied:
//
//	ref_columns = [table.users.column.id]        exit 0
//	ref_columns = [table.other.users.column.id]  exit 1  Unsupported attribute;
//	                                                     This object does not have
//	                                                     an attribute named "other"
//	on = table.users        (trigger)            exit 0
//	on = table.other.users  (trigger)            exit 1  same message
//	for = table.users       (permission)         exit 0
//	for = table.other.users (permission)         exit 1  same message
//
// It is not a cross-schema special case: with the target in the SAME schema as
// the referring table, `table.public.users` is refused the same way. The short
// form is that binary's OWN spelling -- its inspect of the cross-schema foreign
// key above emits `ref_columns = [table.users.column.id]`.
//
// The schema is dropped only when [renderer.documentResolvesTableRef] says this
// document resolves the short form back to the same table, because that is the
// only case where nothing is lost. A reference to a table the document does not
// render -- a filtered export, an orphan trigger -- keeps the qualified form:
// there is no block to read the schema off on the way back, so the short form
// would destroy it irrecoverably, and a document missing the referenced table is
// not readable by that binary under either spelling (stokaro/ptah#1260).
// Everything the short form is not written for -- an empty name, a name that
// does not parse, one carrying no schema, one this document cannot resolve --
// falls through to [objectRef], which is what Ptah wrote for every reference
// before this rule existed.
func (r *renderer) tableRef(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok || !ref.Qualified || !r.documentResolvesTableRef(ref.Schema, ref.Name) {
		return objectRef("table", name)
	}
	return "table" + objectRefPart(ref.Name)
}

// objectRef renders a reference to a block, with whatever schema the name
// carries.
//
// Only [renderer.tableRef] shortens a reference, and only table positions call
// it, so a sequence target keeps `sequence.<schema>.<name>`. That is a
// structural guarantee rather than a policy: a permission naming a sequence
// keeps that sequence block in the document, and the pinned binary refuses any
// PostgreSQL file declaring one with `postgres: sequences are not supported by
// this version` before any reference is resolved, so nothing measured says
// which spelling it would take there.
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

// privilegeList renders a permission block's privileges.
//
// Each privilege is quoted for the same reason PUBLIC is: `privileges = [USAGE]`
// is a list containing an unresolvable variable reference, and the pinned
// community binary v1.3.0 refuses the file over it. Measured on that binary,
// with everything else held fixed:
//
//	to = PUBLIC    privileges = [USAGE]      refused
//	to = "PUBLIC"  privileges = [USAGE]      refused
//	to = PUBLIC    privileges = ["USAGE"]    refused
//	to = "PUBLIC"  privileges = ["USAGE"]    accepted
//
// so both attributes had to move, and each row above is what says so.
//
// Empty entries are dropped rather than emitted as "", which would round trip
// as a privilege named nothing.
func privilegeList(values []string) string {
	items := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		items = append(items, quote(value))
	}
	return "[" + strings.Join(items, ", ") + "]"
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
