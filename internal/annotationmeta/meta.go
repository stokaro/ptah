// Package annotationmeta defines Ptah Go annotation directive metadata.
package annotationmeta

import (
	"slices"
	"sort"
	"strings"
)

// Scope describes where a directive is valid in Go source.
type Scope string

const (
	ScopeFile   Scope = "file"
	ScopeStruct Scope = "struct"
	ScopeField  Scope = "field"
)

// Attribute describes a single directive attribute.
type Attribute struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Value       string `json:"value"`
	Required    bool   `json:"required,omitempty"`
	Boolean     bool   `json:"boolean,omitempty"`
	AliasFor    string `json:"alias_for,omitempty"`
}

// Directive describes one //migrator annotation directive.
type Directive struct {
	Name          string      `json:"name"`
	Description   string      `json:"description"`
	Scopes        []Scope     `json:"scopes"`
	Attributes    []Attribute `json:"attributes"`
	AllowPlatform bool        `json:"allow_platform,omitempty"`
}

// Directives returns every supported annotation directive in stable order.
func Directives() []Directive {
	out := make([]Directive, len(directives))
	copy(out, directives)
	return out
}

// Lookup returns metadata for name, without a leading // comment marker.
func Lookup(name string) (Directive, bool) {
	name = strings.TrimPrefix(strings.TrimSpace(name), "//")
	i := slices.IndexFunc(directives, func(d Directive) bool {
		return d.Name == name
	})
	if i < 0 {
		return Directive{}, false
	}
	return directives[i], true
}

// MatchCommentDirective returns the directive matching a comment line.
func MatchCommentDirective(comment string) (Directive, bool) {
	body := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(comment), "//"))
	for _, directive := range directivesByDescendingLength() {
		if !strings.HasPrefix(body, directive.Name) {
			continue
		}
		rest := body[len(directive.Name):]
		if rest == "" || rest[0] == ' ' || rest[0] == '\t' {
			return directive, true
		}
	}
	return Directive{}, false
}

// KnownAttributes returns a set containing every declared attribute name.
func KnownAttributes(directive string) map[string]bool {
	spec, ok := Lookup(directive)
	if !ok {
		return nil
	}
	out := make(map[string]bool, len(spec.Attributes))
	for _, attr := range spec.Attributes {
		out[attr.Name] = true
	}
	return out
}

// RequiredAttributes returns every required attribute name for directive.
func RequiredAttributes(directive string) []string {
	spec, ok := Lookup(directive)
	if !ok {
		return nil
	}
	var out []string
	for _, attr := range spec.Attributes {
		if attr.Required {
			out = append(out, attr.Name)
		}
	}
	return out
}

// AllowsAttribute reports whether key is valid for directive.
func AllowsAttribute(directive, key string) bool {
	spec, ok := Lookup(directive)
	if !ok {
		return false
	}
	if spec.AllowPlatform && IsPlatformAttribute(key) {
		return true
	}
	return slices.ContainsFunc(spec.Attributes, func(attr Attribute) bool {
		return attr.Name == key
	})
}

// PlatformAttributePattern is the JSON Schema pattern for dialect-specific
// override attributes accepted by the Go annotation parser.
const PlatformAttributePattern = `^platform\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$`

// IsPlatformAttribute reports whether key has the platform.<dialect>.<name>
// shape consumed by parseutils.ParsePlatformSpecific.
func IsPlatformAttribute(key string) bool {
	parts := strings.Split(key, ".")
	if len(parts) < 3 || parts[0] != "platform" {
		return false
	}
	for _, part := range parts[1:] {
		if !isIdentifierPart(part) {
			return false
		}
	}
	return true
}

func isIdentifierPart(part string) bool {
	if part == "" {
		return false
	}
	for _, r := range part {
		if r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}

// AttributeNames returns sorted attribute names for directive.
func AttributeNames(directive string) []string {
	spec, ok := Lookup(directive)
	if !ok {
		return nil
	}
	names := make([]string, 0, len(spec.Attributes))
	for _, attr := range spec.Attributes {
		names = append(names, attr.Name)
	}
	sort.Strings(names)
	return names
}

// BooleanAttributes returns the attributes accepted as bare booleans.
func BooleanAttributes() map[string]bool {
	out := make(map[string]bool)
	for _, directive := range directives {
		for _, attr := range directive.Attributes {
			if attr.Boolean {
				out[attr.Name] = true
			}
		}
	}
	return out
}

// DirectiveTokens returns every directive path segment that must not be
// auto-promoted to a bare boolean attribute by annotation parsers.
func DirectiveTokens() map[string]bool {
	out := map[string]bool{
		"migrator": true,
		"schema":   true,
	}
	for _, directive := range directives {
		for part := range strings.SplitSeq(directive.Name, ":") {
			out[part] = true
		}
	}
	return out
}

// Markdown returns concise documentation for hover cards.
func Markdown(directive Directive) string {
	var b strings.Builder
	b.WriteString("`//")
	b.WriteString(directive.Name)
	b.WriteString("`\n\n")
	b.WriteString(directive.Description)
	if len(directive.Attributes) == 0 {
		return b.String()
	}
	b.WriteString("\n\nAttributes:\n")
	for _, attr := range directive.Attributes {
		b.WriteString("- `")
		b.WriteString(attr.Name)
		b.WriteString("`")
		if attr.Required {
			b.WriteString(" required")
		}
		if attr.AliasFor != "" {
			b.WriteString(" alias for `")
			b.WriteString(attr.AliasFor)
			b.WriteString("`")
		}
		if attr.Description != "" {
			b.WriteString(": ")
			b.WriteString(attr.Description)
		}
		b.WriteString("\n")
	}
	if directive.AllowPlatform {
		b.WriteString("- `platform.<dialect>.<key>`: dialect-specific override attributes.\n")
	}
	return strings.TrimSpace(b.String())
}

func directivesByDescendingLength() []Directive {
	out := Directives()
	sort.SliceStable(out, func(i, j int) bool {
		return len(out[i].Name) > len(out[j].Name)
	})
	return out
}

const (
	valueString  = "string"
	valueBoolean = "boolean"
	valueList    = "comma-list"
	valueSQL     = "sql"
)

var directives = []Directive{
	{
		Name:          "migrator:schema:field",
		Description:   "Maps a Go struct field to a database column.",
		Scopes:        []Scope{ScopeField},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("name", "Column name.", valueString, false, false),
			attr("type", "Database column type.", valueString, false, false),
			attr("not_null", "Marks the column NOT NULL.", valueBoolean, false, true),
			attr("nullable", "Bareword compatibility flag accepted by the parser.", valueBoolean, false, true),
			attr("primary", "Marks the column as part of the primary key.", valueBoolean, false, true),
			attr("auto_increment", "Marks the column as auto-incrementing.", valueBoolean, false, true),
			alias("autoincrement", "auto_increment", "Legacy auto-increment spelling.", valueBoolean, true),
			attr("identity_generation", "SQL identity generation mode.", valueString, false, false),
			attr("identity_start", "SQL identity start value.", valueString, false, false),
			attr("identity_increment", "SQL identity increment value.", valueString, false, false),
			attr("identity_options", "Raw SQL identity options.", valueSQL, false, false),
			attr("unique", "Adds a single-column unique constraint.", valueBoolean, false, true),
			attr("unique_expr", "Unique expression for dialects that support expression indexes.", valueSQL, false, false),
			attr("index", "Bareword compatibility flag accepted by the parser.", valueBoolean, false, true),
			attr("generated", "Generated column expression.", valueSQL, false, false),
			attr("generated_kind", "Generated column kind, such as STORED or VIRTUAL.", valueString, false, false),
			attr("stored", "Shortcut controlling generated column storage.", valueBoolean, false, false),
			attr("default", "Literal column default.", valueString, false, false),
			attr("default_expr", "SQL default expression.", valueSQL, false, false),
			attr("foreign", "Foreign key reference in table(column) form.", valueString, false, false),
			attr("foreign_key_name", "Explicit foreign key constraint name.", valueString, false, false),
			attr("on_delete", "Foreign key ON DELETE action.", valueString, false, false),
			attr("on_update", "Foreign key ON UPDATE action.", valueString, false, false),
			attr("enum", "Comma-separated enum values.", valueList, false, false),
			attr("check", "Column CHECK expression.", valueSQL, false, false),
			attr("check_name", "Explicit CHECK constraint name.", valueString, false, false),
			attr("comment", "Column comment.", valueString, false, false),
		},
	},
	{
		Name:          "migrator:embedded",
		Description:   "Controls how an embedded Go field contributes schema objects.",
		Scopes:        []Scope{ScopeField},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("mode", "Embedding mode: inline, json, or relation.", valueString, false, false),
			attr("prefix", "Column prefix for inline embedded fields.", valueString, false, false),
			attr("name", "Column name for json embedding.", valueString, false, false),
			attr("type", "Column type for json embedding.", valueString, false, false),
			attr("nullable", "Marks generated embedded columns nullable.", valueBoolean, false, true),
			attr("not_null", "Compatibility flag accepted on embedded annotations.", valueBoolean, false, true),
			attr("index", "Requests an index for generated relation columns.", valueBoolean, false, true),
			attr("field", "Generated relation field name.", valueString, false, false),
			attr("ref", "Relation target in table(column) form.", valueString, false, false),
			attr("on_delete", "Generated foreign key ON DELETE action.", valueString, false, false),
			attr("on_update", "Generated foreign key ON UPDATE action.", valueString, false, false),
			attr("comment", "Generated column comment.", valueString, false, false),
		},
	},
	{
		Name:          "migrator:schema:index",
		Description:   "Declares an index for a table.",
		Scopes:        []Scope{ScopeStruct, ScopeField},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("name", "Index name.", valueString, false, false),
			attr("fields", "Comma-separated Go field or column names.", valueList, false, false),
			alias("columns", "fields", "Legacy synonym for fields.", valueList, false),
			attr("unique", "Creates a unique index.", valueBoolean, false, true),
			attr("comment", "Index comment.", valueString, false, false),
			attr("type", "Index type or method.", valueString, false, false),
			attr("condition", "Partial index condition.", valueSQL, false, false),
			alias("where", "condition", "Atlas-style partial index condition alias.", valueSQL, false),
			attr("ops", "PostgreSQL operator class.", valueString, false, false),
			attr("table", "Explicit target table.", valueString, false, false),
			attr("granularity", "ClickHouse data-skipping index granularity.", valueString, false, false),
			attr("nulls_distinct", "Controls NULLS DISTINCT behavior where supported.", valueBoolean, false, false),
		},
	},
	{
		Name:          "migrator:schema:table",
		Description:   "Maps a Go struct to a database table.",
		Scopes:        []Scope{ScopeStruct},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("name", "Table name.", valueString, false, false),
			attr("schema", "Database schema name.", valueString, false, false),
			attr("engine", "MySQL/MariaDB table engine shortcut.", valueString, false, false),
			attr("comment", "Table comment.", valueString, false, false),
			attr("primary_key", "Comma-separated primary key columns.", valueList, false, false),
			attr("checks", "Comma-separated table-level check expressions.", valueList, false, false),
			attr("custom", "Raw custom CREATE TABLE SQL.", valueSQL, false, false),
		},
	},
	{
		Name:          "migrator:schema:schema",
		Description:   "Declares a database schema or namespace.",
		Scopes:        []Scope{ScopeStruct},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("name", "Schema name.", valueString, true, false),
			attr("comment", "Schema comment.", valueString, false, false),
		},
	},
	{
		Name:        "migrator:schema:constraint",
		Description: "Declares a table constraint.",
		Scopes:      []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr("name", "Constraint name.", valueString, false, false),
			attr("type", "Constraint type: CHECK, UNIQUE, PRIMARY KEY, FOREIGN KEY, or EXCLUDE.", valueString, false, false),
			attr("table", "Explicit target table.", valueString, false, false),
			attr("using", "EXCLUDE index method.", valueString, false, false),
			attr("elements", "EXCLUDE constraint elements.", valueSQL, false, false),
			attr("condition", "Constraint WHERE condition.", valueSQL, false, false),
			attr("check", "CHECK expression.", valueSQL, false, false),
			attr("columns", "Comma-separated local columns.", valueList, false, false),
			attr("include", "Comma-separated PostgreSQL INCLUDE columns for covering UNIQUE constraints.", valueList, false, false),
			attr("nulls_distinct", "Controls NULLS DISTINCT behavior where supported.", valueBoolean, false, false),
			attr("foreign_table", "Referenced table for FOREIGN KEY constraints.", valueString, false, false),
			attr("foreign_column", "Single referenced column for FOREIGN KEY constraints.", valueString, false, false),
			attr("foreign_columns", "Comma-separated referenced columns for composite FOREIGN KEY constraints.", valueList, false, false),
			attr("on_delete", "Foreign key ON DELETE action.", valueString, false, false),
			attr("on_update", "Foreign key ON UPDATE action.", valueString, false, false),
			attr("comment", "Constraint comment.", valueString, false, false),
		},
	},
	{
		Name:        "migrator:schema:enum",
		Description: "Declares a reusable enum type.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Enum type name.", valueString, true, false),
			attr("values", "Comma-separated enum values.", valueList, true, false),
		},
	},
	{
		Name:        "migrator:schema:extension",
		Description: "Declares a PostgreSQL extension.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Extension name.", valueString, false, false),
			attr("if_not_exists", "Adds IF NOT EXISTS where supported.", valueBoolean, false, false),
			attr("version", "Extension version.", valueString, false, false),
			attr("comment", "Extension comment.", valueString, false, false),
		},
	},
	{
		Name:        "migrator:schema:function",
		Description: "Declares a database function.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Function name.", valueString, false, false),
			attr("params", "Function parameter list.", valueString, false, false),
			attr("returns", "Return type.", valueString, false, false),
			attr("language", "Function language.", valueString, false, false),
			attr("security", "Security mode, such as DEFINER.", valueString, false, false),
			attr("volatility", "Volatility class.", valueString, false, false),
			attr("body", "Function body SQL.", valueSQL, false, false),
			attr("comment", "Function comment.", valueString, false, false),
		},
	},
	{
		Name:        "migrator:schema:sequence",
		Description: "Declares a standalone PostgreSQL sequence.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Sequence name.", valueString, true, false),
			attr("schema", "Target schema/namespace.", valueString, false, false),
			attr("as", "Underlying integer type, such as bigint.", valueString, false, false),
			attr("start", "START WITH value.", valueString, false, false),
			attr("increment", "INCREMENT BY value; must be non-zero.", valueString, false, false),
			attr("minvalue", "MINVALUE bound.", valueString, false, false),
			attr("maxvalue", "MAXVALUE bound.", valueString, false, false),
			attr("cache", "CACHE size.", valueString, false, false),
			attr("cycle", "Enables CYCLE wrap-around.", valueBoolean, false, false),
			attr("owned_by", "Owning table.column association (OWNED BY).", valueString, false, false),
			attr("if_not_exists", "Adds IF NOT EXISTS where supported.", valueBoolean, false, false),
			attr("comment", "Sequence comment.", valueString, false, false),
		},
	},
	{
		Name:          "migrator:schema:view",
		Description:   "Declares a database view.",
		Scopes:        []Scope{ScopeStruct},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("name", "View name.", valueString, true, false),
			attr("body", "View SELECT body.", valueSQL, true, false),
			attr("with_check", "Controls WITH CHECK OPTION where supported.", valueBoolean, false, false),
			attr("comment", "View comment.", valueString, false, false),
		},
	},
	{
		Name:          "migrator:schema:matview",
		Description:   "Declares a materialized view.",
		Scopes:        []Scope{ScopeStruct},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("name", "Materialized view name.", valueString, true, false),
			attr("body", "Materialized view SELECT body.", valueSQL, true, false),
			attr("refresh_strategy", "Refresh strategy; defaults to manual.", valueString, false, false),
			attr("comment", "Materialized view comment.", valueString, false, false),
		},
	},
	{
		Name:          "migrator:schema:trigger",
		Description:   "Declares a database trigger.",
		Scopes:        []Scope{ScopeStruct},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("name", "Trigger name.", valueString, true, false),
			attr("table", "Target table.", valueString, true, false),
			attr("timing", "Trigger timing, such as BEFORE or AFTER.", valueString, true, false),
			attr("event", "Trigger event, such as INSERT or UPDATE.", valueString, true, false),
			attr("for", "Trigger granularity; defaults to ROW.", valueString, false, false),
			attr("body", "Trigger body SQL.", valueSQL, true, false),
			attr("comment", "Trigger comment.", valueString, false, false),
		},
	},
	{
		Name:        "migrator:schema:rls:policy",
		Description: "Declares a row-level security policy.",
		Scopes:      []Scope{ScopeFile, ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Policy name.", valueString, false, false),
			attr("table", "Target table.", valueString, false, false),
			attr("for", "Policy command, such as ALL or SELECT.", valueString, false, false),
			attr("to", "Comma-separated roles.", valueList, false, false),
			attr("using", "USING expression.", valueSQL, false, false),
			attr("with_check", "WITH CHECK expression.", valueSQL, false, false),
			attr("comment", "Policy comment.", valueString, false, false),
		},
	},
	{
		Name:        "migrator:schema:rls:enable",
		Description: "Enables row-level security on a table.",
		Scopes:      []Scope{ScopeFile, ScopeStruct},
		Attributes: []Attribute{
			attr("table", "Target table.", valueString, false, false),
			attr("comment", "RLS enablement comment.", valueString, false, false),
		},
	},
	{
		Name:        "migrator:schema:role",
		Description: "Declares a database role.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Role name.", valueString, false, false),
			attr("login", "Creates the role with LOGIN.", valueBoolean, false, false),
			attr("password", "Role password.", valueString, false, false),
			attr("superuser", "Creates the role as SUPERUSER.", valueBoolean, false, false),
			attr("createdb", "Allows database creation.", valueBoolean, false, false),
			alias("create_db", "createdb", "Alias for createdb.", valueBoolean, false),
			attr("createrole", "Allows role creation.", valueBoolean, false, false),
			alias("create_role", "createrole", "Alias for createrole.", valueBoolean, false),
			attr("inherit", "Controls role inheritance; defaults to true.", valueBoolean, false, false),
			attr("replication", "Allows replication.", valueBoolean, false, false),
			attr("comment", "Role comment.", valueString, false, false),
		},
	},
	{
		Name:        "migrator:schema:grant",
		Description: "Declares database grants.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("role", "Target role.", valueString, false, false),
			attr("privilege", "Privilege or comma-separated privileges.", valueList, false, false),
			alias("privileges", "privilege", "Alias for privilege.", valueList, false),
			attr("on_table", "Target table.", valueString, false, false),
			attr("on_schema", "Target schema.", valueString, false, false),
			attr("on_sequence", "Target sequence.", valueString, false, false),
			attr("with_option", "Adds WITH GRANT OPTION where supported.", valueBoolean, false, false),
			alias("grant_option", "with_option", "Alias for with_option.", valueBoolean, false),
			attr("comment", "Grant comment.", valueString, false, false),
		},
	},
}

func attr(name, description, value string, required, boolean bool) Attribute {
	return Attribute{
		Name:        name,
		Description: description,
		Value:       value,
		Required:    required,
		Boolean:     boolean,
	}
}

func alias(name, aliasFor, description, value string, boolean bool) Attribute {
	a := attr(name, description, value, false, boolean)
	a.AliasFor = aliasFor
	return a
}
