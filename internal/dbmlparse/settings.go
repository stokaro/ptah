package dbmlparse

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
)

// setting is one entry of a bracketed list: a flag with no value, or a key and
// a value.
type setting struct {
	key string
	// value is the value as written, and kind says which spelling it had, so a
	// literal default and an expression default stay apart.
	value string
	kind  tokenKind
	// hasValue separates `[unique]` from `[name: "x"]`.
	hasValue bool
}

// settings reads a `[ ... ]` list.
func (p *parser) settings() ([]setting, error) {
	if err := p.expectPunct("["); err != nil {
		return nil, err
	}
	out := make([]setting, 0, 4)
	for !p.isPunct("]") {
		if p.tok.kind == tokenEOF {
			return nil, p.errorf("unterminated settings list")
		}
		entry, err := p.setting()
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
		if p.isPunct(",") {
			if err := p.advance(); err != nil {
				return nil, err
			}
		}
	}
	return out, p.advance()
}

// setting reads one entry.
func (p *parser) setting() (setting, error) {
	key, err := p.settingKey()
	if err != nil {
		return setting{}, err
	}
	if !p.isPunct(":") {
		return setting{key: key}, nil
	}
	if err := p.advance(); err != nil {
		return setting{}, err
	}
	if p.tok.kind == tokenEOF {
		return setting{}, p.errorf("setting %q has no value", key)
	}
	// A quoted, numeric or backticked value is one token. A bare one may be
	// several words -- `no action` and `set null` are single referential
	// actions -- so words are joined until the entry ends. Reading only the
	// first turned `update: no action` into the value `no` and a second setting
	// called `action`.
	if p.tok.kind != tokenWord {
		entry := setting{key: key, value: p.tok.text, kind: p.tok.kind, hasValue: true}
		return entry, p.advance()
	}
	parts := make([]string, 0, 2)
	kind := p.tok.kind
	for p.tok.kind == tokenWord {
		parts = append(parts, p.tok.text)
		if err := p.advance(); err != nil {
			return setting{}, err
		}
	}
	return setting{key: key, value: strings.Join(parts, " "), kind: kind, hasValue: true}, nil
}

// settingKey reads a key, which may be several words -- `not null` is one.
func (p *parser) settingKey() (string, error) {
	parts := make([]string, 0, 2)
	for p.tok.kind == tokenWord {
		parts = append(parts, strings.ToLower(p.tok.text))
		if err := p.advance(); err != nil {
			return "", err
		}
		if p.isPunct(":") || p.isPunct(",") || p.isPunct("]") {
			break
		}
	}
	if len(parts) == 0 {
		return "", p.errorf("expected a setting, found %s", p.tok.describe())
	}
	return strings.Join(parts, " "), nil
}

// applyColumnSettings maps a column's bracketed list onto the field.
//
// An unknown setting is refused rather than ignored. A document that says
// something Ptah does not implement is a document whose schema Ptah would apply
// differently than it reads, and silently dropping the difference is how a
// declared property disappears from a database (stokaro/ptah#2065 asks for
// exactly this: an unsupported property fails rather than being removed).
func applyColumnSettings(field *goschema.Field, settings []setting, schema string) error {
	for _, entry := range settings {
		switch entry.key {
		case "pk", "primary key":
			// Primary only. `pk` says the column is the key and says nothing
			// about NULL, and inferring NOT NULL from it made DBML and SQL
			// describe different schemas for one intent: measured, the same
			// primary key rendered as `INTEGER PRIMARY KEY` from a .sql
			// document and `INTEGER NOT NULL PRIMARY KEY` from this one, so a
			// database built from either planned a rebuild against the other.
			//
			// Engines that imply NOT NULL from PRIMARY KEY still do; what
			// changes is only what Ptah's model claims the document said
			// (stokaro/ptah#2065).
			field.Primary = true
		case "increment":
			field.AutoInc = true
		case "unique":
			field.Unique = true
		case "not null":
			field.Nullable = false
		case "null":
			field.Nullable = true
		case "default":
			applyDefault(field, entry)
		case "note":
			field.Comment = entry.value
		case "ref":
			if err := applyInlineRef(field, entry, schema); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported column setting %q", entry.key)
		}
	}
	return nil
}

// applyDefault keeps a literal default and an expression default apart.
//
// DBML writes an expression in backticks and a literal in quotes, and the
// difference decides what the column does: 'now()' is a six-character string
// and `now()` is a call.
func applyDefault(field *goschema.Field, entry setting) {
	if entry.kind == tokenExpr {
		field.DefaultExpr = entry.value
		return
	}
	field.Default = entry.value
	field.DefaultSet = true
}

// applyInlineRef reads the `ref: > table.column` form a column carries.
func applyInlineRef(field *goschema.Field, entry setting, schema string) error {
	target := strings.TrimSpace(entry.value)
	target = strings.TrimPrefix(target, ">")
	target = strings.TrimSpace(target)
	if target == "" {
		return fmt.Errorf("inline ref on column %q names no target", field.Name)
	}
	table, column, ok := splitTargetColumn(target)
	if !ok {
		return fmt.Errorf("inline ref on column %q is not table.column", field.Name)
	}
	if !strings.Contains(table, ".") && schema != "" {
		table = schema + "." + table
	}
	field.Foreign = table + "(" + column + ")"
	return nil
}

// splitTargetColumn separates a dotted reference into its table and its last
// component, which is the column.
func splitTargetColumn(target string) (table, column string, ok bool) {
	dot := strings.LastIndex(target, ".")
	if dot <= 0 || dot == len(target)-1 {
		return "", "", false
	}
	return target[:dot], target[dot+1:], true
}
