package sqlschema

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/rundiag"
)

// Redeclaration is a constraint a document drops by name and then adds again
// under that name with another definition. The later statement is what the
// document declares, which is the server's reading too, so the constraint is
// recorded as it says. A supplemental file that copies a key an ORM's export
// declares in order to change it is the usual source, and the copy goes on
// deciding the key after the export changes it, so the change is reported.
type Redeclaration struct {
	// Kind is the constraint's kind in lower case, such as "foreign key".
	Kind string
	// Table and Name name the constraint as the model records them.
	Table, Name string
	// Changed names the properties the later definition gives another value,
	// in a fixed order, such as "on delete" and "not valid".
	Changed []string
}

// RedeclarationRecord is the run diagnostic for r, read from source.
func RedeclarationRecord(source string, r Redeclaration) rundiag.Record {
	return rundiag.Record{
		Key:  r.Table + "\x00" + r.Name,
		Line: source + ": " + r.String(),
		Attrs: []slog.Attr{
			slog.String("source", source), slog.String("kind", r.Kind), slog.String("table", r.Table),
			slog.String("name", r.Name), slog.String("changed", strings.Join(r.Changed, ", ")),
		},
	}
}

func (r Redeclaration) String() string {
	if slices.Equal(r.Changed, []string{"not valid"}) {
		return fmt.Sprintf("%s %q on table %q is dropped and added again only to allow NOT VALID; "+
			"-- ptah:allow-not-valid CONSTRAINT %q ON %q allows it without declaring the key a second time",
			r.Kind, r.Name, r.Table, r.Name, r.Table)
	}
	return fmt.Sprintf("%s %q on table %q is dropped and added again with another %s; the later declaration decides it",
		r.Kind, r.Name, r.Table, strings.Join(r.Changed, ", "))
}

// rememberDropped keeps the table constraint name names, before a DROP
// removes it, for a later ADD of the name to be compared with.
func (t alterTarget) rememberDropped(name string) {
	if t.document == nil {
		return
	}
	for _, database := range t.databases {
		for _, constraint := range database.Constraints {
			if constraint.StructName == t.structName && constraint.Name == name {
				if t.document.dropped == nil {
					t.document.dropped = make(map[string]schemamodel.Constraint)
				}
				t.document.dropped[memberName(t.qualified, name)] = constraint
				return
			}
		}
	}
}

// noteRedeclaration reports added when it replaces a constraint the document
// dropped under its name with another definition.
func (t alterTarget) noteRedeclaration(added schemamodel.Constraint) {
	if t.document == nil {
		return
	}
	key := memberName(t.qualified, added.Name)
	dropped, ok := t.document.dropped[key]
	if !ok {
		return
	}
	delete(t.document.dropped, key)
	changed := changedProperties(dropped, added)
	if len(changed) == 0 || t.document.redeclared == nil {
		return
	}
	t.document.redeclared(Redeclaration{
		Kind: strings.ToLower(added.Type), Table: added.Table, Name: added.Name, Changed: changed,
	})
}

// changedProperties names the properties b gives another value than a, with
// the spellings that mean the same -- no ON DELETE clause and NO ACTION, no
// MATCH clause and SIMPLE -- read as one.
func changedProperties(a, b schemamodel.Constraint) []string {
	comparisons := []struct {
		name   string
		differ bool
	}{
		{"kind", !strings.EqualFold(a.Type, b.Type)},
		{"columns", !slices.Equal(a.Columns, b.Columns)},
		{"check", strings.TrimSpace(a.CheckExpression) != strings.TrimSpace(b.CheckExpression)},
		{"referenced table", a.ForeignTable != b.ForeignTable},
		{"referenced columns", !slices.Equal(a.ForeignColumnsOrDefault(), b.ForeignColumnsOrDefault())},
		{"on delete", referentialAction(a.OnDelete) != referentialAction(b.OnDelete)},
		{"on update", referentialAction(a.OnUpdate) != referentialAction(b.OnUpdate)},
		{"match", matchType(a.Match) != matchType(b.Match)},
		{"deferrable", a.Deferrable != b.Deferrable || (a.Deferrable && !strings.EqualFold(a.Initially, b.Initially))},
		{"not enforced", a.NotEnforced != b.NotEnforced},
		{"not valid", a.NotValid != b.NotValid},
	}
	var changed []string
	for _, comparison := range comparisons {
		if comparison.differ {
			changed = append(changed, comparison.name)
		}
	}
	return changed
}

func referentialAction(action string) string {
	action = strings.ToUpper(strings.TrimSpace(action))
	if action == "" {
		return "NO ACTION"
	}
	return action
}

func matchType(match string) string {
	match = strings.ToUpper(strings.TrimSpace(match))
	if match == "" {
		return "SIMPLE"
	}
	return match
}
