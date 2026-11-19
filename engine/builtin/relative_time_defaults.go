package builtin

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/defaultlit"
	"ptah.run/internal/tableref"
)

// relativeTimeAdvice ends each refusal of a relative time default.
const relativeTimeAdvice = "write a function the server calls for each row, such as now() or CURRENT_DATE"

// validateRelativeTimeDefault refuses a default on a date or time column that
// the target reads as a moment relative to when it reads it, such as
// PostgreSQL's 'now' ([defaultlit.RelativeTimeDefault]), or an array default
// holding one on an array of such a type. The parser refuses one a CREATE
// TABLE writes on a built-in type; this refuses one a Go or YAML model
// declares, one an ALTER COLUMN ... SET DEFAULT in a schema file sets, which
// reach the model without the column's type beside them, and one on a column
// typed by a domain ([resolveDomains]). Measured on PostgreSQL 18, the server
// reads the default once, when it creates the table, so every row gets that
// moment and every plan changes the column again.
//
// A type name more than one domain answers to, in different schemas, is
// refused when any of them holds a moment: which one the server takes depends
// on its search path, which the model does not know.
func validateRelativeTimeDefault(dialect string, database *schemamodel.Database, field schemamodel.Field) error {
	literals := relativeTimeLiterals(dialect)
	if len(literals) == 0 {
		return nil
	}
	value := field.DefaultExpr
	if field.Default != "" || field.DefaultSet {
		value = sqlLiteral(field.Default)
	}
	verdict := relativeTimeDefaultOn(field.Type, value, literals, schemaext.DefinedTypes(database.FeatureObjects))
	if verdict == relativeTimeKept {
		return nil
	}
	tableName := field.StructName
	if table := tableByStructName(database.Tables, field.StructName); table != nil {
		tableName = table.QualifiedName()
	}
	subject := fmt.Sprintf("column %q of table %q: DEFAULT %s on a %s column", field.Name, tableName, value, field.Type)
	return relativeTimeRefusal(dialect, subject, "the table", "the column", field.Type, verdict)
}

// validateRelativeTimeDomainDefaults refuses a domain whose own default is a
// relative time literal on the date or time type the domain resolves to.
// Measured on PostgreSQL 18, CREATE DOMAIN d AS timestamp DEFAULT 'now'
// stores the moment the domain was created, and a domain over d inherits it,
// so every column typed by either takes that moment for every row.
func validateRelativeTimeDomainDefaults(dialect string, database *schemamodel.Database) error {
	literals := relativeTimeLiterals(dialect)
	if len(literals) == 0 || database.FeatureObjects.IsZero() {
		return nil
	}
	objects, err := database.FeatureObjects.All()
	if err != nil {
		return err
	}
	defined := schemaext.DefinedTypes(database.FeatureObjects)
	for _, object := range objects {
		domain, ok := object.Value.(*pgtype.DesiredDomain)
		if !ok {
			continue
		}
		value := domain.DefaultExpr
		if value == "" && domain.Default != "" {
			value = sqlLiteral(domain.Default)
		}
		verdict := relativeTimeDefaultOn(domain.BaseType, value, literals, defined)
		if verdict == relativeTimeKept {
			continue
		}
		subject := fmt.Sprintf("domain %q: DEFAULT %s on a %s domain", pgtype.QualifiedName(object.Ref), value, domain.BaseType)
		return relativeTimeRefusal(dialect, subject, "the domain", "the domain", domain.BaseType, verdict)
	}
	return nil
}

// relativeTimeLiterals returns the relative time literals the dialect's
// descriptor names, or none for a dialect it does not know.
func relativeTimeLiterals(dialect string) []string {
	described, ok := platform.Lookup(dialect)
	if !ok {
		return nil
	}
	return described.Grammar.Columns.RelativeTimeLiterals
}

// sqlLiteral writes a literal default as SQL: as it is where it already reads
// as a SQL literal, and quoted as a string otherwise.
func sqlLiteral(value string) string {
	if defaultlit.IsSQLLiteral(strings.TrimSpace(value)) {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// relativeTimeVerdict says whether the server reads a default once.
type relativeTimeVerdict int

const (
	// relativeTimeKept is a default the server keeps as written or calls for
	// each row.
	relativeTimeKept relativeTimeVerdict = iota
	// relativeTimeReadOnce is a default the server reads once, on every type
	// the column type resolves to without a name more than one domain answers
	// to.
	relativeTimeReadOnce
	// relativeTimeMaybeReadOnce is a default the server reads once only on a
	// type reached through such a name, which the search path resolves.
	relativeTimeMaybeReadOnce
)

// relativeTimeDefaultOn says whether value, a default on columnType, is a
// relative time default ([defaultlit.RelativeTimeDefault]) on a base type
// columnType resolves to, and whether an ambiguous name took part in every
// such resolution.
func relativeTimeDefaultOn(columnType, value string, literals []string, defined []schemaext.DefinedType) relativeTimeVerdict {
	verdict := relativeTimeKept
	for _, base := range resolveDomains(columnType, defined) {
		if !defaultlit.RelativeTimeDefault(base.spelling, value, literals) {
			continue
		}
		if !base.ambiguous {
			return relativeTimeReadOnce
		}
		verdict = relativeTimeMaybeReadOnce
	}
	return verdict
}

// relativeTimeRefusal is the refusal of a relative time default declared on
// changed, which the server reads when it creates created. One read once says
// so; one that may be read once says which name it could not resolve and how
// to resolve it.
func relativeTimeRefusal(dialect, subject, created, changed, typeName string, verdict relativeTimeVerdict) error {
	message := subject + " is read once, when " + created + " is created, so every row gets that moment and every " +
		"plan changes " + changed + " again; " + relativeTimeAdvice
	if verdict == relativeTimeMaybeReadOnce {
		message = subject + " may be read once, when " + created + " is created: more than one domain answers to " +
			typeName + ", and one holds a date or time, so every row would get that moment; qualify the type with " +
			"the schema of the domain meant, or " + relativeTimeAdvice
	}
	return &ptaherr.RenderError{Dialect: dialect, Err: ptaherr.ErrUnsupportedFeature, Message: message}
}

// resolvedType is a type a column type may stand for once the domains the
// model declares are resolved: a built-in type, or one the model does not
// declare. ambiguous records that a name more than one domain answers to took
// part in the resolution.
type resolvedType struct {
	spelling  string
	ambiguous bool
}

// resolveDomains returns the types columnType may stand for: columnType itself
// where no domain the model declares answers to its name, and otherwise the
// types each such domain's base resolves to. An array of a domain resolves to
// an array of the domain's base, written with [], and so does a domain over
// an array.
//
// Each domain is entered once for each way of reaching it, so a cycle and a
// chain each end within the definitions.
func resolveDomains(columnType string, defined []schemaext.DefinedType) []resolvedType {
	// suffix is "[]" once the resolution has passed through an array.
	type step struct {
		spelling, suffix string
		ambiguous        bool
	}
	type visit struct {
		definition int
		suffix     string
		ambiguous  bool
	}
	var resolved []resolvedType
	seen := make(map[visit]bool)
	queue := []step{{spelling: columnType}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if element, ok := defaultlit.ArrayElementType(current.spelling); ok {
			queue = append(queue, step{spelling: element, suffix: "[]", ambiguous: current.ambiguous})
			continue
		}
		domains := declaredDomains(current.spelling, defined)
		if len(domains) == 0 {
			resolved = append(resolved, resolvedType{spelling: current.spelling + current.suffix, ambiguous: current.ambiguous})
			continue
		}
		ambiguous := current.ambiguous || len(domains) > 1
		for _, i := range domains {
			key := visit{definition: i, suffix: current.suffix, ambiguous: ambiguous}
			if seen[key] {
				continue
			}
			seen[key] = true
			queue = append(queue, step{spelling: defined[i].References[0], suffix: current.suffix, ambiguous: ambiguous})
		}
	}
	return resolved
}

// declaredDomains returns the positions in defined of every domain declared
// under the name spelling writes, matched without case, and by schema where
// spelling names one.
func declaredDomains(spelling string, defined []schemaext.DefinedType) []int {
	ref, ok := tableref.Parse(spelling)
	if !ok {
		return nil
	}
	var matches []int
	for i, definition := range defined {
		if definition.ID.Kind != objectidentity.KindDomain || len(definition.References) == 0 {
			continue
		}
		name, named := tableref.Parse(definition.ID.Name.Source)
		schema, _ := tableref.Parse(definition.ID.Schema.Source)
		if !named || !strings.EqualFold(name.Name, ref.Name) ||
			ref.Qualified && !strings.EqualFold(schema.Name, ref.Schema) {
			continue
		}
		matches = append(matches, i)
	}
	return matches
}
