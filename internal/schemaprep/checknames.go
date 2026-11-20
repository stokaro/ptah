package schemaprep

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// ErrDuplicateCheckName is the class of a CHECK whose name another CHECK
// already holds where the server keeps CHECK names unique: in a table, or in a
// schema across its tables, as the dialect's descriptor states
// ([descriptor.CheckNames]). The server refuses the second CHECK, so a model
// holding both describes a database no server builds. Two CHECKs of one name
// in one table are refused on every dialect, since Ptah tells the constraints
// of a table apart by name.
var ErrDuplicateCheckName = errors.New("two CHECK constraints claim the same name")

// ValidateCheckNames refuses a database that declares two CHECK constraints
// under one name where the server of dialect keeps one: in the scope its
// descriptor states, compared as the descriptor states. The error wraps
// [ErrDuplicateCheckName] and ends with the error the server answers, as the
// dialect's driver reports it.
//
// On a dialect whose descriptor states no scope it refuses two CHECKs of one
// name in one table, compared as the target compares constraint names, with a
// reason of its own: the comparison, the plan and a drop name a constraint by
// its table and its name, so a table cannot hold two of a name. SQLite
// accepts such a pair, and Ptah is stricter there.
//
// It reads the names a declaration writes: a column's CHECK name and a CHECK
// constraint's. A name the server derives for an unnamed CHECK is the
// readers' to settle, since only they know the order the server names them in.
// The tables are read in declaration order, each table's column CHECKs before
// its table CHECKs, which is the order a render creates them in, so the CHECK
// reported second is the one the server refuses. A nil database passes.
func ValidateCheckNames(database *schemamodel.Database, dialect string) error {
	return ValidateCheckNamesAs(database, dialect, identifier.ForDialect(dialect))
}

// ValidateCheckNamesAs is [ValidateCheckNames] for a caller that holds the
// semantics a server resolved. Beside the names the descriptor says the server
// compares as one, it refuses two CHECKs of one table whose names those
// semantics identify, which is the pair the comparison would otherwise reject
// as one owner declared twice, with an error of its own. Without resolved
// names the semantics identify only what the dialect's static comparison does.
func ValidateCheckNamesAs(database *schemamodel.Database, dialect string, semantics identifier.Semantics) error {
	rules := checkNameRules(dialect)
	return validateCheckNames(database, semantics, rules, rules.Scope)
}

// ValidateCheckNamesWithinTables refuses what [ValidateCheckNames] refuses
// inside one table, and leaves two tables alone. A caller that has not yet
// asked the server which dialect it speaks uses it: a MariaDB server behind a
// mysql URL keeps a CHECK name per table, where the MySQL descriptor keeps it
// per schema, and only the pair in one table is refused by both.
func ValidateCheckNamesWithinTables(database *schemamodel.Database, dialect string) error {
	rules := checkNameRules(dialect)
	within := rules.Scope
	if within != "" {
		within = descriptor.CheckNameScopeTable
	}
	return validateCheckNames(database, identifier.ForDialect(dialect), rules, within)
}

// checkNameRules is what the descriptor of dialect states about CHECK names, and
// the zero value for a dialect with no descriptor.
func checkNameRules(dialect string) descriptor.CheckNames {
	if found, ok := platform.Lookup(dialect); ok {
		return found.CheckNames
	}
	return descriptor.CheckNames{}
}

// validateCheckNames refuses two CHECKs of database whose names collide within
// the scope they are checked in. rules word the refusal, and their scope is
// the server's even where within checks a narrower one.
func validateCheckNames(database *schemamodel.Database, semantics identifier.Semantics, rules descriptor.CheckNames, within descriptor.CheckNameScope) error {
	if database == nil {
		return nil
	}
	held := make(map[checkNameKey]declaredCheck)
	identified := make(map[checkNameKey]declaredCheck)
	for _, check := range declaredChecks(database) {
		key := check.key(rules, semantics, within)
		if first, taken := held[key]; taken {
			return duplicateCheckName(rules, first, check)
		}
		held[key] = check
		if rules.Scope == "" {
			continue
		}
		// The identity the comparison gives a constraint is its table and its
		// name as the semantics identify names, so two CHECKs of one table that
		// share it cannot be told apart there.
		identity := checkNameKey{
			scope: semantics.QualifiedTableIdentityKey(check.table.QualifiedName()),
			name:  semantics.IndexIdentityKey(check.name),
		}
		if first, taken := identified[identity]; taken {
			return duplicateCheckName(rules, first, check)
		}
		identified[identity] = check
	}
	return nil
}

// checkNameKey is where a CHECK name is held and the name as the scope
// compares it.
type checkNameKey struct {
	scope string
	name  string
}

// declaredCheck is a CHECK a declaration names, with its table.
type declaredCheck struct {
	name  string
	table schemamodel.Table
}

// key answers where dc's name is held and the name as that scope compares
// it. Where rules state no scope, the name is held in its table and compared
// as the target compares a constraint name, which is how a constraint's
// identity keys it. Otherwise it is held in the scope within names and
// compared as rules state. A table written without a schema is in the target's
// default schema.
func (dc declaredCheck) key(rules descriptor.CheckNames, semantics identifier.Semantics, within descriptor.CheckNameScope) checkNameKey {
	qualified := dc.table.QualifiedName()
	table := semantics.QualifiedTableIdentityKey(qualified)
	if rules.Scope == "" {
		return checkNameKey{scope: table, name: semantics.IndexIdentityKey(dc.name)}
	}
	name := foldRanges(semantics.NameIdentityKey(identifier.Comparison(rules.Comparison), dc.name), rules.FoldedRanges)
	if within == descriptor.CheckNameScopeTable {
		return checkNameKey{scope: table, name: name}
	}
	schema := semantics.DefaultSchema
	if ref, ok := tableref.Parse(qualified); ok && ref.Qualified {
		schema = ref.Schema
	}
	return checkNameKey{scope: semantics.TableIdentityKey(schema), name: name}
}

// declaredChecks lists the named CHECKs of database, table by table in
// declaration order, each table's column CHECKs before its table CHECKs.
func declaredChecks(database *schemamodel.Database) []declaredCheck {
	byStruct := make(map[string]int, len(database.Tables))
	byName := make(map[string]int, len(database.Tables))
	for i, table := range database.Tables {
		byStruct[table.StructName] = i
		byName[table.QualifiedName()] = i
	}
	perTable := make([][]string, len(database.Tables))
	for _, field := range database.Fields {
		i, ok := byStruct[field.StructName]
		if !ok || strings.TrimSpace(field.Check) == "" || field.CheckName == "" {
			continue
		}
		perTable[i] = append(perTable[i], field.CheckName)
	}
	for _, constraint := range database.Constraints {
		if !strings.EqualFold(constraint.Type, "CHECK") || constraint.Name == "" {
			continue
		}
		i, ok := byName[constraint.Table]
		if !ok && constraint.Table == "" {
			i, ok = byStruct[constraint.StructName]
		}
		if !ok {
			continue
		}
		perTable[i] = append(perTable[i], constraint.Name)
	}
	var checks []declaredCheck
	for i, names := range perTable {
		for _, name := range names {
			checks = append(checks, declaredCheck{name: name, table: database.Tables[i]})
		}
	}
	return checks
}

// duplicateCheckName is the refusal of second, whose name first holds.
func duplicateCheckName(rules descriptor.CheckNames, first, second declaredCheck) error {
	firstName, secondName := sqlident.UnquoteDoubleQuoted(first.name), sqlident.UnquoteDoubleQuoted(second.name)
	refusal := rules.Refusal
	where := fmt.Sprintf("table %q declares CHECK %q and table %q CHECK %q",
		first.table.QualifiedName(), firstName, second.table.QualifiedName(), secondName)
	if first.table.QualifiedName() == second.table.QualifiedName() {
		if rules.TableRefusal != "" {
			refusal = rules.TableRefusal
		}
		where = fmt.Sprintf("table %q declares CHECK %q and CHECK %q", second.table.QualifiedName(), firstName, secondName)
	}
	if rules.Scope == "" {
		return fmt.Errorf("%w: %s, and Ptah keeps one CHECK of a name per table", ErrDuplicateCheckName, where)
	}
	return fmt.Errorf("%w: %s, and the server keeps one CHECK of a name per %s: %s",
		ErrDuplicateCheckName, where, rules.Scope, strings.ReplaceAll(refusal, "{name}", secondName))
}

// foldRanges lowercases each letter of name that lies in one of ranges, which
// are the code points the server folds beyond ASCII.
func foldRanges(name string, ranges []descriptor.CodePointRange) string {
	if len(ranges) == 0 {
		return name
	}
	return strings.Map(func(r rune) rune {
		for _, folded := range ranges {
			if r >= folded.First && r <= folded.Last {
				return unicode.ToLower(r)
			}
		}
		return r
	}, name)
}
