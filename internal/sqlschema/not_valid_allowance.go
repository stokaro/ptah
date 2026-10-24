package sqlschema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/ptahdirective"
)

// ErrNotValidAllowance is the refusal of a `ptah:allow-not-valid` directive
// that is malformed or names no CHECK or FOREIGN KEY constraint of the
// desired schema.
var ErrNotValidAllowance = errors.New("ptah:allow-not-valid")

// allowDirectivePrefix is the family the permission directive belongs to. A
// word of the family the reader does not know is refused, so a misspelled
// permission fails instead of being read as a comment and leaving the
// constraint to be validated.
const allowDirectivePrefix = "ptah:allow"

// notValidDirective is the permission directive's word.
const notValidDirective = "allow-not-valid"

// readNotValidAllowances reads each line comment of data that carries
//
//	-- ptah:allow-not-valid CONSTRAINT <name> ON [<schema>.]<table>
//
// The names are read the way the dialect's server reads them, as every name
// the reader records is: quoted or folded, and cut where the server cuts a
// long one. The table is named the way the documents name it, qualified where
// they qualify it, as `COMMENT ON CONSTRAINT ... ON` names it, and it resolves
// the same way: see [NotValidAllowances.Apply].
//
// Only a comment that begins its line is a directive, by the dialect's lexer,
// so the same text inside a string or a routine body is not one. source names
// the document for a refusal to point at.
func readNotValidAllowances(data []byte, dialect, source string) ([]notValidAllowance, error) {
	text := string(data)
	var allowances []notValidAllowance
	for comment := range ptahdirective.LineComments(text, dialectlexer.Options(dialect)) {
		body := strings.TrimSpace(strings.TrimPrefix(comment.Text, "--"))
		if !strings.HasPrefix(body, allowDirectivePrefix) {
			continue
		}
		// A refusal here is wrapped with the document's name by the caller,
		// so it names the line alone; the allowance keeps both for a refusal
		// after the merge, which no caller wraps.
		line := directiveLocation("", text, comment.Start)
		word, arguments := cutWord(strings.TrimPrefix(body, "ptah:"))
		if word != notValidDirective {
			return nil, fmt.Errorf("%w: %s: unknown directive ptah:%s; the permission is written ptah:%s",
				ErrNotValidAllowance, line, word, notValidDirective)
		}
		if !capability.ForDialect(dialect).Has(capability.AddConstraintNotValid) {
			return nil, fmt.Errorf("%w: %s: %s has no NOT VALID constraints; the directive applies to a dialect that has them, such as postgres",
				ErrNotValidAllowance, line, dialectName(dialect))
		}
		name, table, ok := cutConstraintOnTable(arguments)
		if !ok {
			return nil, fmt.Errorf("%w: %s: write ptah:%s CONSTRAINT <name> ON [<schema>.]<table>",
				ErrNotValidAllowance, line, notValidDirective)
		}
		allowances = append(allowances, notValidAllowance{
			table:      normalizeSQLTableReference(dialect, table),
			constraint: normalizeSQLIdentifier(dialect, name),
			dialect:    dialect,
			source:     directiveLocation(source, text, comment.Start),
		})
	}
	return allowances, nil
}

// cutConstraintOnTable splits `CONSTRAINT <name> ON <table>` and answers false
// for anything else, a trailing word included.
func cutConstraintOnTable(text string) (name, table string, ok bool) {
	kind, rest := cutWord(text)
	name, rest = cutQualifiedName(rest)
	on, rest := cutWord(rest)
	table, rest = cutQualifiedName(rest)
	if !strings.EqualFold(kind, "CONSTRAINT") || name == "" || !strings.EqualFold(on, "ON") || table == "" || rest != "" {
		return "", "", false
	}
	return name, table, true
}

// directiveLocation names the line offset falls on, after source when there
// is one.
func directiveLocation(source, text string, offset int) string {
	line := fmt.Sprintf("line %d", strings.Count(text[:offset], "\n")+1)
	if source == "" {
		return line
	}
	return source + ": " + line
}

func dialectName(dialect string) string {
	if dialect == "" {
		return "a document read with no dialect"
	}
	return dialect
}

// NotValidAllowances collects the `ptah:allow-not-valid` directives of the
// SQL documents one desired schema is read from, so that
// [NotValidAllowances.Apply] can resolve them once the documents are merged:
// a directive usually names a constraint another source declares, such as an
// ORM's schema export. A loader of a desired schema puts one in the context it
// reads its sources under; see [WithNotValidAllowances]. The directives are
// instructions about the desired schema rather than part of it, so the model
// carries none of them, only the [schemamodel.Constraint.NotValid] they set.
type NotValidAllowances struct {
	mu         sync.Mutex
	allowances []notValidAllowance
}

// notValidAllowance is one directive: the CHECK or FOREIGN KEY constraint it
// names may stay NOT VALID.
type notValidAllowance struct {
	// table is the constraint's table as the documents name it, read under
	// dialect's rules: qualified where they qualify it.
	table string
	// constraint is the constraint's name, read under dialect's rules.
	constraint string
	// dialect is the dialect the names were read under, which decides how a
	// name reaches a declaration.
	dialect string
	// source is where the directive was written, for a refusal to point at.
	source string
}

type notValidAllowancesKey struct{}

// WithNotValidAllowances returns ctx carrying a new collector, and the
// collector. Every SQL document read under the returned context adds its
// directives to it rather than resolving them on its own, so a loader reads
// its sources, merges them, and calls Apply on the result. A document read
// under no collector resolves its directives against itself.
func WithNotValidAllowances(ctx context.Context) (context.Context, *NotValidAllowances) {
	allowances := &NotValidAllowances{}
	return context.WithValue(ctx, notValidAllowancesKey{}, allowances), allowances
}

func notValidAllowancesFrom(ctx context.Context) *NotValidAllowances {
	if ctx == nil {
		return nil
	}
	allowances, _ := ctx.Value(notValidAllowancesKey{}).(*NotValidAllowances)
	return allowances
}

func (a *NotValidAllowances) add(allowances []notValidAllowance) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.allowances = append(a.allowances, allowances...)
}

// Apply lets each CHECK or FOREIGN KEY constraint a collected directive names
// stay NOT VALID: it sets [schemamodel.Constraint.NotValid] on the constraint
// in database, the desired schema every source has been merged into, and
// changes nothing else about it. A nil collector applies nothing.
//
// A directive reaches the constraint the way `COMMENT ON CONSTRAINT <name> ON
// <table>` would in the same document: exactly after the dialect's fold, and
// by the server's own case rule where it has one. A table the documents name
// unqualified is named unqualified, and one they qualify is qualified, so a
// directive cannot reach two constraints. One that reaches none, or reaches a
// PRIMARY KEY or UNIQUE constraint, is refused with the place it was written.
func (a *NotValidAllowances) Apply(database *schemamodel.Database) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	allowances := slices.Clone(a.allowances)
	a.mu.Unlock()
	return applyNotValidAllowances(database, allowances)
}

func applyNotValidAllowances(database *schemamodel.Database, allowances []notValidAllowance) error {
	if database == nil || len(allowances) == 0 {
		return nil
	}
	names := make([]string, len(database.Constraints))
	for index, constraint := range database.Constraints {
		names[index] = memberName(constraint.Table, constraint.Name)
	}
	var refusals []error
	for _, allowance := range allowances {
		index := resolveDeclaredName(allowance.dialect, memberName(allowance.table, allowance.constraint), names)
		if index < 0 && namesPrimaryKey(database, allowance) {
			refusals = append(refusals, fmt.Errorf("%w: %s: constraint %q on table %q is a PRIMARY KEY; only a CHECK or a FOREIGN KEY can stay NOT VALID",
				ErrNotValidAllowance, allowance.source, allowance.constraint, allowance.table))
			continue
		}
		if index < 0 {
			refusals = append(refusals, fmt.Errorf("%w: %s: the desired schema declares no constraint %q on table %q%s",
				ErrNotValidAllowance, allowance.source, allowance.constraint, allowance.table, qualificationHint(database, allowance)))
			continue
		}
		constraint := &database.Constraints[index]
		switch strings.ToUpper(constraint.Type) {
		case "CHECK", "FOREIGN KEY":
			constraint.NotValid = true
		default:
			refusals = append(refusals, fmt.Errorf("%w: %s: constraint %q on table %q is a %s; only a CHECK or a FOREIGN KEY can stay NOT VALID",
				ErrNotValidAllowance, allowance.source, allowance.constraint, allowance.table, strings.ToUpper(constraint.Type)))
		}
	}
	return errors.Join(refusals...)
}

// qualificationHint says how the desired schema names the table, when a
// directive missed a constraint only because it qualified the table
// differently.
func qualificationHint(database *schemamodel.Database, allowance notValidAllowance) string {
	_, wanted := splitQualifiedTable(allowance.table)
	for _, constraint := range database.Constraints {
		_, table := splitQualifiedTable(constraint.Table)
		if constraint.Table != allowance.table && table == wanted &&
			resolveDeclaredName(allowance.dialect, allowance.constraint, []string{constraint.Name}) == 0 {
			return fmt.Sprintf("; it declares %q on table %q, which the directive names as the documents do", allowance.constraint, constraint.Table)
		}
	}
	return ""
}

// namesPrimaryKey reports whether the directive names the primary key of its
// table, which the model keeps on the table rather than among the constraints.
func namesPrimaryKey(database *schemamodel.Database, allowance notValidAllowance) bool {
	for _, table := range database.Tables {
		if table.PrimaryKeyName != "" && table.QualifiedName() == allowance.table &&
			resolveDeclaredName(allowance.dialect, allowance.constraint, []string{table.PrimaryKeyName}) == 0 {
			return true
		}
	}
	return false
}
