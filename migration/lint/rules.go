package lint

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"go.5x5.cz/ptah/internal/tableref"
)

var registeredRules = struct {
	sync.RWMutex
	rules []Rule
}{}

// Register appends a process-wide custom lint rule. Prefer Options.ExtraRules
// for request-scoped analyzers; Register exists for plugin-style integrations
// that initialize rules once at process startup.
func Register(rule Rule) error {
	if err := validateRule(rule); err != nil {
		return err
	}
	registeredRules.Lock()
	defer registeredRules.Unlock()
	for _, existing := range append(builtinRules(), registeredRules.rules...) {
		if existing.Code == rule.Code {
			return fmt.Errorf("duplicate rule code %s", rule.Code)
		}
	}
	registeredRules.rules = append(registeredRules.rules, cloneRule(rule))
	return nil
}

// Rules returns the built-in rule set plus process-wide registered rules. The
// slice is rebuilt on every call so callers can never corrupt the registry.
func Rules() []Rule {
	rules := builtinRules()
	registeredRules.RLock()
	defer registeredRules.RUnlock()
	return append(rules, cloneRules(registeredRules.rules)...)
}

func cloneRule(rule Rule) Rule {
	rule.Dialects = slices.Clone(rule.Dialects)
	return rule
}

func cloneRules(rules []Rule) []Rule {
	cloned := make([]Rule, len(rules))
	for i, rule := range rules {
		cloned[i] = cloneRule(rule)
	}
	return cloned
}

func builtinRules() []Rule {
	var rules []Rule
	rules = append(rules, dataSafetyRules()...)
	rules = append(rules, constraintDeletionRules()...)
	rules = append(rules, dataDependentRules()...)
	rules = append(rules, migrationFormRules()...)
	rules = append(rules, compatibilityRules()...)
	rules = append(rules, postgresRules()...)
	rules = append(rules, mysqlRules()...)
	rules = append(rules, sqliteRules()...)
	rules = append(rules, transactionRules()...)
	return rules
}

func validateRule(rule Rule) error {
	if !isCanonicalRuleCode(rule.Code) {
		return fmt.Errorf("rule code %q must start with an uppercase ASCII letter and contain only uppercase ASCII letters and digits", rule.Code)
	}
	if strings.TrimSpace(rule.Title) == "" {
		return fmt.Errorf("rule %s title is required", rule.Code)
	}
	switch rule.Severity {
	case SeverityInfo, SeverityWarning, SeverityError:
	default:
		return fmt.Errorf("rule %s has unsupported severity %q", rule.Code, rule.Severity)
	}
	if (rule.CheckStatement != nil) == (rule.CheckFile != nil) {
		return fmt.Errorf("rule %s must set exactly one checker", rule.Code)
	}
	return nil
}

func isCanonicalRuleCode(code string) bool {
	for index, value := range code {
		if value >= 'A' && value <= 'Z' {
			continue
		}
		if index > 0 && value >= '0' && value <= '9' {
			continue
		}
		return false
	}
	return code != ""
}

func validateRules(rules []Rule) error {
	seen := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		if err := validateRule(rule); err != nil {
			return err
		}
		if _, ok := seen[rule.Code]; ok {
			return fmt.Errorf("duplicate rule code %s", rule.Code)
		}
		seen[rule.Code] = struct{}{}
	}
	return nil
}

// dataSafetyRules covers the DS family: statements that destroy data.
func dataSafetyRules() []Rule {
	return []Rule{
		tableDroppedRule(),
		columnDroppedRule(),
		columnTypeChangedRule(),
		notNullDroppedRule(),
		constraintDroppedRule(),
		enumValueRemovedRule(),
		databaseObjectDroppedRule(),
		tableTruncatedRule(),
		rlsDisabledRule(),
	}
}

// constraintDeletionRules covers the CD family: dropping a constraint whose type
// is recoverable from the SQL, split by constraint type so operators get a
// precise per-type signal. The untyped ANSI DROP CONSTRAINT <name> form stays in
// DS105 (dataSafetyRules) because its type is not determinable from the SQL.
func constraintDeletionRules() []Rule {
	return []Rule{
		foreignKeyDroppedRule(),
		checkConstraintDroppedRule(),
		primaryKeyDroppedRule(),
	}
}

// tableDroppedAdvice is the remediation every DS101 finding carries, kept in one
// place because the named and unnamed message forms must not drift apart.
const tableDroppedAdvice = "take a verified backup first and consider a rename-and-retire window instead"

func tableDroppedRule() Rule {
	return Rule{
		Code:     "DS101",
		Title:    "table dropped",
		Severity: SeverityError,
		// File-level: dropping a table this same migration created (the
		// create-staging/backfill/drop pattern) destroys no pre-existing
		// data and is exempt.
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			created := map[string]bool{}
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if ref := createdTableRef(stmt.Words); ref != "" {
					created[ref] = true
					continue
				}
				if !hasWordPrefix(stmt.Words, "DROP", "TABLE") {
					continue
				}
				unsafeTables, complete := droppedTablesNotCreated(stmt.Words, stmt.sourceWords, created)
				if complete && len(unsafeTables) == 0 {
					continue
				}
				findings = append(findings, tableDroppedFindings(file.Path, stmt.Line, i, unsafeTables)...)
			}
			return append(findings, tableRenamedFindings(file)...)
		},
	}
}

// tableRenamedFindings reports the table names a file retires by renaming them,
// on the compatibility surface only. See [renamedNames] for why a rename is
// classified as destructive there and as BC101 natively, and why the
// destructive form has to carry a DS code.
//
// One finding per retired table, ordered by logical name within the statement,
// exactly as [tableDroppedFindings] orders a DROP TABLE target list -- measured
// on `RENAME TABLE users TO accounts, pets TO animals`, which reports "pets"
// before "users". Across statements the source order is kept: two consecutive
// `ALTER TABLE ... RENAME TO` statements report in the order they are written,
// so the sort is per statement rather than per file.
func tableRenamedFindings(file *File) []Finding {
	if file.compatibility != CompatibilityProfileAtlas {
		return nil
	}
	var findings []Finding
	for _, rename := range renamesOfKind(fileRenames(file), SubjectTable) {
		ordered := slices.Clone(rename.names)
		slices.SortStableFunc(ordered, func(a, b renamedName) int {
			return strings.Compare(logicalObjectName(a.name), logicalObjectName(b.name))
		})
		for _, name := range ordered {
			message := "RENAME retires the table name; " + renameRetiredNameAdvice
			var subjects []Subject
			if name.name != "" {
				message = fmt.Sprintf("RENAME retires the table name %s; %s", name.name, renameRetiredNameAdvice)
				subjects = []Subject{{Kind: SubjectTable, Name: name.name}}
			}
			findings = append(findings, Finding{
				Rule:     "DS101",
				Title:    "table dropped",
				Severity: SeverityError,
				File:     file.Path,
				Line:     rename.line,
				Message:  message,
				Context:  statementFindingContext(rename.statementIndex, subjects...),
			})
		}
	}
	return findings
}

// tableDroppedFindings reports one finding per table a single DROP TABLE
// destroys.
//
// `DROP TABLE a, b` destroys two tables independently, so it is two findings,
// not one finding that happens to carry two subjects. The collapsed shape was
// reported as one finding, and every renderer that reads a finding's primary
// subject then showed only the first table -- on a destructive-change analyzer,
// an operator reviewing a release was never told that `b` was going away
// either. Per-target findings also give each table its own message, which is
// what keeps the per-finding SARIF fingerprint (rule, file, line, message)
// distinct so a code-scanning consumer cannot dedupe the other tables away.
//
// Targets are ordered by their logical name -- unqualified and unquoted --
// compared byte-wise, which puts "Mid" and "Zeta" ahead of "alpha". The order a
// comma list is written in carries no meaning, so a stable one makes the report
// diffable, and this is the order measured from the destructive-change analyzer
// this tool is compatible with.
//
// A target list that could not be parsed to the end yields one subject-less
// finding: the statement is still destructive, and failing closed keeps it
// reported rather than letting an unreadable target silence the rule.
func tableDroppedFindings(filePath string, line, statementIndex int, tables []tableReference) []Finding {
	finding := func(message string, subjects ...Subject) Finding {
		return Finding{
			Rule:     "DS101",
			Title:    "table dropped",
			Severity: SeverityError,
			File:     filePath,
			Line:     line,
			Message:  message,
			Context:  statementFindingContext(statementIndex, subjects...),
		}
	}
	if len(tables) == 0 {
		return []Finding{finding("DROP TABLE permanently deletes the table and every row in it; " + tableDroppedAdvice)}
	}

	ordered := slices.Clone(tables)
	slices.SortStableFunc(ordered, func(a, b tableReference) int {
		return strings.Compare(logicalObjectName(a.name), logicalObjectName(b.name))
	})
	findings := make([]Finding, 0, len(ordered))
	for _, table := range ordered {
		findings = append(findings, finding(
			fmt.Sprintf("DROP TABLE permanently deletes table %s and every row in it; %s", table.name, tableDroppedAdvice),
			Subject{Kind: SubjectTable, Name: table.name},
		))
	}
	return findings
}

// logicalObjectName reduces a source-spelled reference to the bare object name
// it denotes, so ordering compares what the reference means rather than how it
// was written: `public."Users"` and `"Users"` both order as `Users`. A
// reference that does not parse orders by its source text, which keeps the sort
// total instead of collapsing unparsable references onto one key.
func logicalObjectName(ref string) string {
	parsed, ok := tableref.Parse(ref)
	if !ok {
		return ref
	}
	return parsed.Name
}

// columnDroppedAdvice is the remediation every DROP COLUMN finding carries.
const columnDroppedAdvice = "deploy readers that no longer use the column first, then drop it in a later release"

// renameRetiredNameAdvice is the remediation every rename-derived DS finding
// carries. It is BC101's advice, so the two classifications of one rename (see
// [renamedNames]) cannot drift into prescribing different remediations.
const renameRetiredNameAdvice = "application versions deployed against the old name fail instantly; prefer add-new/backfill/drop-old across releases"

// columnDroppedRule is file-level rather than statement-level only so that the
// rename form can consult the tables this file created; a plain DROP COLUMN is
// reported exactly as before, with no create-then-drop exemption of its own.
// Whether DROP COLUMN should gain that exemption is a separate question -- the
// analyzer this tool is compatible with does exempt it, so Ptah is stricter
// there today -- and relaxing a destructive check is not something to do as a
// side effect of a rename change.
func columnDroppedRule() Rule {
	return Rule{
		Code:     "DS102",
		Title:    "column dropped",
		Severity: SeverityError,
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if !isAlterTable(stmt.Words) {
					continue
				}
				subjects := droppedColumnSubjects(stmt.Words, stmt.sourceWords)
				if len(subjects) == 0 {
					continue
				}
				findings = append(findings, Finding{
					Rule:     "DS102",
					Title:    "column dropped",
					Severity: SeverityError,
					File:     file.Path,
					Line:     stmt.Line,
					Message:  "DROP COLUMN permanently deletes the column's data; " + columnDroppedAdvice,
					Context:  statementFindingContext(i, subjects...),
				})
			}
			return append(findings, columnRenamedFindings(file)...)
		},
	}
}

// columnRenamedFindings reports the column names a file retires by renaming
// them, on the compatibility surface only. See [tableRenamedFindings].
//
// One finding per statement carrying every column it renames, in clause order
// -- the opposite shape from the table form above, and measured rather than
// assumed. `ALTER TABLE users RENAME COLUMN nick TO handle, RENAME COLUMN email
// TO mail` (MySQL grammar) is one diagnostic naming both columns, under a
// single suggested fix, in the order the clauses are written. That is the same
// shape a multi-clause DROP COLUMN produces, which is what the compatibility
// renderer's plural DS103 wording was measured and built for.
func columnRenamedFindings(file *File) []Finding {
	if file.compatibility != CompatibilityProfileAtlas {
		return nil
	}
	var findings []Finding
	for _, rename := range renamesOfKind(fileRenames(file), SubjectColumn) {
		findings = append(findings, Finding{
			Rule:     "DS102",
			Title:    "column dropped",
			Severity: SeverityError,
			File:     file.Path,
			Line:     rename.line,
			Message:  columnRenamedMessage(rename.names),
			Context:  statementFindingContext(rename.statementIndex, renamedColumnSubjects(rename.names)...),
		})
	}
	return findings
}

// columnRenamedMessage names every column the statement retires, or none when
// the statement is recognizably a rename whose retired names could not be read.
// A partial list is never rendered: naming some of the retired columns reads as
// a complete answer and is not one.
func columnRenamedMessage(names []renamedName) string {
	spelled := make([]string, 0, len(names))
	for _, name := range names {
		if name.name == "" {
			return "RENAME retires the column name; " + renameRetiredNameAdvice
		}
		spelled = append(spelled, name.name)
	}
	if len(spelled) == 1 {
		return fmt.Sprintf("RENAME retires the column name %s; %s", spelled[0], renameRetiredNameAdvice)
	}
	return fmt.Sprintf("RENAME retires the column names %s; %s", strings.Join(spelled, ", "), renameRetiredNameAdvice)
}

// renamedColumnSubjects builds the finding's subjects, failing as a unit for the
// same reason [columnRenamedMessage] does.
func renamedColumnSubjects(names []renamedName) []Subject {
	subjects := make([]Subject, 0, len(names))
	for _, name := range names {
		if name.name == "" {
			return nil
		}
		subjects = append(subjects, Subject{Kind: SubjectColumn, Name: name.name, Parent: name.parent})
	}
	return subjects
}

func columnTypeChangedRule() Rule {
	return Rule{
		Code:     "DS103",
		Title:    "column type changed",
		Severity: SeverityWarning,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !isAlterTable(stmt.Words) {
				return false, ""
			}
			if !scanModifyChange(stmt.Words) && !scanAlterColumnType(stmt.Words) {
				return false, ""
			}
			return true, "changing a column type can truncate or reject existing values and may rewrite the table under a lock; verify the old-to-new value mapping on production data first"
		},
	}
}

func notNullDroppedRule() Rule {
	return Rule{
		Code:     "DS104",
		Title:    "not-null constraint dropped",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !isAlterTable(stmt.Words) || !scanDropNotNull(stmt.Words) {
				return false, ""
			}
			return true, "DROP NOT NULL removes a column-level data protection; verify nullable values are accepted by every deployed reader and writer first"
		},
	}
}

// constraintDroppedRule (DS105) is the untyped fallback for the ANSI
// DROP CONSTRAINT <name> form, whose constraint type is not recoverable from the
// SQL. The typed MySQL-family forms (DROP FOREIGN KEY / CHECK / PRIMARY KEY) are
// reported by the more specific CD family, so a statement is never double-flagged.
func constraintDroppedRule() Rule {
	return Rule{
		Code:     "DS105",
		Title:    "constraint dropped",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !isAlterTable(stmt.Words) || !scanDropNamedConstraint(stmt.Words) {
				return false, ""
			}
			return true, "dropping a constraint removes an existing data protection; verify the replacement safety invariant before applying"
		},
	}
}

// alterTableDropRule builds an all-dialect, error-severity rule that fires when
// an ALTER TABLE statement matches the given typed constraint-drop predicate. It
// is the DROP-side analogue of postgresAlterRule and backs the CD family.
func alterTableDropRule(code, title string, scan func([]string) bool, message string) Rule {
	return Rule{
		Code:     code,
		Title:    title,
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !isAlterTable(stmt.Words) || !scan(stmt.Words) {
				return false, ""
			}
			return true, message
		},
	}
}

func foreignKeyDroppedRule() Rule {
	return alterTableDropRule("CD101", "foreign key dropped", scanDropForeignKey,
		"dropping a foreign key removes referential-integrity enforcement; verify no application invariant relies on the database rejecting orphan rows before applying")
}

func checkConstraintDroppedRule() Rule {
	return alterTableDropRule("CD102", "check constraint dropped", scanDropCheck,
		"dropping a check constraint removes a value-validation guarantee; verify no data invariant depends on it before applying")
}

func primaryKeyDroppedRule() Rule {
	return alterTableDropRule("CD103", "primary key dropped", scanDropPrimaryKey,
		"dropping a primary key removes row-uniqueness and identity enforcement and can break replication; verify a replacement key exists before applying")
}

func enumValueRemovedRule() Rule {
	return Rule{
		Code:     "DS106",
		Title:    "enum value removed",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !scanEnumValueRemoval(stmt.Words) {
				return false, ""
			}
			return true, "removing an enum value can invalidate existing rows; backfill rows away from the value before changing the enum"
		},
	}
}

func databaseObjectDroppedRule() Rule {
	return Rule{
		Code:     "DS107",
		Title:    "database object dropped",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !scanDestructiveObjectDrop(stmt.Words) {
				return false, ""
			}
			return true, "dropping a database object removes existing schema behavior or principals; verify all dependent code and data paths are retired first"
		},
	}
}

func tableTruncatedRule() Rule {
	return Rule{
		Code:     "DS108",
		Title:    "table truncated",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !hasWordPrefix(stmt.Words, "TRUNCATE") {
				return false, ""
			}
			return true, "TRUNCATE deletes all rows from the table; take a verified backup first and require an explicit destructive apply"
		},
	}
}

func rlsDisabledRule() Rule {
	return Rule{
		Code:     "DS109",
		Title:    "row-level security disabled",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !isAlterTable(stmt.Words) || !hasWordSeq(stmt.Words, "DISABLE", "ROW", "LEVEL", "SECURITY") {
				return false, ""
			}
			return true, "DISABLE ROW LEVEL SECURITY removes an access-control protection; verify replacement authorization before applying"
		},
	}
}

// dataDependentRules covers changes whose safety depends on existing row data.
const notNullWithoutDefaultAdvice = "fails or blocks on populated tables; add it nullable, backfill, then enforce NOT NULL in a later migration"

func dataDependentRules() []Rule {
	return []Rule{
		{
			Code:     "DD101",
			Title:    "non-nullable column added without a default",
			Severity: SeverityWarning,
			// File-level: adding a NOT NULL column to a table this same migration
			// created targets an empty table, so the add cannot fail on existing
			// rows and is exempt — the ADD-side analogue of the create-then-drop
			// exemption in DS101 (tableDroppedRule).
			CheckFile: notNullWithoutDefaultFindings,
		},
	}
}

func notNullWithoutDefaultFindings(file *File) []Finding {
	if !file.IsUp {
		return nil
	}
	var findings []Finding
	created := map[string]bool{}
	for i := range file.Statements {
		stmt := &file.Statements[i]
		if ref := createdTableRef(stmt.Words); ref != "" {
			created[ref] = true
			continue
		}
		if !isAlterTable(stmt.Words) {
			continue
		}
		subjects := nonNullableAddedColumnSubjects(stmt.Words, stmt.sourceWords)
		if len(subjects) == 0 {
			continue
		}
		if refersToCreated(created, alterTableReference(stmt.Words, stmt.sourceWords).normalized) {
			continue
		}
		findings = append(findings, notNullWithoutDefaultFindingsFor(file.Path, stmt.Line, i, subjects)...)
	}
	return append(findings, renamedColumnAddFindings(file)...)
}

// renamedColumnAddFindings reports the add side of a column rename, on the
// compatibility surface only.
//
// That surface models a rename structurally, as the retirement of one name plus
// the introduction of another (see [renamedNames]); the retirement is already
// reported as destructive, and this is its other half. If the retired column
// rejected NULL and had no default, the introduced name is a non-nullable column
// with no default, which is exactly what DD101 is about. Measured against the
// pinned community binary v1.3.0 on PostgreSQL 16: `CREATE TABLE users (id int
// NOT NULL)` in one file and `ALTER TABLE users RENAME COLUMN id TO oid` in the
// next reports BOTH the drop of "id" and the add of an `integer` column "oid",
// and exits 1.
//
// The native surface deliberately does not get this. It models a rename as a
// rename, and a rename does not fail on a populated table -- saying it would
// there would be a claim about the statement that is not true of the statement
// Ptah's own analyzer describes. On the compatibility surface the drop-plus-add
// model is the contract, so the claim follows the model.
//
// It needs facts the statement does not carry: the retired column's type,
// nullability and default live in an earlier file or the base schema. They come
// from [Options.Baseline], so a run without a dev database reports nothing here
// rather than guessing. A rename this file's own CREATE TABLE exempts is already
// gone from [fileRenames], which is the same exemption the loop above applies to
// a plain ADD COLUMN and is measured the same way.
func renamedColumnAddFindings(file *File) []Finding {
	var findings []Finding
	for _, rename := range renameAddSideCandidates(file) {
		for _, name := range rename.names {
			subject, ok := renamedColumnAddSubject(file, name)
			if !ok {
				continue
			}
			findings = append(findings, Finding{
				Rule:     "DD101",
				Title:    "non-nullable column added without a default",
				Severity: SeverityWarning,
				File:     file.Path,
				Line:     rename.line,
				Message: fmt.Sprintf(
					"RENAME introduces NOT NULL column %s without a DEFAULT, which %s",
					subject.Name,
					notNullWithoutDefaultAdvice,
				),
				Context: statementFindingContext(rename.statementIndex, subject),
			})
		}
	}
	return findings
}

// renamedColumnAddSubject resolves the column a rename introduces against the
// schema state its version starts from, and reports whether that column is the
// non-nullable, default-less shape DD101 covers.
//
// Every unresolved case returns false: an unreadable rename, a column the
// baseline does not carry, or no baseline at all. Reporting requires having
// established the column's shape, and a run that has not established it must not
// claim the migration will fail on data.
func renamedColumnAddSubject(file *File, name renamedName) (Subject, bool) {
	if name.introduced == "" || name.normalized == "" {
		return Subject{}, false
	}
	column, ok := file.baseline.column(name.owner, name.normalized)
	if !ok || !column.NotNull || column.HasDefault {
		return Subject{}, false
	}
	return Subject{
		Kind:     SubjectColumn,
		Name:     name.introduced,
		Parent:   name.parent,
		DataType: column.DataType,
	}, true
}

// notNullWithoutDefaultFindingsFor reports one finding per column an ALTER
// TABLE adds, for the same reason [tableDroppedFindings] does: each added
// column fails independently against existing rows, and a single finding
// carrying several of them left every column past the first unreported.
//
// Source order is preserved rather than sorted. A DROP TABLE comma list names
// tables whose written order means nothing, but ADD COLUMN clauses are the
// column order the operator wrote and will read the failure in -- and it is the
// order measured from the analyzer this tool is compatible with, which orders
// these by clause and dropped tables by name.
func notNullWithoutDefaultFindingsFor(filePath string, line, statementIndex int, subjects []Subject) []Finding {
	findings := make([]Finding, 0, len(subjects))
	for _, subject := range subjects {
		findings = append(findings, Finding{
			Rule:     "DD101",
			Title:    "non-nullable column added without a default",
			Severity: SeverityWarning,
			File:     filePath,
			Line:     line,
			Message: fmt.Sprintf(
				"adding NOT NULL column %s without a DEFAULT %s",
				subject.Name,
				notNullWithoutDefaultAdvice,
			),
			Context: statementFindingContext(statementIndex, subject),
		})
	}
	return findings
}

// migrationFormRules covers the MF family: file-level migration hygiene.
func migrationFormRules() []Rule {
	return []Rule{
		{
			Code:     "MF101",
			Title:    "missing down migration",
			Severity: SeverityWarning,
			CheckFile: func(file *File) []Finding {
				if !file.IsUp || file.HasPair {
					return nil
				}
				return []Finding{{
					Rule:     "MF101",
					Title:    "missing down migration",
					Severity: SeverityWarning,
					File:     file.Path,
					Message:  "no matching .down.sql exists; a failed deploy cannot be rolled back mechanically",
				}}
			},
		},
		{
			Code:     "MF102",
			Title:    "empty migration",
			Severity: SeverityWarning,
			CheckFile: func(file *File) []Finding {
				if !file.IsUp || len(file.Statements) > 0 {
					return nil
				}
				return []Finding{{
					Rule:     "MF102",
					Title:    "empty migration",
					Severity: SeverityWarning,
					File:     file.Path,
					Message:  "the migration contains no executable statements; delete it or fill it in",
				}}
			},
		},
		{
			Code:     "MF103",
			Title:    "non-conventional file name",
			Severity: SeverityWarning,
			CheckFile: func(file *File) []Finding {
				if file.WellFormedName {
					return nil
				}
				message := "file name does not match NNNNNNNNNN_description.(up|down).sql; the migrator will not pick it up"
				if file.Direction != "" {
					// Defense in depth: unreachable while the migrator's
					// parser matches the strict convention (#245 fixed its
					// lenient regexp), but if they ever diverge again a name
					// the migrator would RUN despite its odd spelling is more
					// surprising than one it skips, so say so.
					message = fmt.Sprintf("ambiguous file name: the migrator runs this as a %s migration even though it does not end in .%s.sql; rename it to NNNNNNNNNN_description.%s.sql", file.Direction, file.Direction, file.Direction)
				}
				return []Finding{{
					Rule:     "MF103",
					Title:    "non-conventional file name",
					Severity: SeverityWarning,
					File:     file.Path,
					Message:  message,
				}}
			},
		},
	}
}

// compatibilityRules covers the BC family: changes that break deployed code.
func compatibilityRules() []Rule {
	return []Rule{
		{
			Code:     "BC101",
			Title:    "rename breaks deployed code",
			Severity: SeverityWarning,
			// File-level so the same-file-created exemption in [fileRenames]
			// applies: renaming an object this migration itself introduced
			// breaks no deployed reader. One finding per statement regardless of
			// how many names the statement renames -- the advice is the same for
			// all of them and is about the statement, not about each name.
			CheckFile: func(file *File) []Finding {
				if file.compatibility == CompatibilityProfileAtlas {
					// The compatibility surface classifies the same event as a
					// destructive change instead; see [renamedNames].
					return nil
				}
				var findings []Finding
				for _, rename := range fileRenames(file) {
					findings = append(findings, Finding{
						Rule:     "BC101",
						Title:    "rename breaks deployed code",
						Severity: SeverityWarning,
						File:     file.Path,
						Line:     rename.line,
						Message:  "renames are not backwards compatible: " + renameRetiredNameAdvice,
						Context:  statementFindingContext(rename.statementIndex),
					})
				}
				return findings
			},
		},
	}
}

// postgresRules covers the PG family: PostgreSQL-specific hazards.
func postgresRules() []Rule {
	return []Rule{
		postgresCreateIndexRule(),
		postgresEnumAddValueRule(),
		postgresConcurrentIndexRule(),
		postgresAddPrimaryKeyRule(),
		postgresAddUniqueConstraintRule(),
		postgresDropIndexRule(),
		postgresColumnAlignmentRule(),
		postgresVolatileDefaultRule(),
		postgresSetNotNullRule(),
		postgresAddCheckRule(),
		postgresAddForeignKeyRule(),
		postgresSetPersistenceRule(),
		postgresCreateTriggerRule(),
		postgresAddStoredGeneratedRule(),
		postgresAddIdentityRule(),
		postgresSetAccessMethodRule(),
	}
}

func postgresCreateIndexRule() Rule {
	return Rule{
		Code:     "PG101",
		Title:    "index built with a table lock",
		Severity: SeverityWarning,
		Dialects: []string{"postgres"},
		// File-level: an index on a table this same migration created is
		// built on an empty table, so there is no lock hazard.
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			created := map[string]bool{}
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if ref := createdTableRef(stmt.Words); ref != "" {
					created[ref] = true
					continue
				}
				if !isCreateIndex(stmt.Words) || slices.Contains(stmt.Words, "CONCURRENTLY") {
					continue
				}
				if refersToCreated(created, indexTargetRef(stmt.Words)) {
					continue
				}
				findings = append(findings, Finding{
					Rule:     "PG101",
					Title:    "index built with a table lock",
					Severity: SeverityWarning,
					File:     file.Path,
					Line:     stmt.Line,
					Message:  "CREATE INDEX without CONCURRENTLY blocks writes to the table for the whole build; on a populated table use CREATE INDEX CONCURRENTLY outside a transaction",
					Context:  statementFindingContext(i),
				})
			}
			return findings
		},
	}
}

func postgresEnumAddValueRule() Rule {
	return postgresStatementRule("PG102", "enum value added inside a transaction", func(stmt *Statement) (bool, string) {
		if !hasWordPrefix(stmt.Words, "ALTER", "TYPE") || !hasWordSeq(stmt.Words, "ADD", "VALUE") {
			return false, ""
		}
		return true, "ALTER TYPE ... ADD VALUE cannot run inside a transaction block before PostgreSQL 12, and the new value stays unusable within the same transaction on newer versions; run it in its own non-transactional migration"
	})
}

func postgresConcurrentIndexRule() Rule {
	return Rule{
		Code:     "PG103",
		Title:    "concurrent index operation in a transactional migration",
		Severity: SeverityWarning,
		Dialects: []string{"postgres"},
		// Either direction: a down file carrying DROP INDEX CONCURRENTLY without
		// the no_transaction marker cannot execute at all, and the rollback half
		// of a concurrent build is exactly where that statement appears.
		CheckFile: func(file *File) []Finding {
			if (!file.IsUp && !file.IsDown) || file.NoTransaction {
				return nil
			}
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if !scanConcurrentIndexOperation(stmt.Words) {
					continue
				}
				findings = append(findings, Finding{
					Rule:     "PG103",
					Title:    "concurrent index operation in a transactional migration",
					Severity: SeverityWarning,
					File:     file.Path,
					Line:     stmt.Line,
					Message:  "CONCURRENTLY cannot run inside PostgreSQL's normal migration transaction; mark the migration non-transactional before applying",
					Context:  statementFindingContext(i),
				})
			}
			return findings
		},
	}
}

func postgresAddPrimaryKeyRule() Rule {
	return postgresAlterRule("PG104", "primary key added with an access-exclusive lock", scanAddPrimaryKey,
		"adding a primary key takes an ACCESS EXCLUSIVE lock and can scan existing rows; build a supporting unique index concurrently first, then attach it")
}

func postgresAddUniqueConstraintRule() Rule {
	return postgresAlterRule("PG105", "unique constraint added with an access-exclusive lock", scanAddUniqueConstraint,
		"adding a unique constraint takes an ACCESS EXCLUSIVE lock and validates existing rows; build a unique index concurrently first when the table is populated")
}

// postgresDropIndexRule reports a blocking index drop in either direction.
//
// The down half is where this rule earns its keep: the statement that removes
// an index is what a rollback file is normally made of, and PostgreSQL takes
// the same ACCESS EXCLUSIVE lock whichever direction asked for it. Confining
// statement rules to up files left a rollback that blocks writes on a populated
// table reported as clean -- including one Ptah generates itself, whenever the
// forward statement was not a concurrent build.
func postgresDropIndexRule() Rule {
	rule := postgresStatementRule("PG106", "index dropped with a table lock", func(stmt *Statement) (bool, string) {
		if !hasWordPrefix(stmt.Words, "DROP", "INDEX") || slices.Contains(stmt.Words, "CONCURRENTLY") {
			return false, ""
		}
		return true, "DROP INDEX without CONCURRENTLY blocks writes while PostgreSQL removes the index; use DROP INDEX CONCURRENTLY outside a transaction for populated tables"
	})
	rule.AppliesToDown = true
	return rule
}

func postgresColumnAlignmentRule() Rule {
	return postgresStatementRule("PG110", "non-optimal column alignment", func(stmt *Statement) (bool, string) {
		if !scanCreateTableMixedAlignment(stmt.Words) {
			return false, ""
		}
		return true, "this PostgreSQL column order can waste tuple padding; place wider fixed-size columns before narrow columns when creating large tables"
	})
}

func postgresVolatileDefaultRule() Rule {
	return postgresAlterRule("PG302", "volatile default rewrites existing rows", scanAddColumnWithVolatileDefault,
		"adding a column with a volatile DEFAULT rewrites or evaluates every existing row; add the column first, backfill in batches, then set the default")
}

func postgresSetNotNullRule() Rule {
	return postgresAlterRule("PG303", "not-null validation scans existing rows", scanSetNotNull,
		"SET NOT NULL scans the table to validate existing rows; backfill first and consider a validated CHECK constraint path on large tables")
}

func postgresAddCheckRule() Rule {
	return postgresAlterRule("PG305", "check constraint validates existing rows", scanAddCheckConstraint,
		"adding a CHECK constraint validates existing rows and can hold locks; add it NOT VALID first, then validate separately")
}

func postgresAddForeignKeyRule() Rule {
	return postgresAlterRule("PG306", "foreign key validates existing rows", scanAddForeignKey,
		"adding a foreign key validates existing rows and can block writes on both tables; add it NOT VALID first, then validate separately")
}

func postgresSetPersistenceRule() Rule {
	return postgresAlterRule("PG307", "table persistence changed", scanSetTablePersistence,
		"changing LOGGED/UNLOGGED rewrites the table and takes heavyweight locks; schedule it as a maintenance operation")
}

func postgresCreateTriggerRule() Rule {
	return postgresStatementRule("PG308", "trigger added with a write-blocking lock", func(stmt *Statement) (bool, string) {
		if !hasWordPrefix(stmt.Words, "CREATE", "TRIGGER") {
			return false, ""
		}
		return true, "CREATE TRIGGER takes a SHARE ROW EXCLUSIVE lock and can block concurrent writes; deploy during a quiet window on hot tables"
	})
}

func postgresAddStoredGeneratedRule() Rule {
	return postgresAlterRule("PG309", "stored generated column rewrites rows", scanAddStoredGeneratedColumn,
		"adding a STORED generated column computes and stores a value for every existing row; plan the rewrite and lock impact")
}

func postgresAddIdentityRule() Rule {
	return postgresAlterRule("PG310", "identity column rewrites rows", scanAddIdentityColumn,
		"adding an identity column can rewrite existing rows and requires sequence ownership changes; use a staged nullable column path on populated tables")
}

func postgresSetAccessMethodRule() Rule {
	return postgresAlterRule("PG311", "table access method changed", scanSetAccessMethod,
		"changing a table's access method rewrites the table; schedule it as a maintenance operation")
}

func postgresAlterRule(code, title string, scan func([]string) bool, message string) Rule {
	return postgresStatementRule(code, title, func(stmt *Statement) (bool, string) {
		if !isAlterTable(stmt.Words) || !scan(stmt.Words) {
			return false, ""
		}
		return true, message
	})
}

func postgresStatementRule(code, title string, check func(*Statement) (bool, string)) Rule {
	return Rule{
		Code:           code,
		Title:          title,
		Severity:       SeverityWarning,
		Dialects:       []string{"postgres"},
		CheckStatement: check,
	}
}

// mysqlRules covers the MY family: MySQL/MariaDB-specific hazards.
func mysqlRules() []Rule {
	return []Rule{
		{
			Code:     "MY101",
			Title:    "lock-heavy ALTER TABLE",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) {
					return false, ""
				}
				heavy := scanModifyChange(stmt.Words) || scanConvertCharset(stmt.Words) ||
					scanAddColumnNotNullWithoutDefault(stmt.Words)
				if !heavy || scanPinnedOnlineDDL(stmt.Words) {
					return false, ""
				}
				return true, "this ALTER TABLE form usually rebuilds the table and blocks writes for the duration on MySQL/MariaDB; " +
					"for large tables use an online-DDL tool (gh-ost, pt-online-schema-change), or pin ALGORITHM=INPLACE/LOCK=NONE " +
					"so the server refuses a blocking rebuild instead of performing it"
			},
		},
		{
			Code:     "MY102",
			Title:    "inline reference ignored on added column",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !scanAddColumnInlineReference(stmt.Words) {
					return false, ""
				}
				return true, "MySQL ignores inline REFERENCES in ADD COLUMN; add an explicit FOREIGN KEY constraint instead"
			},
		},
		{
			Code:     "MY131",
			Title:    "foreign key added with blocking DDL",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !scanAddForeignKey(stmt.Words) {
					return false, ""
				}
				return true, "adding a foreign key can copy or lock the table and block writes on MySQL/MariaDB; use an online migration plan for populated tables"
			},
		},
		{
			Code:     "MY132",
			Title:    "primary key added with table rebuild",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !scanAddPrimaryKey(stmt.Words) {
					return false, ""
				}
				return true, "adding a primary key rebuilds the table and blocks DML on MySQL/MariaDB; use a staged online-DDL path for large tables"
			},
		},
		{
			Code:     "MY134",
			Title:    "fulltext index added with blocking DDL",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !scanAddIndexKind(stmt.Words, "FULLTEXT") {
					return false, ""
				}
				return true, "adding a FULLTEXT index can rebuild the table and block writes on MySQL/MariaDB; use an online-DDL strategy for populated tables"
			},
		},
		{
			Code:     "MY135",
			Title:    "spatial index added with blocking DDL",
			Severity: SeverityWarning,
			Dialects: []string{"mysql", "mariadb"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !scanAddIndexKind(stmt.Words, "SPATIAL") {
					return false, ""
				}
				return true, "adding a SPATIAL index can rebuild the table and block writes on MySQL/MariaDB; use an online-DDL strategy for populated tables"
			},
		},
	}
}

func sqliteRules() []Rule {
	return []Rule{
		{
			Code:     "LT101",
			Title:    "not-null constraint added without a default",
			Severity: SeverityWarning,
			Dialects: []string{"sqlite"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !scanSetNotNull(stmt.Words) {
					return false, ""
				}
				return true, "SQLite cannot safely enforce NOT NULL on existing nullable data without a staged rebuild and backfill"
			},
		},
	}
}

func transactionRules() []Rule {
	return []Rule{
		{
			Code:     "TX101",
			Title:    "transactional and non-transactional statements mixed",
			Severity: SeverityWarning,
			Dialects: []string{"postgres"},
			// Either direction: the migrator wraps a down file in the same
			// transaction it wraps an up file in, so a rollback that mixes
			// CREATE INDEX CONCURRENTLY with transactional DDL is refused by
			// PostgreSQL exactly the same way.
			CheckFile: func(file *File) []Finding {
				if (!file.IsUp && !file.IsDown) || file.NoTransaction {
					return nil
				}
				var nonTransactional *Statement
				transactional := false
				for i := range file.Statements {
					stmt := &file.Statements[i]
					if isTransactionControlStatement(stmt.Words) {
						continue
					}
					if isPostgresNonTransactionalStatement(stmt.Words) {
						nonTransactional = stmt
						continue
					}
					transactional = true
				}
				if nonTransactional == nil || !transactional {
					return nil
				}
				return []Finding{{
					Rule:     "TX101",
					Title:    "transactional and non-transactional statements mixed",
					Severity: SeverityWarning,
					File:     file.Path,
					Line:     nonTransactional.Line,
					Message:  "this migration mixes PostgreSQL statements that require autocommit with transactional DDL; split them into separate migrations",
					Context:  statementFindingContext(nonTransactional.Index),
				}}
			},
		},
		{
			Code:     "TX201",
			Title:    "transaction block embedded in migration",
			Severity: SeverityWarning,
			Dialects: []string{"postgres"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !hasWordPrefix(stmt.Words, "BEGIN") && !hasWordPrefix(stmt.Words, "START", "TRANSACTION") {
					return false, ""
				}
				return true, "explicit transaction blocks conflict with the migrator's transaction management; remove BEGIN/COMMIT or mark the migration non-transactional"
			},
		},
	}
}

// dropTargets are the keywords that can follow ALTER TABLE ... DROP when the
// clause drops something other than a column: constraints, the key family,
// partitioning, system versioning. A column with one of these names must be
// quoted to be valid SQL, and quoted identifiers keep their quotes in Words,
// so real columns never collide with this set.
var dropTargets = map[string]bool{
	"CONSTRAINT":   true,
	"INDEX":        true,
	"KEY":          true,
	"FOREIGN":      true,
	"PRIMARY":      true,
	"UNIQUE":       true,
	"CHECK":        true,
	"PARTITION":    true,
	"PARTITIONING": true,
	"SYSTEM":       true,
}

// isAlterTable reports whether the statement's words begin with ALTER TABLE.
func isAlterTable(w []string) bool {
	return hasWordPrefix(w, "ALTER", "TABLE")
}

// isCreateIndex reports whether the statement's words begin with
// CREATE [UNIQUE] INDEX.
func isCreateIndex(w []string) bool {
	if len(w) == 0 || w[0] != "CREATE" {
		return false
	}
	j := 1
	if j < len(w) && w[j] == "UNIQUE" {
		j++
	}
	return j < len(w) && w[j] == "INDEX"
}

// hasWordPrefix reports whether the word sequence starts with the given words.
func hasWordPrefix(w []string, prefix ...string) bool {
	if len(w) < len(prefix) {
		return false
	}
	for i, p := range prefix {
		if w[i] != p {
			return false
		}
	}
	return true
}

// hasWordSeq reports whether the given words appear consecutively anywhere in
// the sequence.
func hasWordSeq(w []string, seq ...string) bool {
	for i := 0; i+len(seq) <= len(w); i++ {
		if hasWordPrefix(w[i:], seq...) {
			return true
		}
	}
	return false
}

// identLike reports whether a word can name a column or table: a quoted
// identifier, or a bare word starting with a letter, underscore, or digit.
// Single-quoted string literals are values, never identifiers.
func identLike(word string) bool {
	if word == "" {
		return false
	}
	switch word[0] {
	case '`', '"':
		return true
	case '\'':
		return false
	}
	r, _ := utf8.DecodeRuneInString(word)
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// skipIfExists advances past an optional IF EXISTS at w[j].
func skipIfExists(w []string, j int) int {
	if j+1 < len(w) && w[j] == "IF" && w[j+1] == "EXISTS" {
		return j + 2
	}
	return j
}

// clauseStarts returns the word indices where the clauses of an ALTER TABLE
// statement begin: the first word after the table reference and each word
// after a top-level comma. Commas inside parentheses (type parameters,
// expressions) do not start a clause. Anchoring scans to clause heads keeps
// clause keywords (DROP, MODIFY, RENAME, ...) from being confused with
// column names appearing mid-clause, e.g. ADD COLUMN modify TEXT.
func clauseStarts(w []string) []int {
	j := 2
	// PostgreSQL grammar: ALTER TABLE [ IF EXISTS ] [ ONLY ] name [ * ].
	// Skip the modifiers in any order to stay robust.
	for j < len(w) {
		if w[j] == "ONLY" {
			j++
			continue
		}
		if next := skipIfExists(w, j); next != j {
			j = next
			continue
		}
		break
	}
	if ref, next := tableRefAt(w, w, j); ref.normalized != "" {
		j = next
		if j < len(w) && w[j] == "*" {
			j++ // postgres: name * (include descendant tables)
		}
	}
	starts := []int{j}
	depth := 0
	for k := j; k < len(w); k++ {
		switch w[k] {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		case ",":
			if depth == 0 {
				starts = append(starts, k+1)
			}
		}
	}
	return starts
}

// droppedColumnSubjects returns the columns an ALTER TABLE statement drops.
// The COLUMN keyword is optional in PostgreSQL and the MySQL family, so a
// clause-head DROP followed by an identifier counts unless the identifier is
// a known non-column DROP target (DROP CONSTRAINT, DROP PRIMARY KEY, ...).
func droppedColumnSubjects(w, sourceWords []string) []Subject {
	table := alterTableReference(w, sourceWords)
	var subjects []Subject
	for _, i := range clauseStarts(w) {
		if i >= len(w) || w[i] != "DROP" {
			continue
		}
		j := i + 1
		explicit := false
		if j < len(w) && w[j] == "COLUMN" {
			explicit = true
			j++
		}
		j = skipIfExists(w, j)
		if j >= len(w) {
			continue
		}
		if !explicit && dropTargets[w[j]] {
			continue
		}
		if identLike(w[j]) {
			subjects = append(subjects, Subject{
				Kind:   SubjectColumn,
				Name:   sourceWordAt(w, sourceWords, j),
				Parent: table.name,
			})
		}
	}
	return subjects
}

// scanModifyChange reports whether an ALTER TABLE statement rewrites a column
// via the MySQL-family MODIFY/CHANGE clauses (COLUMN keyword optional).
func scanModifyChange(w []string) bool {
	for _, i := range clauseStarts(w) {
		if i >= len(w) || (w[i] != "MODIFY" && w[i] != "CHANGE") {
			continue
		}
		j := i + 1
		if j < len(w) && w[j] == "COLUMN" {
			j++
		}
		j = skipIfExists(w, j)
		if j < len(w) && identLike(w[j]) {
			return true
		}
	}
	return false
}

// scanAlterColumnType reports whether an ALTER TABLE statement changes a
// column's type via ALTER [COLUMN] name [SET DATA] TYPE. The ordered scan
// keeps a column merely named "type" (e.g. ALTER COLUMN type SET NOT NULL)
// from matching.
func scanAlterColumnType(w []string) bool {
	for _, i := range clauseStarts(w) {
		if i >= len(w) || w[i] != "ALTER" {
			continue
		}
		j := i + 1
		if j < len(w) && w[j] == "COLUMN" {
			j++
		}
		j = skipIfExists(w, j)
		if j >= len(w) || !identLike(w[j]) {
			continue
		}
		k := j + 1
		if k+1 < len(w) && w[k] == "SET" && w[k+1] == "DATA" {
			k += 2
		}
		if k < len(w) && w[k] == "TYPE" {
			return true
		}
	}
	return false
}

// scanDropNotNull reports whether an ALTER TABLE statement removes a column's
// NOT NULL attribute via ALTER [COLUMN] name DROP NOT NULL.
func scanDropNotNull(w []string) bool {
	for _, i := range clauseStarts(w) {
		if i >= len(w) || w[i] != "ALTER" {
			continue
		}
		j := i + 1
		if j < len(w) && w[j] == "COLUMN" {
			j++
		}
		j = skipIfExists(w, j)
		if j >= len(w) || !identLike(w[j]) {
			continue
		}
		if hasWordPrefix(w[j+1:], "DROP", "NOT", "NULL") {
			return true
		}
	}
	return false
}

// scanDropClause reports whether any clause of an ALTER TABLE statement begins
// with the given word sequence, anchoring on the clause heads so that only a
// top-level DROP form matches. It backs the typed constraint-drop predicates.
func scanDropClause(w []string, prefix ...string) bool {
	for _, i := range clauseStarts(w) {
		if hasWordPrefix(w[i:], prefix...) {
			return true
		}
	}
	return false
}

// scanDropForeignKey reports whether an ALTER TABLE statement drops a foreign
// key via the MySQL-family DROP FOREIGN KEY form. The ANSI DROP CONSTRAINT
// <name> form does not encode the constraint type, so it is not matched here.
func scanDropForeignKey(w []string) bool { return scanDropClause(w, "DROP", "FOREIGN", "KEY") }

// scanDropCheck reports whether an ALTER TABLE statement drops a check
// constraint via the DROP CHECK form.
func scanDropCheck(w []string) bool { return scanDropClause(w, "DROP", "CHECK") }

// scanDropPrimaryKey reports whether an ALTER TABLE statement drops a primary
// key via the DROP PRIMARY KEY form.
func scanDropPrimaryKey(w []string) bool { return scanDropClause(w, "DROP", "PRIMARY", "KEY") }

// scanDropNamedConstraint reports whether an ALTER TABLE statement drops a
// constraint by name via the ANSI DROP CONSTRAINT <name> form, whose constraint
// type is not recoverable from the SQL. This is the untyped fallback (DS105);
// the typed forms above are reported by the CD family.
func scanDropNamedConstraint(w []string) bool { return scanDropClause(w, "DROP", "CONSTRAINT") }

func scanEnumValueRemoval(w []string) bool {
	return hasWordSeq(w, "DELETE", "FROM", "PG_ENUM") ||
		hasWordSeq(w, "DROP", "VALUE")
}

func scanDestructiveObjectDrop(w []string) bool {
	if len(w) < 2 || w[0] != "DROP" {
		return false
	}
	switch w[1] {
	case "TYPE", "EXTENSION", "FUNCTION", "ROLE", "POLICY", "SCHEMA":
		return true
	default:
		return false
	}
}

// scanConvertCharset reports whether an ALTER TABLE statement converts the
// table to another character set (a full-table rewrite on MySQL/MariaDB).
// CHARSET is the accepted synonym of CHARACTER SET.
func scanConvertCharset(w []string) bool {
	for _, i := range clauseStarts(w) {
		if hasWordPrefix(w[i:], "CONVERT", "TO", "CHARACTER", "SET") ||
			hasWordPrefix(w[i:], "CONVERT", "TO", "CHARSET") {
			return true
		}
	}
	return false
}

func scanAddColumnNotNullWithoutDefault(w []string) bool {
	return len(nonNullableAddedColumnSubjects(w, w)) > 0
}

func nonNullableAddedColumnSubjects(w, sourceWords []string) []Subject {
	table := alterTableReference(w, sourceWords)
	var subjects []Subject
	for _, i := range clauseStarts(w) {
		start, end, ok := addColumnClause(w, i)
		if !ok {
			continue
		}
		clause := w[start:end]
		if hasWordSeq(clause, "NOT", "NULL") && !slices.Contains(clause, "DEFAULT") {
			subjects = append(subjects, Subject{
				Kind:     SubjectColumn,
				Name:     sourceWordAt(w, sourceWords, start-1),
				Parent:   table.name,
				DataType: columnDataType(clause, sourceWords[start:end]),
			})
		}
	}
	return subjects
}

func columnDataType(definition, sourceDefinition []string) string {
	end := len(definition)
	depth := 0
	for i, word := range definition {
		switch word {
		case "(", "[":
			depth++
		case ")", "]":
			depth = max(0, depth-1)
		default:
			if depth == 0 && columnConstraintWord(word) {
				end = i
				return strings.Join(sourceDefinition[:end], " ")
			}
		}
	}
	return strings.Join(sourceDefinition[:end], " ")
}

func columnConstraintWord(word string) bool {
	switch word {
	case "CHECK", "COLLATE", "COMMENT", "CONSTRAINT", "DEFAULT", "GENERATED",
		"IDENTITY", "NOT", "NULL", "PRIMARY", "REFERENCES", "UNIQUE":
		return true
	default:
		return false
	}
}

func scanAddColumnWithVolatileDefault(w []string) bool {
	for _, i := range clauseStarts(w) {
		start, end, ok := addColumnClause(w, i)
		if !ok {
			continue
		}
		clause := w[start:end]
		for j := range clause {
			if clause[j] == "DEFAULT" && j+1 < len(clause) && isVolatilePostgresDefault(clause[j+1]) {
				return true
			}
		}
	}
	return false
}

func scanAddColumnInlineReference(w []string) bool {
	for _, i := range clauseStarts(w) {
		start, end, ok := addColumnClause(w, i)
		if ok && slices.Contains(w[start:end], "REFERENCES") {
			return true
		}
	}
	return false
}

func scanAddStoredGeneratedColumn(w []string) bool {
	for _, i := range clauseStarts(w) {
		start, end, ok := addColumnClause(w, i)
		if ok && hasWordSeq(w[start:end], "GENERATED", "ALWAYS", "AS") && slices.Contains(w[start:end], "STORED") {
			return true
		}
	}
	return false
}

func scanAddIdentityColumn(w []string) bool {
	for _, i := range clauseStarts(w) {
		start, end, ok := addColumnClause(w, i)
		if ok && hasWordSeq(w[start:end], "GENERATED", "ALWAYS", "AS", "IDENTITY") {
			return true
		}
	}
	return false
}

func addColumnClause(w []string, i int) (start int, end int, ok bool) {
	if i >= len(w) || w[i] != "ADD" {
		return 0, 0, false
	}
	j := i + 1
	if j < len(w) && w[j] == "COLUMN" {
		j++
	}
	if j < len(w) && addConstraintTargets[w[j]] {
		return 0, 0, false
	}
	j = skipIfNotExists(w, j)
	if j >= len(w) || !identLike(w[j]) {
		return 0, 0, false
	}
	return j + 1, clauseEnd(w, i), true
}

var addConstraintTargets = map[string]bool{
	"CHECK":      true,
	"CONSTRAINT": true,
	"FOREIGN":    true,
	"FULLTEXT":   true,
	"INDEX":      true,
	"KEY":        true,
	"PRIMARY":    true,
	"SPATIAL":    true,
	"UNIQUE":     true,
}

func skipIfNotExists(w []string, j int) int {
	if j+2 < len(w) && w[j] == "IF" && w[j+1] == "NOT" && w[j+2] == "EXISTS" {
		return j + 3
	}
	return j
}

func clauseEnd(w []string, start int) int {
	depth := 0
	for i := start; i < len(w); i++ {
		switch w[i] {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		case ",":
			if depth == 0 {
				return i
			}
		}
	}
	return len(w)
}

func isVolatilePostgresDefault(word string) bool {
	switch strings.Trim(word, "\"`") {
	case "GEN_RANDOM_UUID", "UUID_GENERATE_V4", "NOW", "CLOCK_TIMESTAMP", "RANDOM":
		return true
	default:
		return false
	}
}

func scanConcurrentIndexOperation(w []string) bool {
	if isCreateIndex(w) {
		return slices.Contains(w, "CONCURRENTLY")
	}
	return hasWordPrefix(w, "DROP", "INDEX") && slices.Contains(w, "CONCURRENTLY")
}

func scanAddPrimaryKey(w []string) bool {
	for _, i := range clauseStarts(w) {
		if hasWordPrefix(w[i:], "ADD", "PRIMARY", "KEY") ||
			hasWordPrefix(w[i:], "ADD", "CONSTRAINT") && slices.Contains(w[i:clauseEnd(w, i)], "PRIMARY") {
			return true
		}
	}
	return false
}

func scanAddUniqueConstraint(w []string) bool {
	for _, i := range clauseStarts(w) {
		clause := w[i:clauseEnd(w, i)]
		if hasWordPrefix(clause, "ADD", "UNIQUE") ||
			hasWordPrefix(clause, "ADD", "CONSTRAINT") && slices.Contains(clause, "UNIQUE") {
			return true
		}
	}
	return false
}

func scanAddCheckConstraint(w []string) bool {
	for _, i := range clauseStarts(w) {
		clause := w[i:clauseEnd(w, i)]
		if hasWordPrefix(clause, "ADD", "CHECK") ||
			hasWordPrefix(clause, "ADD", "CONSTRAINT") && slices.Contains(clause, "CHECK") {
			return true
		}
	}
	return false
}

func scanAddForeignKey(w []string) bool {
	for _, i := range clauseStarts(w) {
		clause := w[i:clauseEnd(w, i)]
		if hasWordPrefix(clause, "ADD", "FOREIGN", "KEY") ||
			hasWordPrefix(clause, "ADD", "CONSTRAINT") && hasWordSeq(clause, "FOREIGN", "KEY") {
			return true
		}
	}
	return false
}

func scanSetTablePersistence(w []string) bool {
	for _, i := range clauseStarts(w) {
		if hasWordPrefix(w[i:], "SET", "LOGGED") || hasWordPrefix(w[i:], "SET", "UNLOGGED") {
			return true
		}
	}
	return false
}

func scanSetAccessMethod(w []string) bool {
	for _, i := range clauseStarts(w) {
		if hasWordPrefix(w[i:], "SET", "ACCESS", "METHOD") {
			return true
		}
	}
	return false
}

func scanSetNotNull(w []string) bool {
	for _, i := range clauseStarts(w) {
		if hasWordPrefix(w[i:], "ALTER", "COLUMN") && hasWordSeq(w[i:], "SET", "NOT", "NULL") {
			return true
		}
		if hasWordPrefix(w[i:], "ALTER") && len(w[i:]) > 1 && identLike(w[i+1]) && hasWordSeq(w[i:], "SET", "NOT", "NULL") {
			return true
		}
	}
	return false
}

func scanAddIndexKind(w []string, kind string) bool {
	for _, i := range clauseStarts(w) {
		clause := w[i:clauseEnd(w, i)]
		if !hasWordPrefix(clause, "ADD") || !slices.Contains(clause, kind) {
			continue
		}
		if slices.Contains(clause, "INDEX") || slices.Contains(clause, "KEY") {
			return true
		}
	}
	return false
}

func scanCreateTableMixedAlignment(w []string) bool {
	if !hasWordPrefix(w, "CREATE", "TABLE") {
		return false
	}
	open := slices.Index(w, "(")
	closeIndex := len(w) - 1
	for closeIndex > open && w[closeIndex] != ")" {
		closeIndex--
	}
	if open < 0 || closeIndex <= open {
		return false
	}
	seenNarrow := false
	for i := open + 1; i < closeIndex; i++ {
		if w[i] == "," {
			continue
		}
		if !identLike(w[i]) || i+1 >= closeIndex {
			continue
		}
		width := postgresTypeWidthClass(w[i+1])
		if width == 0 {
			continue
		}
		if width == 1 {
			seenNarrow = true
			continue
		}
		if seenNarrow && width > 1 {
			return true
		}
	}
	return false
}

func postgresTypeWidthClass(word string) int {
	switch strings.Trim(word, "\"`") {
	case "BOOL", "BOOLEAN", "CHAR", "SMALLINT", "INT2":
		return 1
	case "BIGINT", "INT8", "BIGSERIAL", "DOUBLE", "TIMESTAMP", "TIMESTAMPTZ", "UUID":
		return 2
	default:
		return 0
	}
}

func isTransactionControlStatement(w []string) bool {
	return hasWordPrefix(w, "BEGIN") ||
		hasWordPrefix(w, "START", "TRANSACTION") ||
		hasWordPrefix(w, "COMMIT") ||
		hasWordPrefix(w, "ROLLBACK")
}

func isPostgresNonTransactionalStatement(w []string) bool {
	return scanConcurrentIndexOperation(w)
}

// createdTableRef returns the normalized reference of the table a CREATE
// TABLE statement creates, or "" for any other statement. Temporary-table
// modifiers and IF NOT EXISTS are skipped.
func createdTableRef(w []string) string {
	if len(w) == 0 || w[0] != "CREATE" {
		return ""
	}
	j := 1
	for j < len(w) {
		switch w[j] {
		case "GLOBAL", "LOCAL", "TEMPORARY", "TEMP", "UNLOGGED":
			j++
			continue
		}
		break
	}
	if j >= len(w) || w[j] != "TABLE" {
		return ""
	}
	j++
	if j+2 < len(w) && w[j] == "IF" && w[j+1] == "NOT" && w[j+2] == "EXISTS" {
		j += 3
	}
	ref, _ := tableRefAt(w, w, j)
	return ref.normalized
}

// tableRefAt reads a possibly schema-qualified table reference at w[j] and
// returns separate display and comparison forms plus the index past its end.
func tableRefAt(w, sourceWords []string, j int) (tableReference, int) {
	identifier, next, ok := identifierAt(w, sourceWords, j)
	if !ok {
		return tableReference{}, j
	}
	displayParts := []string{identifier.name}
	normalizedParts := []string{identifier.normalized}
	j = next
	for j < len(w) && w[j] == "." {
		identifier, next, ok = identifierAt(w, sourceWords, j+1)
		if !ok {
			break
		}
		displayParts = append(displayParts, identifier.name)
		normalizedParts = append(normalizedParts, identifier.normalized)
		j = next
	}
	return tableReference{
		name:       strings.Join(displayParts, "."),
		normalized: strings.Join(normalizedParts, "."),
	}, j
}

func identifierAt(w, sourceWords []string, index int) (tableReference, int, bool) {
	if index >= len(w) {
		return tableReference{}, index, false
	}
	if identLike(w[index]) {
		return tableReference{
			name:       sourceWordAt(w, sourceWords, index),
			normalized: normalizeIdent(w[index]),
		}, index + 1, true
	}
	if w[index] != "[" {
		return tableReference{}, index, false
	}
	end := index + 1
	for end < len(w) && w[end] != "]" {
		end++
	}
	if end == index+1 || end >= len(w) {
		return tableReference{}, index, false
	}
	return tableReference{
		name:       "[" + strings.Join(sourceWords[index+1:end], " ") + "]",
		normalized: strings.ToUpper(strings.Join(w[index+1:end], " ")),
	}, end + 1, true
}

type tableReference struct {
	name       string
	normalized string
}

func alterTableReference(w, sourceWords []string) tableReference {
	if !hasWordPrefix(w, "ALTER", "TABLE") {
		return tableReference{}
	}
	j := 2
	for j < len(w) {
		if w[j] == "ONLY" {
			j++
			continue
		}
		if next := skipIfExists(w, j); next != j {
			j = next
			continue
		}
		break
	}
	ref, _ := tableRefAt(w, sourceWords, j)
	return ref
}

func sourceWordAt(words, sourceWords []string, index int) string {
	if index < len(sourceWords) {
		return sourceWords[index]
	}
	return words[index]
}

// normalizeIdent strips identifier quoting and uppercases for comparison.
func normalizeIdent(word string) string {
	return strings.ToUpper(strings.Trim(word, "`\""))
}

// refersToCreated reports whether ref names one of the created tables,
// comparing full references when both sides are schema-qualified and last
// components otherwise.
func refersToCreated(created map[string]bool, ref string) bool {
	if ref == "" {
		return false
	}
	if created[ref] {
		return true
	}
	last := ref[strings.LastIndex(ref, ".")+1:]
	for c := range created {
		if c[strings.LastIndex(c, ".")+1:] != last {
			continue
		}
		if !strings.Contains(ref, ".") || !strings.Contains(c, ".") {
			return true
		}
	}
	return false
}

// droppedTablesNotCreated returns tables that were not created earlier in the
// same file and whether every target was parsed. An incomplete parse must remain
// destructive because treating an unknown target as safe would be fail-open.
func droppedTablesNotCreated(
	w []string,
	sourceWords []string,
	created map[string]bool,
) ([]tableReference, bool) {
	var unsafe []tableReference
	j := skipIfExists(w, 2)
	for {
		if j < len(w) && w[j] == "ONLY" {
			j++
		}
		ref, next := tableRefAt(w, sourceWords, j)
		if ref.normalized == "" {
			return unsafe, false
		}
		if !refersToCreated(created, ref.normalized) {
			unsafe = append(unsafe, ref)
		}
		if next < len(w) && w[next] == "*" {
			next++
		}
		if next < len(w) && w[next] == "," {
			j = next + 1
			continue
		}
		return unsafe, true
	}
}

// indexTargetRef extracts the table a CREATE INDEX statement builds on (the
// reference after ON).
func indexTargetRef(w []string) string {
	for k := range w {
		if w[k] == "ON" {
			ref, _ := tableRefAt(w, w, k+1)
			return ref.normalized
		}
	}
	return ""
}

// scanPinnedOnlineDDL reports whether an ALTER TABLE statement pins a
// non-blocking online-DDL path: with ALGORITHM=INPLACE/INSTANT or LOCK=NONE
// the server errors out instead of silently falling back to a locking
// rebuild, so the lock hazard cannot occur. The = is optional in the MySQL
// grammar.
func scanPinnedOnlineDDL(w []string) bool {
	for _, i := range clauseStarts(w) {
		if i >= len(w) || (w[i] != "ALGORITHM" && w[i] != "LOCK") {
			continue
		}
		j := i + 1
		if j < len(w) && w[j] == "=" {
			j++
		}
		if j >= len(w) {
			continue
		}
		if w[i] == "ALGORITHM" && (w[j] == "INPLACE" || w[j] == "INSTANT") {
			return true
		}
		if w[i] == "LOCK" && w[j] == "NONE" {
			return true
		}
	}
	return false
}

// Describe renders one finding as a single human-readable line.
func Describe(f Finding) string {
	location := f.File
	if f.Line > 0 {
		location = fmt.Sprintf("%s:%d", f.File, f.Line)
	}
	return fmt.Sprintf("%s [%s] %s: %s (%s)", location, f.Severity, f.Rule, f.Message, f.Title)
}
