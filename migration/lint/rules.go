package lint

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/tableref"
)

// Rules returns the rules a lint run with runtime applies: the rules this
// package checks itself, then every rule an engine owner publishes through
// runtime, in code order. It runs nothing, so a catalog reads it to enumerate
// what a run can report.
//
// An owner's rule carries neither [Rule.CheckStatement] nor [Rule.CheckFile]:
// the owner checks it once per run, through [lintext.Runtime.CheckMigrations],
// and an analysis reads its findings from that answer. The slice is rebuilt
// on every call. A nil runtime, and a runtime that cannot list its rules, is
// an error.
func Rules(runtime lintext.Runtime) ([]Rule, error) {
	if runtime == nil {
		return nil, errors.New("lint rules require an engine runtime")
	}
	owned, err := ownerRules(runtime)
	if err != nil {
		return nil, err
	}
	return append(builtinRules(), owned...), nil
}

// ownerRules are the rules every owner in runtime publishes, as rules of this
// package that name the targets each judges as its dialects.
func ownerRules(runtime lintext.Runtime) ([]Rule, error) {
	published, err := runtime.LintRules("")
	if err != nil {
		return nil, fmt.Errorf("read the lint rules the engine owners publish: %w", err)
	}
	rules := make([]Rule, 0, len(published))
	for _, rule := range published {
		input, err := ruleInputOf(rule.Input)
		if err != nil {
			return nil, fmt.Errorf("lint rule %s: %w", rule.Code, err)
		}
		rules = append(rules, Rule{
			Code:          rule.Code,
			Title:         rule.Title,
			Severity:      Severity(rule.Severity),
			Dialects:      slices.Clone(rule.Targets),
			AppliesToDown: rule.AppliesToDown,
			Input:         input,
			Subsumes:      slices.Clone(rule.Subsumes),
			owned:         true,
		})
	}
	return rules, nil
}

// ruleInputOf is the input of this package that a published input names.
func ruleInputOf(input lintext.Input) (RuleInput, error) {
	switch input {
	case lintext.InputStatementText:
		return InputStatementText, nil
	case lintext.InputRoutineBody:
		return InputRoutineBody, nil
	case lintext.InputBaselineSchema:
		return InputBaselineSchema, nil
	case lintext.InputBaselineRefinement:
		return InputBaselineRefinement, nil
	default:
		return 0, fmt.Errorf("unknown input %q", input)
	}
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
	rules = append(rules, uniqueRules()...)
	rules = append(rules, notNullRules()...)
	rules = append(rules, namingRules()...)
	rules = append(rules, injectionRules()...)
	rules = append(rules, sqliteRules()...)
	rules = append(rules, atlasGapRules()...)
	rules = append(rules, ydbRules()...)
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
	if rule.owned {
		// Its owner checks it and names its baseline subjects in the answer,
		// so it carries no function for the checks below to require.
		return nil
	}
	if (rule.CheckStatement != nil) == (rule.CheckFile != nil) {
		return fmt.Errorf("rule %s must set exactly one checker", rule.Code)
	}
	// A declaration nothing enforces is a comment. These two make the input a
	// property of the rule the analysis can act on: a rule asking for the
	// baseline must say which statements it wants it for, or it would ask for
	// every version and resolve nothing; a rule that reads text must not carry
	// the predicate, or a reader has to work out which of the two fields is the
	// real answer (stokaro/ptah#1632).
	switch rule.Input {
	case InputBaselineSchema, InputBaselineRefinement:
		if rule.BaselineSubjects == nil {
			return fmt.Errorf(
				"rule %s declares the %s input and must set BaselineSubjects to say which statements need it",
				rule.Code, rule.Input)
		}
	case InputRoutineBody:
		// No subjects predicate: which statements need a body is not a choice
		// the rule makes, it is which of them define a routine, and the
		// analysis already knows that (stokaro/ptah#2357).
		if rule.BaselineSubjects != nil {
			return fmt.Errorf(
				"rule %s sets BaselineSubjects but declares the %s input; set Input to InputBaselineSchema",
				rule.Code, rule.Input)
		}
	case InputStatementText:
		if rule.BaselineSubjects != nil {
			return fmt.Errorf(
				"rule %s sets BaselineSubjects but declares the %s input; set Input to InputBaselineSchema",
				rule.Code, rule.Input)
		}
	default:
		return fmt.Errorf("rule %s has unsupported input %d", rule.Code, rule.Input)
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
	// A subsumption naming a rule nobody registers would suppress nothing and
	// say nothing, so it is checked against the whole set rather than trusted.
	for _, rule := range rules {
		for _, code := range rule.Subsumes {
			if code == rule.Code {
				return fmt.Errorf("rule %s subsumes itself", rule.Code)
			}
			if _, ok := seen[code]; !ok {
				return fmt.Errorf("rule %s subsumes %s, which no rule registers", rule.Code, code)
			}
		}
	}
	return nil
}

// dataSafetyRules covers the DS family: statements that destroy data.
func dataSafetyRules() []Rule {
	return []Rule{
		tableDroppedRule(),
		columnDroppedRule(),
		columnDroppedWithReadersRule(),
		columnTypeChangedRule(),
		notNullDroppedRule(),
		constraintDroppedRule(),
		enumValueRemovedRule(),
		databaseObjectDroppedRule(),
		routineBodyNotAnalyzedRule(),
		immutableRoutineReadsTheClockRule(),
		tableTruncatedRule(),
		rlsDisabledRule(),
		rlsForceRemovedRule(),
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

// tableDroppedRule reports DS101 on a DROP TABLE and, on MariaDB, on a
// CREATE OR REPLACE TABLE. The replace form drops the named table and
// recreates it empty -- measured on MariaDB 12.3.3, a table holding rows
// holds none once its CREATE OR REPLACE TABLE runs -- so it destroys existing
// data exactly as DROP TABLE does, under a name that never mentions DROP. The
// TEMPORARY form is excluded by [replacedTableRef]: it replaces only a
// session-local table, so it destroys nothing that outlives the session.
func tableDroppedRule() Rule {
	return Rule{
		Code:     "DS101",
		Title:    "table dropped",
		Severity: SeverityError,
		// The text recognizes a table rebuild, whose copy keeps the rows
		// (see [fileRebuilds]); the dev database's starting state refines
		// that, by showing a column the copy leaves out.
		Input:            InputBaselineRefinement,
		BaselineSubjects: rebuildBaselineSubjects,
		// File-level: dropping a table this same migration created (the
		// create-staging/backfill/drop pattern) destroys no pre-existing
		// data and is exempt, and so is the drop that ends a table rebuild.
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			rebuilds := fileRebuilds(file)
			var findings []Finding
			created := make(map[string]bool)
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if ref := lintkit.CreatedTableRef(stmt.Words); ref != "" {
					created[ref] = true
					continue
				}
				if rebuilds.drops[i] {
					continue
				}
				if ref, isReplace := replacedTableRef(stmt.Words, stmt.sourceWords); isReplace {
					if ref.Normalized != "" && lintkit.RefersToCreated(created, ref.Normalized) {
						continue
					}
					findings = append(findings, tableReplacedFinding(file.Path, stmt.Line, i, ref))
					continue
				}
				if !lintkit.HasWordPrefix(stmt.Words, "DROP", "TABLE") {
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
// orderedDropTargets returns a DROP TABLE target list ordered by logical name.
//
// A drop carries two consequences and each is reported per target: the rows it
// destroys, and the name deployed clients still query. A comma list's written
// order carries no meaning, so both reports have to derive the same sequence
// from it -- two sorts would agree until one of them was changed, and the
// symptom would be one statement reporting its tables in two different orders.
func orderedDropTargets(tables []lintkit.TableReference) []lintkit.TableReference {
	ordered := slices.Clone(tables)
	slices.SortStableFunc(ordered, func(a, b lintkit.TableReference) int {
		return strings.Compare(logicalObjectName(a.Name), logicalObjectName(b.Name))
	})
	return ordered
}

func tableDroppedFindings(filePath string, line, statementIndex int, tables []lintkit.TableReference) []Finding {
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

	ordered := orderedDropTargets(tables)
	findings := make([]Finding, 0, len(ordered))
	for _, table := range ordered {
		findings = append(findings, finding(
			fmt.Sprintf("DROP TABLE permanently deletes table %s and every row in it; %s", table.Name, tableDroppedAdvice),
			Subject{Kind: SubjectTable, Name: table.Name},
		))
	}
	return findings
}

// replacedTableRef reads the table a non-temporary CREATE OR REPLACE TABLE
// statement replaces. isReplace is false for any other statement, including
// the TEMPORARY form (CREATE OR REPLACE TEMPORARY TABLE), which is a different
// four-word prefix and so never matches here: it replaces only a table local to
// the session, and destroys no row that outlives it.
//
// A true isReplace with a zero tableReference means the target could not be
// parsed to the end. That is reported as an unnamed finding rather than
// treated as safe, the same fail-closed contract [droppedTablesNotCreated]
// documents for DROP TABLE.
func replacedTableRef(w, sourceWords []string) (ref lintkit.TableReference, isReplace bool) {
	if !lintkit.HasWordPrefix(w, "CREATE", "OR", "REPLACE", "TABLE") {
		return lintkit.TableReference{}, false
	}
	ref, _ = lintkit.TableRefAt(w, sourceWords, 4)
	return ref, true
}

// tableReplacedFinding reports the table a CREATE OR REPLACE TABLE statement
// destroys, in [tableDroppedRule]'s DS101.
func tableReplacedFinding(filePath string, line, statementIndex int, table lintkit.TableReference) Finding {
	message := "CREATE OR REPLACE TABLE permanently deletes the table and every row in it; " + tableDroppedAdvice
	var subjects []Subject
	if table.Name != "" {
		message = fmt.Sprintf("CREATE OR REPLACE TABLE permanently deletes table %s and every row in it; %s",
			table.Name, tableDroppedAdvice)
		subjects = []Subject{{Kind: SubjectTable, Name: table.Name}}
	}
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
				if !lintkit.IsAlterTable(stmt.Words) {
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

// columnDroppedWithReadersRule names what breaks when a column goes.
//
// DS102 says a drop deletes data. It never said what else the column was
// holding up: `DROP COLUMN email` under a view that selects it reported DS102
// alone, and the operator learned about the view when the migration ran, which
// is the whole failure #1270's criterion 9 records.
//
// It reads the starting state rather than the migration text, because a view
// body is not in the file that drops the column. On a run with no dev database
// it resolves nothing and says so through [Analysis.UnmetInputs], for the reason
// [RuleInput] gives: a rule that quietly finds less is worse than one that
// names its missing input.
func columnDroppedWithReadersRule() Rule {
	return Rule{
		Code:     "DS110P",
		Title:    "column dropped while something reads it",
		Severity: SeverityError,
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if !lintkit.IsAlterTable(stmt.Words) {
					continue
				}
				findings = append(findings, droppedColumnReaderFindings(file, stmt, i)...)
			}
			return findings
		},
		Input:            InputBaselineSchema,
		BaselineSubjects: droppedColumnStatements,
	}
}

// droppedColumnStatements names the statements this rule needs the starting
// state for.
func droppedColumnStatements(file *File) []int {
	var indexes []int
	for i := range file.Statements {
		stmt := &file.Statements[i]
		if lintkit.IsAlterTable(stmt.Words) && len(droppedColumnSubjects(stmt.Words, stmt.sourceWords)) > 0 {
			indexes = append(indexes, i)
		}
	}
	return indexes
}

// droppedColumnReaderFindings reports one finding per dropped column that
// something reads, naming every reader.
func droppedColumnReaderFindings(file *File, stmt *Statement, index int) []Finding {
	var findings []Finding
	for _, subject := range droppedColumnSubjects(stmt.Words, stmt.sourceWords) {
		readers := file.dependents.readers(lintkit.NormalizeIdent(subject.Parent), lintkit.NormalizeIdent(subject.Name))
		if len(readers) == 0 {
			continue
		}
		findings = append(findings, Finding{
			Rule:     "DS110P",
			Title:    "column dropped while something reads it",
			Severity: SeverityError,
			File:     file.Path,
			Line:     stmt.Line,
			Message: fmt.Sprintf(
				"dropping %s breaks %s; drop or redefine %s first",
				subject.Name, describeReaders(readers), pluralize("it", "them", len(readers))),
			Context: statementFindingContext(index, subject),
		})
	}
	return findings
}

// describeReaders names the readers in the order the state supplied them,
// each with what it is.
func describeReaders(readers []lintext.BaselineDependent) string {
	parts := make([]string, 0, len(readers))
	for _, reader := range readers {
		parts = append(parts, reader.Kind+" "+reader.Dependent)
	}
	return strings.Join(parts, ", ")
}

// pluralize picks the singular or plural form for a count.
func pluralize(singular, plural string, count int) string {
	if count == 1 {
		return singular
	}
	return plural
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

// columnTypeChangedRule (DS103) reads the statement text and lets the schema
// state the version starts from take away a finding it establishes to be
// wrong: a clause that restates the column's current type; see
// [typeUnchangedByState].
func columnTypeChangedRule() Rule {
	return Rule{
		Code:             "DS103",
		Title:            "column type changed",
		Severity:         SeverityWarning,
		Input:            InputBaselineRefinement,
		BaselineSubjects: typeChangeStatements,
		CheckFile: func(file *File) []Finding {
			var findings []Finding
			for _, index := range typeChangeStatements(file) {
				if typeUnchangedByState(file, index) {
					continue
				}
				stmt := &file.Statements[index]
				findings = append(findings, Finding{
					Rule:     "DS103",
					Title:    "column type changed",
					Severity: SeverityWarning,
					File:     file.Path,
					Line:     stmt.Line,
					Message:  "changing a column type can truncate or reject existing values and may rewrite the table under a lock; verify the old-to-new value mapping on production data first",
					Context:  statementFindingContext(index),
				})
			}
			return findings
		},
	}
}

func notNullDroppedRule() Rule {
	return Rule{
		Code:     "DS104",
		Title:    "not-null constraint dropped",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !lintkit.IsAlterTable(stmt.Words) || !scanDropNotNull(stmt.Words) {
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
			if !lintkit.IsAlterTable(stmt.Words) || !scanDropNamedConstraint(stmt.Words) {
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
			if !lintkit.IsAlterTable(stmt.Words) || !scan(stmt.Words) {
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
	return alterTableDropRule("CD103", "primary key dropped", lintkit.ScanDropPrimaryKey,
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

// routineBodyNotAnalyzedRule says that a migration carried a routine whose body
// this linter did not read.
//
// It reports nothing about the body, deliberately, and that is the point. A
// DROP TABLE inside a procedure body runs when somebody calls the procedure and
// never at migration time, so the rules correctly stay silent about it
// (migration/lint/compound_routine_body_test.go). Silence about the body and
// silence about the whole file are different facts, and until this rule the
// operator could not tell them apart: a migration whose only statement was a
// function dropping a table inside its body reported no findings and exit 0,
// which reads as "checked and safe" rather than "not checked".
//
// #1270 asks for exactly this distinction: "No supported code object is
// silently skipped and reported as clean when analysis was incomplete."
//
// Info severity, so nothing about the migration changes. It ends when routine
// analysis exists (stokaro/ptah#2356, stokaro/ptah#2357).
// definerRoutineWithoutSearchPathRule reports a SECURITY DEFINER routine that
// does not pin its search_path.
//
// Such a routine runs with its owner's privileges and resolves unqualified
// names through whatever the CALLER set, so a caller who puts a schema of their
// own first decides which table the owner's routine writes to. It is the
// classic PostgreSQL privilege-escalation shape.
//
// PRV02 already reports every definer routine (internal/schemasecurity), and
// two differences separate this from it: PRV02 needs a live server and never sees a
// migration directory, and it cannot tell a pinned routine from an unpinned one
// because nothing read proconfig. Its own suggestion asked for what it could
// not check.
//
// Anchored on the header, and that is the whole difficulty. A routine body is
// words, and a `BEGIN ATOMIC` body carrying its own SET satisfied a scan of the
// statement while the header carried none -- the rule's answer would have
// depended on how the author quoted a body it has no business reading
// (stokaro/ptah#2356).
// nonDeterministicCalls are the calls whose result changes between two
// invocations with the same arguments.
//
// A call list rather than reference resolution, and the boundary is stated
// rather than assumed: deciding whether a body reads a TABLE needs references
// parsed out of statement text, which #1270 records as the prerequisite the
// rest waits on and whose lexical shortcut #1280/#1281 measured as wrong.
// These are resolvable from tokens alone.
var nonDeterministicCalls = []string{
	"NOW", "RANDOM", "CLOCK_TIMESTAMP", "TIMEOFDAY", "STATEMENT_TIMESTAMP",
	"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "LOCALTIME",
	"LOCALTIMESTAMP", "NEXTVAL", "UUID_GENERATE_V4", "GEN_RANDOM_UUID",
	"SYSDATE", "RAND", "UUID", "GETDATE", "SYSDATETIME", "NEWID",
}

// immutableRoutineReadsTheClockRule reports a routine declared IMMUTABLE or
// DETERMINISTIC whose body calls something that is neither.
//
// The tree states the hazard twice, in the two packages that had to decide what
// the value means off PostgreSQL: "a DETERMINISTIC function is what a
// function-based index may be built from, and one that reads a table is not
// that" (internal/oracleroutine), and "IMMUTABLE must be DETERMINISTIC -- that
// axis is load-bearing for the optimizer and for replication, and misstating it
// is a lie the server acts on" (internal/mysqlroutine). The same package records
// a routine declared NOT DETERMINISTIC NO SQL whose body reads a table being
// created and answering on MySQL 26.7.0: the engine does not defend itself.
//
// Ptah carried both halves and related them nowhere -- Volatility and Body sit
// beside each other on both models and the comparison writes them as
// independent properties (stokaro/ptah#2357).
//
// The body is read through Statement.Routine rather than Words, because a
// `$$`-quoted body is one word: a rule scanning Words would answer differently
// depending on how the author quoted it.
func immutableRoutineReadsTheClockRule() Rule {
	return Rule{
		Code:     "DD102",
		Title:    "immutable routine calls something that is not",
		Severity: SeverityWarning,
		Input:    InputRoutineBody,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if stmt.Routine == nil || !declaresImmutability(stmt.Words) {
				return false, ""
			}
			call, found := firstNonDeterministicCall(stmt.Routine.BodyWords)
			if !found {
				return false, ""
			}
			return true, "the routine is declared immutable and its body calls " + call +
				", whose result changes between two calls with the same arguments; " +
				"an index built on it returns rows that no longer match"
		},
		AppliesToDown: true,
	}
}

// declaresImmutability reports whether a routine header promises that the
// routine answers the same way every time.
//
// Read from the header for the reason the definer rule is: a body is words, and
// one mentioning IMMUTABLE would otherwise answer for a header that does not.
func declaresImmutability(w []string) bool {
	header, ok := lintkit.RoutineHeaderWords(w)
	if !ok {
		return false
	}
	if lintkit.ContainsWord(header, "IMMUTABLE") {
		return true
	}
	// MySQL and MariaDB spell it DETERMINISTIC, and NOT DETERMINISTIC is the
	// opposite promise rather than a weaker one.
	for i, word := range header {
		if word != "DETERMINISTIC" {
			continue
		}
		if i > 0 && header[i-1] == "NOT" {
			return false
		}
		return true
	}
	return false
}

// firstNonDeterministicCall names the first such call a body makes.
//
// A word match over the tokenized body, where a string literal is one verbatim
// token and a comment is gone, so `EXECUTE 'SELECT now()'` and a commented-out
// call do not fire it.
func firstNonDeterministicCall(bodyWords []string) (string, bool) {
	for _, word := range bodyWords {
		if slices.Contains(nonDeterministicCalls, word) {
			return strings.ToLower(word), true
		}
	}
	return "", false
}

func routineBodyNotAnalyzedRule() Rule {
	return Rule{
		Code:     "AC101",
		Title:    "routine body not analyzed",
		Severity: SeverityInfo,
		CheckStatement: func(stmt *Statement) (bool, string) {
			kind, ok := lintkit.RoutineDefinitionKind(stmt.Words)
			if !ok {
				return false, ""
			}
			return true, "this migration defines a " + kind +
				"; its body is not analyzed, so a clean result here says nothing about what the body does"
		},
		AppliesToDown: true,
	}
}

func databaseObjectDroppedRule() Rule {
	return Rule{
		Code:     "DS107",
		Title:    "database object dropped",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !scanDestructiveObjectDrop(stmt.Words) && !scanPrincipalDrop(stmt.Words, stmt.Target) {
				return false, ""
			}
			return true, "dropping a database object removes existing schema behavior, principals or data; verify all dependent code and data paths are retired first"
		},
	}
}

func tableTruncatedRule() Rule {
	return Rule{
		Code:     "DS108",
		Title:    "table truncated",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !lintkit.HasWordPrefix(stmt.Words, "TRUNCATE") {
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
			if !lintkit.IsAlterTable(stmt.Words) || !lintkit.HasWordSeq(stmt.Words, "DISABLE", "ROW", "LEVEL", "SECURITY") {
				return false, ""
			}
			return true, "DISABLE ROW LEVEL SECURITY removes an access-control protection; verify replacement authorization before applying"
		},
	}
}

// rlsForceRemovedRule reports the other way to take row-level security away
// from a table. NO FORCE leaves the policies in force for every role but the
// table's owner, which then reads and writes past all of them -- and the owner
// is often the role an application connects as. DS109 does not cover it: its
// title says row-level security is disabled, and here it stays enabled.
func rlsForceRemovedRule() Rule {
	return Rule{
		Code:     "DS111P",
		Title:    "row-level security no longer forced",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !lintkit.IsAlterTable(stmt.Words) || !lintkit.HasWordSeq(stmt.Words, "NO", "FORCE", "ROW", "LEVEL", "SECURITY") {
				return false, ""
			}
			return true, "NO FORCE ROW LEVEL SECURITY exempts the table owner from every policy on the table; verify the owner is not a role the policies must hold before applying"
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
			// The ADD COLUMN half reads text. The rename half cannot: the
			// retired column's type, nullability and default live in an earlier
			// file or the base schema, never in the RENAME statement. Declaring
			// the stronger of the two inputs is what puts this rule's files in
			// [Analysis.BaselineVersions] and, on a run that supplies no
			// baseline, in [Analysis.UnmetInputs].
			Input:            InputBaselineSchema,
			BaselineSubjects: renameAddSideStatements,
		},
	}
}

func notNullWithoutDefaultFindings(file *File) []Finding {
	if !file.IsUp {
		return nil
	}
	var findings []Finding
	created := make(map[string]bool)
	for i := range file.Statements {
		stmt := &file.Statements[i]
		if ref := lintkit.CreatedTableRef(stmt.Words); ref != "" {
			created[ref] = true
			continue
		}
		if !lintkit.IsAlterTable(stmt.Words) {
			continue
		}
		subjects := reportSubjects(lintkit.NonNullableAddedColumnSubjects(stmt.Words, stmt.sourceWords))
		if len(subjects) == 0 {
			continue
		}
		if lintkit.RefersToCreated(created, lintkit.AlterTableReference(stmt.Words, stmt.sourceWords).Normalized) {
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
	column, ok := file.baseline.Column(name.owner, name.normalized)
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
			// MF101P, MF102P: Ptah's own rules inside the Atlas MF family, so
			// the suffix the identifier convention asks for; the bare codes
			// are the Atlas checks unique.go reports (stokaro/ptah#2942).
			Code:     "MF101P",
			Title:    "missing down migration",
			Severity: SeverityWarning,
			CheckFile: func(file *File) []Finding {
				if !file.IsUp || file.HasPair {
					return nil
				}
				return []Finding{{
					Rule:     "MF101P",
					Title:    "missing down migration",
					Severity: SeverityWarning,
					File:     file.Path,
					Message:  "no matching .down.sql exists; a failed deploy cannot be rolled back mechanically",
				}}
			},
		},
		{
			Code:     "MF102P",
			Title:    "empty migration",
			Severity: SeverityWarning,
			CheckFile: func(file *File) []Finding {
				if !file.IsUp || len(file.Statements) > 0 {
					return nil
				}
				return []Finding{{
					Rule:     "MF102P",
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

func sqliteRules() []Rule {
	return []Rule{
		{
			Code:     "LT101",
			Title:    "not-null constraint added without a default",
			Severity: SeverityWarning,
			Dialects: []string{"sqlite"},
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !lintkit.IsAlterTable(stmt.Words) || !lintkit.ScanSetNotNull(stmt.Words) {
					return false, ""
				}
				return true, "SQLite cannot safely enforce NOT NULL on existing nullable data without a staged rebuild and backfill"
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

// droppedColumnSubjects returns the columns an ALTER TABLE statement drops.
// The COLUMN keyword is optional in PostgreSQL and the MySQL family, so a
// clause-head DROP followed by an identifier counts unless the identifier is
// a known non-column DROP target (DROP CONSTRAINT, DROP PRIMARY KEY, ...).
func droppedColumnSubjects(w, sourceWords []string) []Subject {
	table := lintkit.AlterTableReference(w, sourceWords)
	var subjects []Subject
	for _, i := range lintkit.ClauseStarts(w) {
		if i >= len(w) || w[i] != "DROP" {
			continue
		}
		j := i + 1
		explicit := false
		if j < len(w) && w[j] == "COLUMN" {
			explicit = true
			j++
		}
		j = lintkit.SkipIfExists(w, j)
		if j >= len(w) {
			continue
		}
		if !explicit && dropTargets[w[j]] {
			continue
		}
		if lintkit.IdentLike(w[j]) {
			subjects = append(subjects, Subject{
				Kind:   SubjectColumn,
				Name:   lintkit.SourceWordAt(w, sourceWords, j),
				Parent: table.Name,
			})
		}
	}
	return subjects
}

// scanAlterColumnType reports whether an ALTER TABLE statement changes a
// column's type via ALTER [COLUMN] name [SET DATA] TYPE. The ordered scan
// keeps a column merely named "type" (e.g. ALTER COLUMN type SET NOT NULL)
// from matching.
func scanAlterColumnType(w []string) bool {
	for _, i := range lintkit.ClauseStarts(w) {
		if i >= len(w) || w[i] != "ALTER" {
			continue
		}
		j := i + 1
		if j < len(w) && w[j] == "COLUMN" {
			j++
		}
		j = lintkit.SkipIfExists(w, j)
		if j >= len(w) || !lintkit.IdentLike(w[j]) {
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
	for _, i := range lintkit.ClauseStarts(w) {
		if i >= len(w) || w[i] != "ALTER" {
			continue
		}
		j := i + 1
		if j < len(w) && w[j] == "COLUMN" {
			j++
		}
		j = lintkit.SkipIfExists(w, j)
		if j >= len(w) || !lintkit.IdentLike(w[j]) {
			continue
		}
		if lintkit.HasWordPrefix(w[j+1:], "DROP", "NOT", "NULL") {
			return true
		}
	}
	return false
}

// scanDropForeignKey reports whether an ALTER TABLE statement drops a foreign
// key via the MySQL-family DROP FOREIGN KEY form. The ANSI DROP CONSTRAINT
// <name> form does not encode the constraint type, so it is not matched here.
func scanDropForeignKey(w []string) bool { return lintkit.ScanDropClause(w, "DROP", "FOREIGN", "KEY") }

// scanDropCheck reports whether an ALTER TABLE statement drops a check
// constraint via the DROP CHECK form.
func scanDropCheck(w []string) bool { return lintkit.ScanDropClause(w, "DROP", "CHECK") }

// scanDropNamedConstraint reports whether an ALTER TABLE statement drops a
// constraint by name via the ANSI DROP CONSTRAINT <name> form, whose constraint
// type is not recoverable from the SQL. This is the untyped fallback (DS105);
// the typed forms above are reported by the CD family.
func scanDropNamedConstraint(w []string) bool { return lintkit.ScanDropClause(w, "DROP", "CONSTRAINT") }

// scanEnumValueRemoval reports whether a statement removes an enum value.
//
// Both forms are anchored on the statement they belong to, which is the whole
// point. Unanchored, the scan walked every word a statement carried -- and a
// routine body is words. `CREATE FUNCTION f() ... BEGIN ATOMIC DELETE FROM
// pg_enum; END` reported DS106 at error severity, against the one family the
// apply gate refuses on, for a migration that creates a function and removes no
// enum value. The `$$`-quoted spelling of the same body did not, because a
// dollar-quoted body is one token: the rule's answer depended on how the author
// quoted a body it had no business reading (stokaro/ptah#2358).
//
// The tree already pins the principle this restores: a DROP TABLE inside a
// procedure body runs when somebody calls the procedure and never at migration
// time (migration/lint/compound_routine_body_test.go). DS101 and DS103 were
// anchored and held it; this scan was the one that was not.
func scanEnumValueRemoval(w []string) bool {
	if lintkit.HasWordPrefix(w, "DELETE", "FROM", "PG_ENUM") {
		return true
	}
	// DROP VALUE is a clause of ALTER TYPE, so it is read only there.
	return lintkit.HasWordPrefix(w, "ALTER", "TYPE") && lintkit.HasWordSeq(w, "DROP", "VALUE")
}

func scanDestructiveObjectDrop(w []string) bool {
	if len(w) < 2 || w[0] != "DROP" {
		return false
	}
	switch w[1] {
	// PROCEDURE and TRIGGER are here for the reason FUNCTION is: dropping one
	// removes behavior a caller depends on, and this is the family that decides
	// whether an apply proceeds (stokaro/ptah#2358).
	// TOPIC is YDB's: dropping one drops every message it holds and every
	// consumer's position in it.
	case "TYPE", "EXTENSION", "FUNCTION", "PROCEDURE", "TRIGGER", "ROLE", "POLICY", "SCHEMA", "TOPIC":
		return true
	// TRANSFER and ASYNC REPLICATION are YDB's. Dropping a transfer stops it
	// and drops the topic consumer YDB created for it, with its position; an
	// async replication dropped with CASCADE drops its replica tables, and one
	// dropped without it is YD115's question.
	case "TRANSFER":
		return true
	case "ASYNC":
		return len(w) > 2 && w[2] == "REPLICATION" && w[len(w)-1] == "CASCADE"
	// A coordination node is YDB's too, dropped through Ptah's own statement
	// (see dialect/ydb/ydbcoordination). Measured on 26.2.1.14 and 25.1.4.7:
	// DropNode succeeds while a session holds a semaphore on the node, the
	// holder's session and lease end about five seconds later, and a node
	// created again under the same path holds none of the persistent
	// semaphores the dropped one held. Its rate limiter resources go with it.
	case "COORDINATION":
		return len(w) > 2 && w[2] == "NODE"
	case "STREAMING":
		return len(w) > 2 && w[2] == "QUERY"
	default:
		return false
	}
}

// scanPrincipalDrop reports DROP USER and DROP GROUP on a target whose
// principals are users and groups ([capability.GroupPrincipals]), which is
// YDB: there a user or a group is the principal a role is elsewhere, and
// dropping one removes what it was granted as DROP ROLE does.
func scanPrincipalDrop(w []string, target Target) bool {
	if len(w) < 2 || w[0] != "DROP" || !target.Capabilities.Has(capability.GroupPrincipals) {
		return false
	}
	return w[1] == "USER" || w[1] == "GROUP"
}

// droppedTablesNotCreated returns tables that were not created earlier in the
// same file and whether every target was parsed. An incomplete parse must remain
// destructive because treating an unknown target as safe would be fail-open.
func droppedTablesNotCreated(
	w,
	sourceWords []string,
	created map[string]bool,
) ([]lintkit.TableReference, bool) {
	var unsafe []lintkit.TableReference
	j := lintkit.SkipIfExists(w, 2)
	for {
		if j < len(w) && w[j] == "ONLY" {
			j++
		}
		ref, next := lintkit.TableRefAt(w, sourceWords, j)
		if ref.Normalized == "" {
			return unsafe, false
		}
		if !lintkit.RefersToCreated(created, ref.Normalized) {
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

// Describe renders one finding as a single human-readable line naming the
// file, the line when the finding has one, the severity, the rule code, the
// message and the rule title. File-level findings — naming and pairing — carry
// no line number and render without one.
//
// The line is meant to be read, not parsed: consume [Finding] itself when the
// fields matter to a caller.
func Describe(f Finding) string {
	location := f.File
	if f.Line > 0 {
		location = fmt.Sprintf("%s:%d", f.File, f.Line)
	}
	return fmt.Sprintf("%s [%s] %s: %s (%s)", location, f.Severity, f.Rule, f.Message, f.Title)
}
