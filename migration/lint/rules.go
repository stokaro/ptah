package lint

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
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
	registeredRules.rules = append(registeredRules.rules, rule)
	return nil
}

// Rules returns the built-in rule set plus process-wide registered rules. The
// slice is rebuilt on every call so callers can never corrupt the registry.
func Rules() []Rule {
	rules := builtinRules()
	registeredRules.RLock()
	defer registeredRules.RUnlock()
	return append(rules, registeredRules.rules...)
}

func builtinRules() []Rule {
	var rules []Rule
	rules = append(rules, dataSafetyRules()...)
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
	if strings.TrimSpace(rule.Code) == "" {
		return fmt.Errorf("rule code is required")
	}
	if strings.TrimSpace(rule.Title) == "" {
		return fmt.Errorf("rule %s title is required", rule.Code)
	}
	switch rule.Severity {
	case SeverityWarning, SeverityError:
	default:
		return fmt.Errorf("rule %s has unsupported severity %q", rule.Code, rule.Severity)
	}
	if (rule.CheckStatement != nil) == (rule.CheckFile != nil) {
		return fmt.Errorf("rule %s must set exactly one checker", rule.Code)
	}
	return nil
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
				if !hasWordPrefix(stmt.Words, "DROP", "TABLE") || dropsOnlyCreatedTables(stmt.Words, created) {
					continue
				}
				findings = append(findings, Finding{
					Rule:     "DS101",
					Title:    "table dropped",
					Severity: SeverityError,
					File:     file.Path,
					Line:     stmt.Line,
					Message:  "DROP TABLE permanently deletes the table and every row in it; take a verified backup first and consider a rename-and-retire window instead",
				})
			}
			return findings
		},
	}
}

func columnDroppedRule() Rule {
	return Rule{
		Code:     "DS102",
		Title:    "column dropped",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !isAlterTable(stmt.Words) || !scanDropColumn(stmt.Words) {
				return false, ""
			}
			return true, "DROP COLUMN permanently deletes the column's data; deploy readers that no longer use the column first, then drop it in a later release"
		},
	}
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

func constraintDroppedRule() Rule {
	return Rule{
		Code:     "DS105",
		Title:    "constraint dropped",
		Severity: SeverityError,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !isAlterTable(stmt.Words) || !scanDropConstraint(stmt.Words) {
				return false, ""
			}
			return true, "dropping a constraint removes an existing data protection; verify the replacement safety invariant before applying"
		},
	}
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
func dataDependentRules() []Rule {
	return []Rule{
		{
			Code:     "DD101",
			Title:    "non-nullable column added without a default",
			Severity: SeverityWarning,
			CheckStatement: func(stmt *Statement) (bool, string) {
				if !isAlterTable(stmt.Words) || !scanAddColumnNotNullWithoutDefault(stmt.Words) {
					return false, ""
				}
				return true, "adding a NOT NULL column without a DEFAULT fails or blocks on populated tables; add it nullable, backfill, then enforce NOT NULL in a later migration"
			},
		},
	}
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
			CheckStatement: func(stmt *Statement) (bool, string) {
				standalone := hasWordPrefix(stmt.Words, "RENAME", "TABLE")
				if !standalone && (!isAlterTable(stmt.Words) || !scanAlterRename(stmt.Words)) {
					return false, ""
				}
				return true, "renames are not backwards compatible: application versions deployed against the old name fail instantly; prefer add-new/backfill/drop-old across releases"
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
		CheckFile: func(file *File) []Finding {
			if !file.IsUp || file.NoTransaction {
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

func postgresDropIndexRule() Rule {
	return postgresStatementRule("PG106", "index dropped with a table lock", func(stmt *Statement) (bool, string) {
		if !hasWordPrefix(stmt.Words, "DROP", "INDEX") || slices.Contains(stmt.Words, "CONCURRENTLY") {
			return false, ""
		}
		return true, "DROP INDEX without CONCURRENTLY blocks writes while PostgreSQL removes the index; use DROP INDEX CONCURRENTLY outside a transaction for populated tables"
	})
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
			CheckFile: func(file *File) []Finding {
				if !file.IsUp || file.NoTransaction {
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
	if j < len(w) && identLike(w[j]) {
		j++
		for j+1 < len(w) && w[j] == "." && identLike(w[j+1]) {
			j += 2 // schema-qualified reference: schema.tbl
		}
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

// scanDropColumn reports whether an ALTER TABLE statement drops a column.
// The COLUMN keyword is optional in PostgreSQL and the MySQL family, so a
// clause-head DROP followed by an identifier counts unless the identifier is
// a known non-column DROP target (DROP CONSTRAINT, DROP PRIMARY KEY, ...).
func scanDropColumn(w []string) bool {
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
			return true
		}
	}
	return false
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

// scanDropConstraint reports whether an ALTER TABLE statement removes a data
// protection constraint. Index/key/partition drops stay out of this rule.
func scanDropConstraint(w []string) bool {
	for _, i := range clauseStarts(w) {
		if i >= len(w) || w[i] != "DROP" {
			continue
		}
		switch {
		case hasWordPrefix(w[i:], "DROP", "CONSTRAINT"):
			return true
		case hasWordPrefix(w[i:], "DROP", "FOREIGN", "KEY"):
			return true
		case hasWordPrefix(w[i:], "DROP", "PRIMARY", "KEY"):
			return true
		case hasWordPrefix(w[i:], "DROP", "CHECK"):
			return true
		}
	}
	return false
}

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
	for _, i := range clauseStarts(w) {
		start, end, ok := addColumnClause(w, i)
		if !ok {
			continue
		}
		clause := w[start:end]
		if hasWordSeq(clause, "NOT", "NULL") && !slices.Contains(clause, "DEFAULT") {
			return true
		}
	}
	return false
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
	ref, _ := tableRefAt(w, j)
	return ref
}

// tableRefAt reads a possibly schema-qualified table reference at w[j] and
// returns it normalized plus the index past its end; "" when w[j] does not
// start a reference.
func tableRefAt(w []string, j int) (string, int) {
	if j >= len(w) || !identLike(w[j]) {
		return "", j
	}
	parts := []string{normalizeIdent(w[j])}
	j++
	for j+1 < len(w) && w[j] == "." && identLike(w[j+1]) {
		parts = append(parts, normalizeIdent(w[j+1]))
		j += 2
	}
	return strings.Join(parts, "."), j
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

// dropsOnlyCreatedTables reports whether every table named by a DROP TABLE
// statement was created earlier in the same file.
func dropsOnlyCreatedTables(w []string, created map[string]bool) bool {
	j := skipIfExists(w, 2)
	for {
		ref, next := tableRefAt(w, j)
		if ref == "" || !refersToCreated(created, ref) {
			return false
		}
		if next < len(w) && w[next] == "," {
			j = next + 1
			continue
		}
		return true
	}
}

// indexTargetRef extracts the table a CREATE INDEX statement builds on (the
// reference after ON).
func indexTargetRef(w []string) string {
	for k := range w {
		if w[k] == "ON" {
			ref, _ := tableRefAt(w, k+1)
			return ref
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

// scanAlterRename reports whether an ALTER TABLE statement renames the table
// or a column. Handles RENAME TO/AS (table), RENAME COLUMN a TO b, the
// PostgreSQL form without the COLUMN keyword, and MySQL's bare RENAME
// new_name. Index, key, and constraint renames are invisible to applications
// and are deliberately skipped.
func scanAlterRename(w []string) bool {
	for _, i := range clauseStarts(w) {
		if i+1 >= len(w) || w[i] != "RENAME" {
			continue
		}
		switch w[i+1] {
		case "INDEX", "KEY", "CONSTRAINT":
			continue
		case "TO", "AS", "COLUMN":
			return true
		}
		if identLike(w[i+1]) {
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
