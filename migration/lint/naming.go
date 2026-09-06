package lint

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// A naming convention is a project's, not the engine's, so the six rules in
// this file carry no pattern of their own: they read the one the project
// configures and stay silent without it. The configuration is the same model
// on both surfaces -- `naming:` in .ptah-lint.yaml and `lint { naming { } }`
// in atlas.hcl -- so a convention behaves identically whichever file it was
// written in, which is what Atlas NM101 through NM106 report against.
//
// What is checked is every name a migration introduces: a schema it creates
// or renames to, a table it creates or renames to, a column it declares, adds
// or renames to, an index it creates (a unique or primary key constraint is
// an index to the engine and to Atlas, so it is one here), and a foreign key
// or check constraint it names. A name the migration merely refers to is not
// checked; that is the existing schema's business.
//
// The patterns are Go regular expressions, which is what Atlas evaluates
// too, and a match is a full-string question only when the pattern anchors
// it: `^[a-z_]+$` is the convention, `[a-z]` accepts every name with a
// letter in it. Quoting is stripped before the match, so "Users" and `Users`
// are the name Users.

// NamingConfig is the naming convention a lint policy enforces, the `naming`
// section of .ptah-lint.yaml.
//
// Match is the default pattern every kind of name must satisfy, and each
// kind may carry a pattern of its own, which replaces the default for that
// kind. A kind with no pattern from either source is not checked. Message
// is appended to every finding; a kind's own Message replaces it for that
// kind. Severity is the level the six findings report at, warning when
// empty; a `rules:` entry for one of the codes still overrides it.
type NamingConfig struct {
	Match      string         `yaml:"match,omitempty"`
	Message    string         `yaml:"message,omitempty"`
	Severity   Severity       `yaml:"severity,omitempty"`
	Schema     *NamingPattern `yaml:"schema,omitempty"`
	Table      *NamingPattern `yaml:"table,omitempty"`
	Column     *NamingPattern `yaml:"column,omitempty"`
	Index      *NamingPattern `yaml:"index,omitempty"`
	ForeignKey *NamingPattern `yaml:"foreign-key,omitempty"`
	Check      *NamingPattern `yaml:"check,omitempty"`
}

// NamingPattern is the convention for one kind of name.
type NamingPattern struct {
	Match   string `yaml:"match,omitempty"`
	Message string `yaml:"message,omitempty"`
}

// nameKind is one class of object a migration can name.
type nameKind int

const (
	nameSchema nameKind = iota
	nameTable
	nameColumn
	nameIndex
	nameForeignKey
	nameCheck
)

// namingCodes are the rule codes, in nameKind order: the Atlas codes, since
// the rules report what the Atlas naming analyzer reports.
var namingCodes = [...]string{"NM101", "NM102", "NM103", "NM104", "NM105", "NM106"}

var nameKindLabels = [...]string{"schema", "table", "column", "index", "foreign key", "check constraint"}

func (k nameKind) String() string { return nameKindLabels[k] }

// compiledPattern is one kind's convention, ready to match.
type compiledPattern struct {
	expr    *regexp.Regexp
	source  string
	message string
}

// namingPolicy is a NamingConfig compiled: one pattern per kind that has
// one.
type namingPolicy struct {
	patterns map[nameKind]compiledPattern
}

// compileNamingConfig validates and compiles a configuration. A pattern that
// does not compile is an error naming the kind it was written for; a
// configuration with no pattern at all is one too, because a naming block
// that checks nothing reads as a convention enforced.
func compileNamingConfig(config *NamingConfig) (*namingPolicy, error) {
	if config == nil {
		return nil, nil
	}
	if err := validateSeverity(config.Severity); err != nil {
		return nil, fmt.Errorf("naming: %w", err)
	}
	policy := &namingPolicy{patterns: make(map[nameKind]compiledPattern)}
	var base *compiledPattern
	if config.Match != "" {
		compiled, err := compileNamePattern("naming", config.Match, config.Message)
		if err != nil {
			return nil, err
		}
		base = &compiled
	}
	kinds := []struct {
		kind    nameKind
		key     string
		pattern *NamingPattern
	}{
		{nameSchema, "schema", config.Schema},
		{nameTable, "table", config.Table},
		{nameColumn, "column", config.Column},
		{nameIndex, "index", config.Index},
		{nameForeignKey, "foreign-key", config.ForeignKey},
		{nameCheck, "check", config.Check},
	}
	for _, entry := range kinds {
		switch {
		case entry.pattern != nil && entry.pattern.Match != "":
			message := entry.pattern.Message
			if message == "" {
				message = config.Message
			}
			compiled, err := compileNamePattern("naming."+entry.key, entry.pattern.Match, message)
			if err != nil {
				return nil, err
			}
			policy.patterns[entry.kind] = compiled
		case entry.pattern != nil:
			return nil, fmt.Errorf("naming.%s: a pattern block needs a match", entry.key)
		case base != nil:
			policy.patterns[entry.kind] = *base
		}
	}
	if len(policy.patterns) == 0 {
		return nil, fmt.Errorf("naming: no match pattern is set, so the block would enforce nothing")
	}
	return policy, nil
}

func compileNamePattern(scope, match, message string) (compiledPattern, error) {
	expr, err := regexp.Compile(match)
	if err != nil {
		return compiledPattern{}, fmt.Errorf("%s: match %q is not a valid regular expression: %w", scope, match, err)
	}
	return compiledPattern{expr: expr, source: match, message: message}, nil
}

func validateSeverity(severity Severity) error {
	switch severity {
	case "", SeverityInfo, SeverityWarning, SeverityError:
		return nil
	default:
		return fmt.Errorf("unsupported severity %q: expected info, warning or error", severity)
	}
}

// withNamingSeverity gives the six naming rules the severity the naming block
// asks for, where the policy's own `rules:` entries do not already say.
func withNamingSeverity(configs map[string]RuleConfig, naming *NamingConfig) map[string]RuleConfig {
	if naming == nil || naming.Severity == "" {
		return configs
	}
	merged := make(map[string]RuleConfig, len(configs)+len(namingCodes))
	maps.Copy(merged, configs)
	for _, code := range namingCodes {
		config := merged[code]
		if config.Severity == "" {
			config.Severity = naming.Severity
		}
		merged[code] = config
	}
	return merged
}

// namedObject is one name a migration introduces.
type namedObject struct {
	statement int
	kind      nameKind
	// name is the identifier without its quoting; spelled is how the author
	// wrote it, for the message.
	name    string
	spelled string
	// owner is the table a column, index, or constraint belongs to, for the
	// finding's subject.
	owner string
}

// namedObjects lists every name an up migration introduces, in file order.
func namedObjects(file *File) []namedObject {
	if !file.IsUp {
		return nil
	}
	var objects []namedObject
	for index := range file.Statements {
		stmt := &file.Statements[index]
		w := stmt.Words
		collect := func(kind nameKind, wordIndex int, owner string) {
			spelled := sourceWordAt(w, stmt.sourceWords, wordIndex)
			objects = append(objects, namedObject{
				statement: index,
				kind:      kind,
				name:      bareIdent(lastComponent(spelled)),
				spelled:   spelled,
				owner:     owner,
			})
		}
		switch {
		case hasWordPrefix(w, "CREATE", "SCHEMA"):
			if j := skipIfNotExists(w, 2); j < len(w) && w[j] != "AUTHORIZATION" && identLike(w[j]) {
				collect(nameSchema, j, "")
			}
		case hasWordPrefix(w, "ALTER", "SCHEMA"):
			if j := renameTarget(w, 3); j >= 0 {
				collect(nameSchema, j, "")
			}
		case createdTableRef(w) != "":
			collectCreateTable(w, stmt.sourceWords, collect)
		case isCreateIndex(w):
			collectCreateIndex(w, stmt.sourceWords, collect)
		case hasWordPrefix(w, "ALTER", "INDEX"):
			if j := renameTarget(w, 3); j >= 0 {
				collect(nameIndex, j, "")
			}
		case isAlterTable(w):
			collectAlterTable(w, stmt.sourceWords, collect)
		}
	}
	return objects
}

// bareIdent strips the quoting an identifier was written with.
func bareIdent(word string) string {
	return strings.Trim(word, "`\"[]")
}

// renameTarget returns the index of the name after RENAME TO, scanning from
// start, or -1.
func renameTarget(w []string, start int) int {
	for j := start; j+2 < len(w); j++ {
		if w[j] == "RENAME" && w[j+1] == "TO" && identLike(w[j+2]) {
			return j + 2
		}
	}
	return -1
}

// renamedTo returns the index of the new name in `old TO new` starting at
// w[from], bounded by end, or -1.
func renamedTo(w []string, from, end int) int {
	if from+2 < end && identLike(w[from]) && w[from+1] == "TO" && identLike(w[from+2]) {
		return from + 2
	}
	return -1
}

// lastIdentOf advances over the `.name` components of a qualified name that
// starts at w[j] and returns the index of its last component.
func lastIdentOf(w []string, j int) int {
	for j+2 < len(w) && w[j+1] == "." && identLike(w[j+2]) {
		j += 2
	}
	return j
}

// collectCreateTable reads the table name and every name its body declares.
func collectCreateTable(w, sourceWords []string, collect func(nameKind, int, string)) {
	j := 2
	for j < len(w) && (w[j] == "TEMP" || w[j] == "TEMPORARY" || w[j] == "UNLOGGED" || w[j] == "TABLE") {
		j++
	}
	j = skipIfNotExists(w, j)
	if j >= len(w) || !identLike(w[j]) {
		return
	}
	tableIndex := lastIdentOf(w, j)
	collect(nameTable, tableIndex, "")
	owner := bareIdent(sourceWordAt(w, sourceWords, tableIndex))
	j = tableIndex + 1
	if j >= len(w) || w[j] != "(" {
		return
	}
	for _, element := range topLevelElements(w, j) {
		collectTableElement(w, element, owner, collect)
	}
}

// topLevelElements splits the parenthesized list starting at w[open] into
// its top-level comma-separated elements, each as a start index.
func topLevelElements(w []string, open int) []int {
	var starts []int
	depth := 0
	for k := open; k < len(w); k++ {
		switch w[k] {
		case "(":
			depth++
			if depth == 1 {
				starts = append(starts, k+1)
			}
		case ")":
			depth--
			if depth == 0 {
				return starts
			}
		case ",":
			if depth == 1 {
				starts = append(starts, k+1)
			}
		}
	}
	return starts
}

// collectTableElement classifies one element of a CREATE TABLE body.
func collectTableElement(w []string, start int, owner string, collect func(nameKind, int, string)) {
	if start >= len(w) {
		return
	}
	switch w[start] {
	case "CONSTRAINT":
		if start+2 < len(w) && identLike(w[start+1]) {
			if kind, ok := constraintKind(w, start+2); ok {
				collect(kind, start+1, owner)
			}
		}
	case "PRIMARY", "FOREIGN", "CHECK", "LIKE", "PERIOD", ")":
	case "UNIQUE", "INDEX", "KEY", "FULLTEXT", "SPATIAL":
		if j := indexNameAfter(w, start); j >= 0 {
			collect(nameIndex, j, owner)
		}
	default:
		if identLike(w[start]) {
			collect(nameColumn, start, owner)
		}
	}
}

// constraintKind reads the keyword after CONSTRAINT name.
func constraintKind(w []string, j int) (nameKind, bool) {
	switch w[j] {
	case "FOREIGN":
		return nameForeignKey, true
	case "CHECK":
		return nameCheck, true
	case "UNIQUE", "PRIMARY":
		return nameIndex, true
	}
	return 0, false
}

// indexNameAfter finds the name in [UNIQUE|FULLTEXT|SPATIAL] [INDEX|KEY]
// name (...), returning -1 when the index is unnamed.
func indexNameAfter(w []string, start int) int {
	j := start
	if j < len(w) && (w[j] == "UNIQUE" || w[j] == "FULLTEXT" || w[j] == "SPATIAL") {
		j++
	}
	if j < len(w) && (w[j] == "INDEX" || w[j] == "KEY") {
		j++
	}
	if j < len(w) && w[j] != "(" && w[j] != "USING" && identLike(w[j]) {
		return j
	}
	return -1
}

// collectCreateIndex reads CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT
// EXISTS] name ON table.
func collectCreateIndex(w, sourceWords []string, collect func(nameKind, int, string)) {
	j := 2
	if w[1] == "UNIQUE" {
		j = 3
	}
	if j < len(w) && w[j] == "CONCURRENTLY" {
		j++
	}
	j = skipIfNotExists(w, j)
	if j >= len(w) || w[j] == "ON" || !identLike(w[j]) {
		return
	}
	owner := ""
	for k := j; k+1 < len(w); k++ {
		if w[k] == "ON" && identLike(w[k+1]) {
			owner = bareIdent(sourceWordAt(w, sourceWords, lastIdentOf(w, k+1)))
			break
		}
	}
	collect(nameIndex, lastIdentOf(w, j), owner)
}

// collectAlterTable reads the names an ALTER TABLE introduces: added
// columns, constraints and indexes, and the targets of its renames.
func collectAlterTable(w, sourceWords []string, collect func(nameKind, int, string)) {
	owner := bareIdent(lastComponent(alterTableReference(w, sourceWords).name))
	for _, i := range clauseStarts(w) {
		end := clauseEnd(w, i)
		switch w[i] {
		case "RENAME":
			collectRenameClause(w, i, end, owner, collect)
		case "CHANGE":
			collectChangeClause(w, sourceWords, i, end, owner, collect)
		case "ADD":
			collectAddClause(w, i, end, owner, collect)
		}
	}
}

// collectRenameClause reads the target of RENAME [TO|AS] name, RENAME
// COLUMN a TO b, RENAME INDEX|KEY a TO b, and MySQL's RENAME name.
func collectRenameClause(w []string, i, end int, owner string, collect func(nameKind, int, string)) {
	clause := w[i:end]
	switch {
	case hasWordPrefix(clause, "RENAME", "TO") || hasWordPrefix(clause, "RENAME", "AS"):
		if i+2 < end && identLike(w[i+2]) {
			collect(nameTable, i+2, "")
		}
	case hasWordPrefix(clause, "RENAME", "COLUMN"):
		if j := renamedTo(w, i+2, end); j >= 0 {
			collect(nameColumn, j, owner)
		}
	case hasWordPrefix(clause, "RENAME", "INDEX") || hasWordPrefix(clause, "RENAME", "KEY"):
		if j := renamedTo(w, i+2, end); j >= 0 {
			collect(nameIndex, j, owner)
		}
	case i+1 < end && identLike(w[i+1]) && !slices.Contains(clause, "TO"):
		collect(nameTable, i+1, "")
	}
}

// collectChangeClause reads the new name of MySQL's CHANGE [COLUMN] old new,
// which is a rename only when the two differ.
func collectChangeClause(w, sourceWords []string, i, end int, owner string, collect func(nameKind, int, string)) {
	j := i + 1
	if j < end && w[j] == "COLUMN" {
		j++
	}
	if j+1 < end && identLike(w[j]) && identLike(w[j+1]) &&
		bareIdent(sourceWordAt(w, sourceWords, j)) != bareIdent(sourceWordAt(w, sourceWords, j+1)) {
		collect(nameColumn, j+1, owner)
	}
}

// collectAddClause reads the name an ADD clause introduces: a named
// constraint, an index, or a column.
func collectAddClause(w []string, i, end int, owner string, collect func(nameKind, int, string)) {
	clause := w[i:end]
	switch {
	case hasWordPrefix(clause, "ADD", "CONSTRAINT"):
		if i+3 < end && identLike(w[i+2]) {
			if kind, ok := constraintKind(w, i+3); ok {
				collect(kind, i+2, owner)
			}
		}
	case hasWordPrefix(clause, "ADD", "UNIQUE") || hasWordPrefix(clause, "ADD", "INDEX") || hasWordPrefix(clause, "ADD", "KEY") ||
		hasWordPrefix(clause, "ADD", "FULLTEXT") || hasWordPrefix(clause, "ADD", "SPATIAL"):
		if j := indexNameAfter(w, i+1); j >= 0 && j < end {
			collect(nameIndex, j, owner)
		}
	default:
		if start, _, ok := addColumnClause(w, i); ok {
			collect(nameColumn, start-1, owner)
		}
	}
}

// namingRules is the family: one rule per kind of name, each reading the
// project's convention.
func namingRules() []Rule {
	rules := make([]Rule, 0, len(namingCodes))
	for kind := range nameKind(len(namingCodes)) {
		rules = append(rules, namingRule(kind))
	}
	return rules
}

func namingRule(kind nameKind) Rule {
	title := kind.String() + " name violates the naming convention"
	return Rule{
		Code:     namingCodes[kind],
		Title:    title,
		Severity: SeverityWarning,
		CheckFile: func(file *File) []Finding {
			if file.naming == nil {
				return nil
			}
			pattern, ok := file.naming.patterns[kind]
			if !ok {
				return nil
			}
			var findings []Finding
			for _, object := range namedObjects(file) {
				if object.kind != kind || pattern.expr.MatchString(object.name) {
					continue
				}
				message := fmt.Sprintf("%s name %s does not match the naming convention %s", kind, object.spelled, pattern.source)
				if pattern.message != "" {
					message += ": " + pattern.message
				}
				stmt := &file.Statements[object.statement]
				findings = append(findings, Finding{
					Rule:     namingCodes[kind],
					Title:    title,
					Severity: SeverityWarning,
					File:     file.Path,
					Line:     stmt.Line,
					Message:  message,
					Context:  statementFindingContext(object.statement, namingSubject(object)),
				})
			}
			return findings
		},
	}
}

// namingSubject names the object a finding is about, in the two kinds the
// context model has; every other kind is placed on its owning table.
func namingSubject(object namedObject) Subject {
	switch object.kind {
	case nameColumn:
		return Subject{Kind: SubjectColumn, Name: object.name, Parent: object.owner}
	case nameTable:
		return Subject{Kind: SubjectTable, Name: object.name}
	default:
		return Subject{Kind: SubjectTable, Name: object.owner}
	}
}
