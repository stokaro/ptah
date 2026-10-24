package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// LintFeature advertises offline migration lint for the engines that declare it.
// The provider owns the rule catalog, the statement splitter and the findings.
// The host owns output formats, severity thresholds and suppression.
const LintFeature = "lint.migrations"

// Linter splits and judges migration files without opening a session. Every
// operation is a complete batch: an error or an incomplete result exposes no
// finding, and the host never reads silence as a clean run.
type Linter interface {
	// SplitLintStatements splits each source with the engine's own lexing and
	// reports the facts it judges.
	SplitLintStatements(context.Context, *protocol.SplitLintRequest) (*protocol.SplitLint, error)
	// AnnotateLintStatements reports facts for statements a dialect-blind scanner
	// split. The owner declines a statement its own lexing would split differently.
	AnnotateLintStatements(context.Context, *protocol.AnnotateLintRequest) (*protocol.AnnotatedLint, error)
	// CheckLintMigrations judges every file of a run under the enabled rule codes.
	CheckLintMigrations(context.Context, *protocol.CheckLintRequest) (*protocol.CheckedLint, error)
}

// validateLintRules checks the rule catalog an engine declares. A code belongs
// to one rule in the whole description.
func validateLintRules(description *protocol.Description, engine *protocol.Engine) error {
	rules := engine.GetLintRules()
	if len(rules) == 0 {
		if slices.Contains(engine.GetFeatures(), LintFeature) {
			return fmt.Errorf("%w: engine lint declares no rule", ErrProtocol)
		}
		return nil
	}
	if !slices.Contains(engine.GetFeatures(), LintFeature) || !slices.Contains(description.GetFeatures(), LintFeature) {
		return fmt.Errorf("%w: lint rules require the lint service", ErrProtocol)
	}
	for _, rule := range rules {
		if !canonicalLintCode(rule.GetCode()) || strings.TrimSpace(rule.GetTitle()) == "" || !utf8.ValidString(rule.GetTitle()) {
			return fmt.Errorf("%w: invalid lint rule", ErrProtocol)
		}
		switch rule.GetSeverity() {
		case protocol.LintRule_INFO, protocol.LintRule_WARNING, protocol.LintRule_ERROR:
		default:
			return fmt.Errorf("%w: lint rule %s has no severity", ErrProtocol, rule.GetCode())
		}
		if slices.Contains(rule.GetSubsumes(), rule.GetCode()) {
			return fmt.Errorf("%w: lint rule %s subsumes itself", ErrProtocol, rule.GetCode())
		}
		if lintCodeCount(description, rule.GetCode()) != 1 {
			return fmt.Errorf("%w: lint rule code %s is declared twice", ErrProtocol, rule.GetCode())
		}
	}
	return nil
}

func lintCodeCount(description *protocol.Description, code string) int {
	count := 0
	for _, engine := range description.GetEngines() {
		for _, rule := range engine.GetLintRules() {
			if rule.GetCode() == code {
				count++
			}
		}
	}
	return count
}

// canonicalLintCode reports an uppercase ASCII letter followed by uppercase
// ASCII letters and digits.
func canonicalLintCode(code string) bool {
	for i, char := range code {
		if (char < 'A' || char > 'Z') && (i == 0 || char < '0' || char > '9') {
			return false
		}
	}
	return code != ""
}

func lintEngine(description *protocol.Description, name string) (*protocol.Engine, error) {
	if !slices.Contains(description.GetFeatures(), LintFeature) {
		return nil, fmt.Errorf("%w: lint is unavailable", ErrProtocol)
	}
	engine, _, err := valueTarget(description, name, nil)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(engine.GetFeatures(), LintFeature) {
		return nil, fmt.Errorf("%w: engine lint is unavailable", ErrProtocol)
	}
	return engine, nil
}

// statementFacts are the lint facts of contract 25.1. An engine judges them
// only when its negotiated features list LintStatementFactsFeature.
var statementFacts = []protocol.LintFact{
	protocol.LintFact_LINT_FACT_STATEMENT_KIND, protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY, protocol.LintFact_LINT_FACT_REQUIRED_CAPABILITY,
}

func validLintFact(engine *protocol.Engine, fact protocol.LintFact) bool {
	_, known := protocol.LintFact_name[int32(fact)]
	if slices.Contains(statementFacts, fact) && !slices.Contains(engine.GetFeatures(), LintStatementFactsFeature) {
		return false
	}
	return known && fact != protocol.LintFact_LINT_FACT_UNSPECIFIED
}

func validateLintFacts(engine *protocol.Engine, facts []protocol.LintFact) error {
	for i, fact := range facts {
		if !validLintFact(engine, fact) || slices.Contains(facts[:i], fact) {
			return fmt.Errorf("%w: invalid, unnegotiated, or repeated lint fact", ErrProtocol)
		}
	}
	return nil
}

func validateLintSources(sources []*protocol.LintSource) error {
	for _, source := range sources {
		if source.GetName() == "" || !utf8.ValidString(source.GetSql()) {
			return fmt.Errorf("%w: lint source needs a name and valid text", ErrProtocol)
		}
	}
	return nil
}

// ValidateSplitLintRequest checks the negotiated service and the sources.
func ValidateSplitLintRequest(description *protocol.Description, request *protocol.SplitLintRequest) error {
	if request == nil {
		return fmt.Errorf("%w: lint split request is missing", ErrProtocol)
	}
	if _, err := lintEngine(description, request.Engine); err != nil {
		return err
	}
	return validateLintSources(request.Sources)
}

// ValidateSplitLint refuses a split that does not cover its sources: a wrong file
// count, a statement that does not match its source bytes, overlapping or
// unordered statements, and an effect of a fact the split does not judge.
func ValidateSplitLint(description *protocol.Description, request *protocol.SplitLintRequest, result *protocol.SplitLint) error {
	if err := ValidateSplitLintRequest(description, request); err != nil {
		return err
	}
	if result == nil || len(result.Files) != len(request.Sources) {
		return fmt.Errorf("%w: lint split does not answer every source", ErrProtocol)
	}
	engine, err := lintEngine(description, request.Engine)
	if err != nil {
		return err
	}
	if err := validateLintFacts(engine, result.Judged); err != nil {
		return err
	}
	if slices.Contains(result.Judged, protocol.LintFact_LINT_FACT_REBUILD_DROP) != slices.Contains(result.Judged, protocol.LintFact_LINT_FACT_REBUILD_RENAME) {
		return fmt.Errorf("%w: a split judges rebuild drops and renames together or neither", ErrProtocol)
	}
	for i, file := range result.Files {
		sql := request.Sources[i].GetSql()
		end := uint32(0)
		for _, statement := range file.GetStatements() {
			if statement.Start < end || statement.End < statement.Start || int(statement.End) > len(sql) ||
				sql[statement.Start:statement.End] != statement.Sql || statement.Line < 1 {
				return fmt.Errorf("%w: lint statement does not match its source", ErrProtocol)
			}
			if err := validateLintEffects(statement.Effects, result.Judged); err != nil {
				return err
			}
			if err := requireStatementKind(statement.Effects, result.Judged); err != nil {
				return err
			}
			end = statement.End
		}
	}
	return nil
}

// validateLintEffects checks one statement's effects against the facts that
// were judged: fields that belong to another fact, a second kind, and a
// missing primary key on a statement whose kind creates no table.
func validateLintEffects(effects []*protocol.LintEffect, allowed []protocol.LintFact) error {
	safety := 0
	var kind *protocol.LintEffect
	for _, effect := range effects {
		if !slices.Contains(allowed, effect.GetFact()) {
			return fmt.Errorf("%w: lint effect of a fact that was not judged", ErrProtocol)
		}
		if err := validateLintEffectFields(effect); err != nil {
			return err
		}
		switch effect.Fact {
		case protocol.LintFact_LINT_FACT_STATEMENT_KIND:
			if kind != nil {
				return fmt.Errorf("%w: a lint statement has two kinds", ErrProtocol)
			}
			kind = effect
		case protocol.LintFact_LINT_FACT_SAFETY_EFFECT:
			safety++
			if (effect.Impact != protocol.LintImpact_LINT_IMPACT_BEHAVIORAL && effect.Impact != protocol.LintImpact_LINT_IMPACT_DESTRUCTIVE) ||
				strings.TrimSpace(effect.Reason) == "" || safety > 1 {
				return fmt.Errorf("%w: invalid safety effect", ErrProtocol)
			}
		}
	}
	for _, effect := range effects {
		if effect.Fact == protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY && kind != nil &&
			kind.StatementClass != protocol.LintStatementClass_LINT_STATEMENT_CLASS_CREATE_TABLE {
			return fmt.Errorf("%w: a missing primary key on a statement that creates no table", ErrProtocol)
		}
	}
	return nil
}

// requireStatementKind refuses a judged statement with no kind where the
// statement kind is among the judged facts. A declined statement carries no
// effect and is not asked.
func requireStatementKind(effects []*protocol.LintEffect, judged []protocol.LintFact) error {
	if !slices.Contains(judged, protocol.LintFact_LINT_FACT_STATEMENT_KIND) {
		return nil
	}
	if !slices.ContainsFunc(effects, func(effect *protocol.LintEffect) bool {
		return effect.Fact == protocol.LintFact_LINT_FACT_STATEMENT_KIND
	}) {
		return fmt.Errorf("%w: a judged lint statement has no kind", ErrProtocol)
	}
	return nil
}

// validateLintEffectFields checks the fields that belong to one fact alone.
func validateLintEffectFields(effect *protocol.LintEffect) error {
	if effect.Fact != protocol.LintFact_LINT_FACT_SAFETY_EFFECT && (effect.Impact != protocol.LintImpact_LINT_IMPACT_UNSPECIFIED || effect.Reason != "") {
		return fmt.Errorf("%w: only a safety effect carries an impact or a reason", ErrProtocol)
	}
	if (effect.Fact == protocol.LintFact_LINT_FACT_STATEMENT_KIND) != (effect.StatementClass != protocol.LintStatementClass_LINT_STATEMENT_CLASS_UNSPECIFIED) {
		return fmt.Errorf("%w: a statement kind and only a statement kind carries a statement class", ErrProtocol)
	}
	if (effect.Fact == protocol.LintFact_LINT_FACT_REQUIRED_CAPABILITY) != (effect.Capability != "") {
		return fmt.Errorf("%w: a required capability and only a required capability names a capability", ErrProtocol)
	}
	switch effect.Fact {
	case protocol.LintFact_LINT_FACT_STATEMENT_KIND:
		if _, known := protocol.LintStatementClass_name[int32(effect.StatementClass)]; !known || !diagnosticLine(effect.Kind) {
			return fmt.Errorf("%w: a statement kind needs a known class and a one-line name", ErrProtocol)
		}
	case protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY:
		if strings.TrimSpace(effect.Table) == "" {
			return fmt.Errorf("%w: a missing primary key names no table", ErrProtocol)
		}
	case protocol.LintFact_LINT_FACT_REQUIRED_CAPABILITY:
		if !comparisonReason(effect.Capability) {
			return fmt.Errorf("%w: invalid required capability name", ErrProtocol)
		}
	}
	return nil
}

// ValidateAnnotateLintRequest checks that sources and statement lists pair up.
func ValidateAnnotateLintRequest(description *protocol.Description, request *protocol.AnnotateLintRequest) error {
	if request == nil {
		return fmt.Errorf("%w: lint annotation request is missing", ErrProtocol)
	}
	engine, err := lintEngine(description, request.Engine)
	if err != nil {
		return err
	}
	if len(request.Facts) == 0 || len(request.Sources) != len(request.Files) {
		return fmt.Errorf("%w: lint annotation needs facts and one statement list per source", ErrProtocol)
	}
	if err := validateLintFacts(engine, request.Facts); err != nil {
		return err
	}
	return validateLintSources(request.Sources)
}

// ValidateAnnotatedLint refuses an annotation that does not pair with the
// request's statements, reports a fact nobody asked about, or gives a declined
// statement an effect.
func ValidateAnnotatedLint(description *protocol.Description, request *protocol.AnnotateLintRequest, result *protocol.AnnotatedLint) error {
	if err := ValidateAnnotateLintRequest(description, request); err != nil {
		return err
	}
	if result == nil || len(result.Files) != len(request.Files) {
		return fmt.Errorf("%w: lint annotation does not answer every file", ErrProtocol)
	}
	for i, file := range result.Files {
		if len(file.GetStatements()) != len(request.Files[i].GetStatements()) {
			return fmt.Errorf("%w: lint annotation does not answer every statement", ErrProtocol)
		}
		for _, annotation := range file.Statements {
			if annotation.Declined && len(annotation.Effects) > 0 {
				return fmt.Errorf("%w: a declined lint statement carries effects", ErrProtocol)
			}
			if err := validateLintEffects(annotation.Effects, request.Facts); err != nil {
				return err
			}
			if annotation.Declined {
				continue
			}
			if err := requireStatementKind(annotation.Effects, request.Facts); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateCheckLintRequest checks that every enabled code is a rule the engine declares.
func ValidateCheckLintRequest(description *protocol.Description, request *protocol.CheckLintRequest) error {
	if request == nil {
		return fmt.Errorf("%w: lint check request is missing", ErrProtocol)
	}
	engine, err := lintEngine(description, request.Engine)
	if err != nil {
		return err
	}
	for i, code := range request.Codes {
		if slices.Contains(request.Codes[:i], code) || !slices.ContainsFunc(engine.GetLintRules(), func(rule *protocol.LintRule) bool { return rule.GetCode() == code }) {
			return fmt.Errorf("%w: lint check enables a repeated code or a rule the engine does not declare", ErrProtocol)
		}
	}
	for _, file := range request.Files {
		if file.GetName() == "" {
			return fmt.Errorf("%w: lint file needs a name", ErrProtocol)
		}
	}
	return nil
}

// ValidateCheckedLint refuses an incomplete result and any finding that names a
// rule the request did not enable, a file or statement it did not hold, no
// message, or a subject without a known kind or a name. A finding about a whole
// file (statement -1) carries no subject.
func ValidateCheckedLint(description *protocol.Description, request *protocol.CheckLintRequest, result *protocol.CheckedLint) error {
	if err := ValidateCheckLintRequest(description, request); err != nil {
		return err
	}
	if result == nil || !result.Complete {
		return fmt.Errorf("%w: incomplete lint check", ErrProtocol)
	}
	for _, finding := range result.Findings {
		if !slices.Contains(request.Codes, finding.GetCode()) || finding.File < 0 || int(finding.File) >= len(request.Files) ||
			finding.Statement < -1 || int(finding.Statement) >= len(request.Files[finding.File].GetStatements()) || finding.Message == "" ||
			!utf8.ValidString(finding.Message) {
			return fmt.Errorf("%w: lint finding does not answer the request", ErrProtocol)
		}
		if len(finding.Subjects) > 0 && finding.Statement < 0 {
			return fmt.Errorf("%w: a whole-file lint finding names subjects", ErrProtocol)
		}
		for _, subject := range finding.Subjects {
			if (subject.Kind != protocol.LintSubject_TABLE && subject.Kind != protocol.LintSubject_COLUMN) || subject.Name == "" {
				return fmt.Errorf("%w: invalid lint subject", ErrProtocol)
			}
		}
	}
	return nil
}
