package providerhost

import (
	"context"
	"fmt"
	"math"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/lintext"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// supportsLint reports that the engine's owner publishes lint rules and splits
// its own migrations. Without it, lint falls back to the owner's SQL analysis.
func (t *target) supportsLint() bool {
	return slices.Contains(t.engine.GetFeatures(), provider.LintFeature)
}

// lint publishes the engine's rules and the service that checks them.
func (t *target) lint() engine.Lint {
	severities := map[protocol.LintRule_Severity]lintext.Severity{
		protocol.LintRule_INFO: lintext.Info, protocol.LintRule_WARNING: lintext.Warning, protocol.LintRule_ERROR: lintext.Error,
	}
	rules := make([]lintext.Rule, 0, len(t.engine.GetLintRules()))
	for _, rule := range t.engine.GetLintRules() {
		rules = append(rules, lintext.Rule{Code: rule.GetCode(), Title: rule.GetTitle(), Severity: severities[rule.GetSeverity()],
			Targets: []string{t.engine.GetId()}, AppliesToDown: rule.GetAppliesToDown(), Subsumes: slices.Clone(rule.GetSubsumes())})
	}
	return engine.Lint{Rules: rules, Service: lintService{target: t}}
}

var (
	lintFacts = map[lintext.Fact]protocol.LintFact{
		lintext.FactStatefulDrop: protocol.LintFact_LINT_FACT_STATEFUL_DROP, lintext.FactUniqueIndex: protocol.LintFact_LINT_FACT_UNIQUE_INDEX,
		lintext.FactRebuildDrop: protocol.LintFact_LINT_FACT_REBUILD_DROP, lintext.FactRebuildRename: protocol.LintFact_LINT_FACT_REBUILD_RENAME,
		lintext.FactSafetyEffect: protocol.LintFact_LINT_FACT_SAFETY_EFFECT,
	}
	lintImpacts = map[schemaext.Impact]protocol.LintImpact{
		schemaext.Behavioral: protocol.LintImpact_LINT_IMPACT_BEHAVIORAL, schemaext.Destructive: protocol.LintImpact_LINT_IMPACT_DESTRUCTIVE,
	}
)

func lintSources(sources []lintext.Source) []*protocol.LintSource {
	result := make([]*protocol.LintSource, len(sources))
	for i, source := range sources {
		result[i] = &protocol.LintSource{Name: source.Name, Sql: source.SQL, Version: source.Version, Up: source.Up, Down: source.Down, Repeatable: source.Repeatable}
	}
	return result
}

func lintStatements(statements []lintext.Statement) []*protocol.LintStatement {
	result := make([]*protocol.LintStatement, len(statements))
	for i, statement := range statements {
		value := &protocol.LintStatement{Start: unsigned(statement.Start), End: unsigned(statement.End), Line: unsigned(statement.Line), Sql: statement.SQL,
			Canonical: statement.Canonical, Words: slices.Clone(statement.Words), SourceWords: slices.Clone(statement.SourceWords), Comments: slices.Clone(statement.Comments)}
		for _, effect := range statement.Effects {
			value.Effects = append(value.Effects, lintEffect(effect))
		}
		result[i] = value
	}
	return result
}

// unsigned clamps a position into the wire's range. A clamped position cannot
// match the source text, so the owner's reply is refused rather than misread.
func unsigned(n int) uint32 {
	return uint32(min(max(int64(n), 0), math.MaxUint32)) // #nosec G115 -- clamped into range on this line
}

func lintEffect(effect lintext.Effect) *protocol.LintEffect {
	return &protocol.LintEffect{Fact: lintFacts[effect.Fact], Object: effect.Object, Kind: effect.Kind, Table: effect.Table, Columns: slices.Clone(effect.Columns),
		Clause: effect.Clause, Failure: effect.Failure, Impact: lintImpacts[effect.Impact], Reason: effect.Reason}
}

func lintEffects(effects []*protocol.LintEffect) []lintext.Effect {
	var result []lintext.Effect
	for _, effect := range effects {
		value := lintext.Effect{Object: effect.GetObject(), Kind: effect.GetKind(), Table: effect.GetTable(), Columns: slices.Clone(effect.GetColumns()),
			Clause: effect.GetClause(), Failure: effect.GetFailure(), Reason: effect.GetReason()}
		for fact, wire := range lintFacts {
			if wire == effect.GetFact() {
				value.Fact = fact
			}
		}
		for impact, wire := range lintImpacts {
			if wire == effect.GetImpact() {
				value.Impact = impact
			}
		}
		result = append(result, value)
	}
	return result
}

func (t *target) splitLint(ctx context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	reply, err := t.host.client.SplitLintStatements(ctx, &protocol.SplitLintRequest{Engine: t.engine.GetId(), Sources: lintSources(request.Sources)})
	if err != nil {
		return lintext.SplitResult{}, err
	}
	result := lintext.SplitResult{Files: make([][]lintext.Statement, len(reply.GetFiles()))}
	for fact, wire := range lintFacts {
		if slices.Contains(reply.GetJudged(), wire) {
			result.Judged = append(result.Judged, fact)
		}
	}
	slices.Sort(result.Judged)
	for i, file := range reply.GetFiles() {
		for _, statement := range file.GetStatements() {
			result.Files[i] = append(result.Files[i], lintext.Statement{Start: int(statement.GetStart()), End: int(statement.GetEnd()), Line: int(statement.GetLine()),
				SQL: statement.GetSql(), Canonical: statement.GetCanonical(), Words: slices.Clone(statement.GetWords()), SourceWords: slices.Clone(statement.GetSourceWords()),
				Comments: slices.Clone(statement.GetComments()), Effects: lintEffects(statement.GetEffects())})
		}
	}
	return result, nil
}

// AnnotateStatements asks the owner for the facts of statements the host split
// without a dialect. A lint run that names this engine splits through the owner
// and never reaches it.
func (t *target) AnnotateStatements(ctx context.Context, request lintext.AnnotateRequest) (lintext.AnnotateResult, error) {
	if err := lintext.ValidateAnnotateRequest(request); err != nil {
		return lintext.AnnotateResult{}, err
	}
	wire := &protocol.AnnotateLintRequest{Engine: t.engine.GetId(), Sources: lintSources(request.Sources)}
	for _, fact := range request.Facts {
		wire.Facts = append(wire.Facts, lintFacts[fact])
	}
	for i, statements := range request.Files {
		wire.Files = append(wire.Files, &protocol.LintFile{Name: request.Sources[i].Name, Statements: lintStatements(statements)})
	}
	reply, err := t.host.client.AnnotateLintStatements(ctx, wire)
	if err != nil {
		return lintext.AnnotateResult{}, err
	}
	result := lintext.AnnotateResult{Files: make([][]lintext.Annotation, len(reply.GetFiles()))}
	for i, file := range reply.GetFiles() {
		for _, annotation := range file.GetStatements() {
			result.Files[i] = append(result.Files[i], lintext.Annotation{Effects: lintEffects(annotation.GetEffects()), Declined: annotation.GetDeclined()})
		}
	}
	return result, nil
}

// lintService checks one engine's rules through its owner.
type lintService struct {
	target *target
}

func (s lintService) CheckMigrations(ctx context.Context, request lintext.CheckRequest) (lintext.CheckResult, error) {
	name := s.target.engine.GetId()
	if request.Target.Name != name {
		return lintext.CheckResult{}, fmt.Errorf("%w: lint target changed", lintext.ErrInvalid)
	}
	if err := request.Target.Capabilities.Validate(); err != nil {
		return lintext.CheckResult{}, err
	}
	wire := &protocol.CheckLintRequest{Engine: name, ServerVersion: request.Target.Version, Capabilities: make(map[string]bool),
		Codes: slices.Clone(request.Codes), Compatibility: request.Compatibility}
	for key, value := range request.Target.Capabilities.All() {
		wire.Capabilities[string(key)] = value
	}
	for _, file := range request.Files {
		wire.Files = append(wire.Files, &protocol.LintFile{Name: file.Name, Version: file.Version, Up: file.Up, Down: file.Down, HasPair: file.HasPair,
			NoTransaction: file.NoTransaction, Repeatable: file.Repeatable, Statements: lintStatements(file.Statements)})
	}
	reply, err := s.target.host.client.CheckLintMigrations(ctx, wire)
	if err != nil {
		return lintext.CheckResult{}, err
	}
	result := lintext.CheckResult{Complete: reply.GetComplete()}
	kinds := map[protocol.LintSubject_Kind]lintext.SubjectKind{protocol.LintSubject_TABLE: lintext.SubjectTable, protocol.LintSubject_COLUMN: lintext.SubjectColumn}
	for _, finding := range reply.GetFindings() {
		value := lintext.Finding{Code: finding.GetCode(), File: int(finding.GetFile()), Statement: int(finding.GetStatement()), Message: finding.GetMessage()}
		for _, subject := range finding.GetSubjects() {
			value.Subjects = append(value.Subjects, lintext.Subject{Kind: kinds[subject.GetKind()], Name: subject.GetName(), Parent: subject.GetParent(), DataType: subject.GetDataType()})
		}
		result.Findings = append(result.Findings, value)
	}
	return result, nil
}
